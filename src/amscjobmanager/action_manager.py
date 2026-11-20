import pickle
from abc import ABC, abstractmethod
from enum import Enum
from threading import RLock
from typing import Callable
import time

from .database import DatabaseClosed, LockedConnection
from .manager_functions import FunctionDefinition
from .logging import wfmanLog
from .workers import Workers, WorkerInterrupted, clearThreadCurrentWorkerInterruptUnlessStopping

def _ser(obj):
    """Serialize an object for storage in an action or workflow record.

    obj (object) must support pickle serialization. Return bytes containing
    its serialized representation using the highest available pickle protocol.
    """
    return pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
def _unser(ser):
    """Restore an object from a serialized action or workflow record.

    ser (bytes) is trusted pickle data read from the manager's database.
    Return the stored object, whose type depends on the serialized definition.
    """
    return pickle.loads(ser)

class ActionStatus(Enum):
    PENDING = 0 #not yet started
    SCHEDULING = 1 #submission is in progress and must be reconciled after a crash
    ACTIVE = 2 #provider-owned work progressing normally, including queued/new jobs
    COMPLETED = 3 #action completed successfully
    FAILED = 4 #action failed
    CANCELED = 5 #cancellation was requested or the action was removed before submission
    CANCELING = 6 #cancellation intent is persisted but not yet reconciled
    PROVIDER_HELD = 7 #accepted work suspended by its provider; still live and cancellable


class ActionStatusGroup(Enum):
    """Reusable action-state groups shared by Python checks and SQL views.

    Each value is a tuple of ActionStatus members. LIVE contains accepted work
    still monitored remotely. EXECUTING adds submission/reconciliation in
    progress; it does not imply that the provider is running a process.
    """

    LIVE = (ActionStatus.ACTIVE, ActionStatus.PROVIDER_HELD)
    EXECUTING = (ActionStatus.SCHEDULING, *LIVE)


def initializeActionStatusViews(connection: LockedConnection) -> None:
    """Create read-only SQL views for action groups and head-to-workflow mapping.

    connection (LockedConnection) supplies the shared SQLite connection. Return
    None. action_status_groups has group_name/action_status (str enum names),
    one row per membership. action_status_mapping has action_status/workflow_status
    (str enum names), one row per ActionStatus. EXECUTING and COMPLETED heads
    map to ACTIVE workflows; other statuses map to themselves. Final workflow
    completion and cancellation intent remain explicit JobData transitions.
    Existing views are retained; no action or workflow records are changed.
    """
    # Generate SQL literals exclusively from library enum names. The same group
    # definitions drive in-memory checks and database conditions without drift.
    memberships = ', '.join(f"('{group.name}', '{status.name}')"
                            for group in ActionStatusGroup for status in group.value)
    mapping = []
    for status in ActionStatus:
        workflow_status = ActionStatus.ACTIVE if (
            status in ActionStatusGroup.EXECUTING.value or status == ActionStatus.COMPLETED) else status
        mapping.append(f"('{status.name}', '{workflow_status.name}')")
    with connection as conn:
        conn.execute('CREATE VIEW IF NOT EXISTS action_status_groups(group_name, action_status) AS VALUES ' + memberships)
        conn.execute('CREATE VIEW IF NOT EXISTS action_status_mapping(action_status, workflow_status) AS VALUES ' + ', '.join(mapping))


class SimulatedCrash(RuntimeError):
    """Test-only exception raised at an explicitly requested crash boundary."""

    def __init__(self, point):
        """Create an exception identifying an injected crash boundary.

        point (str) names the test crash boundary and is retained as the point
        attribute. Return None; constructing the exception does not raise it.
        """
        self.point = point
        super().__init__(f"Simulated crash at {point}")
    
class ActionManager(ABC):
    """Workflow-facing action-manager interface, independent of storage schema.

    JobData uses the public methods to start/cancel actions and observe their
    recorded state. It allocates every action ID and supplies the same ID during
    recovery; action managers never allocate or replace IDs.

    The base supplies worker lifecycle handling. Subclasses own storage and
    explicitly pass execution/cancellation callables to the worker starters.
    _recordWorkerFailure remains an internal subclass hook for storing unexpected
    errors; JobData does not call the underscored methods.
    """

    def __init__(self, *, workers: Workers | None = None):
        """Initialize worker tracking and synchronization for an action manager.

        workers (Workers | None) is the registry used to launch and signal
        execution threads; None creates a registry for this manager. Return None.
        Subclass constructors must also open/create their own action records.
        This constructor neither starts actions nor defines a polling policy."""
        self.workers = workers if workers is not None else Workers()
        self._action_workers = {}
        self._worker_lock = RLock()

    def getMCPFunctions(self) -> tuple[FunctionDefinition, ...]:
        """Describe explicitly exposed MCP operations supplied by this implementation.

        Take no arguments. Return tuple[FunctionDefinition, ...], empty by
        default. Definitions contain annotated callables receiving the current
        manager explicitly. Discovery performs no provider requests or state changes.
        """
        return ()

    def getAdminFunctions(self) -> tuple[FunctionDefinition, ...]:
        """Describe operations requiring authenticated local administration.

        Take no arguments. Return tuple[FunctionDefinition, ...], empty by
        default. These definitions are never exposed as MCP tools; discovery
        performs no provider requests or state changes.
        """
        return ()

    def _startExecutionWorker(self, action_id: int, *, execute_action: Callable[[int], None],
                              complete_cancellation: Callable[[int], None]):
        """Start supplied execution work and hand off to cancellation when requested.

        action_id (int) is a positive, JobData-issued ID with intent already
        recorded. execute_action (callable) takes that ID, recovers/executes work
        and persists progress until terminal or CANCELING. complete_cancellation
        (callable) takes the same ID and resolves persisted cancellation intent.
        Both return None and must allow WorkerInterrupted to propagate.

        Return the execution Worker handle without waiting; reuse a live worker.
        After execution has finished running, a recorded CANCELING intent starts
        a separate worker with complete_cancellation, unless stopping. No execution
        or cancellation method names are required on the derived class.
        """
        def handoff() -> None:
            """Start supplied cancellation for the captured action_id if requested.

            Return None. Called after execution finishes under the shared worker
            lock, so no execution API call is still in progress at the handoff.
            """
            status, _ = self.queryRecordedStatus(action_id)
            if status == ActionStatus.CANCELING:
                self._startCancellationWorker(action_id, complete_cancellation=complete_cancellation)

        return self._startWorker(action_id, execute_action, on_completion=handoff)

    def _startCancellationWorker(self, action_id: int, *, complete_cancellation: Callable[[int], None]):
        """Start supplied cancellation work for a recorded cancellation request.

        action_id (int) is a positive global ID in CANCELING. complete_cancellation
        (callable) takes that ID, safely recovers/retries cancellation and persists
        its outcome, returning None when resolved. It must allow WorkerInterrupted
        to propagate on shutdown. Return a Worker handle without waiting.

        Reuse an existing execution worker until its handoff, rather than running
        cancellation concurrently. With no live worker, start cancellation
        directly, including during constructor recovery.
        """
        def run_cancellation(record_id: int) -> None:
            """Clear the handled interrupt and run supplied cancellation for record_id.

            record_id (int) is the positive global action ID. Return None when
            the captured complete_cancellation callable resolves intent. Shutdown
            exits without changing durable intent or starting cancellation calls.
            """
            clearThreadCurrentWorkerInterruptUnlessStopping()
            complete_cancellation(record_id)

        return self._startWorker(action_id, run_cancellation)

    def _startWorker(self, action_id: int, operation, *, on_completion=None):
        """Launch one action worker with shared registration, errors and cleanup.

        action_id (int) is its positive global ID. operation (callable) takes
        that ID and performs execution or cancellation. on_completion (callable
        | None) optionally takes no arguments and runs after the operation has
        finished running, under the worker lock; it is skipped on shutdown or a simulated
        crash. Return the new/existing Worker handle without waiting. Retain
        simulated crashes for inspection until recovery with a new manager.
        """
        with self._worker_lock:
            worker = self._action_workers.get(action_id)
            if worker is not None and not worker.future.done():
                return worker
            if worker is not None and isinstance(worker.future.exception(), SimulatedCrash):
                return worker

            def run() -> None:
                """Run the captured operation/action_id with shared error handling.

                Return None on completion or cooperative interruption. Persist
                ordinary errors through this manager; retain simulated crashes
                in the Future. Closed storage leaves intent for restart recovery.
                """
                try:
                    try:
                        operation(action_id)
                    except WorkerInterrupted:
                        pass
                    except (SimulatedCrash, DatabaseClosed):
                        raise
                    except Exception as exc:
                        wfmanLog(f'Action worker {action_id} failed: {type(exc).__name__}: {exc}')
                        self._recordWorkerFailure(action_id, exc)
                except DatabaseClosed:
                    pass

            def finished(future) -> None:
                """Release this worker and run its optional completion handoff.

                future (Future) holds the operation result. Return None. Keep
                failed Futures observable. Serialize cleanup/handoff with stop
                and new requests; never remove a newer replacement worker.
                """
                with self._worker_lock:
                    if self._action_workers.get(action_id) is not worker:
                        return
                    if future.exception() is not None:
                        return
                    self._action_workers.pop(action_id, None)
                    self.workers.discard(action_id)
                    if on_completion is not None and not worker.stop.is_set():
                        try:
                            on_completion()
                        except DatabaseClosed:
                            pass
                        except Exception as exc:
                            wfmanLog(f'Action worker handoff {action_id} failed: {type(exc).__name__}: {exc}')
                            self._recordWorkerFailure(action_id, exc)

            # Register before completion cleanup can acquire the worker lock.
            # RLock also permits an immediate Future callback or a handoff to
            # start its successor without releasing the lifecycle lock.
            self.workers.discard(action_id)
            worker = self.workers.start(action_id, run)
            self._action_workers[action_id] = worker
            worker.future.add_done_callback(finished)
            return worker

    @abstractmethod
    def _recordWorkerFailure(self, action_id: int, error: Exception) -> None:
        """Record an unexpected worker failure in the action's persistent state.

        Called by the worker when execution or cancellation completion raises an
        ordinary exception its implementation did not handle. action_id (int) is
        the positive recorded action ID; error (Exception) is that failure. Return None after
        recording the reason and marking the action FAILED, unless a concurrent
        CANCELING intent must be preserved for cancellation to finish. This hook
        must not change the owning workflow's record or retry the operation."""
        raise NotImplementedError

    @abstractmethod
    def startOrResumeAction(self, action, job_id: int, action_id: int) -> None:
        """Start or recover an action's execution in the background.

        Called by JobData when selecting a workflow stage or recovering its head.
        "Resume" means restoring supervision from persisted state after the
        previous worker stopped, using a newly constructed manager after shutdown
        or process failure. A stopped JobManager instance cannot be restarted.
        Normal execution progresses independently; repeated calls while its worker
        is live simply reuse it. This is not a workflow pause/resume operation.
        action (object) is the serializable action definition, used only if its
        record is missing. job_id (int) is the positive owning workflow ID;
        action_id (int) is the positive global action ID allocated by JobData,
        not a workflow-stage index. Use that same ID during recovery.

        Return None after persisting intent and starting/reusing its worker;
        do not wait for submission or completion. Repeated calls retain the
        stored definition and progress and must not launch duplicate execution.
        Existing terminal records remain unchanged. Observe the outcome through
        queryRecordedStatus; all action-record transitions belong to this manager."""
        raise NotImplementedError

    @abstractmethod
    def cancelOrResumeCancelingAction(self, action_id: int, *, recovery: tuple[object, int] | None = None) -> None:
        """Request cancellation or recover a previously recorded cancellation.

        Called by JobData for its current workflow stage, including recovery of
        interrupted cancellation. action_id (int) is the positive global action
        ID. recovery (tuple[object, int] | None) optionally supplies its stored
        definition at index 0 and positive workflow ID at index 1, allowing a
        missing action record to be restored. None requires an existing record.

        Return None after recording intent and signaling execution, or recording
        cancellation directly when no work needs to stop. Repeated calls must be
        safe. Do not submit new remote work or undo earlier file effects. The
        worker completes any necessary remote cancellation; queryRecordedStatus reports
        its outcome. JobData separately updates the workflow's cancellation."""
        raise NotImplementedError

    @abstractmethod
    def hasAction(self, action_id: int) -> bool:
        """Check whether an action is recorded by this manager.

        action_id (int) is the positive global action ID, not a stage index.
        Return bool: True if its record exists, including a terminal record;
        False otherwise. Do not start execution or contact a provider."""
        raise NotImplementedError

    @abstractmethod
    def getMaxActionId(self) -> int | None:
        """Find the highest action ID in this execution manager's records.

        Take no arguments. Return int for the highest global action ID in this
        manager's records, including terminal actions, or None if there are no
        records. Queued stages may have higher IDs in JobData's actions table;
        execution-manager records are only created when a stage is started.
        Do not allocate a new ID or change any record."""
        raise NotImplementedError

    @abstractmethod
    def queryRecordedStatus(self, action_id: int, *, force_update=False) -> tuple[ActionStatus, str | None]:
        """Read an action's recorded status and optionally request a provider poll.

        Used by JobData to copy action state into its workflow head and by
        callers inspecting progress. action_id (int) is the positive global
        action ID. force_update (bool, default False) requests an earlier API
        poll from an existing worker without waiting for its result. Managers
        without provider polling ignore this option. This method neither
        changes the configured poll interval nor starts/recovers an action.

        Return tuple[ActionStatus, str | None]: index 0 is the recorded action
        status; index 1 is its latest raw provider status, or None if unavailable
        or inapplicable. A requested poll may finish after this call returns,
        so the returned state can precede that poll. An unknown ID is an error."""
        raise NotImplementedError

    @abstractmethod
    def updateStatuses(self) -> None:
        """Request fresh provider status for this manager's ACTIVE actions.

        Take no arguments and return None. Wake existing workers to poll; do
        not wait for those polls, submit actions, or advance workflow stages.
        Read their eventual recorded results with queryRecordedStatus. Managers whose
        actions have no independently changing provider state do nothing."""
        raise NotImplementedError

    def waitForAction(self, action_id: int, check_freq=30) -> ActionStatus:
        """Block the caller until a started action finishes and return its status.

        action_id (int) is the positive global action ID; check_freq (float,
        default 30) is the delay in seconds between reading its recorded state.
        Return ActionStatus, normally COMPLETED, FAILED or CANCELED, once it is
        no longer SCHEDULING, ACTIVE, PROVIDER_HELD or CANCELING. This convenience method does
        not start workers, perform provider polls or advance workflow stages.
        Exceptions retained by a worker, including simulated crashes, propagate
        to the caller. Call outside locks needed by the worker."""
        while True:
            status, _ = self.queryRecordedStatus(action_id)
            with self._worker_lock:
                worker = self._action_workers.get(action_id)
            if worker is not None and worker.future.done():
                worker.future.result()
            if status not in ActionStatusGroup.EXECUTING.value and status != ActionStatus.CANCELING:
                if worker is not None:
                    worker.future.result()
                return status
            time.sleep(check_freq)

    @abstractmethod
    def getLiveActions(self) -> list[dict]:
        """List LIVE actions and their monitoring information without provider requests.

        Take no arguments. Return list[dict], one entry per recorded action
        whose status is ACTIVE or PROVIDER_HELD (the LIVE group). Each entry has
        job_id (positive workflow ID, int), api_key (provider identifier, str or
        None), api_status (latest provider state, str or None), plus action-type
        details described by the implementation. List positions are not action
        IDs. API sequences remain SCHEDULING while running, so they are omitted."""
        raise NotImplementedError

    @abstractmethod
    def getActionInfo(self, action_id: int) -> dict:
        """Retrieve a stored action's details and execution progress.

        action_id (int) is its positive global action ID; an unknown ID is an
        error. Return dict containing job_id (positive workflow ID, int), api_key
        (provider identifier, str or None), api_status (latest provider state,
        str or None), and implementation-specific details/checkpoints. None
        denotes an unavailable or inapplicable provider field. This is a cached
        inspection operation: it neither executes work nor contacts providers."""
        raise NotImplementedError

    def stopWorkers(self) -> None:
        """Stop background action workers while preserving unfinished work for recovery.

        Called during manager shutdown. Take no arguments and return None after
        signaling workers; do not wait for them or change durable action state.
        Cooperative query waits and polling delays can wake promptly, while
        noninterruptible API calls may finish later. Persisted intent and progress
        allow a subsequent start/recovery to resume unfinished actions. Accepted
        remote jobs remain running unless cancelOrResumeCancelingAction is requested separately."""
        with self._worker_lock:
            self.workers.stop()
