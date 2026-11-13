from pathlib import Path
import sqlite3
from .actions_base import ActionClass, actionClass
from .globus_transfer_action import GlobusDataTransfers
from .compute_action_manager import ComputeActions
from .api_action_manager import APIActions
from .database import LockedConnection
from .action_manager import ActionManager, ActionStatus, SimulatedCrash, _ser, _unser
from .logging import wfmanLog
import time
import json
from typing import Tuple

class JobData:
    """Submit, track and progress workflows backed by a database.

    Use one live JobData instance per database. Its connection lock serializes
    threads sharing this instance; separate instances/processes are not
    coordinated. Close the previous instance before reopening for recovery.
    """

    def __init__(self, filename: str | None = None, max_workflows_active=10,
                 *, _test_crash_at: str | None = None,
                 _test_action_manager_factories=None, poll_freq=30):
        """Initialize workflow storage and recover unfinished action workers.

        filename (str | None) is the SQLite path, or None for memory-only storage.
        max_workflows_active (int, default 10) caps automatic starts; zero disables
        automatic starts. poll_freq (float, default 30) is remote-action supervisor
        delay in seconds; API sequences have no polling interval. _test_crash_at (str | None) selects a test crash boundary;
        _test_action_manager_factories (dict[ActionClass, callable] | None) overrides
        manager constructors for tests. Return None after restoring supervision
        of unfinished stored actions; pending workflows are not started.
        """
        if filename is not None and "~" in str(filename):
            raise ValueError("Database paths containing '~' are not supported")
        db_path = ":memory:" if filename is None else str(Path(filename))
        self.max_workflows_active = max_workflows_active
        connection = sqlite3.connect(db_path, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        self.conn = LockedConnection(connection)
        
        self._test_crash_at = _test_crash_at
        manager_factories = {
            ActionClass.TRANSFER: GlobusDataTransfers,
            ActionClass.COMPUTE: ComputeActions,
            ActionClass.API: APIActions,
        }
        if _test_action_manager_factories:
            manager_factories.update(_test_action_manager_factories)
        self.action_man: dict[ActionClass, ActionManager] = {
            action_class: factory(self.conn, _test_crash_at=self._test_crash_at,
                                  **({} if action_class == ActionClass.API else {"poll_freq": poll_freq}))
            for action_class, factory in manager_factories.items()
        }

        with self.conn as conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
            job_id INTEGER PRIMARY KEY,
            job_group TEXT,
            submission_key TEXT UNIQUE,
            workflow BLOB NOT NULL,
            workflow_stage INTEGER NOT NULL,
            head_action_type TEXT,
            head_action_class TEXT,
            head_action_status TEXT,
            head_action_id INTEGER,
            last_status_change INTEGER
            )
            """)
            # Action IDs are allocated here, rather than independently by each
            # action manager, so a job head and its persisted action always use
            # the same durable key.
            conn.execute("CREATE TABLE IF NOT EXISTS action_ids (action_id INTEGER PRIMARY KEY AUTOINCREMENT)")
            recorded_action_ids = [
                conn.execute("SELECT MAX(head_action_id) FROM jobs").fetchone()[0],
                *(manager.getMaxActionId() for manager in self.action_man.values()),
            ]
            max_existing_action_id = max(
                (action_id for action_id in recorded_action_ids if action_id is not None),
                default=None,
            )
            if max_existing_action_id is not None:
                conn.execute(
                    "INSERT OR IGNORE INTO action_ids(action_id) VALUES (?)",
                    (max_existing_action_id,),
                )

        # Recover existing work only after every manager/table and the shared
        # ID allocator are ready. Pending workflows still require explicit start.
        try:
            self.resumeActions()
        except BaseException:
            self.stopWorkers()
            self.conn.close()
            raise

    def _allocateActionId(self, conn):
        """Allocate the durable, cross-action-class ID for a new action."""
        return conn.execute("INSERT INTO action_ids DEFAULT VALUES").lastrowid

    def _maybeSimulateCrash(self, point):
        if point == self._test_crash_at:
            raise SimulatedCrash(point)
       
    def enqueueJob(self, workflow, job_group=None, *, submission_key: str | None = None) -> int:
        """Queue a workflow, or return its original ID when a submission key is reused.

        workflow (action object | nonempty list[action]) contains the ordered
        workflow stages. job_group (str | None) is optional, nonunique metadata.
        submission_key (str | None) is an optional identifier unique within this
        database, compared exactly and retained for completed/canceled workflows.
        None always creates a new workflow. An existing key returns its original
        workflow without comparing or changing the supplied stages or group.

        Return the positive integer database workflow ID, not a stage index.
        A new workflow and its key are committed together, without starting it.
        Reuse the same key after a lost response; use a new key for changed work.
        Access through JobManager's context/callable interface while it runs.
        """
        if submission_key is not None and not isinstance(submission_key, str):
            raise TypeError('submission_key must be a string or None')

        with self.conn as conn:
            # Return retries without serializing replacement payloads or touching
            # the original record, including its terminal status and timestamps.
            if submission_key is not None:
                existing = conn.execute('SELECT job_id FROM jobs WHERE submission_key = ?',
                                        (submission_key,)).fetchone()
                if existing is not None:
                    return existing['job_id']

            if not isinstance(workflow, list):
                workflow = [workflow]
            assert len(workflow) > 0

            # The connection lock covers both lookup and insertion, so concurrent
            # callers sharing this JobData cannot insert the same key twice.
            cur = conn.execute(
                "INSERT INTO jobs(job_group, submission_key, workflow, workflow_stage, head_action_type, "
                "head_action_class, head_action_status, last_status_change) VALUES (?,?,?,?,?,?,?,?)",
                (job_group, submission_key, _ser(workflow), -1, type(workflow[0]).__name__,
                 actionClass(workflow[0]).name, ActionStatus.PENDING.name, int(time.time())),
            )
            return cur.lastrowid

    def cancelJob(self, job_id: int, cancel_running: bool = True) -> None:
        """Mark a workflow canceled and optionally cancel its remote action.

        ``job_id`` identifies the workflow. With the default
        ``cancel_running=True``, the workflow first enters ``CANCELING`` and
        cancellation is requested for its current compute or Globus action.
        API failures propagate but leave that intent in place for retry. The
        workflow and earlier action records remain in the database. Call this
        through ``JobManager``
        when its background thread is running so cancellation is serialized
        with scheduling. Raise ``ValueError`` for an unknown job.
        """
        if type(job_id) is not int or job_id < 1:
            raise ValueError("job_id must be a positive integer")
        if type(cancel_running) is not bool:
            raise TypeError("cancel_running must be a boolean")

        with self.conn as conn:
            job = conn.execute(
                "SELECT head_action_id, head_action_class, head_action_status "
                "FROM jobs WHERE job_id = ?", (job_id,),
            ).fetchone()
        if job is None:
            raise ValueError(f"Unknown job_id: {job_id}")

        if job["head_action_status"] == ActionStatus.CANCELED.name:
            return
        if job["head_action_status"] == ActionStatus.CANCELING.name:
            self.reconcileCancelingActions([job_id], raise_errors=True)
            return

        action_class = ActionClass[job["head_action_class"]]
        needs_cancellation = job["head_action_status"] in (
            ActionStatus.SCHEDULING.name, ActionStatus.ACTIVE.name
        ) or (action_class == ActionClass.TRANSFER and
              job["head_action_status"] == ActionStatus.FAILED.name)
        if cancel_running and needs_cancellation:
            if job["head_action_id"] is None:
                raise RuntimeError(f"Job {job_id} has no current action ID")
            with self.conn as conn:
                conn.execute(
                    "UPDATE jobs SET head_action_status = ?, last_status_change = ? "
                    "WHERE job_id = ?",
                    (ActionStatus.CANCELING.name, int(time.time()), job_id),
                )
            self._maybeSimulateCrash("job_head_canceling_persisted")
            self.reconcileCancelingActions([job_id], raise_errors=True)
            return

        with self.conn as conn:
            conn.execute(
                "UPDATE jobs SET head_action_status = ?, last_status_change = ? "
                "WHERE job_id = ?",
                (ActionStatus.CANCELED.name, int(time.time()), job_id),
            )

    def reconcileCancelingActions(self, job_ids=None, *, raise_errors=False):
        """Reconcile canceling workflow heads through their action managers.

        ``job_ids`` optionally limits reconciliation to specific workflows.
        Supply the stored action definition so its ActionManager can recover
        a missing action record and reconcile cancellation without submission.
        Leave the workflow ``CANCELING`` while remote discovery may lag.
        ``raise_errors`` propagates API errors to a direct caller; otherwise
        failed requests remain ``CANCELING`` for the next manager pass.
        """
        with self.conn as conn:
            if job_ids is None:
                canceling_jobs = conn.execute(
                    "SELECT job_id, workflow, workflow_stage, head_action_id, "
                    "head_action_class FROM jobs WHERE head_action_status = ?",
                    (ActionStatus.CANCELING.name,),
                ).fetchall()
            else:
                placeholders = ",".join("?" for _ in job_ids)
                canceling_jobs = conn.execute(
                    "SELECT job_id, workflow, workflow_stage, head_action_id, "
                    "head_action_class FROM jobs WHERE head_action_status = ? "
                    f"AND job_id IN ({placeholders})",
                    (ActionStatus.CANCELING.name, *job_ids),
                ).fetchall()

        for job in canceling_jobs:
            try:
                # A crash during submission may leave only the job head. Its
                # action name lets us find remote work without submitting any.
                action_id = job["head_action_id"]
                if action_id is not None:
                    action_class = ActionClass[job["head_action_class"]]
                    action_manager = self.action_man[action_class]
                    workflow = _unser(job["workflow"])
                    action = workflow[job["workflow_stage"]]
                    action_manager.cancelOrResumeCancelingAction(action_id, recovery=(action, job["job_id"]))
                    status, _ = action_manager.queryRecordedStatus(action_id)
                    if status in (ActionStatus.SCHEDULING, ActionStatus.ACTIVE, ActionStatus.CANCELING):
                        continue

                self._maybeSimulateCrash("action_canceled_before_job_head_update")
                with self.conn as conn:
                    conn.execute(
                        "UPDATE jobs SET head_action_status = ?, last_status_change = ? "
                        "WHERE job_id = ? AND head_action_status = ?",
                        (ActionStatus.CANCELED.name, int(time.time()), job["job_id"],
                         ActionStatus.CANCELING.name),
                    )
            except SimulatedCrash:
                raise
            except Exception as exc:
                if raise_errors:
                    raise
                wfmanLog(f"Could not reconcile cancellation for job {job['job_id']}: {exc}")
   
    def jobState(self, job_id):
        """Return the complete state of the workflow"""
        with self.conn as conn:
            row = dict(conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone())
            row['head_action_status'] = ActionStatus[row['head_action_status']]
            row['head_action_class'] = ActionClass[row['head_action_class']]
            return row

    def listWorkflows(self, job_id: int | None = None,
                      statuses: ActionStatus | list[ActionStatus] | None = None) -> list[dict]:
        """Return workflow state dictionaries, ordered by job ID.

        Args:
            job_id (int | None): Positive database workflow ID, not a list
                index. None selects all recorded workflows, including terminal
                records. An existing ID excluded by statuses returns no rows.
            statuses (ActionStatus | list[ActionStatus] | None): Allowed overall
                workflow statuses, as returned by jobStatus. List entries are
                combined with OR and the ID/status filters with AND. None
                permits all statuses; [] matches none. COMPLETED means every
                step finished, not intermediate head-action completion.

        Returns:
            list[dict]: Matching jobState dictionaries in ascending job_id
            order, or [] if nothing matches. The list positions are zero-based
            and are independent of workflow/action IDs. Each dictionary has:
                job_id (int): Positive workflow database key.
                job_group (str | None): Group supplied when enqueuing.
                submission_key (str | None): Optional database-unique submission
                    identifier, retained after completion/cancellation.
                workflow (bytes): Pickled list of action objects; not JSON.
                workflow_stage (int): Zero-based action position, -1 before
                    the first action, or the action count after full completion.
                head_action_id (int | None): Positive action database key,
                    independent of job_id; None when no action is assigned.
                head_action_type (str | None): Action class name; 'NoneType'
                    after normal workflow completion.
                head_action_class (ActionClass): COMPUTE, TRANSFER, API, or NONE.
                head_action_status (ActionStatus): Exact recorded head status,
                    including SCHEDULING and intermediate COMPLETED.
                last_status_change (int | None): Recorded Unix timestamp in
                    whole seconds, not a guarantee of the last API poll time.

        Raises:
            ValueError: job_id is invalid or an explicit ID does not exist.
            TypeError: statuses is not an ActionStatus or a list of them.

        This reads recorded state without polling or progressing workflows.
        Use the JobManager lock when its background thread is running.
        """
        if job_id is not None and (type(job_id) is not int or job_id < 1):
            raise ValueError("job_id must be a positive integer")
        if isinstance(statuses, ActionStatus):
            statuses = [statuses]
        elif statuses is not None and (
            not isinstance(statuses, list) or any(not isinstance(status, ActionStatus) for status in statuses)
        ):
            raise TypeError("statuses must be an ActionStatus or a list of ActionStatus values")
        with self.conn as conn:
            if job_id is None:
                rows = conn.execute("SELECT job_id FROM jobs ORDER BY job_id").fetchall()
            else:
                rows = conn.execute("SELECT job_id FROM jobs WHERE job_id = ?", (job_id,)).fetchall()
                if not rows:
                    raise ValueError(f"Workflow {job_id} does not exist")
            return [self.jobState(row['job_id']) for row in rows
                    if statuses is None or self.jobStatus(row['job_id']) in statuses]

    def headActionStatus(self, job_id, *, force_poll=False, return_api_status=False):
        """Return the head action's ActionStatus, optionally with its API status.

        job_id identifies a workflow. If force_poll is True, poll all ACTIVE
        workflow heads immediately and propagate their statuses to the workflow
        database. This does not submit actions or advance workflow steps.
        Poll failures are logged and leave the last known status in place.

        By default return the recorded head ActionStatus, including SCHEDULING
        and intermediate COMPLETED states. If return_api_status is True, return
        (action_status, api_status), where api_status is the provider's status
        string (for example 'queued'), or None before a remote status is known
        or when no head action exists. This is not the full API response body.
        Use jobStatus to distinguish intermediate completion from a finished
        workflow. Access through JobManager's lock while its thread is running.
        """
        state = self.jobState(job_id)
        if force_poll:
            self.progressActiveActions(force_poll=True)
            state = self.jobState(job_id)
        action_status = state['head_action_status']
        if not return_api_status:
            return action_status

        api_status = None
        if state['head_action_class'] != ActionClass.NONE and state['head_action_id'] is not None:
            # Read the cached provider status without causing a second poll.
            _, api_status = self.action_man[state['head_action_class']].queryRecordedStatus(
                state['head_action_id']
            )
        return action_status, api_status

    def jobStatus(self, job_id)->ActionStatus:
        """Return the status of the workflow"""
        state = self.jobState(job_id)
        if state['head_action_status'] == ActionStatus.PENDING:
            return ActionStatus.PENDING
        elif state['head_action_status'] == ActionStatus.FAILED:
            return ActionStatus.FAILED
        elif state['head_action_status'] == ActionStatus.CANCELED:
            return ActionStatus.CANCELED
        elif state['head_action_status'] == ActionStatus.CANCELING:
            return ActionStatus.CANCELING
        elif state['head_action_class'] == ActionClass.NONE and state['head_action_status'] == ActionStatus.COMPLETED:
            return ActionStatus.COMPLETED
        else:
            return ActionStatus.ACTIVE
 
    def progressWorkflows(self, condition: Tuple[str, list | None]):
        """
        Find actions matching a certain condition and initiate the next stage of the workflow
        condition:
           ("COMPLETE",None) - Progress all active workflows whose head action status is ActionStatus.COMPLETED and for which there are remaining workflow actions
           ("VALID_IN", [job_id1, job_id2, ...]) - Progress valid workflows (those whose head action status is either ActionStatus.PENDING or ActionStatus.COMPLETED, not in a failure state) based on a list of job indices.
        """

        pending_actions = []        
        completed_actions = [] #list of action ids of completed actions
        
        with self.conn as conn:
            if condition[0] == "COMPLETE" and condition[1] == None:
                progress_actions = conn.execute("SELECT job_id, workflow, workflow_stage, head_action_type, head_action_status, head_action_id, head_action_class FROM jobs WHERE head_action_class != ? AND head_action_status = ?",
                                                (ActionClass.NONE.name,ActionStatus.COMPLETED.name)).fetchall()
            elif condition[0] == "VALID_IN" and isinstance(condition[1],list):
                placeholders = ",".join("?" for _ in condition[1])
                progress_actions = conn.execute(f"SELECT job_id, workflow, workflow_stage, head_action_type, head_action_status, head_action_id, head_action_class FROM jobs WHERE head_action_class != ? AND job_id IN ({placeholders}) AND head_action_status IN (?,?)",
                                                ( ActionClass.NONE.name, *condition[1], ActionStatus.PENDING.name, ActionStatus.COMPLETED.name )
                                                )
            else:
                raise Exception("Unknown condition" + str(condition))
                
            for a in progress_actions:              
                #Get information on the next workflow task
                workflow = _unser(a['workflow'])
                workflow_stage = a['workflow_stage']

                #Record completed actions so we can update any monitors
                if a['head_action_status'] == ActionStatus.COMPLETED.name:
                    completed_actions.append(  (a['head_action_id'], getattr(ActionClass, a['head_action_class'], None) ) )
                
                job_id = a['job_id']
                next_workflow_stage = workflow_stage+1

                next_action = None if next_workflow_stage == len(workflow) else workflow[next_workflow_stage]
                next_action_class = ActionClass.NONE if next_action == None else actionClass(next_action)
                next_action_status = ActionStatus.COMPLETED if next_action == None else ActionStatus.SCHEDULING
                action_id = None if next_action is None else self._allocateActionId(conn)
                
                wfmanLog(f"Progressing job {job_id} action {a['head_action_type']} status {a['head_action_status']} to action {type(next_action).__name__}")
                
                # Persist the workflow's scheduling intent before any remote API call.
                conn.execute("UPDATE jobs SET head_action_type = ?, head_action_class = ?, head_action_status = ?, head_action_id = ?, last_status_change = ?, workflow_stage = ? WHERE job_id = ?",
                             (type(next_action).__name__,  next_action_class.name, next_action_status.name, action_id, int(time.time()), next_workflow_stage, job_id )
                              )

                #Gather information to initiate next action
                if next_action_status == ActionStatus.SCHEDULING:
                    pending_actions.append( (next_action_class, next_action, job_id, action_id ) )

        self._maybeSimulateCrash("job_head_scheduling_persisted")


        # #Inform GUI regarding completed actions (requires database activity)
        # for action_id, action_class in completed_actions:
        #     info = self.action_man[action_class].getActionInfo(action_id)
        #     if action_class == ActionClass.TRANSFER:
        #         updateGUI('update_transfer', json.dumps(info))
        #     elif action_class == ActionClass.COMPUTE:
        #         updateGUI('update_compute', json.dumps(info))

        #Initiate the required actions
        head_action_updates = [] #(job_id, head_action_id, head_action_status, head_action_class)

        for action_class, action, job_id, action_id in pending_actions:
            wfmanLog(f"Initiating action of type {action_class.name} for {job_id}")
            aman = self.action_man[action_class]
            aman.startOrResumeAction(action, job_id, action_id)
            try:
                action_status, _ = aman.queryRecordedStatus(action_id)
            except SimulatedCrash:
                raise
            except Exception as exc:
                # Keep the durable scheduling head until its status is readable.
                wfmanLog(f"Could not poll action {action_id} for job {job_id}: {exc}")
                continue
            head_action_updates.append( (job_id, action_id, action_status, action_class) )

        self._maybeSimulateCrash("action_state_persisted_before_job_head_update")
        
        #Update job state DB
        with self.conn as conn:
            for job_id, action_id, status, _ in head_action_updates:
                conn.execute("UPDATE jobs SET head_action_status = ?, last_status_change = ? WHERE job_id = ? AND head_action_id = ?",
                             (status.name, int(time.time()), job_id, action_id )
                             )

        #Inform GUI regarding new actions (requires database activity)
        # for _, action_id, _, action_class in head_action_updates:
        #     info = self.action_man[action_class].getActionInfo(action_id)
        #     if action_class == ActionClass.TRANSFER:
        #         updateGUI('add_transfer', json.dumps(info))
        #     elif action_class == ActionClass.COMPUTE:
        #         updateGUI('add_compute', json.dumps(info))

                


    def startWorkflows(self, job_ids : list | None = None):
        """
        Start the workflows specified by the list of job ids. If None, additional workflows will be started until the total number of active workflows reaches the maximum
        """
        self.reconcileSchedulingActions()

        if job_ids == None:        
            with self.conn as conn:
                count = int(conn.execute("SELECT COUNT(*) FROM jobs WHERE head_action_status IN (?, ?, ?)", (ActionStatus.SCHEDULING.name, ActionStatus.ACTIVE.name, ActionStatus.CANCELING.name) ).fetchone()[0])
                rem =  self.max_workflows_active - count

                if rem > 0:
                    toschedule = conn.execute("SELECT job_id FROM jobs WHERE head_action_status = ? ORDER BY job_id ASC LIMIT ?", (ActionStatus.PENDING.name, rem)).fetchall()                   
                    job_ids = [ j[0] for j in toschedule ]
                    if len(job_ids) > 0:
                        wfmanLog("Number of active workflows",count,"want to activate",rem,"more.\nActivating",len(job_ids),"workflows with job ids", job_ids)

        if job_ids:
            self.progressWorkflows(("VALID_IN",job_ids))
                        


    def progressActiveWorkflows(self):
        """
        Find COMPLETE actions and initiate the next stage of the workflow
        """
        self.progressWorkflows(("COMPLETE",None))

    def reconcileSchedulingActions(self):
        """Repair job heads whose submission crossed a process crash boundary.

        Supply the stored action and its JobData-issued ID to the action
        manager, which owns recovery of its record and any remote submission.
        Resume missing worker supervision for recovery; a live worker is reused
        and already progresses independently. Read cached state to update workflow
        heads. Repeated calls do not drive submission attempts individually.
        """
        with self.conn as conn:
            scheduling_jobs = conn.execute(
                "SELECT job_id, workflow, workflow_stage, head_action_class, head_action_id FROM jobs WHERE head_action_status = ?",
                (ActionStatus.SCHEDULING.name,),
            ).fetchall()

        updates = []
        for job in scheduling_jobs:
            action_class = ActionClass[job["head_action_class"]]
            action_id = job["head_action_id"]
            if action_id is None:
                raise Exception(f"SCHEDULING job {job['job_id']} has no action ID")
            aman = self.action_man[action_class]
            workflow = _unser(job["workflow"])
            action = workflow[job["workflow_stage"]]
            aman.startOrResumeAction(action, job["job_id"], action_id)
            try:
                action_status, _ = aman.queryRecordedStatus(action_id)
            except SimulatedCrash:
                raise
            except Exception as exc:
                wfmanLog(f"Could not read action {action_id} for job {job['job_id']}: {exc}")
                continue
            updates.append((job["job_id"], action_id, action_status))

        if updates:
            with self.conn as conn:
                for job_id, action_id, action_status in updates:
                    conn.execute(
                        "UPDATE jobs SET head_action_status = ?, last_status_change = ? WHERE job_id = ? AND head_action_id = ?",
                        (action_status.name, int(time.time()), job_id, action_id),
                    )
        
    def progressActiveActions(self, force_poll=False):
        """Copy recorded action states to ACTIVE workflow heads.

        force_poll (bool, default False) wakes supervisors for an earlier poll;
        this call does not wait for API results. Return dict[int, ActionStatus]
        mapping changed workflow IDs to their new head status. Read failures are
        logged and leave that workflow unchanged until a later pass.
        """
        with self.conn as conn:
            active_actions = conn.execute("SELECT head_action_id, job_id, head_action_type, head_action_class FROM jobs WHERE head_action_status = ?", (ActionStatus.ACTIVE.name, )).fetchall()

        updates = {}
        for t in active_actions:
            job_id = t['job_id']
            action_class = getattr(ActionClass, t['head_action_class'], None)
            aman = self.action_man[action_class] 
            # Observe each supervisor independently without waiting for provider I/O.
            try:
                action_status, _ = aman.queryRecordedStatus(t['head_action_id'], force_update = force_poll)
            except SimulatedCrash:
                raise
            except Exception as exc:
                wfmanLog(f"Could not poll action {t['head_action_id']} for job {job_id}: {exc}")
                continue
            if action_status != ActionStatus.ACTIVE:
                updates[job_id] = action_status
            if job_id in updates:
                wfmanLog(f"Progressed job {job_id} action {t['head_action_type']} of class {action_class.name} to {updates[job_id].name}")
                
        #Update head action state
        if len(updates) > 0:
            with self.conn as conn:
                for job_id, status in updates.items():
                    conn.execute("UPDATE jobs SET head_action_status = ? WHERE job_id = ?",(status.name, job_id))
        return updates


    def progressActiveState(self, force_poll=False):
        """Observe action supervision and advance completed workflow stages.

        force_poll (bool, default False) requests earlier polls without waiting.
        Return None. Existing workers progress independently of these calls.
        """
        self.reconcileCancelingActions()
        self.reconcileSchedulingActions()
        self.progressActiveActions(force_poll=force_poll)
        self.progressActiveWorkflows()
    
    def countWorkflowsWithStatus(self, statuses : ActionStatus | list[ActionStatus]):
        """
        Count the number of workflows for which the head action (if not none) has a status in the provided list
        """
        
        with self.conn as conn:
            if isinstance(statuses, ActionStatus):            
                return int(conn.execute("SELECT COUNT(*) FROM jobs WHERE head_action_class != ? AND head_action_status = ?", (ActionClass.NONE.name, statuses.name,) ).fetchone()[0])
            elif isinstance(statuses, list):
                placeholders = ",".join("?" for _ in statuses)
                names = [s.name for s in statuses]                
                return int(conn.execute(f"SELECT COUNT(*) FROM jobs WHERE head_action_class != ? AND head_action_status IN ({placeholders})", (ActionClass.NONE.name, *names) ).fetchone()[0])
            else:
                raise Exception("Unexpected type for 'statuses'", type(statuses))

    def getActiveActions(self, action_class : ActionClass)->dict:
        """
        Get all active actions and returning a list of dictionaries, each containing "api_key", "api_status" and other custom fields defined on a per-action basis
        """
        return self.action_man[action_class].getActiveActions()

    def stopWorkers(self) -> None:
        """Take no arguments; signal all action managers' workers and return None.

        Does not cancel accepted remote jobs or alter durable workflow state.
        """
        for manager in self.action_man.values():
            manager.stopWorkers()

    def resumeActions(self) -> None:
        """Recover worker supervision for current unfinished workflow heads.

        Take no arguments; return None. Called during construction after storage
        and action managers are initialized. This restores persisted work, not
        individual execution steps or paused workflows. Historical action records
        are not scanned or restarted,
        including work deliberately abandoned with cancel_running=False.
        Supervisors recover their own state and run independently afterward.
        """
        with self.conn as conn:
            jobs = conn.execute('SELECT job_id, workflow, workflow_stage, head_action_id, head_action_class, '
                                'head_action_status FROM jobs WHERE head_action_status IN (?, ?, ?)',
                                (ActionStatus.SCHEDULING.name, ActionStatus.ACTIVE.name, ActionStatus.CANCELING.name)).fetchall()
        for job in jobs:
            if job['head_action_id'] is None:
                raise RuntimeError(f"Unfinished job {job['job_id']} has no action ID")
            action = _unser(job['workflow'])[job['workflow_stage']]
            manager = self.action_man[ActionClass[job['head_action_class']]]
            if job['head_action_status'] == ActionStatus.CANCELING.name:
                manager.cancelOrResumeCancelingAction(job['head_action_id'], recovery=(action, job['job_id']))
            else:
                manager.startOrResumeAction(action, job['job_id'], job['head_action_id'])
