"""Database-backed management of asynchronous compute and transfer actions."""

import math
import time
from .action_manager import ActionManager, ActionStatus, ActionStatusGroup, SimulatedCrash, initializeActionStatusViews, _ser, _unser
from .logging import wfmanLog
from .database import DatabaseClosed, LockedConnection
from .workers import Workers, WorkerInterrupted, checkForThreadCurrentWorkerInterruption, sleepCurrentThreadWorker


class AsyncActionManager(ActionManager):
    """Persist and reconcile asynchronous remote submissions and cancellation."""

    def _queryStatusInternal(self, machine, api_key):
        """Return the API status"""        
        raise NotImplementedError("Derived class must implement _queryStatusInternal")

    def _findRemoteActionByName(self, machine, name):
        """Return the remote API key for an action name, or None if absent."""
        return None

    def _cancelActionInternal(self, machine, api_key):
        """Request cancellation of a submitted remote action."""
        raise NotImplementedError("Derived class must implement _cancelActionInternal")

    @staticmethod
    def _remoteActionName(action_id):
        """Return the deterministic name used to find an action after a crash."""
        return f"amscjm_{action_id}"
    
    def __init__(self, connection: LockedConnection, table_name, api_action_status_map : dict,
                 *, _test_crash_at: str | None = None,
                 scheduling_retry_delay=30, max_submission_attempts=3, workers: Workers | None = None, poll_freq=30):
        """Initialize remote-action storage, shared status views, and supervision settings.

        connection (LockedConnection) is JobData's shared locking context manager.
        table_name (str) names this manager's table; api_action_status_map
        (dict[str, ActionStatus]) maps provider states. scheduling_retry_delay
        (float, default 30) is initial backoff in seconds; max_submission_attempts
        (int, default 3) limits submissions. workers (Workers | None) supplies
        supervisor execution; poll_freq (float, default 30) is seconds between
        polls/recovery attempts. _test_crash_at (str | None) selects a test crash
        boundary. Return None; existing records remain unchanged.
        """
        super().__init__(workers=workers)
        self.poll_freq = poll_freq
        self.conn = connection
        self.table_name = table_name
        self.api_action_status_map = api_action_status_map #
        self._test_crash_at = _test_crash_at
        self.scheduling_retry_delay = scheduling_retry_delay
        self.max_submission_attempts = max_submission_attempts
        initializeActionStatusViews(self.conn)
        
        with self.conn as conn:        
            conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
            action_id INTEGER PRIMARY KEY,
            machine TEXT,
            details BLOB,
            api_key TEXT,
            api_status TEXT,
            action_status TEXT,
            last_update INTEGER,
            job_id INTEGER,
            submission_attempts INTEGER NOT NULL DEFAULT 0,
            last_submission_attempt INTEGER,
            last_submission_error TEXT
            )
            """)

    def _maybeSimulateCrash(self, point):
        """Raise SimulatedCrash for matching point (str), otherwise return None."""
        if point == self._test_crash_at:
            raise SimulatedCrash(point)

    def _callApiOperation(self, action_id: int, operation, *args, failure_message: str,
                          record_error=False, update_timestamp=True, **kwargs):
        """Call an operation with shared failure logging and optional error persistence.

        action_id (int) is the positive global action ID. operation (callable)
        receives args (tuple) and kwargs (dict[str, object]) unchanged and may
        perform API calls and related database work.
        failure_message (str) describes the operation/context for the log; append
        the exception type and server/error text on failure. record_error (bool,
        default False) stores last_submission_error for this action. When storing
        an error, update_timestamp (bool, default True) also sets last_update to
        the current Unix time in seconds; False leaves that timestamp unchanged.

        Return (bool, object): index 0 indicates success; index 1 holds the result
        on success, including a legitimate None, or None on failure. Interruption,
        simulated crashes and closed storage propagate. No retry, attempt counter
        or action-status transition is performed here. This helper acquires its
        database lock only for failure persistence, after the operation finishes.
        """
        # Keep provider calls outside this helper's database transaction.
        try:
            return True, operation(*args, **kwargs)
        except (WorkerInterrupted, SimulatedCrash, DatabaseClosed):
            # These control worker lifetime/recovery rather than API retries.
            raise
        except Exception as exc:
            # Record diagnostics only; callers retain retry and status policy.
            wfmanLog(f'{failure_message}: {type(exc).__name__}: {exc}')
            if record_error:
                with self.conn as conn:
                    if update_timestamp:
                        conn.execute(
                            f'UPDATE {self.table_name} SET last_submission_error = ?, last_update = ? WHERE action_id = ?',
                            (str(exc), int(time.time()), action_id),
                        )
                    else:
                        conn.execute(
                            f'UPDATE {self.table_name} SET last_submission_error = ? WHERE action_id = ?',
                            (str(exc), action_id),
                        )
            return False, None

    def _recordSchedulingIntentIfNew(self, action, job_id, action_id):
        """Create a SCHEDULING record only for a previously unseen action ID.

        Args:
            action (object): Serializable action definition from the workflow.
            job_id (int): Positive workflow ID owning the action.
            action_id (int): Positive, JobData-issued action ID.

        Returns:
            int: The action ID, whether its record was created or already
                existed. Existing definitions and execution state are retained.
        """
        with self.conn as conn:
            existing = conn.execute(
                f"SELECT action_id FROM {self.table_name} WHERE action_id = ?",
                (action_id,),
            ).fetchone()
            if existing is not None:
                return existing["action_id"]

            conn.execute(
                f"INSERT INTO {self.table_name}(action_id, machine, details, api_key, api_status, action_status, last_update, job_id) VALUES (?,?,?,?,?,?,?,?)",
                (
                    action_id,
                    getattr(action, "machine", None),
                    _ser(action),
                    None,
                    None,
                    ActionStatus.SCHEDULING.name,
                    int(time.time()),
                    job_id,
                ),
            )
        self._maybeSimulateCrash("action_intent_persisted")
        return action_id

    def startOrResumeAction(self, action, job_id, action_id) -> None:
        """Start an action or recover its submission, monitoring or cancellation.

        action (object) is the serialized definition used only for a missing
        record; job_id (int) is its positive workflow ID; action_id (int) is
        always allocated by JobData and reused for recovery. Resumption restores
        supervision after the previous worker stopped; a live worker is reused,
        and progresses without repeated calls. This call never waits for API work.
        Return None; queryRecordedStatus observes progress independently.
        """
        self._recordSchedulingIntentIfNew(action, job_id, action_id)
        status, _ = self.queryRecordedStatus(action_id)
        if status == ActionStatus.CANCELING:
            self._startCancellationWorker(action_id, complete_cancellation=self._completeCancellation)
        elif status in ActionStatusGroup.EXECUTING.value:
            self._startExecutionWorker(
                action_id, execute_action=self._executeAction,
                complete_cancellation=self._completeCancellation,
            )

    def hasAction(self, action_id):
        """Return whether this manager has persisted the supplied action ID."""
        with self.conn as conn:
            return conn.execute(
                f"SELECT 1 FROM {self.table_name} WHERE action_id = ?", (action_id,)
            ).fetchone() is not None

    def getMaxActionId(self) -> int | None:
        """Return the highest action ID recorded by this manager.

        Takes no arguments. Returns a positive int, or None when this manager
        has no action records. This inspects execution history only; JobData
        assigns IDs to every queued stage using its actions-table primary key.
        """
        with self.conn as conn:
            return conn.execute(
                f"SELECT MAX(action_id) FROM {self.table_name}"
            ).fetchone()[0]

    def _recordCancelingIntentIfNotAlready(self, action_id) -> bool:
        """Persist cancellation for positive integer action_id before API calls.

        Return True for a new CANCELING intent, False if already CANCELING or
        CANCELED. Repeated calls must not interrupt the cancellation worker's
        own authentication queries. Raise ValueError for an unknown action.
        """
        with self.conn as conn:
            action = conn.execute(
                f"SELECT action_status FROM {self.table_name} WHERE action_id = ?",
                (action_id,),
            ).fetchone()
            if action is None:
                raise ValueError(f"Unknown action_id: {action_id}")
            if action["action_status"] in (ActionStatus.CANCELING.name, ActionStatus.CANCELED.name):
                return False
            conn.execute(
                f"UPDATE {self.table_name} SET action_status = ?, last_update = ? WHERE action_id = ?",
                (ActionStatus.CANCELING.name, int(time.time()), action_id),
            )
        self._maybeSimulateCrash("action_canceling_persisted")
        return True

    def _reconcileCancelingAction(self, action_id):
        """Finish a recorded cancellation without submitting new work.

        ``action_id`` identifies an action in ``CANCELING``. If submission
        completed before its remote ID was saved, look it up by name. Query
        remote status before requesting cancellation; API errors leave the
        action in ``CANCELING`` for a later retry. Return ``action_id`` when
        cancellation is resolved, or ``None`` while remote discovery may lag.
        """
        with self.conn as conn:
            action = conn.execute(
                f"SELECT machine, api_key, action_status, submission_attempts, "
                f"last_submission_attempt FROM {self.table_name} "
                "WHERE action_id = ?", (action_id,),
            ).fetchone()
        if action is None:
            raise ValueError(f"Unknown action_id: {action_id}")
        if action["action_status"] == ActionStatus.CANCELED.name:
            return action_id
        if action["action_status"] != ActionStatus.CANCELING.name:
            raise ValueError(f"Action {action_id} is not awaiting cancellation")

        api_key = action["api_key"]
        api_status = None
        action_status = ActionStatus.CANCELED

        # Recover uncertain acceptance by name before deciding that there is
        # nothing to cancel. No database lock is held during provider calls.
        if api_key is None:
            api_key = self._findRemoteActionByName(
                action["machine"], self._remoteActionName(action_id)
            )
            if api_key is None and action["submission_attempts"] > 0:
                # The submission may have reached the remote service before
                # its ID was saved. Give name indexing the same delay used
                # before retrying that submission.
                retry_delay = self.scheduling_retry_delay * (
                    2 ** (action["submission_attempts"] - 1)
                )
                if int(time.time()) < action["last_submission_attempt"] + retry_delay:
                    return None
            elif api_key is not None:
                with self.conn as conn:
                    conn.execute(
                        f"UPDATE {self.table_name} SET api_key = ?, last_update = ? "
                        "WHERE action_id = ?",
                        (api_key, int(time.time()), action_id),
                    )
        if api_key is not None:
            api_status = self._queryStatusInternal(action["machine"], api_key)
            if api_status in self.terminal_api_statuses:
                action_status = self.api_action_status_map[api_status]
            else:
                self._cancelActionInternal(action["machine"], api_key)
                self._maybeSimulateCrash("remote_cancellation_completed")

        with self.conn as conn:
            conn.execute(
                f"UPDATE {self.table_name} SET api_status = ?, action_status = ?, "
                "last_update = ? WHERE action_id = ?",
                (api_status, action_status.name, int(time.time()), action_id),
            )
        return action_id

    def cancelOrResumeCancelingAction(self, action_id, *, recovery: tuple[object, int] | None = None):
        """Request or recover cancellation without submitting new work.

        Args:
            action_id (int): Positive, JobData-issued action ID.
            recovery (tuple[object, int] | None): Optional pair containing the
                stored action definition at index 0 and its positive workflow
                ID at index 1. Used to recover a missing action record after a
                crash. An existing record is left unchanged. Defaults to None.

        Returns:
            None: Cancellation intent is persisted and the supervisor is
                signaled. Observe its outcome with queryRecordedStatus; remote API
                failures retain intent for that supervisor to retry.

        Raises:
            ValueError: If the action is unknown and no recovery definition
                is supplied.
        """
        if recovery is not None:
            action, job_id = recovery
            # A crash may leave the workflow head persisted without an action
            # record. Recreate that record so cancellation can persist its
            # intent and reconcile any remote work; this does not submit the
            # action, and an existing record is left unchanged.
            self._recordSchedulingIntentIfNew(action, job_id, action_id)
        # Publish intent and interrupt execution before its worker can enter
        # cancellation. Repeated requests must not interrupt cancellation queries.
        with self._worker_lock:
            new_intent = self._recordCancelingIntentIfNotAlready(action_id)
            if self.queryRecordedStatus(action_id)[0] == ActionStatus.CANCELED:
                return
            if new_intent:
                self.workers.interrupt(action_id)
        self._startCancellationWorker(action_id, complete_cancellation=self._completeCancellation)

    def _reconcileScheduledAction(self, action_id):
        """Submit or recover a SCHEDULING action from its durable intent.

        ``action_id`` identifies the action. A fresh intent is submitted
        directly; after an attempt, the remote action is sought by name
        before retrying with backoff. Return the action ID when its local
        state can be read, or ``None`` when recovery must wait.
        """
        with self.conn as conn:
            action = conn.execute(
                f"SELECT machine, details, job_id, api_key, action_status, submission_attempts, "
                f"last_submission_attempt FROM {self.table_name} WHERE action_id = ?",
                (action_id,),
            ).fetchone()
        if action is None:
            raise Exception(f"Unknown action {action_id}")
        if action["action_status"] != ActionStatus.SCHEDULING.name:
            return action_id

        api_key = action["api_key"]

        # Recover uncertain acceptance using the deterministic provider name
        # before submitting again. The worker keeps this call stack while waiting.
        if api_key is None and action["submission_attempts"] > 0:
            succeeded, api_key = self._callApiOperation(
                action_id, self._findRemoteActionByName,
                action["machine"], self._remoteActionName(action_id),
                failure_message=(
                    f"Remote lookup failed for {self.table_name} action {action_id} "
                    f"(job {action['job_id']}, machine {action['machine']!r}); deferring reconciliation"
                ),
                record_error=True,
            )
            if not succeeded:
                # A failed lookup cannot safely authorize another submission.
                return None

        # If no remote action was found, either retry submission or stop after
        # the permitted number of attempts.
        if api_key is None:

            # Wait out the submission backoff before querying resource status
            # for an attempt that is not yet due. A final exhausted attempt
            # still checks the resource before it can be marked failed.
            if 0 < action["submission_attempts"] < self.max_submission_attempts:
                retry_delay = self.scheduling_retry_delay * (
                    2 ** (action["submission_attempts"] - 1)
                )
                if int(time.time()) < action["last_submission_attempt"] + retry_delay:
                    return None

            submitted_action = _unser(action["details"])
            self._maybeSimulateCrash("before_resource_status_check")
            succeeded, resource_status = self._callApiOperation(
                action_id, submitted_action.queryResourceStatus,
                failure_message=f"Could not query resource status for action {action_id}",
            )
            if not succeeded:
                return None
            self._maybeSimulateCrash("resource_status_checked")
            checkForThreadCurrentWorkerInterruption()
            if resource_status is False:
                wfmanLog(f"Resource for action {action_id} is down; deferring submission")
                return None
            if resource_status is not None and resource_status is not True:
                raise TypeError("queryResourceStatus must return True, False, or None")

            # Discovery has already tried to recover the final accepted attempt.
            if action["submission_attempts"] >= self.max_submission_attempts:
                error = "Remote action was not found after the maximum number of submission attempts"
                wfmanLog(
                    f"Submission retries exhausted for {self.table_name} action {action_id} "
                    f"(job {action['job_id']}, machine {action['machine']!r}, "
                    f"{action['submission_attempts']} attempts); marking FAILED: {error}"
                )
                with self.conn as conn:
                    conn.execute(
                        f"UPDATE {self.table_name} SET api_status = ?, action_status = ?, "
                        "last_submission_error = ?, last_update = ? WHERE action_id = ? AND action_status = ?",
                        (error, ActionStatus.FAILED.name, error, int(time.time()), action_id, ActionStatus.SCHEDULING.name),
                    )
                return action_id

            checkForThreadCurrentWorkerInterruption()
            # Persist the attempt before contacting the remote service. A
            # crash during submission then triggers lookup and retry backoff.
            with self.conn as conn:
                conn.execute(
                    f"UPDATE {self.table_name} SET submission_attempts = submission_attempts + 1, "
                    "last_submission_attempt = ?, last_submission_error = NULL "
                    "WHERE action_id = ?",
                    (int(time.time()), action_id),
                )
            self._maybeSimulateCrash("before_remote_submission")
            checkForThreadCurrentWorkerInterruption()
            succeeded, api_key = self._callApiOperation(
                action_id, submitted_action.initiateAction, action["job_id"],
                name=self._remoteActionName(action_id),
                failure_message=(
                    f"Submission failed for {self.table_name} action {action_id} "
                    f"(job {action['job_id']}, machine {action['machine']!r}, "
                    f"attempt {action['submission_attempts'] + 1}/{self.max_submission_attempts}); "
                    "retaining SCHEDULING for reconciliation"
                ),
                record_error=True, update_timestamp=False,
            )
            if not succeeded:
                # Discover any accepted remote work before retrying submission.
                return action_id
            self._maybeSimulateCrash("remote_submission_completed")

        # Save the remote ID before polling, whether submitted now or found
        # during recovery. A status outage must not make the action unfindable.
        with self.conn as conn:
            conn.execute(
                f"UPDATE {self.table_name} SET api_key = ?, last_update = ?, "
                "last_submission_error = NULL WHERE action_id = ?",
                (api_key, int(time.time()), action_id),
            )
        self._maybeSimulateCrash("remote_id_persisted")
        # Cancellation may have arrived during submission. Preserve acceptance,
        # then let this supervisor process the recorded cancellation intent.
        with self.conn as conn:
            current_status = conn.execute(
                f"SELECT action_status FROM {self.table_name} WHERE action_id = ?", (action_id,)
            ).fetchone()[0]
        if current_status != ActionStatus.SCHEDULING.name:
            return action_id
        succeeded, api_status = self._callApiOperation(
            action_id, self._queryStatusInternal, action["machine"], api_key,
            failure_message=(
                f"Status poll failed for {self.table_name} action {action_id} "
                f"(job {action['job_id']}, machine {action['machine']!r}, remote ID {api_key!r}); "
                "deferring reconciliation"
            ),
            record_error=True,
        )
        if not succeeded:
            return action_id
        self._maybeSimulateCrash("remote_status_obtained")
        action_status = self.api_action_status_map[api_status]
        with self.conn as conn:
            conn.execute(
                f"UPDATE {self.table_name} SET api_key = ?, api_status = ?, action_status = ?, last_update = ? WHERE action_id = ? AND action_status = ?",
                (api_key, api_status, action_status.name, int(time.time()), action_id, ActionStatus.SCHEDULING.name),
            )
        self._maybeSimulateCrash("action_state_persisted")
        return action_id

    def _executeAction(self, action_id: int) -> None:
        """Submit/recover a remote action and poll until execution ends.

        action_id (int) is its positive global ID. Return None when terminal or
        CANCELING, leaving cancellation to _completeCancellation. Transient API
        failures retain state and retry after interruptible poll_freq delays in
        seconds. WorkerInterrupted propagates to the shared worker entry point.
        """
        executing = ActionStatusGroup.EXECUTING.value
        while True:
            # Follow durable execution state; cancellation hands control back
            # to the worker instead of sharing this submission/polling loop.
            status, _ = self.queryRecordedStatus(action_id)
            if status not in executing:
                return
            checkForThreadCurrentWorkerInterruption()
            if status == ActionStatus.SCHEDULING:
                self._reconcileScheduledAction(action_id)
            else:
                self._callApiOperation(
                    action_id, self._pollAction, action_id,
                    failure_message=f'Status poll failed for action {action_id}; retaining cached state',
                )

            # queryRecordedStatus reads SQLite only, but each ACTIVE/PROVIDER_HELD iteration above
            # calls _pollAction and contacts the provider. This delay therefore
            # controls API polling too; the 0.01s floor prevents a tight loop
            # when poll_freq is zero, rather than defining a separate cache poll.
            status, _ = self.queryRecordedStatus(action_id)
            if status in executing:
                delay = None if math.isinf(self.poll_freq) else max(.01, self.poll_freq)
                sleepCurrentThreadWorker(delay)

    def _completeCancellation(self, action_id: int) -> None:
        """Reconcile remote cancellation until its recorded intent is resolved.

        action_id (int) is the positive global ID of an action in CANCELING.
        Return None once resolved. Recover uncertain submission acceptance before
        canceling, and retry provider failures after poll_freq seconds. Shutdown
        raises WorkerInterrupted and leaves intent for a subsequent recovery.
        """
        while self.queryRecordedStatus(action_id)[0] == ActionStatus.CANCELING:
            # Resolve remote state before repeating a potentially accepted cancel.
            checkForThreadCurrentWorkerInterruption()
            self._callApiOperation(
                action_id, self._reconcileCancelingAction, action_id,
                failure_message=f'Cancellation failed for action {action_id}; retaining CANCELING',
            )

            # Reconciliation may query/cancel remote work on every attempt.
            # Delay those API retries during outages without busy waiting.
            if self.queryRecordedStatus(action_id)[0] == ActionStatus.CANCELING:
                delay = None if math.isinf(self.poll_freq) else max(.01, self.poll_freq)
                sleepCurrentThreadWorker(delay)

    def _pollAction(self, action_id: int) -> None:
        """Read provider status for positive action_id and persist it; return None.

        Release the database lock before querying. A concurrent cancellation
        intent wins over the returned poll result.
        """
        with self.conn as conn:
            action = conn.execute(f'SELECT machine, api_key, action_status FROM {self.table_name} WHERE action_id = ?',
                                  (action_id,)).fetchone()
        if ActionStatus[action['action_status']] not in ActionStatusGroup.LIVE.value:
            return
        api_status = self._queryStatusInternal(action['machine'], action['api_key'])
        status = self.api_action_status_map[api_status]
        with self.conn as conn:
            conn.execute(f'UPDATE {self.table_name} SET api_status = ?, action_status = ?, last_update = ? '
                         'WHERE action_id = ? AND action_status = ?',
                         (api_status, status.name, int(time.time()), action_id, action['action_status']))

    def _recordWorkerFailure(self, action_id: int, error: Exception) -> None:
        """Record error (Exception) for positive action_id; return None.

        Preserve concurrent CANCELING intent; otherwise an unexpected supervisor
        failure becomes FAILED instead of leaving an apparently live action.
        """
        with self.conn as conn:
            conn.execute(f'UPDATE {self.table_name} SET action_status = ?, last_submission_error = ?, last_update = ? '
                         'WHERE action_id = ? AND action_status != ?',
                         (ActionStatus.FAILED.name, f'{type(error).__name__}: {error}', int(time.time()),
                          action_id, ActionStatus.CANCELING.name))

    def updateStatuses(self):
        """Take no arguments; wake ACTIVE and PROVIDER_HELD supervisors to poll, returning None."""
        with self.conn as conn:
            actions = conn.execute(f'SELECT action_id FROM {self.table_name} WHERE action_status IN '
                                   "(SELECT action_status FROM action_status_groups WHERE group_name = 'LIVE')").fetchall()
        for action in actions:
            self.queryRecordedStatus(action['action_id'], force_update=True)

    def __str__(self):
        with self.conn as conn:
            actions = conn.execute(f"SELECT api_key, action_status, api_status, last_update FROM {self.table_name}").fetchall()
            out = ""
            for t in actions:
                out = out + f"({t['api_key']},{t['action_status']}[ {t['api_status']} ],{t['last_update']})\n"
            return out
                    
            
    def queryRecordedStatus(self, action_id, *, force_update=False):
        """Read recorded action status and optionally request an early API poll.

        action_id (int) is the positive global action ID. force_update (bool,
        default False) wakes an existing worker to poll the provider, without
        waiting. Return (ActionStatus, provider str | None) from the database;
        the requested poll may finish later. This call does not change the
        configured polling interval or start/recover a worker.
        """
        if force_update:
            with self._worker_lock:
                worker = self._action_workers.get(action_id)
                if worker is not None:
                    worker.wake.set()
        with self.conn as conn:
            action = conn.execute(f'SELECT action_status, api_status FROM {self.table_name} WHERE action_id = ?',
                                  (action_id,)).fetchone()
        if action is None:
            raise ValueError(f'Unknown action_id: {action_id}')
        return ActionStatus[action['action_status']], action['api_status']

    def getLiveActions(self):
        """List LIVE actions (ACTIVE and PROVIDER_HELD) without contacting providers.

        Take no arguments. Return list[dict] with job_id (int workflow ID),
        api_key (str | None provider ID), api_status (str | None raw state),
        and the action definition's getInfo fields.
        """
        out = []
        with self.conn as conn:
            entries = conn.execute(f'SELECT job_id, api_key, api_status, details FROM {self.table_name} WHERE action_status IN '
                                   "(SELECT action_status FROM action_status_groups WHERE group_name = 'LIVE')").fetchall()
            for entry in entries:
                action = _unser(entry['details'])
                dc = action.getInfo()
                dc['job_id'] = entry['job_id']
                dc['api_key'] = entry['api_key']
                dc['api_status'] = entry['api_status']
                out.append(dc)
        return out

    def getActionInfo(self, action_id)->dict:
        """
        For the given action, return a dictionary containing "api_key", "api_status" and other custom fields defined on a per-action basis
        """        
        with self.conn as conn:
            entry = conn.execute(f"SELECT job_id, details, api_status, api_key FROM {self.table_name} WHERE action_id = ?", (action_id,) ).fetchone()
            action = _unser(entry['details'])
            dc = action.getInfo()
            dc['job_id'] = entry['job_id']
            dc['api_key'] = entry['api_key']
            dc['api_status'] = entry['api_status']
            return dc
