"""Durable execution of synchronous API operation sequences."""

import time

from .action_manager import ActionManager, ActionStatus, SimulatedCrash, initializeActionStatusViews, _ser, _unser
from .api_actions import APIAction
from .logging import wfmanLog
from .database import DatabaseClosed, LockedConnection
from .workers import Workers, WorkerInterrupted, checkForThreadCurrentWorkerInterruption


class APIActions(ActionManager):
    """Own API action records and checkpoint each completed operation.

    Operations run on workers outside the caller's manager lock. Interrupted
    operations must be replay-safe; previously checkpointed operations are
    skipped. Ordinary operation failures mark the action FAILED.
    """

    def __init__(self, connection: LockedConnection, *, _test_crash_at: str | None = None,
                 workers: Workers | None = None):
        """Create API action storage and shared status views on connection (LockedConnection).

        connection yields a SQLite connection with sqlite3.Row records and is
        owned by WorkflowData. All SQL access stays inside its context blocks.
        workers (Workers | None) supplies execution, default a new registry.
        _test_crash_at (str | None) selects a test-only crash boundary; default
        None disables injection. Return None; no remote API is contacted.
        """
        super().__init__(workers=workers)
        self.conn = connection
        initializeActionStatusViews(self.conn)
        self._test_crash_at = _test_crash_at
        with self.conn as conn:
            conn.execute('''CREATE TABLE IF NOT EXISTS api_actions (
                action_id INTEGER PRIMARY KEY,
                workflow_id INTEGER NOT NULL,
                details BLOB NOT NULL,
                action_status TEXT NOT NULL,
                next_operation INTEGER NOT NULL DEFAULT 0
                    CHECK(typeof(next_operation) = 'integer' AND next_operation >= 0),
                last_update INTEGER NOT NULL,
                error TEXT
            )''')

    def _maybeSimulateCrash(self, point: str) -> None:
        """Raise SimulatedCrash when point (str) matches the test hook; otherwise return None."""
        if point == self._test_crash_at:
            raise SimulatedCrash(point)

    def _recordAction(self, action: APIAction, workflow_id: int, action_id: int, status: ActionStatus) -> bool:
        """Persist a missing API record, retaining existing records unchanged.

        action (APIAction) is the stored definition; workflow_id/action_id (positive
        ints) identify workflow/action; status (ActionStatus) is the initial
        intent. Return True for a new record, False if it already exists.
        """
        with self.conn as conn:
            if self.hasAction(action_id):
                return False
            conn.execute('INSERT INTO api_actions '
                         '(action_id, workflow_id, details, action_status, last_update) VALUES (?, ?, ?, ?, ?)',
                         (action_id, workflow_id, _ser(action), status.name, int(time.time())))
        return True

    def startOrResumeAction(self, action: APIAction, workflow_id: int, action_id: int) -> None:
        """Start an API sequence or recover execution from its saved checkpoint.

        action supplies the APIAction definition for a missing record. workflow_id
        and action_id (positive ints) are assigned by WorkflowData. Existing terminal
        records are retained. Resumption is recovery after the previous worker
        stopped: unfinished work continues from its saved cursor. A live worker
        is reused and executes the sequence without repeated calls.
        Return None without waiting for the sequence or cancellation to finish.
        """
        if self._recordAction(action, workflow_id, action_id, ActionStatus.SCHEDULING):
            self._maybeSimulateCrash('action_intent_persisted')
        status, _ = self.queryRecordedStatus(action_id)
        if status == ActionStatus.CANCELING:
            self._startCancellationWorker(action_id, complete_cancellation=self._completeCancellation)
        elif status == ActionStatus.SCHEDULING:
            self._startExecutionWorker(
                action_id, execute_action=self._executeAction,
                complete_cancellation=self._completeCancellation,
            )

    def _executeAction(self, action_id: int) -> None:
        """Execute remaining operations sequentially for positive action_id; return None.

        The worker owns this call stack across user queries. It commits a
        cursor after each success without holding a database lock during the
        operation. Recovery replays the unfinished operation after a crash.
        A recorded CANCELING intent returns without replay; WorkerInterrupted
        propagates out of an interrupted call before _completeCancellation starts.
        """
        with self.conn as conn:
            row = conn.execute('SELECT * FROM api_actions WHERE action_id = ?', (action_id,)).fetchone()
        if row['action_status'] != ActionStatus.SCHEDULING.name:
            return

        index = row['next_operation']
        try:
            # Recover the persisted definition, never a caller's amended sequence.
            action = _unser(row['details'])
            if not isinstance(action, APIAction):
                raise TypeError('Stored action must be an APIAction')
            if not 0 <= index <= len(action.operations):
                raise ValueError(f'Invalid next_operation {index} for {len(action.operations)} operations')
            for index in range(index, len(action.operations)):
                # Keep the entire remaining sequence on this worker's
                # stack; another workflow can progress while an operation waits.
                checkForThreadCurrentWorkerInterruption()
                self._maybeSimulateCrash(f'api_operation_{index}_started')
                action.operations[index].execute(row['workflow_id'])
                self._maybeSimulateCrash(f'api_operation_{index}_completed')
                # Commit effects' checkpoint only after success. The final
                # cursor and terminal status must commit in the same transaction.
                # Keep intermediate checkpoints SCHEDULING so the remaining
                # operations can be resumed through the same recovery path.
                status = (ActionStatus.COMPLETED if index + 1 == len(action.operations)
                          else ActionStatus.SCHEDULING)
                with self.conn as conn:
                    conn.execute("UPDATE api_actions SET next_operation = ?, action_status = CASE WHEN action_status = 'CANCELING' THEN action_status ELSE ? END, "
                                 'last_update = ? WHERE action_id = ?',
                                 (index + 1, status.name, int(time.time()), action_id))
                self._maybeSimulateCrash(f'api_operation_{index}_checkpoint_persisted')
                checkForThreadCurrentWorkerInterruption()
            # Empty sequences also complete without leaving a scheduling record.
            if not action.operations or row['next_operation'] == len(action.operations):
                with self.conn as conn:
                    conn.execute('UPDATE api_actions SET action_status = ?, last_update = ? '
                                 'WHERE action_id = ? AND action_status = ?',
                                 (ActionStatus.COMPLETED.name, int(time.time()), action_id, ActionStatus.SCHEDULING.name))
            self._maybeSimulateCrash('api_action_completed')
        except (WorkerInterrupted, SimulatedCrash, DatabaseClosed):
            raise
        except Exception as exc:
            error = f'{type(exc).__name__}: {exc}'
            wfmanLog(f'API action {action_id} (workflow {row["workflow_id"]}), operation {index} failed: {error}')
            self._recordWorkerFailure(action_id, exc)

    def cancelOrResumeCancelingAction(self, action_id: int, *, recovery: tuple[APIAction, int] | None = None) -> None:
        """Request API-sequence cancellation or recover its pending completion.

        action_id is a positive int. recovery optionally supplies the stored
        APIAction at index 0 and its workflow ID at index 1. Without a live
        operation, mark CANCELED directly. A live sequence first gets CANCELING
        intent and a cooperative interrupt; earlier file effects remain.
        Return None without waiting; an existing cancellation worker is reused.
        """
        if recovery is not None:
            action, workflow_id = recovery
            self._recordAction(action, workflow_id, action_id, ActionStatus.CANCELED)
        # Record intent and signal under the handoff lock so an interrupt cannot
        # arrive late and abort the worker's cancellation-completion phase.
        with self._worker_lock:
            status, _ = self.queryRecordedStatus(action_id)
            if status == ActionStatus.CANCELED:
                return
            worker = self._action_workers.get(action_id)
            running = worker is not None and not worker.future.done()
            if status != ActionStatus.CANCELING:
                status = ActionStatus.CANCELING if running else ActionStatus.CANCELED
                with self.conn as conn:
                    conn.execute('UPDATE api_actions SET action_status = ?, last_update = ? WHERE action_id = ?',
                                 (status.name, int(time.time()), action_id))
                if running:
                    self.workers.interrupt(action_id)
        if status == ActionStatus.CANCELING:
            self._startCancellationWorker(action_id, complete_cancellation=self._completeCancellation)

    def _completeCancellation(self, action_id: int) -> None:
        """Acknowledge API-sequence cancellation after its operation has finished running.

        action_id (int) is its positive global ID. Return None after changing
        CANCELING to CANCELED. Keep the operation cursor and previous file effects;
        no remote cancellation or replay of the sequence is needed.
        """
        with self.conn as conn:
            conn.execute('UPDATE api_actions SET action_status = ?, last_update = ? '
                         'WHERE action_id = ? AND action_status = ?',
                         (ActionStatus.CANCELED.name, int(time.time()), action_id, ActionStatus.CANCELING.name))

    def _recordWorkerFailure(self, action_id: int, error: Exception) -> None:
        """Record unexpected error for positive action_id; preserve cancellation, return None."""
        with self.conn as conn:
            conn.execute('UPDATE api_actions SET action_status = ?, error = ?, last_update = ? '
                         'WHERE action_id = ? AND action_status != ?',
                         (ActionStatus.FAILED.name, f'{type(error).__name__}: {error}', int(time.time()),
                          action_id, ActionStatus.CANCELING.name))

    def hasAction(self, action_id: int) -> bool:
        """Return whether positive int action_id has a stored API action record."""
        with self.conn as conn:
            return conn.execute('SELECT 1 FROM api_actions WHERE action_id = ?', (action_id,)).fetchone() is not None

    def getMaxActionId(self) -> int | None:
        """Take no arguments; return the highest stored int ID, or None if empty."""
        with self.conn as conn:
            return conn.execute('SELECT MAX(action_id) FROM api_actions').fetchone()[0]

    def queryRecordedStatus(self, action_id: int, *, force_update=False) -> tuple[ActionStatus, None]:
        """Read the recorded state of an API sequence.

        action_id (int) is its positive global ID. Return (ActionStatus, None):
        index 0 is recorded state; index 1 is None because there is no provider
        status for API sequences. force_update (bool, default False) is accepted for interface
        compatibility and ignored. No polling or execution
        occurs. Raise ValueError for an unknown action ID.
        """
        with self.conn as conn:
            row = conn.execute('SELECT action_status FROM api_actions WHERE action_id = ?', (action_id,)).fetchone()
        if row is None:
            raise ValueError(f'Unknown action_id: {action_id}')
        return ActionStatus[row['action_status']], None

    def updateStatuses(self) -> None:
        """Take no arguments; return None because there is no external state to poll."""

    def getLiveActions(self) -> list[dict]:
        """Take no arguments; return [] because API sequences never enter the LIVE group."""
        return []

    def getActionInfo(self, action_id: int) -> dict:
        """Return progress/details for positive integer action_id; raise ValueError if unknown.

        The dictionary includes workflow_id (int), api_key/api_status (both None),
        next_operation (zero-based int, equal to operation_count on completion),
        operation_count (int), operations (list[str] names), and error (str | None).
        """
        with self.conn as conn:
            row = conn.execute('SELECT * FROM api_actions WHERE action_id = ?', (action_id,)).fetchone()
        if row is None:
            raise ValueError(f'Unknown action_id: {action_id}')
        return dict(_unser(row['details']).getInfo(), workflow_id=row['workflow_id'], api_key=None,
                    api_status=None, next_operation=row['next_operation'], error=row['error'])
