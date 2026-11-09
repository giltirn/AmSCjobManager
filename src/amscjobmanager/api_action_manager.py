"""Durable execution of synchronous API operation sequences."""

import sqlite3
import time

from .action_manager import ActionManager, ActionStatus, SimulatedCrash, _ser, _unser
from .api_actions import APIAction
from .logging import wfmanLog


class APIActions(ActionManager):
    """Own API action records and checkpoint each completed operation.

    Execution is synchronous under the caller's manager lock. Interrupted
    operations must be replay-safe; previously checkpointed operations are
    skipped. Ordinary operation failures mark the action FAILED.
    """

    def __init__(self, connection: sqlite3.Connection, *, _test_crash_at: str | None = None):
        """Create the API action table on connection (sqlite3.Connection).

        connection must use sqlite3.Row records and is owned by JobData.
        _test_crash_at (str | None) selects a test-only crash boundary; default
        None disables injection. Return None; no remote API is contacted.
        """
        self.conn = connection
        self._test_crash_at = _test_crash_at
        with self.conn as conn:
            conn.execute('''CREATE TABLE IF NOT EXISTS api_actions (
                action_id INTEGER PRIMARY KEY,
                job_id INTEGER NOT NULL,
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

    def startAction(self, action: APIAction, job_id: int, action_id: int) -> bool:
        """Execute or recover an API sequence under its caller-assigned identity.

        action (APIAction) supplies the definition only for a missing record.
        job_id (int) is the positive workflow ID; action_id (int) is the positive
        global ID always allocated by JobData. Return True when the stored
        status can be read (including failure). Execution is synchronous, so
        this implementation never returns False. Existing terminal records are
        not executed again. Simulated crashes propagate without marking failure.
        """
        # SCHEDULING covers the entire unfinished synchronous sequence, including
        # operations already in progress. JobData keeps the workflow head in
        # SCHEDULING until this call returns, so its existing scheduling recovery
        # calls startAction again after a crash and resumes at next_operation.
        # ACTIVE is reserved here for externally running work that needs polling;
        # API status queries only read records and never execute operations.
        created = False
        with self.conn as conn:
            if not self.hasAction(action_id):
                conn.execute(
                    'INSERT INTO api_actions '
                    '(action_id, job_id, details, action_status, last_update) VALUES (?, ?, ?, ?, ?)',
                    (action_id, job_id, _ser(action), ActionStatus.SCHEDULING.name, int(time.time())),
                )
                created = True
        if created:
            self._maybeSimulateCrash('action_intent_persisted')
        row = self.conn.execute('SELECT * FROM api_actions WHERE action_id = ?', (action_id,)).fetchone()
        if row['action_status'] != ActionStatus.SCHEDULING.name:
            return True

        index = row['next_operation']
        try:
            # Recover the persisted definition, never a caller's amended sequence.
            action = _unser(row['details'])
            if not isinstance(action, APIAction):
                raise TypeError('Stored action must be an APIAction')
            action.__post_init__()
            if not 0 <= index <= len(action.operations):
                raise ValueError(f'Invalid next_operation {index} for {len(action.operations)} operations')
            for index in range(index, len(action.operations)):
                self._maybeSimulateCrash(f'api_operation_{index}_started')
                action.operations[index].execute(row['job_id'])
                self._maybeSimulateCrash(f'api_operation_{index}_completed')
                # Commit effects' checkpoint only after success. The final
                # cursor and terminal status must commit in the same transaction.
                # Keep intermediate checkpoints SCHEDULING so the remaining
                # operations can be resumed through the same recovery path.
                status = (ActionStatus.COMPLETED if index + 1 == len(action.operations)
                          else ActionStatus.SCHEDULING)
                with self.conn as conn:
                    conn.execute('UPDATE api_actions SET next_operation = ?, action_status = ?, '
                                 'last_update = ? WHERE action_id = ?',
                                 (index + 1, status.name, int(time.time()), action_id))
                self._maybeSimulateCrash(f'api_operation_{index}_checkpoint_persisted')
            # Empty sequences also complete without leaving a scheduling record.
            if not action.operations or row['next_operation'] == len(action.operations):
                with self.conn as conn:
                    conn.execute('UPDATE api_actions SET action_status = ?, last_update = ? '
                                 'WHERE action_id = ?',
                                 (ActionStatus.COMPLETED.name, int(time.time()), action_id))
            self._maybeSimulateCrash('api_action_completed')
        except SimulatedCrash:
            raise
        except Exception as exc:
            error = f'{type(exc).__name__}: {exc}'
            wfmanLog(f'API action {action_id} (job {row["job_id"]}), operation {index} failed: {error}')
            with self.conn as conn:
                conn.execute('UPDATE api_actions SET action_status = ?, error = ?, last_update = ? '
                             'WHERE action_id = ?',
                             (ActionStatus.FAILED.name, error, int(time.time()), action_id))
        return True

    def cancelAction(self, action_id: int, *, recovery: tuple[APIAction, int] | None = None) -> int:
        """Persist cancellation without executing or undoing operations.

        action_id (int) is the positive global ID. recovery optionally contains
        the stored APIAction at index 0 and positive workflow ID at index 1, to
        restore a missing record. Return action_id after resolving cancellation.
        Raise ValueError if the record is missing without recovery information.
        A running operation cannot be interrupted through the manager lock.
        """
        # Cancellation has no external side effect to reconcile. Commit CANCELED
        # directly, including when restoring a record missing after a crash.
        with self.conn as conn:
            if recovery is not None and not self.hasAction(action_id):
                action, job_id = recovery
                conn.execute('INSERT INTO api_actions '
                             '(action_id, job_id, details, action_status, last_update) VALUES (?, ?, ?, ?, ?)',
                             (action_id, job_id, _ser(action), ActionStatus.CANCELED.name, int(time.time())))
            else:
                updated = conn.execute(
                    'UPDATE api_actions SET action_status = ?, last_update = ? WHERE action_id = ?',
                    (ActionStatus.CANCELED.name, int(time.time()), action_id),
                ).rowcount
                if not updated:
                    raise ValueError(f'Unknown action_id: {action_id}')
        return action_id

    def hasAction(self, action_id: int) -> bool:
        """Return whether positive int action_id has a stored API action record."""
        return self.conn.execute('SELECT 1 FROM api_actions WHERE action_id = ?', (action_id,)).fetchone() is not None

    def getMaxActionId(self) -> int | None:
        """Take no arguments; return the highest stored int ID, or None if empty."""
        return self.conn.execute('SELECT MAX(action_id) FROM api_actions').fetchone()[0]

    def queryStatus(self, action_id: int, update_freq=30, force_update=False) -> tuple[ActionStatus, None]:
        """Return (ActionStatus, None) for positive integer action_id.

        update_freq (float, default 30 seconds) and force_update (bool, default
        False) are accepted for interface compatibility. No polling or execution
        occurs. Raise ValueError for an unknown action ID.
        """
        row = self.conn.execute('SELECT action_status FROM api_actions WHERE action_id = ?', (action_id,)).fetchone()
        if row is None:
            raise ValueError(f'Unknown action_id: {action_id}')
        return ActionStatus[row['action_status']], None

    def updateStatuses(self) -> None:
        """Take no arguments; return None because there is no external state to poll."""

    def waitForAction(self, action_id: int, check_freq=30) -> ActionStatus:
        """Return the stored ActionStatus for positive integer action_id.

        check_freq (float, default 30 seconds) is unused: operations execute
        synchronously, so no ACTIVE work exists to wait for. Execution/recovery
        is performed by startAction, never by status queries.
        """
        return self.queryStatus(action_id)[0]

    def getActiveActions(self) -> list[dict]:
        """Take no arguments; return [] since synchronous actions never enter ACTIVE."""
        return []

    def getActionInfo(self, action_id: int) -> dict:
        """Return progress/details for positive integer action_id; raise ValueError if unknown.

        The dictionary includes job_id (int), api_key/api_status (both None),
        next_operation (zero-based int, equal to operation_count on completion),
        operation_count (int), operations (list[str] names), and error (str | None).
        """
        row = self.conn.execute('SELECT * FROM api_actions WHERE action_id = ?', (action_id,)).fetchone()
        if row is None:
            raise ValueError(f'Unknown action_id: {action_id}')
        return dict(_unser(row['details']).getInfo(), job_id=row['job_id'], api_key=None,
                    api_status=None, next_operation=row['next_operation'], error=row['error'])
