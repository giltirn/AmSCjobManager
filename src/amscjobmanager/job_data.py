from pathlib import Path
import sqlite3
from .actions_base import ActionClass, actionClass
from .globus_transfer_action import GlobusDataTransfers
from .compute_action_manager import ComputeActions
from .api_action_manager import APIActions
from .database import LockedConnection
from .action_manager import ActionManager, ActionStatus, ActionStatusGroup, SimulatedCrash, initializeActionStatusViews, _ser, _unser
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
        connection.execute('PRAGMA foreign_keys = ON')
        self.conn = LockedConnection(connection)
        initializeActionStatusViews(self.conn)
        
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
            # Workflow records identify the sequential head and retain durable
            # overall workflow status independently of action history.
            conn.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id INTEGER PRIMARY KEY,
                    job_group TEXT,
                    submission_key TEXT UNIQUE,
                    workflow_stage INTEGER NOT NULL,
                    workflow_status TEXT NOT NULL CHECK (workflow_status IN (
                        'PENDING', 'ACTIVE', 'COMPLETED', 'FAILED', 'CANCELING', 'CANCELED')),
                    last_status_change INTEGER NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS actions (
                    action_id INTEGER PRIMARY KEY,
                    job_id INTEGER NOT NULL REFERENCES jobs(job_id),
                    workflow_stage INTEGER NOT NULL CHECK (workflow_stage >= 0),
                    action_class TEXT NOT NULL,
                    action_type TEXT NOT NULL,
                    action_status TEXT NOT NULL,
                    depends_on TEXT,
                    producer_key TEXT,
                    details BLOB NOT NULL,
                    last_status_change INTEGER NOT NULL,
                    UNIQUE (job_id, workflow_stage)
                )
            """)
            conn.execute('CREATE INDEX IF NOT EXISTS actions_by_producer_key ON actions(producer_key)')
            conn.execute('CREATE INDEX IF NOT EXISTS jobs_by_workflow_status ON jobs(workflow_status)')

            # This read-only projection keeps all head lookups consistent without
            # duplicating action metadata or execution status in the jobs table.
            # Stage -1 retains the public convention for a workflow not yet started.
            conn.execute("""
                CREATE VIEW IF NOT EXISTS workflow_heads AS
                SELECT j.job_id, j.job_group, j.submission_key, j.workflow_stage,
                       j.workflow_status,
                       MAX(j.last_status_change, COALESCE(a.last_status_change, 0)) AS last_status_change,
                       a.action_id AS head_action_id,
                       COALESCE(a.action_class, 'NONE') AS head_action_class,
                       COALESCE(a.action_type, 'NoneType') AS head_action_type,
                       COALESCE(a.action_status, 'COMPLETED') AS head_action_status,
                       a.details AS action_details
                FROM jobs j LEFT JOIN actions a ON a.job_id = j.job_id
                    AND a.workflow_stage = CASE WHEN j.workflow_stage = -1 THEN 0 ELSE j.workflow_stage END
            """)

        # Only current unfinished heads recover execution workers. Queued action
        # records already have permanent IDs, but must not start during recovery.
        try:
            self.resumeActions()
        except BaseException:
            self.stopWorkers()
            self.conn.close()
            raise

    def _maybeSimulateCrash(self, point):
        """Raise SimulatedCrash if point (str) matches the test hook; otherwise return None."""
        if point == self._test_crash_at:
            raise SimulatedCrash(point)
       
    def enqueueJob(self, workflow, job_group=None, *, submission_key: str | None = None) -> int:
        """Persist a sequential workflow and assign all its action IDs without starting it.

        workflow (action | nonempty list[action]) supplies ordered definitions;
        job_group (str | None) is optional nonunique metadata. submission_key
        (str | None) is an exact, database-unique retry key. An existing key returns
        its original positive workflow ID without changing any records; None
        always creates a workflow. Return int, the workflow ID, not a stage index.
        Each action receives a distinct positive actions-table primary key.
        Dependency metadata is stored but is not yet evaluated or checked against
        producers. Use JobManager's lock while its background thread is running.
        """
        if submission_key is not None and not isinstance(submission_key, str):
            raise TypeError('submission_key must be a string or None')

        with self.conn as conn:
            # Keep retry handling ahead of payload validation and insert the whole
            # chain atomically, including IDs for stages that may never execute.
            if submission_key is not None:
                existing = conn.execute('SELECT job_id FROM jobs WHERE submission_key = ?',
                                        (submission_key,)).fetchone()
                if existing is not None:
                    return existing['job_id']
            if not isinstance(workflow, list):
                workflow = [workflow]
            assert len(workflow) > 0
            now = int(time.time())
            job_id = conn.execute(
                'INSERT INTO jobs(job_group, submission_key, workflow_stage, workflow_status, last_status_change) '
                'VALUES (?, ?, -1, ?, ?)',
                (job_group, submission_key, ActionStatus.PENDING.name, now),
            ).lastrowid
            for stage, action in enumerate(workflow):
                condition = getattr(action, 'depends_on', None)
                conn.execute(
                    'INSERT INTO actions(job_id, workflow_stage, action_class, action_type, action_status, '
                    'depends_on, producer_key, details, last_status_change) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    (job_id, stage, actionClass(action).name, type(action).__name__, ActionStatus.PENDING.name,
                     None if condition is None else json.dumps(condition), getattr(action, 'producer_key', None),
                     _ser(action), now),
                )
            return job_id

    def cancelJob(self, job_id: int, cancel_running: bool = True) -> None:
        """Cancel a workflow while retaining its action definitions and completed history.

        job_id (positive int) is a workflow database ID. cancel_running (bool,
        default True) also requests cancellation of a scheduling, active, or
        provider-held head. False abandons that execution without stopping it.
        Return None. Persist intent before external operations; recovery retries
        unfinished cancellation. Unknown IDs raise ValueError. Access under the
        JobManager lock when running. Dependency cascades are not implemented.
        """
        if type(job_id) is not int or job_id < 1:
            raise ValueError('job_id must be a positive integer')
        if type(cancel_running) is not bool:
            raise TypeError('cancel_running must be a boolean')

        with self.conn as conn:
            job = conn.execute('SELECT * FROM workflow_heads WHERE job_id = ?', (job_id,)).fetchone()
            if job is None:
                raise ValueError(f'Unknown job_id: {job_id}')
            if job['workflow_status'] == ActionStatus.CANCELED.name:
                return
            already_canceling = job['workflow_status'] == ActionStatus.CANCELING.name
            needs_cancellation = ActionStatus[job['head_action_status']] in ActionStatusGroup.EXECUTING.value
            remote_cancel = already_canceling or (cancel_running and needs_cancellation)

            # Record chain-wide intent and close queued stages before releasing
            # the lock. Preserve genuinely completed stages, including the head.
            status = ActionStatus.CANCELING if remote_cancel else ActionStatus.CANCELED
            now = int(time.time())
            conn.execute('UPDATE jobs SET workflow_status = ?, last_status_change = ? WHERE job_id = ?',
                         (status.name, now, job_id))
            conn.execute('UPDATE actions SET action_status = ?, last_status_change = ? '
                         'WHERE job_id = ? AND action_status = ?',
                         (ActionStatus.CANCELED.name, now, job_id, ActionStatus.PENDING.name))
            if job['head_action_id'] is not None and (needs_cancellation or already_canceling):
                conn.execute('UPDATE actions SET action_status = ?, last_status_change = ? WHERE action_id = ?',
                             (status.name, now, job['head_action_id']))

        if remote_cancel:
            self._maybeSimulateCrash('job_head_canceling_persisted')
            self.reconcileCancelingActions([job_id], raise_errors=True)

    def reconcileCancelingActions(self, job_ids=None, *, raise_errors=False):
        """Finish durable workflow cancellation through the head's action manager.

        job_ids (list[int] | None) restricts positive workflow IDs; None selects
        every CANCELING workflow. raise_errors (bool, default False) propagates
        API errors instead of logging and retrying later. Return None. Completed
        historical actions remain unchanged; dependency propagation is disabled.
        """
        with self.conn as conn:
            query = 'SELECT * FROM workflow_heads WHERE workflow_status = ?'
            params = [ActionStatus.CANCELING.name]
            if job_ids is not None:
                query += ' AND job_id IN (' + ','.join('?' for _ in job_ids) + ')'
                params.extend(job_ids)
            jobs = conn.execute(query, params).fetchall()

        for job in jobs:
            try:
                # The definition and preallocated ID suffice even if a crash
                # occurred before the action manager created its own record.
                action_id = job['head_action_id']
                if action_id is not None:
                    manager = self.action_man[ActionClass[job['head_action_class']]]
                    manager.cancelOrResumeCancelingAction(
                        action_id, recovery=(_unser(job['action_details']), job['job_id']))
                    status, _ = manager.queryRecordedStatus(action_id)
                    if status in ActionStatusGroup.EXECUTING.value or status == ActionStatus.CANCELING:
                        continue

                self._maybeSimulateCrash('action_canceled_before_job_head_update')
                with self.conn as conn:
                    conn.execute('UPDATE actions SET action_status = ?, last_status_change = ? '
                                 'WHERE action_id = ? AND action_status = ?',
                                 (ActionStatus.CANCELED.name, int(time.time()), action_id, ActionStatus.CANCELING.name))
                    conn.execute('UPDATE jobs SET workflow_status = ?, last_status_change = ? WHERE job_id = ?',
                                 (ActionStatus.CANCELED.name, int(time.time()), job['job_id']))
            except SimulatedCrash:
                raise
            except Exception as exc:
                if raise_errors:
                    raise
                wfmanLog(f"Could not reconcile cancellation for job {job['job_id']}: {exc}")

    def jobState(self, job_id):
        """Read a workflow and derive its head fields from the ordered action rows.

        job_id (int) is a positive workflow database ID. Return dict with the
        fields documented by listWorkflows; unknown IDs raise ValueError. Return
        only workflow/head metadata, without loading serialized action definitions.
        No execution or API poll occurs.
        """
        with self.conn as conn:
            result = conn.execute('SELECT job_id, job_group, submission_key, workflow_stage, workflow_status, '
                                  'last_status_change, head_action_id, head_action_class, head_action_type, '
                                  'head_action_status FROM workflow_heads WHERE job_id = ?', (job_id,)).fetchone()
            if result is None:
                raise ValueError(f'Workflow {job_id} does not exist')
            row = dict(result)
            row['workflow_status'] = ActionStatus[row['workflow_status']]
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
                workflow_status (ActionStatus): Persisted overall status; ACTIVE
                    includes scheduling, provider-held, and completed intermediate heads.
                workflow_stage (int): Zero-based action position, -1 before
                    the first action, or the action count after full completion.
                head_action_id (int | None): Positive action database key,
                    assigned at enqueue, independent of job_id; None after full completion.
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
            # Filter by the persisted overall status in SQL, retaining the error
            # for an unknown explicit ID even when a status filter is supplied.
            if job_id is not None and conn.execute('SELECT 1 FROM jobs WHERE job_id = ?', (job_id,)).fetchone() is None:
                raise ValueError(f'Workflow {job_id} does not exist')
            query = 'SELECT job_id FROM jobs'
            has_where = False
            params = []
            if job_id is not None:
                query += (' AND ' if has_where else ' WHERE ') + 'job_id = ?'
                has_where = True
                params.append(job_id)
            if statuses is not None:
                query += (' AND ' if has_where else ' WHERE ') + 'workflow_status IN (' + ','.join('?' for _ in statuses) + ')'
                params.extend(status.name for status in statuses)
            rows = conn.execute(query + ' ORDER BY job_id', params).fetchall()
            return [self.jobState(row['job_id']) for row in rows]

    def headActionStatus(self, job_id, *, force_poll=False, return_api_status=False):
        """Return the head action's ActionStatus, optionally with its API status.

        job_id (int) identifies a workflow database row. force_poll (bool,
        default False) requests earlier API polls for all ACTIVE/PROVIDER_HELD heads and copies
        their currently recorded statuses without waiting for provider replies.
        This does not submit actions or advance workflow steps. Poll failures
        are logged and leave the last known status in place.

        By default return the recorded head ActionStatus, including SCHEDULING
        and intermediate COMPLETED states. If return_api_status (bool, default
        False) is True, return
        (action_status, api_status), where api_status is the provider's status
        string (for example 'queued'), or None before execution begins, before a remote status is known
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
        if (state['head_action_class'] != ActionClass.NONE and state['head_action_id'] is not None
                and self.action_man[state['head_action_class']].hasAction(state['head_action_id'])):
            # Read the cached provider status without causing a second poll.
            _, api_status = self.action_man[state['head_action_class']].queryRecordedStatus(
                state['head_action_id']
            )
        return action_status, api_status

    def jobStatus(self, job_id: int) -> ActionStatus:
        """Read the persisted overall status of a workflow without polling its head.

        job_id (int) is a positive workflow database ID, not a stage position.
        Return ActionStatus; unknown IDs raise ValueError. ACTIVE includes
        scheduling, provider-held, and completed intermediate heads. No workflow advancement,
        action deserialization, or API request occurs.
        """
        with self.conn as conn:
            row = conn.execute('SELECT workflow_status FROM jobs WHERE job_id = ?', (job_id,)).fetchone()
            if row is None:
                raise ValueError(f'Workflow {job_id} does not exist')
            return ActionStatus[row['workflow_status']]

    def _recordActionStatuses(self, updates: list[tuple[str, int, int]],
                              expected_status: ActionStatus | tuple[ActionStatus, ...]) -> None:
        """Atomically copy execution results into action rows and overall workflow status.

        updates (list[tuple[str, int, int]]) contains action-status enum names,
        Unix timestamps in seconds, and positive action IDs, respectively.
        expected_status (ActionStatus | tuple[ActionStatus, ...]) lists allowed
        prior recorded action states; a single state is also accepted.
        Return None. Ignore stale action updates, preserve workflow cancellation
        and other terminal outcomes, and only update the workflow's current head.
        SCHEDULING, ACTIVE, PROVIDER_HELD and COMPLETED heads correspond to ACTIVE
        workflows until progression advances beyond the final stage.
        """
        expected = (expected_status,) if isinstance(expected_status, ActionStatus) else expected_status
        with self.conn as conn:
            for status, timestamp, action_id in updates:
                changed = conn.execute('UPDATE actions SET action_status = ?, last_status_change = ? '
                                       'WHERE action_id = ? AND action_status IN (' + ','.join('?' for _ in expected) + ')',
                                       (status, timestamp, action_id, *(item.name for item in expected)))
                if changed.rowcount == 0:
                    continue

                # Keep the execution projection and overall outcome in one
                # transaction. A late poll must never clear cancellation intent.
                conn.execute('UPDATE jobs SET workflow_status = '
                             '(SELECT workflow_status FROM action_status_mapping WHERE action_status = ?), '
                             'last_status_change = ? '
                             'WHERE (job_id, workflow_stage) = '
                             '(SELECT job_id, workflow_stage FROM actions WHERE action_id = ?) '
                             'AND workflow_status IN (?, ?)',
                             (status, timestamp, action_id, ActionStatus.PENDING.name, ActionStatus.ACTIVE.name))

    def progressWorkflows(self, condition: Tuple[str, list | None]):
        """Start or advance selected sequential workflows using their stored actions.

        condition is ('COMPLETE', None) to advance completed heads, or
        ('VALID_IN', list[int]) to start/advance specified workflow database IDs.
        Return None. Persist SCHEDULING on the selected action before dispatch;
        retain prior statuses and preallocated IDs. Dependency fields are ignored.
        """
        pending_actions = []
        with self.conn as conn:
            # Select current heads without consulting any action-manager tables.
            query = 'SELECT * FROM workflow_heads WHERE head_action_class != ? AND workflow_status IN (?, ?)'
            params = [ActionClass.NONE.name, ActionStatus.PENDING.name, ActionStatus.ACTIVE.name]
            if condition == ('COMPLETE', None):
                query += ' AND head_action_status = ?'
                params.append(ActionStatus.COMPLETED.name)
            elif condition[0] == 'VALID_IN' and isinstance(condition[1], list):
                query += ' AND job_id IN (' + ','.join('?' for _ in condition[1]) + ')'
                query += ' AND head_action_status IN (?, ?)'
                params.extend([*condition[1], ActionStatus.PENDING.name, ActionStatus.COMPLETED.name])
            else:
                raise ValueError(f'Unknown condition {condition}')
            jobs = conn.execute(query, params).fetchall()

            for job in jobs:
                # PENDING selects its current action; COMPLETED advances once.
                # The -1 pre-start convention becomes stage zero on first start.
                stage = max(job['workflow_stage'], 0)
                if job['head_action_status'] == ActionStatus.COMPLETED.name:
                    stage += 1
                action = conn.execute('SELECT * FROM actions WHERE job_id = ? AND workflow_stage = ?',
                                      (job['job_id'], stage)).fetchone()
                now = int(time.time())
                workflow_status = ActionStatus.COMPLETED if action is None else ActionStatus.ACTIVE
                conn.execute('UPDATE jobs SET workflow_stage = ?, workflow_status = ?, last_status_change = ? WHERE job_id = ?',
                             (stage, workflow_status.name, now, job['job_id']))
                if action is None:
                    continue
                conn.execute('UPDATE actions SET action_status = ?, last_status_change = ? WHERE action_id = ?',
                             (ActionStatus.SCHEDULING.name, now, action['action_id']))
                pending_actions.append((ActionClass[action['action_class']], _unser(action['details']),
                                        job['job_id'], action['action_id']))

        self._maybeSimulateCrash('job_head_scheduling_persisted')

        # Hand off the same durable IDs on initial submission and crash recovery.
        updates = []
        for action_class, action, job_id, action_id in pending_actions:
            wfmanLog(f'Initiating action of type {action_class.name} for {job_id}')
            manager = self.action_man[action_class]
            manager.startOrResumeAction(action, job_id, action_id)
            try:
                status, _ = manager.queryRecordedStatus(action_id)
            except SimulatedCrash:
                raise
            except Exception as exc:
                wfmanLog(f'Could not poll action {action_id} for job {job_id}: {exc}')
                continue
            updates.append((status.name, int(time.time()), action_id))
        self._maybeSimulateCrash('action_state_persisted_before_job_head_update')
        self._recordActionStatuses(updates, ActionStatus.SCHEDULING)

    def startWorkflows(self, job_ids: list | None = None):
        """Start selected pending workflows or automatically fill available capacity.

        job_ids (list[int] | None) contains workflow database IDs. None starts
        pending workflows in ID order up to max_workflows_active; an explicit
        list bypasses that cap. Return None. Recover scheduling heads first.
        Dependency conditions are not yet checked.
        """
        self.reconcileSchedulingActions()
        if job_ids is None:
            with self.conn as conn:
                count = conn.execute('SELECT COUNT(*) FROM jobs WHERE workflow_status IN (?, ?)',
                                     (ActionStatus.ACTIVE.name, ActionStatus.CANCELING.name)).fetchone()[0]
                remaining = max(0, self.max_workflows_active - count)
                rows = conn.execute('SELECT job_id FROM jobs WHERE workflow_status = ? '
                                    'ORDER BY job_id LIMIT ?', (ActionStatus.PENDING.name, remaining)).fetchall()
                job_ids = [row['job_id'] for row in rows]
        if job_ids:
            self.progressWorkflows(('VALID_IN', job_ids))

    def progressActiveWorkflows(self):
        """Advance successfully completed heads to their next stage or workflow completion.

        Take no arguments; return None. Earlier action rows and their completion
        statuses remain available after the head advances.
        """
        self.progressWorkflows(('COMPLETE', None))

    def reconcileSchedulingActions(self):
        """Recover SCHEDULING heads and copy their managers' recorded execution status.

        Take no arguments; return None. Reuse existing workers or restore them
        after a crash using the action row's original ID and definition.
        """
        with self.conn as conn:
            jobs = conn.execute('SELECT * FROM workflow_heads WHERE head_action_status = ? AND workflow_status = ?',
                                (ActionStatus.SCHEDULING.name, ActionStatus.ACTIVE.name)).fetchall()
        updates = []
        for job in jobs:
            manager = self.action_man[ActionClass[job['head_action_class']]]
            action_id = job['head_action_id']
            manager.startOrResumeAction(_unser(job['action_details']), job['job_id'], action_id)
            try:
                status, _ = manager.queryRecordedStatus(action_id)
            except SimulatedCrash:
                raise
            except Exception as exc:
                wfmanLog(f"Could not read action {action_id} for job {job['job_id']}: {exc}")
                continue
            updates.append((status.name, int(time.time()), action_id))
        self._recordActionStatuses(updates, ActionStatus.SCHEDULING)

    def progressActiveActions(self, force_poll=False):
        """Copy recorded execution states into ACTIVE or PROVIDER_HELD workflow heads.

        force_poll (bool, default False) requests earlier supervisor API polls
        without waiting. Return dict[int, ActionStatus] mapping changed workflow
        IDs to their new head statuses. Log read failures and retain cached state.
        """
        with self.conn as conn:
            jobs = conn.execute('SELECT * FROM workflow_heads WHERE head_action_status IN '
                                "(SELECT action_status FROM action_status_groups WHERE group_name = 'LIVE') "
                                'AND workflow_status = ?', (ActionStatus.ACTIVE.name,)).fetchall()
        updates = {}
        recorded = []
        for job in jobs:
            manager = self.action_man[ActionClass[job['head_action_class']]]
            try:
                status, _ = manager.queryRecordedStatus(job['head_action_id'], force_update=force_poll)
            except SimulatedCrash:
                raise
            except Exception as exc:
                wfmanLog(f"Could not poll action {job['head_action_id']} for job {job['job_id']}: {exc}")
                continue
            if status.name != job['head_action_status']:
                updates[job['job_id']] = status
                recorded.append((status.name, int(time.time()), job['head_action_id']))
                wfmanLog(f"Progressed job {job['job_id']} action {job['head_action_type']} to {status.name}")
        self._recordActionStatuses(recorded, ActionStatusGroup.LIVE.value)
        return updates

    def progressActiveState(self, force_poll=False):
        """Observe action managers and advance completed workflow stages.

        force_poll (bool, default False) requests earlier polls without waiting.
        Return None. Existing workers progress independently of these calls.
        """
        self.reconcileCancelingActions()
        self.reconcileSchedulingActions()
        self.progressActiveActions(force_poll=force_poll)
        self.progressActiveWorkflows()

    def countWorkflowsWithStatus(self, statuses: ActionStatus | list[ActionStatus]):
        """Count workflows matching any supplied persisted overall status.

        statuses (ActionStatus | list[ActionStatus]) selects overall workflow
        states. [] selects none. Return int including terminal records.
        SCHEDULING matches none because submitting workflows are ACTIVE.
        """
        if isinstance(statuses, ActionStatus):
            statuses = [statuses]
        elif not isinstance(statuses, list):
            raise TypeError('statuses must be an ActionStatus or list of ActionStatus')
        with self.conn as conn:
            return conn.execute('SELECT COUNT(*) FROM jobs WHERE workflow_status IN (' +
                                ','.join('?' for _ in statuses) + ')',
                                tuple(status.name for status in statuses)).fetchone()[0]

    def getLiveActions(self, action_class: ActionClass) -> list[dict]:
        """List provider-owned LIVE actions of the selected category without polling.

        action_class (ActionClass) selects COMPUTE, TRANSFER, or API. Return
        list[dict] for ACTIVE/PROVIDER_HELD actions, containing job_id (positive
        workflow ID), api_key (str | None provider ID), api_status (str | None),
        and action-specific getInfo fields. API returns [] because its unfinished
        operations are SCHEDULING, outside LIVE. List positions are not action IDs.
        """
        return self.action_man[action_class].getLiveActions()

    def stopWorkers(self) -> None:
        """Take no arguments; signal all action managers' workers and return None.

        Does not cancel accepted remote jobs or alter durable workflow state.
        """
        for manager in self.action_man.values():
            manager.stopWorkers()

    def resumeActions(self) -> None:
        """Restore execution supervision for current unfinished heads after reopening.

        Take no arguments; return None. Restore SCHEDULING, ACTIVE, PROVIDER_HELD,
        and CANCELING heads using their existing IDs. Do not start pending stages
        or restart historical actions abandoned with cancel_running=False.
        """
        with self.conn as conn:
            jobs = conn.execute('SELECT * FROM workflow_heads WHERE workflow_status = ? OR '
                                '(workflow_status = ? AND (head_action_status IN '
                                "(SELECT action_status FROM action_status_groups WHERE group_name = 'EXECUTING') "
                                'OR head_action_status = ?))',
                                (ActionStatus.CANCELING.name, ActionStatus.ACTIVE.name,
                                 ActionStatus.CANCELING.name)).fetchall()
        for job in jobs:
            action = _unser(job['action_details'])
            manager = self.action_man[ActionClass[job['head_action_class']]]
            if job['workflow_status'] == ActionStatus.CANCELING.name or job['head_action_status'] == ActionStatus.CANCELING.name:
                manager.cancelOrResumeCancelingAction(job['head_action_id'], recovery=(action, job['job_id']))
            else:
                manager.startOrResumeAction(action, job['job_id'], job['head_action_id'])
