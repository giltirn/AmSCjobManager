"""Nonblocking query waits and process-crash recovery through the real managers.

Only provider calls and the user query hook are replaced. Workers use real
threads, checkpoints use SQLite, and crash cases exit a child process while
its worker is alive so no in-memory stack can survive recovery.
"""

import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
from threading import Event, Thread, get_ident
import time
import unittest
from unittest.mock import patch

from amscjobmanager import logging as manager_logging
from amscjobmanager.action_manager import ActionStatus, SimulatedCrash, _ser
from amscjobmanager.actions_base import ActionClass
from amscjobmanager.api_actions import APIAction, WriteFile
from amscjobmanager.compute_actions import ExecuteJobComputeAction
from amscjobmanager.compute_action_manager import ComputeActions
from amscjobmanager.database import LockedConnection
from amscjobmanager.workflow_data import WorkflowData
from amscjobmanager.manager import WorkflowManager
from amscjobmanager.workers import Workers, WorkerInterrupted, getThreadCurrentWorkerInterruptionEvent, clearThreadCurrentWorkerInterruptUnlessStopping


class ActionWorkerTests(unittest.TestCase):
    def setUp(self):
        """Create temporary durable state, capture logs, and initialize events."""
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.entered, self.answer = Event(), Event()
        self.addCleanup(self.answer.set)
        log = patch.object(manager_logging, 'wfman_log_func', lambda *args, **kwargs: None)
        log.start()
        self.addCleanup(log.stop)

    def _ask(self, title, query):
        """Wait for a fake answer to title/query (str); return 'yes' or cancel.

        Honor the worker's interrupt Event, as a future server query hook
        will do without implementing message handling in this test.
        """
        self.entered.set()
        while not self.answer.wait(.01):
            event = getThreadCurrentWorkerInterruptionEvent()
            if event is not None and event.is_set():
                raise WorkerInterrupted('query interrupted')
        return 'yes'

    def _progress_until(self, workflows, condition):
        """Progress workflows (WorkflowData) until condition (zero-argument callable) is true.

        Bound the test to five seconds and return None. Uses normal manager
        passes rather than waiting for worker results on the caller's thread.
        """
        deadline = time.monotonic() + 5
        while not condition():
            if time.monotonic() >= deadline:
                self.fail('Workflow did not reach the expected state')
            workflows.progressActiveState(force_poll=True)
            time.sleep(.005)

    def test_clearing_worker_interrupt_preserves_shutdown_request(self):
        """Distinguish continuing after an interrupt from exiting on shutdown.

        Pause real workers before clearing an interrupt and checking for shutdown. A normal
        interrupt must allow continuation; stop must still interrupt the call.
        Neither operation invokes an API or changes an action record.
        """
        for request in ('interrupt', 'stop'):
            with self.subTest(request=request):
                workers = Workers()
                entered, proceed = Event(), Event()

                def operation():
                    """Wait for a signal, then return 'continued' unless shutdown interrupts."""
                    entered.set()
                    if not proceed.wait(2):
                        raise TimeoutError('Test did not release the worker')
                    clearThreadCurrentWorkerInterruptUnlessStopping()
                    return 'continued'

                try:
                    worker = workers.start('operation', operation)
                    self.assertTrue(entered.wait(1))
                    if request == 'interrupt':
                        workers.interrupt('operation')
                    else:
                        workers.stop()
                    proceed.set()
                    if request == 'interrupt':
                        self.assertEqual(worker.future.result(timeout=2), 'continued')
                    else:
                        with self.assertRaises(WorkerInterrupted):
                            worker.future.result(timeout=2)
                finally:
                    proceed.set()
                    workers.stop()

    def test_api_sequence_finishes_without_workflow_data_progress_calls(self):
        """Start once, then observe autonomous API execution through recorded status.

        WorkflowData's workflow head stays cached until its next pass; the supervisor
        must write both files and its final checkpoint without that pass.
        """
        workflows = WorkflowData(self.root / 'autonomous-api.sqlite', poll_freq=.01)
        try:
            first, second = self.root / 'first', self.root / 'second'
            workflow_id = workflows.enqueueWorkflow(APIAction([WriteFile('local', str(first), '1'),
                                               WriteFile('local', str(second), '2')]))
            workflows.startWorkflows([workflow_id])
            manager = workflows.action_man[ActionClass.API]
            action_id = workflows.workflowState(workflow_id)['head_action_id']
            self.assertEqual(manager.waitForAction(action_id, check_freq=.005), ActionStatus.COMPLETED)
            self.assertEqual((first.read_text(), second.read_text()), ('1', '2'))
            self.assertEqual(manager.getActionInfo(action_id)['next_operation'], 2)
            workflows.progressActiveState()
            self.assertEqual(workflows.workflowStatus(workflow_id), ActionStatus.COMPLETED)
        finally:
            workflows.stopWorkers()
            workflows.conn.close()

    def test_compute_supervisor_polls_to_completion_without_workflow_data_calls(self):
        """Wait for resource availability, then submit/poll without workflow passes.

        Cached status reads only observe the supervisor. A final WorkflowData pass
        propagates the terminal action state to the workflow record.
        """
        workflows = WorkflowData(self.root / 'autonomous-compute.sqlite', poll_freq=.01)
        action = ExecuteJobComputeAction('machine', 'queue', 'account', '30', 'sleep', ['1'], '/tmp', 1, 0, nodes=1)
        checked, available = Event(), Event()

        def resource_status():
            """Signal a resource query and return the test-controlled availability bool."""
            checked.set()
            return available.is_set()

        try:
            with patch.object(ExecuteJobComputeAction, 'queryResourceStatus', side_effect=resource_status), \
                 patch.object(ExecuteJobComputeAction, 'initiateAction', return_value='remote-1') as submit, \
                 patch('amscjobmanager.compute_action_manager.getJobState', side_effect=['queued', 'active', 'completed']) as poll:
                workflow_id = workflows.enqueueWorkflow(action)
                workflows.startWorkflows([workflow_id])
                manager = workflows.action_man[ActionClass.COMPUTE]
                action_id = workflows.workflowState(workflow_id)['head_action_id']
                self.assertTrue(checked.wait(1))
                submit.assert_not_called()
                available.set()
                self.assertEqual(manager.waitForAction(action_id, check_freq=.005), ActionStatus.COMPLETED)
                self.assertEqual(submit.call_count, 1)
                self.assertEqual(poll.call_count, 3)
                self.assertEqual(manager.queryRecordedStatus(action_id),
                                 (ActionStatus.COMPLETED, 'completed'))
                workflows.progressActiveState()
                self.assertEqual(workflows.workflowStatus(workflow_id), ActionStatus.COMPLETED)
        finally:
            workflows.stopWorkers()
            workflows.conn.close()

    def test_recorded_status_reads_preserve_api_polling_interval(self):
        """Read cached status repeatedly without changing the provider polling rate.

        Configure an hour between API polls, then perform recorded reads and a
        normal WorkflowData pass. Only a forced refresh should trigger the next fake
        provider request, and it must retain the configured interval afterward.
        """
        workflows = WorkflowData(self.root / 'recorded-polling.sqlite', poll_freq=3600)
        action = ExecuteJobComputeAction('local', '', '', '60', '/bin/true', [], str(self.root), 1, 0, nodes=1)
        try:
            with patch('amscjobmanager.compute_actions.executeJob', return_value='remote-1'), \
                 patch('amscjobmanager.compute_action_manager.getJobState', return_value='active') as poll:
                workflow_id = workflows.enqueueWorkflow(action)
                workflows.startWorkflows([workflow_id])
                manager = workflows.action_man[ActionClass.COMPUTE]
                action_id = workflows.workflowState(workflow_id)['head_action_id']
                deadline = time.monotonic() + 2
                while manager.queryRecordedStatus(action_id)[0] != ActionStatus.ACTIVE:
                    if time.monotonic() > deadline:
                        self.fail('Initial provider status was not recorded')
                    time.sleep(.005)
                for _ in range(25):
                    self.assertEqual(manager.queryRecordedStatus(action_id), (ActionStatus.ACTIVE, 'active'))
                workflows.progressActiveState()
                self.assertEqual(poll.call_count, 1)
                self.assertEqual(manager.poll_freq, 3600)

                poll.return_value = 'completed'
                manager.queryRecordedStatus(action_id, force_update=True)
                self.assertEqual(manager.waitForAction(action_id, check_freq=.005), ActionStatus.COMPLETED)
                self.assertEqual(poll.call_count, 2)
                self.assertEqual(manager.poll_freq, 3600)
        finally:
            workflows.stopWorkers()
            workflows.conn.close()

    def test_new_manager_recovers_existing_work_during_construction(self):
        """Recover an ACTIVE action without starting workflow progression.

        Save one fake submitted job plus a still-pending workflow. A fresh
        WorkflowManager must automatically monitor the existing job in its constructor,
        without resubmission, while leaving the pending workflow untouched.
        """
        database = self.root / 'constructor-recovery.sqlite'
        workflows = WorkflowData(database, poll_freq=.01)
        action = ExecuteJobComputeAction('local', '', '', '60', '/bin/true', [], str(self.root), 1, 0, nodes=1)
        try:
            with patch('amscjobmanager.compute_actions.executeJob', return_value='remote-1'), \
                 patch('amscjobmanager.compute_action_manager.getJobState', return_value='active'):
                workflow_id = workflows.enqueueWorkflow(action)
                pending = workflows.enqueueWorkflow(APIAction([WriteFile('local', str(self.root / 'not-started'), 'never')]))
                workflows.startWorkflows([workflow_id])
                self._progress_until(workflows, lambda: workflows.headActionStatus(workflow_id) == ActionStatus.ACTIVE)
                action_id = workflows.workflowState(workflow_id)['head_action_id']
                worker = workflows.action_man[ActionClass.COMPUTE]._action_workers[action_id]
                workflows.stopWorkers()
                worker.future.result(timeout=2)
        finally:
            workflows.stopWorkers()
            workflows.conn.close()

        manager = None
        try:
            with patch('amscjobmanager.compute_actions.executeJob', side_effect=AssertionError('Duplicate submission')), \
                 patch('amscjobmanager.compute_action_manager.getJobState', return_value='completed') as poll:
                manager = WorkflowManager(database, poll_freq=.01)
                actions = manager.workflow_data.action_man[ActionClass.COMPUTE]
                self.assertEqual(actions.waitForAction(action_id, check_freq=.005), ActionStatus.COMPLETED)
                poll.assert_called_once_with('local', 'remote-1')
                self.assertFalse(manager.isAlive())
                self.assertIsNone(manager._thread)
                self.assertEqual(manager.workflow_data.workflowStatus(pending), ActionStatus.PENDING)
                self.assertFalse((self.root / 'not-started').exists())
        finally:
            if manager is not None:
                manager.stop(wait_until_done=False, close_database=True)

    def test_recovery_does_not_restart_canceled_workflows(self):
        """Cancel an interrupted submission, then reopen without restarting it.

        Model a crash before submission starts, then let the real cancellation
        worker confirm no remote job exists. Recovery must retain its terminal
        outcome without issuing another submission or cancellation lookup.
        """
        database = self.root / 'canceled.sqlite'
        workflows = WorkflowData(database, _test_crash_at='action_intent_persisted')
        action = ExecuteJobComputeAction('local', '', '', '60', '/bin/true', [], str(self.root), 1, 0, nodes=1)
        workflow_id = workflows.enqueueWorkflow(action)
        with self.assertRaises(SimulatedCrash):
            workflows.startWorkflows([workflow_id])
        # Complete cancellation before reopening; no submission worker may run.
        with patch('amscjobmanager.compute_action_manager.findJobByName', return_value=None), \
             patch.object(ExecuteJobComputeAction, 'initiateAction') as submit:
            workflows.cancelWorkflow(workflow_id)
            self._progress_until(workflows, lambda: workflows.workflowStatus(workflow_id) == ActionStatus.CANCELED)
            submit.assert_not_called()
        workflows.stopWorkers()
        workflows.conn.close()
        recovered = None
        try:
            with patch.object(ExecuteJobComputeAction, 'initiateAction') as submit, \
                 patch('amscjobmanager.compute_action_manager.findJobByName') as find:
                recovered = WorkflowData(database, poll_freq=.01)
                recovered.progressActiveState()
                self.assertEqual(recovered.workflowStatus(workflow_id), ActionStatus.CANCELED)
                self.assertEqual(recovered.action_man[ActionClass.COMPUTE]._action_workers, {})
                submit.assert_not_called()
                find.assert_not_called()
        finally:
            if recovered is not None:
                recovered.stopWorkers()
                recovered.conn.close()

    def test_shutdown_interrupts_waiting_query_without_canceling_remote_work(self):
        """Stop a queried supervisor, retaining SCHEDULING for restart recovery.

        The fake query honors the cancellation event. Shutdown must finish its
        worker without treating the stop signal as a request to cancel the job.
        """
        workflows = WorkflowData(self.root / 'shutdown.sqlite', poll_freq=.01)
        action = ExecuteJobComputeAction('machine', 'queue', 'account', '30', 'sleep', ['1'], '/tmp', 1, 0, nodes=1)
        try:
            with patch.object(manager_logging, 'api_query_user_func', self._ask), \
                 patch.object(ExecuteJobComputeAction, 'queryResourceStatus', return_value=True), \
                 patch.object(ExecuteJobComputeAction, 'initiateAction',
                              side_effect=lambda *args, **kwargs: manager_logging.wfapiUserQuery('submit', 'authenticate?')), \
                 patch('amscjobmanager.compute_action_manager.cancelJob') as cancel:
                workflow_id = workflows.enqueueWorkflow(action)
                workflows.startWorkflows([workflow_id])
                self.assertTrue(self.entered.wait(1))
                manager = workflows.action_man[ActionClass.COMPUTE]
                action_id = workflows.workflowState(workflow_id)['head_action_id']
                worker = manager._action_workers[action_id]
                workflows.stopWorkers()
                worker.future.result(timeout=2)
                self.assertEqual(manager.queryRecordedStatus(action_id)[0], ActionStatus.SCHEDULING)
                cancel.assert_not_called()
        finally:
            self.answer.set()
            workflows.stopWorkers()
            workflows.conn.close()

    def test_api_question_does_not_block_another_workflow_or_write_sql(self):
        """Keep a real API worker waiting while another API workflow completes.

        Trace SQLite thread IDs to verify supervisor checkpoints occur in the
        background through the shared locked connection, while WorkflowData remains
        responsive on the caller thread.
        """
        workflows = WorkflowData(self.root / 'api.sqlite', poll_freq=.01)
        sql_threads = []
        with workflows.conn as conn:
            conn.set_trace_callback(lambda statement: sql_threads.append(get_ident()))
        original = WriteFile.execute
        blocked, independent = self.root / 'blocked', self.root / 'independent'

        def execute(operation, workflow_id):
            """Ask only for the blocked WriteFile operation, then perform its write."""
            if operation.path == str(blocked):
                manager_logging.wfapiUserQuery('write', 'allow write?')
            original(operation, workflow_id)

        try:
            with patch.object(manager_logging, 'api_query_user_func', self._ask), patch.object(WriteFile, 'execute', execute):
                waiting = workflows.enqueueWorkflow(APIAction([WriteFile('local', str(blocked), 'done')]))
                other = workflows.enqueueWorkflow(APIAction([WriteFile('local', str(independent), 'done')]))
                workflows.startWorkflows()
                self.assertTrue(self.entered.wait(1))
                self._progress_until(workflows, lambda: workflows.workflowStatus(other) == ActionStatus.COMPLETED)
                self.assertEqual(workflows.headActionStatus(waiting), ActionStatus.SCHEDULING)
                self.assertFalse(blocked.exists())
                self.assertEqual(independent.read_text(), 'done')
                self.assertIn(get_ident(), sql_threads)
                self.assertGreater(len(set(sql_threads)), 1)
                self.answer.set()
                self._progress_until(workflows, lambda: workflows.workflowStatus(waiting) == ActionStatus.COMPLETED)
                self.assertEqual(blocked.read_text(), 'done')
        finally:
            self.answer.set()
            workflows.stopWorkers()
            workflows.conn.close()

    def test_submission_question_is_shared_across_passes_without_retrying(self):
        """Pending submission keeps one attempt and no remote ID; other work progresses."""
        workflows = WorkflowData(self.root / 'submit.sqlite', poll_freq=.01)
        calls = []

        def submit(*args, **kwargs):
            """Ask once, record acceptance and return a fake remote ID string."""
            manager_logging.wfapiUserQuery('submit', 'authenticate?')
            calls.append(kwargs['name'])
            return 'remote-1'

        action = ExecuteJobComputeAction('local', '', '', '60', '/bin/true', [], str(self.root), 1, 0, nodes=1)
        try:
            with patch('amscjobmanager.compute_actions.executeJob', side_effect=submit), \
                 patch('amscjobmanager.compute_action_manager.getJobState', return_value='active'), \
                 patch.object(manager_logging, 'api_query_user_func', self._ask):
                waiting = workflows.enqueueWorkflow(action)
                other = workflows.enqueueWorkflow(APIAction([WriteFile('local', str(self.root / 'other'), 'done')]))
                workflows.startWorkflows()
                self._progress_until(workflows, self.entered.is_set)
                self._progress_until(workflows, lambda: workflows.workflowStatus(other) == ActionStatus.COMPLETED)
                with workflows.conn as conn:
                    row = conn.execute('SELECT api_key, submission_attempts FROM computes').fetchone()
                self.assertEqual(tuple(row), (None, 1))
                self.assertEqual(calls, [])
                self.answer.set()
                self._progress_until(workflows, lambda: workflows.headActionStatus(waiting) == ActionStatus.ACTIVE)
                self.assertEqual(len(calls), 1)
        finally:
            self.answer.set()
            workflows.stopWorkers()
            workflows.conn.close()

    def test_repeated_cancel_reconciliation_preserves_cancellation_question(self):
        """Wait for authentication during cancellation across normal manager passes.

        Repeated cancellation reconciliation must reuse the supervisor without
        repeatedly interrupting its own query or issuing duplicate cancellations.
        """
        workflows = WorkflowData(self.root / 'cancel-query.sqlite', poll_freq=.01)
        action = ExecuteJobComputeAction('local', '', '', '60', '/bin/true', [], str(self.root), 1, 0, nodes=1)

        def cancel(*args, **kwargs):
            """Ask for authentication, then return None as a successful provider cancel."""
            manager_logging.wfapiUserQuery('cancel', 'authenticate?')

        try:
            with patch('amscjobmanager.compute_actions.executeJob', return_value='remote-1'), \
                 patch('amscjobmanager.compute_action_manager.getJobState', return_value='active'), \
                 patch('amscjobmanager.compute_action_manager.cancelJob', side_effect=cancel) as request, \
                 patch.object(manager_logging, 'api_query_user_func', self._ask):
                workflow_id = workflows.enqueueWorkflow(action)
                workflows.startWorkflows([workflow_id])
                self._progress_until(workflows, lambda: workflows.headActionStatus(workflow_id) == ActionStatus.ACTIVE)
                workflows.cancelWorkflow(workflow_id)
                self.assertTrue(self.entered.wait(1))
                for _ in range(5):
                    workflows.progressActiveState()
                    time.sleep(.01)
                self.assertEqual(request.call_count, 1)
                self.assertEqual(workflows.headActionStatus(workflow_id), ActionStatus.CANCELING)
                self.answer.set()
                self._progress_until(workflows, lambda: workflows.workflowStatus(workflow_id) == ActionStatus.CANCELED)
                self.assertEqual(request.call_count, 1)
        finally:
            self.answer.set()
            workflows.stopWorkers()
            workflows.conn.close()

    def test_cancellation_collects_submission_that_returns_after_request(self):
        """Do not discard a late accepted job when cancellation races submission.

        The simulated provider cannot be interrupted once called. Cancellation
        waits, saves its returned ID, and cancels that remote job exactly once.
        """
        workflows = WorkflowData(self.root / 'cancel.sqlite', poll_freq=.01)
        accepted, canceled = [], []
        execution_finished = Event()
        manager = workflows.action_man[ActionClass.COMPUTE]
        original_execution = manager._executeAction

        def execute(action_id):
            """Run normal execution for action_id (int) and record when execution has finished running."""
            try:
                original_execution(action_id)
            finally:
                execution_finished.set()

        def cancel(machine, key):
            """Cancel key (remote str ID) on machine (str) after execution has finished running."""
            self.assertTrue(execution_finished.is_set(), 'Cancellation overlapped execution')
            canceled.append(key)

        def submit(*args, **kwargs):
            """Wait inside an uninterruptible provider call and return its remote ID."""
            self.entered.set()
            if not self.answer.wait(3):
                raise RuntimeError('Test did not release submission')
            accepted.append('remote-1')
            return 'remote-1'

        action = ExecuteJobComputeAction('local', '', '', '60', '/bin/true', [], str(self.root), 1, 0, nodes=1)
        try:
            with patch.object(manager, '_executeAction', side_effect=execute), \
                 patch('amscjobmanager.compute_actions.executeJob', side_effect=submit), \
                 patch('amscjobmanager.compute_action_manager.getJobState', return_value='active'), \
                 patch('amscjobmanager.compute_action_manager.cancelJob', side_effect=cancel):
                workflow_id = workflows.enqueueWorkflow(action)
                workflows.startWorkflows()
                self._progress_until(workflows, self.entered.is_set)
                workflows.cancelWorkflow(workflow_id)
                self.assertEqual(workflows.workflowStatus(workflow_id), ActionStatus.CANCELING)
                self.answer.set()
                self._progress_until(workflows, lambda: workflows.workflowStatus(workflow_id) == ActionStatus.CANCELED)
                self.assertEqual(accepted, ['remote-1'])
                self.assertEqual(canceled, ['remote-1'])
                with workflows.conn as conn:
                    self.assertEqual(conn.execute('SELECT api_key FROM computes').fetchone()[0], 'remote-1')
        finally:
            self.answer.set()
            workflows.stopWorkers()
            workflows.conn.close()

    def test_shutdown_suppresses_pending_cancellation_handoff(self):
        """Stop after cancellation intent while submission is still in flight.

        Release an uninterruptible submission only after shutdown. Its accepted
        ID must be saved, while Future cleanup must leave CANCELING recoverable
        instead of starting a remote cancellation worker during shutdown.
        """
        workflows = WorkflowData(self.root / 'stop-handoff.sqlite', poll_freq=.01)
        finished = Event()

        def submit(*args, **kwargs):
            """Hold fake provider submission, then return its accepted remote ID."""
            self.entered.set()
            if not self.answer.wait(2):
                raise TimeoutError('Test did not release submission')
            return 'remote-1'

        try:
            with patch('amscjobmanager.compute_actions.executeJob', side_effect=submit), \
                 patch('amscjobmanager.compute_action_manager.cancelJob') as cancel:
                action = ExecuteJobComputeAction('local', '', '', '60', '/bin/true', [], str(self.root), 1, 0, nodes=1)
                workflow_id = workflows.enqueueWorkflow(action)
                workflows.startWorkflows([workflow_id])
                self.assertTrue(self.entered.wait(1))
                manager = workflows.action_man[ActionClass.COMPUTE]
                action_id = workflows.workflowState(workflow_id)['head_action_id']
                worker = manager._action_workers[action_id]
                # This callback follows production cleanup, so the assertion
                # observes both the operation return and the handoff decision.
                worker.future.add_done_callback(lambda future: finished.set())
                workflows.cancelWorkflow(workflow_id)
                workflows.stopWorkers()
                self.answer.set()
                self.assertTrue(finished.wait(2))
                cancel.assert_not_called()
                self.assertEqual(manager.queryRecordedStatus(action_id)[0], ActionStatus.CANCELING)
                self.assertEqual(manager.getActionInfo(action_id)['api_key'], 'remote-1')
                self.assertEqual(manager._action_workers, {})
        finally:
            self.answer.set()
            workflows.stopWorkers()
            workflows.conn.close()

    def test_active_poll_query_keeps_cached_state_and_does_not_resubmit(self):
        """A query during active polling leaves the job ACTIVE and other work moving."""
        workflows = WorkflowData(self.root / 'poll.sqlite', poll_freq=.01)
        action = ExecuteJobComputeAction('local', '', '', '60', '/bin/true', [], str(self.root), 1, 0, nodes=1)
        challenge = Event()
        poll_questions = []

        def status(machine, remote_id):
            """Return active, or ask on the next poll and then return completed."""
            if challenge.is_set():
                poll_questions.append(remote_id)
                manager_logging.wfapiUserQuery('poll', 'authenticate?')
                return 'completed'
            return 'active'

        try:
            with patch('amscjobmanager.compute_actions.executeJob', return_value='remote-1') as submit, \
                 patch('amscjobmanager.compute_action_manager.getJobState', side_effect=status), \
                 patch.object(manager_logging, 'api_query_user_func', self._ask):
                workflow_id = workflows.enqueueWorkflow(action)
                workflows.startWorkflows()
                self._progress_until(workflows, lambda: workflows.headActionStatus(workflow_id) == ActionStatus.ACTIVE)
                challenge.set()
                self._progress_until(workflows, self.entered.is_set)
                other = workflows.enqueueWorkflow(APIAction([WriteFile('local', str(self.root / 'poll-other'), 'done')]))
                workflows.startWorkflows()
                self._progress_until(workflows, lambda: workflows.workflowStatus(other) == ActionStatus.COMPLETED)
                self.assertEqual(workflows.headActionStatus(workflow_id, return_api_status=True), (ActionStatus.ACTIVE, 'active'))
                self.assertEqual(poll_questions, ['remote-1'])
                self.answer.set()
                self._progress_until(workflows, lambda: workflows.workflowStatus(workflow_id) == ActionStatus.COMPLETED)
                submit.assert_called_once()
        finally:
            self.answer.set()
            workflows.stopWorkers()
            workflows.conn.close()

    def test_existing_remote_id_recovers_without_scheduling_counter(self):
        """Recover an existing remote ID without adding or consulting a cursor.

        Use the shared locked SQLite connection for supervisor commits. Also
        ignore an obsolete counter
        from the previous prototype, even if its value has no recognized meaning.
        """
        raw_connection = sqlite3.connect(self.root / 'legacy.sqlite', check_same_thread=False)
        raw_connection.row_factory = sqlite3.Row
        connection = LockedConnection(raw_connection)
        action = ExecuteJobComputeAction('local', '', '', '60', '/bin/true', [], str(self.root), 1, 0, nodes=1)
        with connection as conn:
            conn.execute('CREATE TABLE computes (action_id INTEGER PRIMARY KEY, machine TEXT, details BLOB, '
                         'api_key TEXT, api_status TEXT, action_status TEXT, last_update INTEGER, workflow_id INTEGER, '
                         'submission_attempts INTEGER NOT NULL DEFAULT 0, last_submission_attempt INTEGER, last_submission_error TEXT)')
            conn.execute('INSERT INTO computes(action_id, machine, details, api_key, action_status, last_update, workflow_id, submission_attempts) '
                         'VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                         (17, 'local', _ser(action), 'remote-1', 'SCHEDULING', 0, 1, 1))
        manager = ComputeActions(connection)
        try:
            with connection as conn:
                self.assertNotIn('next_step', {row[1] for row in conn.execute('PRAGMA table_info(computes)')})
            with patch('amscjobmanager.compute_action_manager.getJobState', return_value='active'), \
                 patch('amscjobmanager.compute_actions.executeJob', side_effect=AssertionError('duplicate submission')):
                deadline = time.monotonic() + 2
                self.assertIsNone(manager.startOrResumeAction(action, 1, 17))
                while manager.queryRecordedStatus(17)[0] != ActionStatus.ACTIVE:
                    if time.monotonic() > deadline:
                        self.fail('Legacy action did not recover')
                    time.sleep(.005)
            self.assertEqual(manager.queryRecordedStatus(17), (ActionStatus.ACTIVE, 'active'))
            manager.stopWorkers()
            for worker in list(manager._action_workers.values()):
                worker.future.result(timeout=2)
            with connection as conn:
                conn.execute('ALTER TABLE computes ADD COLUMN next_step INTEGER DEFAULT 99')
                conn.execute("UPDATE computes SET action_status = 'SCHEDULING'")
            recovered = ComputeActions(connection)
            try:
                with patch('amscjobmanager.compute_action_manager.getJobState', return_value='active'), \
                     patch('amscjobmanager.compute_actions.executeJob', side_effect=AssertionError('duplicate submission')):
                    deadline = time.monotonic() + 2
                    self.assertIsNone(recovered.startOrResumeAction(action, 1, 17))
                    while recovered.queryRecordedStatus(17)[0] != ActionStatus.ACTIVE:
                        if time.monotonic() > deadline:
                            self.fail('Obsolete counter prevented recovery')
                        time.sleep(.005)
                self.assertEqual(recovered.queryRecordedStatus(17), (ActionStatus.ACTIVE, 'active'))
            finally:
                recovered.stopWorkers()

        finally:
            manager.stopWorkers()
            connection.close()

    def test_cancel_waiting_api_worker_stops_remaining_operations(self):
        """Wake an API query cooperatively, retaining earlier completed effects."""
        workflows = WorkflowData(self.root / 'cancel-api.sqlite', poll_freq=.01)
        original = WriteFile.execute

        def execute(operation, workflow_id):
            """Ask before a write, allowing cancellation to interrupt the wait."""
            manager_logging.wfapiUserQuery('write', 'allow?')
            original(operation, workflow_id)

        manager = workflows.action_man[ActionClass.API]
        try:
            with patch.object(WriteFile, 'execute', execute), \
                 patch.object(manager_logging, 'api_query_user_func', self._ask), \
                 patch.object(manager, '_executeAction', wraps=manager._executeAction) as run, \
                 patch.object(manager, '_completeCancellation', wraps=manager._completeCancellation) as finish:
                workflow_id = workflows.enqueueWorkflow(APIAction([WriteFile('local', str(self.root / 'never'), 'never')]))
                workflows.startWorkflows()
                self.assertTrue(self.entered.wait(1))
                workflows.cancelWorkflow(workflow_id)
                self._progress_until(workflows, lambda: workflows.workflowStatus(workflow_id) == ActionStatus.CANCELED)
                self.assertFalse((self.root / 'never').exists())
                run.assert_called_once()
                finish.assert_called_once()
        finally:
            workflows.stopWorkers()
            workflows.conn.close()

    def test_cancel_during_api_operation_preserves_checkpoint_and_skips_later_work(self):
        """Cancel a file operation that cannot be interrupted while it is running.

        Hold the first operation after its write, record cancellation, then let
        it return. Both final-operation and intermediate-operation cases must
        checkpoint that successful write, finish cancellation promptly, and
        avoid starting any remaining operation.
        """
        original = WriteFile.execute
        for operation_count in (1, 2):
            with self.subTest(operation_count=operation_count):
                self.entered.clear()
                self.answer.clear()
                workflows = WorkflowData(self.root / f'cancel-running-{operation_count}.sqlite', poll_freq=60)
                first = self.root / f'written-{operation_count}'
                second = self.root / f'skipped-{operation_count}'

                def execute(operation, workflow_id):
                    """Write operation (WriteFile) for workflow_id (int), then block the first call."""
                    original(operation, workflow_id)
                    if operation.path == str(first):
                        self.entered.set()
                        if not self.answer.wait(2):
                            raise TimeoutError('Test did not release the file operation')

                try:
                    with patch.object(WriteFile, 'execute', execute):
                        operations = [WriteFile('local', str(first), 'done'),
                                      WriteFile('local', str(second), 'never')][:operation_count]
                        workflow_id = workflows.enqueueWorkflow(APIAction(operations))
                        workflows.startWorkflows([workflow_id])
                        self.assertTrue(self.entered.wait(1))
                        manager = workflows.action_man[ActionClass.API]
                        action_id = workflows.workflowState(workflow_id)['head_action_id']
                        workflows.cancelWorkflow(workflow_id)
                        self.assertEqual(manager.queryRecordedStatus(action_id)[0], ActionStatus.CANCELING)
                        self.answer.set()
                        self.assertEqual(manager.waitForAction(action_id, check_freq=.005), ActionStatus.CANCELED)
                        self.assertEqual(manager.queryRecordedStatus(action_id)[0], ActionStatus.CANCELED)
                        self.assertEqual(manager.getActionInfo(action_id)['next_operation'], 1)
                        self.assertEqual(first.read_text(), 'done')
                        self.assertFalse(second.exists())
                        workflows.progressActiveState()
                        self.assertEqual(workflows.workflowStatus(workflow_id), ActionStatus.CANCELED)
                finally:
                    self.answer.set()
                    workflows.stopWorkers()
                    workflows.conn.close()

    def test_cancel_before_worker_entry_skips_submission_and_completes(self):
        """Deliver cancellation before the action's thread enters its worker function.

        Gate the real thread at startup, cancel through WorkflowData, then release it.
        The worker must handle the recorded intent and finish without a remote
        submission or any further workflow-progression call.
        """
        ready, release = Event(), Event()
        workflows = WorkflowData(self.root / 'cancel-before-entry.sqlite', poll_freq=.01)
        action = ExecuteJobComputeAction('local', '', '', '60', '/bin/true', [], str(self.root), 1, 0, nodes=1)

        def gated_thread(*, target, args=(), name, daemon):
            """Return a real Thread that delays target (callable) until released.

            args (tuple) is forwarded to target. name (str) and daemon (bool)
            retain the production thread settings.
            """
            def enter():
                """Wait at the startup gate, then invoke the captured worker target."""
                ready.set()
                if not release.wait(2):
                    raise TimeoutError('Test did not release the worker')
                target(*args)
            return Thread(target=enter, name=name, daemon=daemon)

        try:
            with patch('amscjobmanager.workers.Thread', side_effect=gated_thread), \
                 patch('amscjobmanager.compute_actions.executeJob') as submit, \
                 patch('amscjobmanager.compute_action_manager.findJobByName', return_value=None):
                workflow_id = workflows.enqueueWorkflow(action)
                workflows.startWorkflows([workflow_id])
                self.assertTrue(ready.wait(1))
                manager = workflows.action_man[ActionClass.COMPUTE]
                action_id = workflows.workflowState(workflow_id)['head_action_id']
                workflows.cancelWorkflow(workflow_id)
                release.set()
                self.assertEqual(manager.waitForAction(action_id, check_freq=.005), ActionStatus.CANCELED)
                self.assertEqual(manager.queryRecordedStatus(action_id)[0], ActionStatus.CANCELED)
                submit.assert_not_called()
        finally:
            release.set()
            workflows.stopWorkers()
            workflows.conn.close()

    def test_cancellation_crash_recovers_without_reentering_execution(self):
        """Recover a crash after remote cancellation but before its database update.

        Keep fake provider state across a database reopen. Recovery must enter
        cancellation completion directly, recognize the canceled remote job,
        and avoid resubmission or a second provider cancellation.
        """
        database = self.root / 'cancel-crash.sqlite'
        state = {'status': 'active'}

        def cancel(machine, remote_id):
            """Cancel remote_id (str) on machine (str), retaining the fake external effect."""
            state['status'] = 'canceled'

        workflows = WorkflowData(database, _test_crash_at='remote_cancellation_completed', poll_freq=.01)
        action = ExecuteJobComputeAction('local', '', '', '60', '/bin/true', [], str(self.root), 1, 0, nodes=1)
        try:
            with patch('amscjobmanager.compute_actions.executeJob', return_value='remote-1'), \
                 patch('amscjobmanager.compute_action_manager.getJobState', side_effect=lambda *args: state['status']), \
                 patch('amscjobmanager.compute_action_manager.cancelJob', side_effect=cancel) as request:
                workflow_id = workflows.enqueueWorkflow(action)
                workflows.startWorkflows([workflow_id])
                self._progress_until(workflows, lambda: workflows.headActionStatus(workflow_id) == ActionStatus.ACTIVE)
                manager = workflows.action_man[ActionClass.COMPUTE]
                action_id = workflows.workflowState(workflow_id)['head_action_id']
                workflows.cancelWorkflow(workflow_id)
                with self.assertRaises(SimulatedCrash):
                    manager.waitForAction(action_id, check_freq=.005)
                request.assert_called_once_with('local', 'remote-1')
                self.assertEqual(manager.queryRecordedStatus(action_id)[0], ActionStatus.CANCELING)
        finally:
            workflows.stopWorkers()
            workflows.conn.close()

        recovered = None
        try:
            with patch.object(ComputeActions, '_executeAction', side_effect=AssertionError('Execution re-entered')), \
                 patch('amscjobmanager.compute_actions.executeJob', side_effect=AssertionError('Duplicate submission')), \
                 patch('amscjobmanager.compute_action_manager.getJobState', return_value=state['status']), \
                 patch('amscjobmanager.compute_action_manager.cancelJob') as request:
                recovered = WorkflowData(database, poll_freq=.01)
                manager = recovered.action_man[ActionClass.COMPUTE]
                self._progress_until(recovered, lambda: recovered.workflowStatus(workflow_id) == ActionStatus.CANCELED)
                self.assertEqual(manager.queryRecordedStatus(action_id), (ActionStatus.CANCELED, 'canceled'))
                request.assert_not_called()
        finally:
            if recovered is not None:
                recovered.stopWorkers()
                recovered.conn.close()

    def test_process_crashes_recover_api_cursor_and_compute_submission(self):
        """Kill child processes while workers wait or before submission checkpoints.

        Reopen SQLite in the parent: API recovery skips a completed write and
        asks again; compute recovery either uses its saved ID or discovers the
        accepted job by name. No persisted question/condition record is needed.
        """
        script = r'''
import json, os, sys, time
from pathlib import Path
from threading import Event
from unittest.mock import patch
from amscjobmanager import logging
from amscjobmanager.action_manager import SimulatedCrash
from amscjobmanager.actions_base import ActionClass
from amscjobmanager.api_actions import APIAction, WriteFile
from amscjobmanager.compute_actions import ExecuteJobComputeAction
from amscjobmanager.workflow_data import WorkflowData
root, mode = Path(sys.argv[1]), sys.argv[2]
def ask(title, query):
    """Record a question and wait until the process is killed."""
    (root / 'question').write_text('old-session')
    Event().wait()
def submit(*args, **kwargs):
    """Persist the accepted remote name, returning its ID string."""
    (root / 'remote.json').write_text(json.dumps({'name': kwargs['name'], 'id': 'remote-1'}))
    return 'remote-1'
def status(*args):
    """Ask during the initial status request, then return active."""
    if mode != 'active':
        logging.wfapiUserQuery('status', 'authenticate?')
    return 'active'
original = WriteFile.execute
def write(operation, workflow_id):
    """Ask during the second write, retaining the first checkpoint."""
    if operation.path.endswith('second'):
        logging.wfapiUserQuery('write', 'allow?')
    original(operation, workflow_id)
point = 'remote_submission_completed' if mode == 'lost-id' else None
workflows = WorkflowData(root / 'workflow.sqlite', _test_crash_at=point, poll_freq=.01)
with patch.object(logging, 'api_query_user_func', ask), patch.object(WriteFile, 'execute', write), \
     patch('amscjobmanager.compute_actions.executeJob', side_effect=submit), \
     patch('amscjobmanager.compute_action_manager.getJobState', side_effect=status):
    action = (APIAction([WriteFile('local', str(root / 'first'), 'first'), WriteFile('local', str(root / 'second'), 'second')])
              if mode == 'api' else ExecuteJobComputeAction('local', '', '', '60', '/bin/true', [], str(root), 1, 0, nodes=1))
    workflows.enqueueWorkflow(action)
    deadline = time.monotonic() + 5
    try:
        workflows.startWorkflows()
        if mode == 'lost-id':
            action_id = workflows.workflowState(1)['head_action_id']
            workflows.action_man[ActionClass.COMPUTE].waitForAction(action_id, check_freq=.005)
        while time.monotonic() < deadline:
            if mode == 'active' and workflows.workflowState(1)['head_action_status'].name == 'ACTIVE':
                os._exit(0)
            if (root / 'question').exists():
                os._exit(0)
            workflows.progressActiveState(force_poll=True)
            time.sleep(.005)
    except SimulatedCrash:
        os._exit(0)
os._exit(2)
'''
        for mode in ('api', 'saved-id', 'lost-id', 'active'):
            with self.subTest(mode=mode):
                root = self.root / mode
                root.mkdir()
                child = subprocess.run([sys.executable, '-c', script, str(root), mode], capture_output=True, text=True, timeout=10)
                self.assertEqual(child.returncode, 0, child.stdout + child.stderr)
                workflows = None
                questions = []
                original = WriteFile.execute

                def ask(title, query):
                    """Record the fresh question and return a new-session answer string."""
                    questions.append((title, query))
                    return 'yes'

                def write(operation, workflow_id):
                    """Reissue the unfinished write's question before executing it."""
                    if operation.path.endswith('second'):
                        manager_logging.wfapiUserQuery('write', 'allow?')
                    original(operation, workflow_id)

                def status(machine, remote_id):
                    """Return remote completion, with a fresh authorization question."""
                    manager_logging.wfapiUserQuery('status', 'authenticate?')
                    return 'completed'

                try:
                    # Inspect the crash snapshot before construction starts any
                    # recovery worker or can change the persisted checkpoints.
                    conn = sqlite3.connect(root / 'workflow.sqlite')
                    try:
                        if mode == 'api':
                            self.assertEqual(conn.execute('SELECT next_operation FROM api_actions').fetchone()[0], 1)
                            (root / 'first').write_text('checkpointed-sentinel')
                        else:
                            row = conn.execute('SELECT api_key, submission_attempts FROM computes').fetchone()
                            self.assertEqual(tuple(row), ('remote-1', 1) if mode in ('saved-id', 'active') else (None, 1))
                            remote = json.loads((root / 'remote.json').read_text())
                    finally:
                        conn.close()
                    with patch.object(manager_logging, 'api_query_user_func', ask), \
                         patch.object(WriteFile, 'execute', write), \
                         patch('amscjobmanager.compute_actions.executeJob', side_effect=AssertionError('duplicate submission')), \
                         patch('amscjobmanager.compute_action_manager.findJobByName', return_value='remote-1') as find, \
                         patch('amscjobmanager.compute_action_manager.getJobState', side_effect=status):
                        workflows = WorkflowData(root / 'workflow.sqlite', poll_freq=.01)
                        self._progress_until(workflows, lambda: workflows.workflowStatus(1) == ActionStatus.COMPLETED)
                        if mode == 'api':
                            self.assertEqual((root / 'first').read_text(), 'checkpointed-sentinel')
                            self.assertEqual((root / 'second').read_text(), 'second')
                        elif mode == 'lost-id':
                            find.assert_called_once_with('local', remote['name'])
                        else:
                            find.assert_not_called()
                    self.assertEqual(len(questions), 1)
                    with workflows.conn as conn:
                        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                    self.assertNotIn('conditions', tables)
                finally:
                    if workflows is not None:
                        workflows.stopWorkers()
                        workflows.conn.close()


if __name__ == '__main__':
    unittest.main()
