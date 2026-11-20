"""API operation tests using real local files and durable workflow databases.

Remote transfer tests mock only the transfer service. Crash tests reopen SQLite
and inspect file effects to distinguish replay from skipped checkpoints.
"""



from pydantic import ValidationError
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from amscjobmanager import local_api
from support.action_manager_helpers import DirectAPIActions, DirectComputeActions, DirectGlobusActions
from amscjobmanager.action_manager import ActionManager, ActionStatus, SimulatedCrash
from amscjobmanager.actions_base import ActionClass, actionClass
from amscjobmanager.api_action_manager import APIActions
from amscjobmanager.api_actions import (
    APIAction, MakeDirectory, WriteFile, UploadFile, DownloadFile, CopyFile, RunLocal,
)
from amscjobmanager.compute_actions import ExecuteJobComputeAction
from amscjobmanager.globus_transfer_action import GlobusTransfer
from amscjobmanager.workflow_data import WorkflowData


class APIActionTests(unittest.TestCase):
    def setUp(self):
        """Create an isolated local workspace/database and capture manager logs."""
        # These tests isolate the checked-in reconciliation bodies; real
        # asynchronous lifecycles and crash recovery have separate integration tests.
        for name, implementation in [('APIActions', DirectAPIActions),
                                     ('ComputeActions', DirectComputeActions),
                                     ('GlobusDataTransfers', DirectGlobusActions)]:
            adapter = patch(f'amscjobmanager.workflow_data.{name}', implementation)
            adapter.start()
            self.addCleanup(adapter.stop)
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.database = self.root / 'workflow.sqlite'
        self.logs = []
        logger = patch('amscjobmanager.logging.wfman_log_func', side_effect=self.logs.append)
        logger.start()
        self.addCleanup(logger.stop)

    def test_operations_order_replay_and_placeholder_paths(self):
        """Run all file operations locally twice, repairing a partial destination.

        The sequence creates a workflow-specific directory, writes literal UTF-8
        content, uploads it, copies it and downloads it. Replaying must accept
        existing directories and overwrite truncated files, not skip by existence.
        """
        directory = str(self.root / '<WORKFLOWID>')
        operations = [MakeDirectory('local', directory),
                      WriteFile('local', directory + '/source', 'hello <WORKFLOWID> π'),
                      UploadFile('local', directory + '/upload', directory + '/source'),
                      CopyFile(directory + '/copy', directory + '/upload'),
                      DownloadFile('local', directory + '/download', directory + '/copy')]
        for operation in operations:
            operation.execute(7)
        (self.root / '7' / 'upload').write_text('partial')
        for operation in operations:
            operation.execute(7)
        self.assertEqual((self.root / '7' / 'download').read_text(), 'hello <WORKFLOWID> π')
        self.assertEqual(actionClass(APIAction(operations)), ActionClass.API)
        with self.assertRaises(ValidationError):
            APIAction([object()])

    def test_directory_file_conflict_and_binary_write(self):
        """A directory operation rejects an existing file; byte contents stay binary."""
        path = str(self.root / 'artifact')
        WriteFile('local', path, b'\x00\xff').execute(1)
        self.assertEqual(Path(path).read_bytes(), b'\x00\xff')
        with self.assertRaises(Exception):
            MakeDirectory('local', path).execute(1)

    def test_operation_sandbox_checks_and_tilde_rejection(self):
        """Reject unsafe destinations and ambiguous '~' through the real local API.

        Use /var paths without creating them; the configured local sandbox is
        empty, while temporary sources remain readable. Also test a symlink
        inside /tmp pointing outside it so lexical containment cannot bypass checks.
        """
        source = self.root / 'source'
        source.write_bytes(b'data')
        (self.root / 'escape').symlink_to('/var', target_is_directory=True)
        unsafe = str(self.root / 'escape' / 'amsc-api-actions-denied')
        operations = [MakeDirectory('local', unsafe), WriteFile('local', unsafe, 'x'),
                      UploadFile('local', unsafe, str(source)), CopyFile(unsafe, str(source)),
                      DownloadFile('local', unsafe, str(source)), RunLocal(sys.executable, [], '/var')]
        with patch('amscjobmanager.globals.remote_workdir', {'local': []}):
            for operation in operations:
                with self.subTest(operation=type(operation).__name__), self.assertRaises(Exception):
                    operation.execute(1)
        for operation in [MakeDirectory('local', '~/test'), WriteFile('local', '~/test', 'x'),
                          CopyFile('~/test', str(source)), RunLocal(sys.executable, [], '~/test')]:
            with self.subTest(operation=type(operation).__name__), self.assertRaises(ValueError):
                operation.execute(1)

    def test_local_command_arguments_failures_and_timeout(self):
        """Run literal argv without a shell, then check exit diagnostics and timeout.

        The successful program writes its cwd and literal argument to a file;
        failure includes stderr. A short timeout bounds a sleeping program.
        """
        RunLocal(sys.executable, ['-c', 'import pathlib,sys; pathlib.Path("args").write_text(sys.argv[1])',
                                 '$(touch injected) <WORKFLOWID>'], str(self.root)).execute(9)
        self.assertEqual((self.root / 'args').read_text(), '$(touch injected) 9')
        self.assertFalse((self.root / 'injected').exists())
        with self.assertRaisesRegex(RuntimeError, 'exit.*4.*bad input'):
            RunLocal(sys.executable, ['-c', 'import sys; print("bad input",file=sys.stderr); sys.exit(4)'], str(self.root)).execute(1)
        import subprocess
        with self.assertRaises(subprocess.TimeoutExpired):
            RunLocal(sys.executable, ['-c', 'import time; time.sleep(10)'], str(self.root), timeout=.05).execute(1)
        with self.assertRaises(ValueError):
            RunLocal(sys.executable, [], str(self.root), timeout=0).execute(1)

    def test_crash_boundaries_checkpoint_and_final_state(self):
        """Reopen at every boundary and verify exactly which operations replay.

        A first write must remain skipped after its checkpoint; an interrupted
        second write must repeat until its own checkpoint commits. Once the
        final checkpoint is committed, recovery must only repair the workflow
        head and must not touch either artifact again.
        """
        points = ['workflow_head_scheduling_persisted', 'action_intent_persisted',
                  'api_operation_0_started', 'api_operation_0_completed',
                  'api_operation_0_checkpoint_persisted', 'api_operation_1_started',
                  'api_operation_1_completed', 'api_operation_1_checkpoint_persisted',
                  'api_action_completed', 'action_state_persisted_before_workflow_head_update']
        for number, point in enumerate(points):
            with self.subTest(point=point):
                database = self.root / f'{number}.sqlite'
                first, second = self.root / f'{number}-first', self.root / f'{number}-second'
                action = APIAction([WriteFile('local', str(first), 'first'), WriteFile('local', str(second), 'second')])
                workflows = WorkflowData(database, _test_crash_at=point)
                with self.assertRaises(SimulatedCrash):
                    workflow_id = workflows.enqueueWorkflow(action)
                    workflows.startWorkflows([workflow_id])
                manager = workflows.action_man[ActionClass.API]
                action_id = workflows.workflowState(workflow_id)['head_action_id']
                cursor = manager.getActionInfo(action_id)['next_operation'] if manager.hasAction(action_id) else 0
                first.write_text('sentinel')
                second.write_text('sentinel')
                workflows.conn.close()
                recovered = WorkflowData(database)
                try:
                    recovered.submitSchedulingActions()
                    self.assertEqual(first.read_text(), 'sentinel' if cursor >= 1 else 'first')
                    self.assertEqual(second.read_text(), 'sentinel' if cursor >= 2 else 'second')
                    self.assertEqual(recovered.headActionStatus(workflow_id), ActionStatus.COMPLETED)
                    info = recovered.action_man[ActionClass.API].getActionInfo(action_id)
                    self.assertEqual(info['next_operation'], 2)
                    self.assertEqual(info['operation_count'], 2)
                finally:
                    recovered.conn.close()

    def test_failure_stops_sequence_and_downstream_stage(self):
        """A real local command failure persists its cursor and blocks later work."""
        workflows = WorkflowData(self.database)
        try:
            action = APIAction([WriteFile('local', str(self.root / 'first'), 'ok'),
                                RunLocal(sys.executable, ['-c', 'raise RuntimeError("bad input")'], str(self.root)),
                                WriteFile('local', str(self.root / 'third'), 'never')])
            workflow_id = workflows.enqueueWorkflow([action, APIAction([WriteFile('local', str(self.root / 'next'), 'never')])])
            workflows.startWorkflows([workflow_id])
            workflows.progressActiveState(force_poll=True)
            self.assertEqual(workflows.workflowStatus(workflow_id), ActionStatus.FAILED)
            self.assertFalse((self.root / 'third').exists())
            self.assertFalse((self.root / 'next').exists())
            manager = workflows.action_man[ActionClass.API]
            info = manager.getActionInfo(workflows.workflowState(workflow_id)['head_action_id'])
            self.assertEqual(info['next_operation'], 1)
            self.assertIn('bad input', info['error'])
            self.assertTrue(any('operation 1 failed' in line for line in self.logs))
        finally:
            workflows.conn.close()

    def test_cancel_interrupted_or_missing_record_without_replay(self):
        """Cancel before record creation or after one checkpoint, then crash
        before updating the workflow head. The API record must already be CANCELED;
        recovery must retain effects and skip later writes.
        """
        for number, point in enumerate(['workflow_head_scheduling_persisted', 'api_operation_0_checkpoint_persisted']):
            with self.subTest(point=point):
                database = self.root / f'cancel-{number}.sqlite'
                target = self.root / f'cancel-{number}'
                workflows = WorkflowData(database, _test_crash_at=point)
                workflow_id = workflows.enqueueWorkflow(APIAction([WriteFile('local', str(target), 'first'), WriteFile('local', str(target), 'second')]))
                with self.assertRaises(SimulatedCrash):
                    workflows.startWorkflows([workflow_id])
                manager = workflows.action_man[ActionClass.API]
                workflows._test_crash_at = 'action_canceled_before_workflow_head_update'
                with self.assertRaises(SimulatedCrash):
                    workflows.cancelWorkflow(workflow_id)
                action_id = workflows.workflowState(workflow_id)['head_action_id']
                self.assertEqual(manager.queryRecordedStatus(action_id)[0], ActionStatus.CANCELED)
                self.assertEqual(workflows.workflowStatus(workflow_id), ActionStatus.CANCELING)
                self.assertIsNone(manager.cancelOrResumeCancelingAction(action_id))
                workflows.conn.close()
                recovered = WorkflowData(database)
                try:
                    recovered.progressActiveState()
                    self.assertEqual(recovered.workflowStatus(workflow_id), ActionStatus.CANCELED)
                    self.assertEqual(target.read_text() if target.exists() else None, 'first' if number else None)
                finally:
                    recovered.conn.close()

    def test_empty_sequence_status_queries_and_schema_constraints(self):
        """Complete an empty stage; reads never execute work and the cursor is constrained."""
        workflows = WorkflowData(self.database)
        try:
            manager = workflows.getManager(ActionClass.API)
            self.assertIsInstance(manager, ActionManager)
            workflow_id = workflows.enqueueWorkflow(APIAction())
            workflows.startWorkflows([workflow_id])
            action_id = workflows.workflowState(workflow_id)['head_action_id']
            self.assertEqual(manager.queryRecordedStatus(action_id, force_update=True), (ActionStatus.COMPLETED, None))
            self.assertEqual(manager.waitForAction(action_id), ActionStatus.COMPLETED)
            self.assertIsNone(manager.updateStatuses())
            self.assertEqual(manager.getLiveActions(), [])
            for cursor in [-1, 1.5, 'bad']:
                with self.assertRaises(sqlite3.IntegrityError), workflows.conn as conn:
                    conn.execute('UPDATE api_actions SET next_operation = ? WHERE action_id = ?', (cursor, action_id))
            with self.assertRaises(ValueError):
                manager.queryRecordedStatus(999)
            with self.assertRaises(ValueError):
                manager.cancelOrResumeCancelingAction(999)
            workflows.progressActiveState()
            self.assertEqual(workflows.workflowStatus(workflow_id), ActionStatus.COMPLETED)
        finally:
            workflows.conn.close()

    def test_cursor_above_sequence_length_fails_without_skipping_work(self):
        """A corrupt cursor beyond the sequence length is reported as failure."""
        workflows = WorkflowData(self.database, _test_crash_at='action_intent_persisted')
        workflow_id = workflows.enqueueWorkflow(APIAction([WriteFile('local', str(self.root / 'bad-cursor'), 'never')]))
        with self.assertRaises(SimulatedCrash):
            workflows.startWorkflows([workflow_id])
        with workflows.conn as conn:
            conn.execute('UPDATE api_actions SET next_operation = 2')
        workflows.conn.close()
        recovered = WorkflowData(self.database)
        try:
            recovered.submitSchedulingActions()
            self.assertEqual(recovered.workflowStatus(workflow_id), ActionStatus.FAILED)
            self.assertFalse((self.root / 'bad-cursor').exists())
        finally:
            recovered.conn.close()

    def test_recovery_uses_stored_definition_and_status_reads_do_not_execute(self):
        """Keep the original sequence when a recovery caller supplies amended inputs.

        Stop after intent persistence, force status reads, then supply a different
        definition to startOrResumeAction. Only the original persisted write may execute.
        """
        workflows = WorkflowData(self.database, _test_crash_at='action_intent_persisted')
        original, amended = self.root / 'original', self.root / 'amended'
        workflow_id = workflows.enqueueWorkflow(APIAction([WriteFile('local', str(original), 'original')]))
        try:
            with self.assertRaises(SimulatedCrash):
                workflows.startWorkflows([workflow_id])
            manager = workflows.action_man[ActionClass.API]
            action_id = workflows.workflowState(workflow_id)['head_action_id']
            self.assertEqual(manager.queryRecordedStatus(action_id, force_update=True), (ActionStatus.SCHEDULING, None))
            self.assertFalse(original.exists())
            manager._test_crash_at = None
            self.assertIsNone(manager.startOrResumeAction(APIAction([WriteFile('local', str(amended), 'amended')]), workflow_id, action_id))
            self.assertTrue(original.exists())
            self.assertFalse(amended.exists())
        finally:
            workflows.conn.close()

    def test_completed_api_ids_remain_reserved_after_reopening(self):
        """Keep completed API rows so new queued stages cannot reuse their IDs.

        Finish an API workflow and reopen SQLite, then queue a two-stage chain.
        Both future stages must already have distinct IDs above the completed
        action, even though neither has an execution-manager record yet.
        """
        workflows = WorkflowData(self.database)
        workflow_id = workflows.enqueueWorkflow(APIAction())
        action_id = workflows.workflowState(workflow_id)['head_action_id']
        workflows.startWorkflows([workflow_id])
        workflows.progressActiveState()
        self.assertEqual(workflows.workflowStatus(workflow_id), ActionStatus.COMPLETED)
        workflows.conn.close()
        recovered = WorkflowData(self.database)
        try:
            next_workflow = recovered.enqueueWorkflow([APIAction(), APIAction()])
            with recovered.conn as conn:
                rows = conn.execute('SELECT action_id FROM actions WHERE workflow_id = ? ORDER BY workflow_stage',
                                    (next_workflow,)).fetchall()
                self.assertEqual(conn.execute('SELECT action_status FROM actions WHERE action_id = ?',
                                              (action_id,)).fetchone()[0], 'COMPLETED')
            self.assertEqual(len(rows), 2)
            self.assertTrue(all(row['action_id'] > action_id for row in rows))
            self.assertFalse(any(recovered.action_man[ActionClass.API].hasAction(row['action_id']) for row in rows))
        finally:
            recovered.conn.close()

    def test_mixed_local_compute_transfer_and_processing(self):
        """Prepare real inputs, execute a real local job, wait for mocked Globus,
        then process results locally. Only the asynchronous transfer is mocked;
        processing must not run before its reported success.
        """
        status = local_api.LocalJobStatusWrapper(None)
        workflows = WorkflowData(self.database)
        try:
            with patch.object(local_api, 'status_man', status), patch.object(local_api, '_mpi_launcher', None), \
                 patch('amscjobmanager.globus_transfer_action.globusCopy', return_value='transfer-1') as submit, \
                 patch('amscjobmanager.globus_transfer_action.globusTransferStatus', return_value='ACTIVE') as poll:
                work = self.root / 'compute'
                result = self.root / 'processed'
                workflow = [APIAction([MakeDirectory('local', str(work)), WriteFile('local', str(work / 'input'), '42')]),
                            ExecuteJobComputeAction('local', 'local', 'local', '30', sys.executable,
                                ['-c', 'from pathlib import Path; Path("output").write_text(Path("input").read_text())'],
                                str(work), 1, 0, nodes=1),
                            GlobusTransfer('destination', '/destination', 'source', '/source'),
                            APIAction([CopyFile(str(result), str(work / 'output'))])]
                workflow_id = workflows.enqueueWorkflow(workflow)
                workflows.startWorkflows([workflow_id])
                deadline = time.monotonic() + 5
                while workflows.workflowState(workflow_id)['workflow_stage'] < 2:
                    if time.monotonic() > deadline:
                        self.fail('Local compute did not complete')
                    workflows.progressActiveState(force_poll=True)
                    time.sleep(.01)
                self.assertEqual(submit.call_count, 1)
                self.assertFalse(result.exists())
                self.assertEqual(workflows.headActionStatus(workflow_id), ActionStatus.ACTIVE)
                poll.return_value = 'SUCCEEDED'
                workflows.progressActiveState(force_poll=True)
                self.assertEqual(result.read_text(), '42')
                workflows.progressActiveState()
                self.assertEqual(workflows.workflowStatus(workflow_id), ActionStatus.COMPLETED)
        finally:
            # Reap worker threads before closing the temporary status database.
            with local_api._local_jobs_lock:
                workers = list(local_api._local_jobs.items())
            for jobid, worker in workers:
                with patch.object(local_api, 'status_man', status):
                    local_api.cancelJob(jobid)
                worker['thread'].join(3)
            workflows.conn.close()
            status._status_man.conn.close()


if __name__ == '__main__':
    unittest.main()
