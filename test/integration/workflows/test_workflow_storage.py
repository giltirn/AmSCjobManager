"""Normalized queued-action storage, historical state, and transaction recovery.

Use real SQLite and local file operations. Deterministic API dispatch isolates
database crash boundaries; the worker regression suite covers real supervision.
"""

import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from pydantic import ValidationError

from support.action_manager_helpers import DirectAPIActions
from amscjobmanager.action_manager import ActionStatus, SimulatedCrash, _unser
from amscjobmanager.actions_base import ActionClass
from amscjobmanager.api_actions import APIAction, WriteFile
from amscjobmanager.compute_actions import ExecuteJobComputeAction
from amscjobmanager.globus_transfer_action import GlobusTransfer
from amscjobmanager.workflow_data import WorkflowData
from amscjobmanager.manager import WorkflowManager
from amscjobmanager.workflow import workflow_adapter


class WorkflowStorageTests(unittest.TestCase):
    def setUp(self):
        """Create temporary storage and make API execution deterministic; return None."""
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.database = self.root / 'workflows.sqlite'
        adapter = patch('amscjobmanager.workflow_data.APIActions', DirectAPIActions)
        adapter.start()
        self.addCleanup(adapter.stop)
        self.workflows = WorkflowData(self.database)
        self.addCleanup(self.workflows.conn.close)
        self.addCleanup(self.workflows.stopWorkers)

    def test_every_stage_has_a_queryable_row_and_id_before_execution(self):
        """Queue mixed action types and inspect SQL rows before any worker starts.

        Definitions, dependency expressions, categories, and stable unique IDs
        must exist for future stages. Workflow heads are derived rather than stored.
        """
        expression = ['prepared', 'AND', ['prepared', 'OR', 'prepared']]
        stages = [APIAction(producer_key='prepared'),
                  ExecuteJobComputeAction('local', '', '', '60', '/bin/true', [], '/tmp', 1, 0,
                                          depends_on=expression, producer_key='computed', nodes=1),
                  GlobusTransfer('dest', '/output', 'source', '/input', depends_on='computed')]
        workflow_id = self.workflows.enqueueWorkflow(stages)
        with self.workflows.conn as conn:
            rows = conn.execute('SELECT * FROM actions WHERE workflow_id = ? ORDER BY workflow_stage',
                                (workflow_id,)).fetchall()
            columns = {row['name'] for row in conn.execute('PRAGMA table_info(workflows)')}
            tables = {row['name'] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
            action_columns = {row['name'] for row in conn.execute('PRAGMA table_info(actions)')}
            foreign_keys = conn.execute('PRAGMA foreign_key_list(actions)').fetchall()
        # Workflow identity uses one name in workflow storage and every owned
        # action record; local compute-job storage is a separate API contract.
        self.assertIn('workflows', tables)
        self.assertNotIn('jobs', tables)
        self.assertTrue({'workflow_id', 'workflow_group'} <= columns)
        self.assertTrue({'job_id', 'job_group'}.isdisjoint(columns))
        self.assertIn('workflow_id', action_columns)
        self.assertNotIn('job_id', action_columns)
        self.assertTrue(any(row['table'] == 'workflows' and row['from'] == 'workflow_id'
                            and row['to'] == 'workflow_id' for row in foreign_keys))
        for table in ('computes', 'transfers', 'api_actions'):
            with self.subTest(table=table), self.workflows.conn as conn:
                owned_columns = {row['name'] for row in conn.execute(f'PRAGMA table_info({table})')}
                self.assertIn('workflow_id', owned_columns)
                self.assertNotIn('job_id', owned_columns)
        self.assertTrue({'workflow', 'head_action_id', 'head_action_type', 'head_action_class',
                         'head_action_status', 'cancellation_status'}.isdisjoint(columns))
        self.assertIn('workflow_status', columns)
        self.assertNotIn('action_ids', tables)
        self.assertEqual([row['workflow_stage'] for row in rows], [0, 1, 2])
        self.assertEqual([row['action_class'] for row in rows], ['API', 'COMPUTE', 'TRANSFER'])
        self.assertEqual([row['action_type'] for row in rows], [type(stage).__name__ for stage in stages])
        self.assertEqual([_unser(row['details']) for row in rows], stages)
        self.assertTrue(all(row['action_status'] == 'PENDING' for row in rows))
        self.assertEqual(json.loads(rows[1]['depends_on']), expression)
        self.assertEqual(json.loads(rows[2]['depends_on']), 'computed')
        self.assertEqual([row['producer_key'] for row in rows], ['prepared', 'computed', None])
        self.assertEqual(len({row['action_id'] for row in rows}), 3)
        # Status summaries must not deserialize or rebuild action definitions.
        with patch('amscjobmanager.workflow_data._unser', side_effect=AssertionError('Unexpected action read')), \
             patch('amscjobmanager.workflow_data._ser', side_effect=AssertionError('Unexpected action serialization')):
            summary = self.workflows.workflowState(workflow_id)
            self.assertEqual(self.workflows.listWorkflows(), [summary])
        self.assertNotIn('workflow', summary)
        self.assertNotIn('action_details', summary)
        self.assertEqual(summary['head_action_id'], rows[0]['action_id'])
        self.assertTrue(all(manager.getMaxActionId() is None for manager in self.workflows.action_man.values()))

    def test_history_and_earlier_stage_dependencies_survive_reopening(self):
        """Execute a dependent chain, retaining per-stage success and IDs.

        The second action depends on the earlier stage publishing the same key.
        Reopen between stages and verify the completed stage is not replayed.
        """
        first, second = self.root / 'first', self.root / 'second'
        workflow_id = self.workflows.enqueueWorkflow([
            APIAction([WriteFile('local', str(first), 'first')], producer_key='same'),
            APIAction([WriteFile('local', str(second), 'second')], depends_on='same', producer_key='same'),
        ])
        with self.workflows.conn as conn:
            ids = [row[0] for row in conn.execute('SELECT action_id FROM actions ORDER BY workflow_stage')]
        self.workflows.startWorkflows([workflow_id])
        self.assertTrue(first.exists())
        self.assertFalse(second.exists())
        first.write_text('preserve completed effect')
        self.workflows.conn.close()

        recovered = WorkflowData(self.database)
        self.addCleanup(recovered.conn.close)
        self.addCleanup(recovered.stopWorkers)
        recovered.progressActiveState()
        recovered.progressActiveState()
        self.assertEqual(recovered.workflowStatus(workflow_id), ActionStatus.COMPLETED)
        self.assertEqual(first.read_text(), 'preserve completed effect')
        self.assertEqual(second.read_text(), 'second')
        with recovered.conn as conn:
            rows = conn.execute('SELECT action_id, action_status FROM actions ORDER BY workflow_stage').fetchall()
        self.assertEqual([row['action_id'] for row in rows], ids)
        self.assertEqual([row['action_status'] for row in rows], ['COMPLETED', 'COMPLETED'])

    def test_submission_key_retry_and_failed_enqueue_are_atomic(self):
        """Reuse a whole chain on retry and roll back partially inserted invalid chains."""
        workflow_id = self.workflows.enqueueWorkflow([APIAction(), APIAction()], submission_key='request')
        self.assertEqual(self.workflows.enqueueWorkflow([APIAction()] * 3, submission_key='request'), workflow_id)
        # The first stage is inserted before classifying the invalid second one;
        # failure must roll back its workflow, action row, and submission key.
        with self.assertRaises(Exception):
            self.workflows.enqueueWorkflow([APIAction(), object()], submission_key='invalid')
        with self.workflows.conn as conn:
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM workflows').fetchone()[0], 1)
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM actions').fetchone()[0], 2)
            self.assertIsNone(conn.execute('SELECT workflow_id FROM workflows WHERE submission_key = ?', ('invalid',)).fetchone())

    def test_cancel_preserves_success_and_cancels_remaining_queued_stages(self):
        """Cancel between stages and retain the successful producer's historical status.

        A canceled chain must never start its queued file write after reopening.
        Canceling an already completed workflow must also preserve action success.
        """
        output = self.root / 'must-not-exist'
        workflow_id = self.workflows.enqueueWorkflow([APIAction(producer_key='done'),
                                      APIAction([WriteFile('local', str(output), 'unexpected')])])
        self.workflows.startWorkflows([workflow_id])
        self.workflows.cancelWorkflow(workflow_id)
        self.assertEqual(self.workflows.workflowStatus(workflow_id), ActionStatus.CANCELED)
        self.assertEqual(self.workflows.headActionStatus(workflow_id), ActionStatus.COMPLETED)
        self.assertEqual(self.workflows.countWorkflowsWithStatus(
            [ActionStatus.PENDING, ActionStatus.ACTIVE, ActionStatus.CANCELING]), 0)
        self.assertEqual(self.workflows.countWorkflowsWithStatus(ActionStatus.CANCELED), 1)
        with self.workflows.conn as conn:
            self.assertEqual([row[0] for row in conn.execute(
                'SELECT action_status FROM actions ORDER BY workflow_stage')], ['COMPLETED', 'CANCELED'])
        self.workflows.conn.close()

        recovered = WorkflowData(self.database)
        self.addCleanup(recovered.conn.close)
        self.addCleanup(recovered.stopWorkers)
        recovered.startWorkflows()
        recovered.progressActiveState()
        self.assertFalse(output.exists())
        self.assertEqual(recovered.workflowStatus(workflow_id), ActionStatus.CANCELED)
        completed = recovered.enqueueWorkflow(APIAction())
        action_id = recovered.workflowState(completed)['head_action_id']
        recovered.startWorkflows([completed])
        recovered.progressActiveState()
        recovered.cancelWorkflow(completed)
        with recovered.conn as conn:
            self.assertEqual(conn.execute('SELECT action_status FROM actions WHERE action_id = ?',
                                          (action_id,)).fetchone()[0], 'COMPLETED')

    def test_recover_scheduling_intent_with_all_future_ids_already_assigned(self):
        """Crash after head intent, reopen, and execute only the selected stored stage.

        The later stage already has an ID but must remain unstarted on recovery.
        Normal progression then runs it and retains both successful records.
        """
        first, second = self.root / 'first', self.root / 'second'
        workflow_id = self.workflows.enqueueWorkflow([APIAction([WriteFile('local', str(first), 'first')]),
                                      APIAction([WriteFile('local', str(second), 'second')])])
        with self.workflows.conn as conn:
            ids = [row[0] for row in conn.execute('SELECT action_id FROM actions ORDER BY workflow_stage')]
        self.workflows._test_crash_at = 'workflow_head_scheduling_persisted'
        with self.assertRaises(SimulatedCrash):
            self.workflows.startWorkflows([workflow_id])
        self.assertFalse(first.exists())
        self.workflows.conn.close()

        recovered = WorkflowData(self.database)
        self.addCleanup(recovered.conn.close)
        self.addCleanup(recovered.stopWorkers)
        self.assertTrue(first.exists())
        self.assertFalse(second.exists())
        self.assertEqual(recovered.workflowState(workflow_id)['head_action_id'], ids[0])
        self.assertFalse(recovered.action_man[ActionClass.API].hasAction(ids[1]))
        recovered.progressActiveState()
        recovered.progressActiveState()
        self.assertTrue(second.exists())
        self.assertEqual(recovered.workflowStatus(workflow_id), ActionStatus.COMPLETED)

    def test_rows_enforce_stage_uniqueness_and_workflow_ownership(self):
        """Use real SQL writes to reject duplicate stage positions and orphan actions."""
        workflow_id = self.workflows.enqueueWorkflow(APIAction())
        for replacement_workflow in (workflow_id, workflow_id + 99):
            with self.subTest(workflow_id=replacement_workflow), self.assertRaises(sqlite3.IntegrityError):
                with self.workflows.conn as conn:
                    conn.execute('INSERT INTO actions(workflow_id, workflow_stage, action_class, action_type, '
                                 'action_status, details, last_status_change) '
                                 'SELECT ?, workflow_stage, action_class, action_type, action_status, details, '
                                 'last_status_change FROM actions WHERE workflow_id = ?', (replacement_workflow, workflow_id))

    def test_recover_cancellation_before_any_execution_record_exists(self):
        """Crash at scheduling and cancellation intents without running either stage.

        Reopening must finish cancellation from stored definitions and stable
        IDs, leave both action rows CANCELED, and create neither output file.
        """
        outputs = [self.root / 'first', self.root / 'second']
        workflow_id = self.workflows.enqueueWorkflow([APIAction([WriteFile('local', str(path), 'unexpected')])
                                       for path in outputs])
        self.workflows._test_crash_at = 'workflow_head_scheduling_persisted'
        with self.assertRaises(SimulatedCrash):
            self.workflows.startWorkflows([workflow_id])
        self.workflows._test_crash_at = 'workflow_head_canceling_persisted'
        with self.assertRaises(SimulatedCrash):
            self.workflows.cancelWorkflow(workflow_id)
        self.assertEqual(self.workflows.workflowStatus(workflow_id), ActionStatus.CANCELING)
        self.workflows.conn.close()

        recovered = WorkflowData(self.database)
        self.addCleanup(recovered.conn.close)
        self.addCleanup(recovered.stopWorkers)
        recovered.progressActiveState()
        recovered.startWorkflows()
        self.assertEqual(recovered.workflowStatus(workflow_id), ActionStatus.CANCELED)
        with recovered.conn as conn:
            self.assertEqual([row[0] for row in conn.execute(
                'SELECT action_status FROM actions ORDER BY workflow_stage')], ['CANCELED', 'CANCELED'])
        self.assertFalse(any(path.exists() for path in outputs))

    def test_dependency_metadata_roundtrips_json_and_rejects_invalid_shapes(self):
        """Round-trip nested Boolean lists through schema validation alone.

        Python positional constructors remain usable; JSON restores typed nested
        lists and string keys. Reject missing operands and unsupported operators.
        """
        stages = [APIAction([], depends_on=['a', 'OR', ['b', 'AND', 'c']], producer_key='result')]
        encoded = workflow_adapter.dump_json(stages)
        self.assertEqual(workflow_adapter.validate_json(encoded), stages)
        for condition in ([], ['a', 'AND'], ['OR', 'a'], ['a', 'NOT', 'b'],
                          ['a', 'AND', 1], {'all_of': ['a']}):
            with self.subTest(condition=condition), self.assertRaises(ValidationError):
                APIAction(depends_on=condition)

    def test_workflow_status_persists_across_intermediate_completion_and_restart(self):
        """Record overall progress separately from each completed sequential head.

        Execute real writes in two stages. Check that intermediate completion is
        ACTIVE, final advancement is COMPLETED, and reopening retains the result.
        Overall status reads must not deserialize action definitions.
        """
        outputs = [self.root / 'first', self.root / 'second']
        workflow_id = self.workflows.enqueueWorkflow([APIAction([WriteFile('local', str(path), 'done')]) for path in outputs])
        with patch('amscjobmanager.workflow_data._unser', side_effect=AssertionError('Unexpected definition read')):
            self.assertEqual(self.workflows.workflowStatus(workflow_id), ActionStatus.PENDING)
        self.workflows.startWorkflows([workflow_id])
        self.assertEqual(self.workflows.headActionStatus(workflow_id), ActionStatus.COMPLETED)
        self.assertEqual(self.workflows.workflowState(workflow_id)['workflow_status'], ActionStatus.ACTIVE)
        self.assertEqual(self.workflows.countWorkflowsWithStatus(ActionStatus.ACTIVE), 1)
        self.assertEqual(self.workflows.countWorkflowsWithStatus(ActionStatus.COMPLETED), 0)
        self.assertEqual(self.workflows.listWorkflows(statuses=ActionStatus.COMPLETED), [])
        self.assertEqual([row['workflow_id'] for row in self.workflows.listWorkflows(statuses=ActionStatus.ACTIVE)], [workflow_id])
        self.workflows.progressActiveState()
        self.assertEqual(self.workflows.headActionStatus(workflow_id), ActionStatus.COMPLETED)
        self.assertEqual(self.workflows.workflowStatus(workflow_id), ActionStatus.ACTIVE)
        self.workflows.progressActiveState()
        self.assertTrue(all(path.read_text() == 'done' for path in outputs))
        self.assertEqual(self.workflows.countWorkflowsWithStatus(ActionStatus.ACTIVE), 0)
        self.assertEqual(self.workflows.countWorkflowsWithStatus(ActionStatus.COMPLETED), 1)
        with self.workflows.conn as conn:
            self.assertEqual(conn.execute('SELECT workflow_status FROM workflows WHERE workflow_id = ?',
                                          (workflow_id,)).fetchone()[0], 'COMPLETED')
        self.workflows.conn.close()

        recovered = WorkflowData(self.database)
        self.addCleanup(recovered.conn.close)
        self.addCleanup(recovered.stopWorkers)
        self.assertEqual(recovered.workflowStatus(workflow_id), ActionStatus.COMPLETED)
        self.assertEqual(len(recovered.listWorkflows(statuses=ActionStatus.COMPLETED)), 1)

    def test_failed_workflow_status_persists_without_starting_remaining_stages(self):
        """Record an API failure on both layers and keep later work unstarted after recovery."""
        output = self.root / 'must-not-exist'
        # Writing text over the existing workspace directory fails through the
        # normal API; no provider or execution-state database is mocked.
        workflow_id = self.workflows.enqueueWorkflow([APIAction([WriteFile('local', str(self.root), 'invalid')]),
                                      APIAction([WriteFile('local', str(output), 'unexpected')])])
        self.workflows.startWorkflows([workflow_id])
        self.assertEqual(self.workflows.workflowState(workflow_id)['workflow_status'], ActionStatus.FAILED)
        with self.workflows.conn as conn:
            self.assertEqual(conn.execute('SELECT workflow_status FROM workflows WHERE workflow_id = ?',
                                          (workflow_id,)).fetchone()[0], 'FAILED')
            self.assertEqual([row[0] for row in conn.execute('SELECT action_status FROM actions ORDER BY workflow_stage')],
                             ['FAILED', 'PENDING'])
        self.workflows.conn.close()

        recovered = WorkflowData(self.database)
        self.addCleanup(recovered.conn.close)
        self.addCleanup(recovered.stopWorkers)
        recovered.startWorkflows()
        recovered.progressActiveState()
        self.assertEqual(recovered.workflowStatus(workflow_id), ActionStatus.FAILED)
        self.assertEqual(len(recovered.listWorkflows(statuses=ActionStatus.FAILED)), 1)
        self.assertFalse(output.exists())

    def test_late_execution_result_cannot_clear_workflow_cancellation(self):
        """Cancel between execution and its status copy, then deliver the stale result.

        The first API stage has finished externally, but WorkflowData still records
        SCHEDULING. Simulate a cancellation during that status read; its returned
        completion must not restore ACTIVE or start the remaining stage.
        """
        output = self.root / 'must-not-exist'
        workflow_id = self.workflows.enqueueWorkflow([APIAction(), APIAction([WriteFile('local', str(output), 'unexpected')])])
        manager = self.workflows.action_man[ActionClass.API]
        query = manager.queryRecordedStatus

        def cancel_before_returning(action_id, *, force_update=False):
            """Read action_id (int), cancel its chain, and return the prior status tuple.

            force_update (bool) is forwarded unchanged. Cancellation uses the
            original query method to avoid recursively entering this hook.
            """
            result = query(action_id, force_update=force_update)
            with patch.object(manager, 'queryRecordedStatus', query):
                self.workflows.cancelWorkflow(workflow_id)
            return result

        with patch.object(manager, 'queryRecordedStatus', side_effect=cancel_before_returning):
            self.workflows.startWorkflows([workflow_id])
        self.workflows.progressActiveState()
        self.workflows.startWorkflows()
        self.assertEqual(self.workflows.workflowStatus(workflow_id), ActionStatus.CANCELED)
        self.assertFalse(output.exists())
        with self.workflows.conn as conn:
            self.assertEqual(conn.execute('SELECT workflow_status FROM workflows WHERE workflow_id = ?',
                                          (workflow_id,)).fetchone()[0], 'CANCELED')

    def test_action_and_workflow_status_updates_roll_back_together(self):
        """Abort the workflow-status write after action projection and recover SQLite.

        A temporary trigger simulates database failure in the shared transaction.
        The action projection must roll back to SCHEDULING while workflow intent
        stays ACTIVE. Reopening reconciles the completed execution without replay.
        """
        output = self.root / 'artifact'
        workflow_id = self.workflows.enqueueWorkflow(APIAction([WriteFile('local', str(output), 'original')]))
        with self.workflows.conn as conn:
            conn.execute("""CREATE TRIGGER fail_workflow_status BEFORE UPDATE OF workflow_status ON workflows
                            WHEN OLD.workflow_status = 'ACTIVE' AND NEW.workflow_status = 'ACTIVE'
                            BEGIN SELECT RAISE(ABORT, 'simulated workflow status write failure'); END""")
        with self.assertRaisesRegex(sqlite3.IntegrityError, 'simulated workflow status write failure'):
            self.workflows.startWorkflows([workflow_id])
        self.assertEqual(output.read_text(), 'original')
        self.assertEqual(self.workflows.workflowStatus(workflow_id), ActionStatus.ACTIVE)
        self.assertEqual(self.workflows.headActionStatus(workflow_id), ActionStatus.SCHEDULING)
        output.write_text('preserve completed effect')
        with self.workflows.conn as conn:
            conn.execute('DROP TRIGGER fail_workflow_status')
        self.workflows.conn.close()

        recovered = WorkflowData(self.database)
        self.addCleanup(recovered.conn.close)
        self.addCleanup(recovered.stopWorkers)
        recovered.progressActiveState()
        self.assertEqual(recovered.workflowStatus(workflow_id), ActionStatus.COMPLETED)
        self.assertEqual(output.read_text(), 'preserve completed effect')

    def test_shutdown_does_not_wait_for_a_canceled_chain_with_a_completed_head(self):
        """Stop a real manager after cancellation between stages without a drain delay.

        Keep the completed head for history while canceling the overall chain.
        An attempted drain sleep fails immediately instead of hanging the test.
        """
        manager = WorkflowManager(self.root / 'manager.sqlite', max_workflows_active=0)
        self.addCleanup(manager.stop, wait_until_done=False, close_database=True)
        manager.start()
        with manager as workflows:
            workflow_id = workflows.enqueueWorkflow([APIAction(), APIAction()])
            workflows.startWorkflows([workflow_id])
            workflows.cancelWorkflow(workflow_id)
            self.assertEqual(workflows.headActionStatus(workflow_id), ActionStatus.COMPLETED)
            self.assertEqual(workflows.workflowStatus(workflow_id), ActionStatus.CANCELED)
        with patch('amscjobmanager.manager.time.sleep', side_effect=AssertionError('Unexpected drain wait')):
            manager.stop(close_database=True)


if __name__ == '__main__':
    unittest.main()
