"""Recorded compute-action inspection, filtering, and manager ownership boundaries.

Run: python test/run_tests.py integration --pattern test_compute_action_queries.py -v
Fixtures create execution/history records without starting workers or contacting
providers. Live manager/MCP wallet-wait coverage is in test_mcp_server.
"""

import unittest
from unittest.mock import patch

from pydantic import ValidationError

from support.action_manager_helpers import DirectComputeActions
from amscjobmanager.action_manager import ActionStatus
from amscjobmanager.actions_base import ActionClass
from amscjobmanager.compute_action_manager import ComputeActionFilter
from amscjobmanager.compute_actions import ExecuteBatchScriptComputeAction, ExecuteJobComputeAction
from amscjobmanager.workflow_data import WorkflowData
from amscjobmanager.server.functions import FunctionRegistry


class ComputeActionQueryTests(unittest.TestCase):
    """Query actual manager-owned records while leaving future workflow stages unstarted."""

    def setUp(self):
        """Create isolated storage with deterministic supervision and no provider operations."""
        self.workflows = WorkflowData(_test_action_manager_factories={ActionClass.COMPUTE: DirectComputeActions},
                            poll_freq=float('inf'))
        self.addCleanup(self.workflows.conn.close)
        self.computes = self.workflows.getManager(ActionClass.COMPUTE)
        self.action = ExecuteJobComputeAction(
            machine='frontier', account='project', queue='batch', time='300', nodes=2,
            executable='/secret/program', arguments=['private-argument'], rundir='/tmp/run',
            ranks_per_node=4, gpus_per_rank=1,
        )

    def record(self, action, status, api_status):
        """Persist an owned fixture and return (workflow ID, action ID), both positive ints.

        action is a compute definition; status is ActionStatus; api_status is
        str | None. The workflow has a future second stage with no manager record.
        Record state directly to isolate inspection from submission/cancellation.
        """
        workflow_id = self.workflows.enqueueWorkflow([action, action])
        action_id = self.workflows.workflowState(workflow_id)['head_action_id']
        self.computes._recordSchedulingIntentIfNew(action, workflow_id, action_id)
        with self.computes.conn as conn:
            conn.execute('''UPDATE computes SET action_status = ?, api_status = ?,
                api_key = ?, submission_attempts = ?, last_submission_attempt = ?,
                last_submission_error = ? WHERE action_id = ?''',
                         (status.name, api_status, None if status == ActionStatus.SCHEDULING else 'provider-id',
                          0 if status == ActionStatus.SCHEDULING else 1,
                          None if status == ActionStatus.SCHEDULING else 1234,
                          'server diagnostic' if status == ActionStatus.FAILED else None, action_id))
        return workflow_id, action_id

    def test_lists_current_and_past_records_but_not_future_stages(self):
        """Return both compute definition types across all execution states, with independent IDs."""
        self.assertEqual(self.computes.listActiveAndPastComputeActions().result, [])
        # This entire chain remains pending and must not acquire compute records.
        self.workflows.enqueueWorkflow([self.action, self.action])
        expected = []
        for status in (ActionStatus.SCHEDULING, ActionStatus.ACTIVE, ActionStatus.PROVIDER_HELD,
                       ActionStatus.COMPLETED, ActionStatus.FAILED, ActionStatus.CANCELING, ActionStatus.CANCELED):
            expected.append(self.record(self.action, status, None))
        batch = ExecuteBatchScriptComputeAction(
            machine='other', account='', queue='', time='60', nodes=1,
            script_body='private batch script', rundir='/tmp', ranks_per_node=1, gpus_per_rank=0)
        expected.append(self.record(batch, ActionStatus.COMPLETED, 'completed'))
        result = self.computes.listActiveAndPastComputeActions().result
        self.assertEqual([(row.workflow_id, row.action_id) for row in result], expected)
        self.assertNotEqual(result[0].workflow_id, result[0].action_id)
        self.assertEqual([row.action_status for row in result[:-1]], [status.name for status in (
            ActionStatus.SCHEDULING, ActionStatus.ACTIVE, ActionStatus.PROVIDER_HELD,
            ActionStatus.COMPLETED, ActionStatus.FAILED, ActionStatus.CANCELING, ActionStatus.CANCELED)])
        self.assertEqual(result[-1].action_type, 'ExecuteBatchScriptComputeAction')
        self.assertEqual(result[0].nodes, 2)
        self.assertEqual(result[0].time, '300')
        self.assertEqual(result[0].queue, 'batch')
        self.assertEqual(result[0].account, 'project')
        self.assertIsNone(result[0].api_key)
        self.assertIsNone(result[0].last_submission_attempt)
        self.assertEqual(result[4].last_submission_error, 'server diagnostic')
        self.assertNotIn('private', self.computes.listActiveAndPastComputeActions().model_dump_json())
        self.assertNotIn('/secret/program', self.computes.listActiveAndPastComputeActions().model_dump_json())

    def test_filters_combine_using_and_and_statuses_use_or(self):
        """Match wallet waits and provider statuses with exact casing, IDs, and machine normalization."""
        blocked_workflow, blocked_id = self.record(self.action, ActionStatus.SCHEDULING, 'INSUFFICIENT_WALLET_BALANCE')
        active_workflow, active_id = self.record(self.action, ActionStatus.ACTIVE, 'queued')
        _, failed_id = self.record(self.action, ActionStatus.FAILED, 'failed')
        cases = [
            (ComputeActionFilter(machine='FRONTIER', workflow_id=blocked_workflow, action_id=blocked_id,
                                 statuses=['SCHEDULING', 'ACTIVE'], api_status='INSUFFICIENT_WALLET_BALANCE'), [blocked_id]),
            (ComputeActionFilter(statuses=['ACTIVE', 'FAILED']), [active_id, failed_id]),
            (ComputeActionFilter(workflow_id=active_workflow, api_status='queued'), [active_id]),
            (ComputeActionFilter(api_status='QUEUED'), []),
            (ComputeActionFilter(statuses=[]), []),
            (ComputeActionFilter(statuses=['PENDING']), []),
            (ComputeActionFilter(workflow_id=blocked_workflow, statuses=['ACTIVE']), []),
            (ComputeActionFilter(action_id=999), []),
            (ComputeActionFilter(workflow_id=999), []),
            (ComputeActionFilter(machine='unknown'), []),
            (ComputeActionFilter(api_status="queued' OR 1=1 --"), []),
        ]
        for filters, ids in cases:
            with self.subTest(filters=filters):
                self.assertEqual([row.action_id for row in self.computes.listActiveAndPastComputeActions(filters).result], ids)

    def test_inspection_never_polls_submits_or_progresses_workflows(self):
        """Read cached records only, preserving the owning workflow's stage and all recorded fields."""
        workflow, action_id = self.record(self.action, ActionStatus.ACTIVE, 'queued')
        state = self.workflows.workflowState(workflow)
        with patch.object(self.computes, '_queryStatusInternal', side_effect=AssertionError('Inspection polled')), \
             patch.object(self.computes, '_reconcileScheduledAction', side_effect=AssertionError('Inspection submitted')), \
             patch.object(self.workflows, 'progressActiveState', side_effect=AssertionError('Inspection progressed')):
            result = self.computes.listActiveAndPastComputeActions(ComputeActionFilter(action_id=action_id))
        self.assertEqual(result.result[0].api_status, 'queued')
        self.assertEqual(result.result[0].last_submission_attempt, 1234)
        self.assertEqual(result.result[0].submission_attempts, 1)
        self.assertEqual(self.workflows.workflowState(workflow), state)

    def test_registry_exposes_typed_query_and_rejects_invalid_filters(self):
        """Discover and invoke the new manager-owned function without editing either transport."""
        _, action_id = self.record(self.action, ActionStatus.SCHEDULING, 'INSUFFICIENT_WALLET_BALANCE')
        registry = FunctionRegistry(self.workflows)
        function = next(item for item in registry.describe('mcp') if item.name == 'list_active_and_past_compute_actions')
        self.assertTrue(function.read_only)
        self.assertTrue(function.idempotent)
        self.assertIn('api_status', function.input_schema['properties'])
        result, _ = registry.invoke(self.workflows, 'mcp', function.name, {'api_status': 'INSUFFICIENT_WALLET_BALANCE'})
        self.assertEqual(result['result'][0]['action_id'], action_id)
        for arguments in ({'action_id': True}, {'workflow_id': 0}, {'statuses': ['UNKNOWN']},
                          {'api_status': 42}, {'machine': False}, {'refresh': True}):
            with self.subTest(arguments=arguments), self.assertRaises(ValidationError):
                registry.invoke(self.workflows, 'mcp', function.name, arguments)


if __name__ == '__main__':
    unittest.main()
