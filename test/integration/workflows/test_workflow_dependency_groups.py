"""Inspect declared workflow graphs using real normalized storage and no provider operations.

Run: python test/run_tests.py integration --pattern test_workflow_dependency_groups.py -v
"""

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from amscjobmanager.action_manager import ActionStatus
from amscjobmanager.api_actions import APIAction
from amscjobmanager.workflow_data import WorkflowData


class WorkflowDependencyGroupTests(unittest.TestCase):
    """Query connected components and per-action metadata without executing queued work."""

    def setUp(self):
        """Create a temporary database with automatic workflow starts disabled; return None."""
        temporary = self.enterContext(tempfile.TemporaryDirectory())
        self.database = Path(temporary) / 'workflows.sqlite'
        self.workflows = WorkflowData(self.database, max_workflows_active=0)
        self.addCleanup(self.workflows.conn.close)
        self.addCleanup(self.workflows.stopWorkers)

    def test_empty_independent_and_internal_only_workflows(self):
        """Keep singleton groups despite matching group labels or wholly internal dependencies."""
        self.assertEqual(self.workflows.listWorkflowDependencyGroups(), [])
        first = self.workflows.enqueueWorkflow([
            APIAction(producer_key='internal'), APIAction(depends_on='internal'),
        ], workflow_group='same campaign')
        second = self.workflows.enqueueWorkflow(APIAction(), workflow_group='same campaign')
        third = self.workflows.enqueueWorkflow(APIAction(producer_key='unconsumed'))
        groups = self.workflows.listWorkflowDependencyGroups()
        self.assertEqual([[row['workflow_id'] for row in group] for group in groups], [[first], [second], [third]])
        self.assertEqual(self.workflows.listWorkflowDependencyGroups(second), [groups[1]])
        self.assertEqual(groups[1][0]['producer_keys'], [])
        self.assertEqual(groups[1][0]['dependency_conditions'], [])

        # IDs belong to distinct tables: the second stage of the first workflow
        # means the next workflow's first action ID differs from its workflow ID.
        with self.workflows.conn as conn:
            action_id = conn.execute('SELECT action_id FROM actions WHERE workflow_id = ?', (second,)).fetchone()[0]
        self.assertNotEqual(action_id, second)

    def test_split_join_and_nested_alternatives_include_both_directions(self):
        """Connect prerequisites, siblings, and downstream consumers through every expression branch."""
        root = self.workflows.enqueueWorkflow(APIAction(producer_key='prepared'))
        left = self.workflows.enqueueWorkflow(APIAction(depends_on='prepared', producer_key='left'))
        right = self.workflows.enqueueWorkflow(APIAction(depends_on='prepared', producer_key='right'))
        isolated = self.workflows.enqueueWorkflow(APIAction(producer_key='unrelated'))
        alternative = self.workflows.enqueueWorkflow(APIAction(producer_key='alternative'))
        condition = ['left', 'AND', ['right', 'OR', 'alternative']]
        join = self.workflows.enqueueWorkflow(APIAction(depends_on=condition))
        expected = [root, left, right, alternative, join]
        groups = self.workflows.listWorkflowDependencyGroups()
        self.assertEqual([[row['workflow_id'] for row in group] for group in groups], [expected, [isolated]])
        for workflow_id in expected:
            with self.subTest(workflow_id=workflow_id):
                self.assertEqual(self.workflows.listWorkflowDependencyGroups(workflow_id), [groups[0]])
        self.assertEqual(groups[0][-1]['dependency_conditions'][0]['depends_on'], condition)

    def test_future_stages_keep_action_ids_duplicate_keys_and_conditions(self):
        """Retain each action's metadata in stage order, without deduplication or unpickling."""
        parent = self.workflows.enqueueWorkflow(APIAction(producer_key='ready'))
        condition = ['ready', 'OR', ['ready', 'AND', 'ready']]
        child = self.workflows.enqueueWorkflow([
            APIAction(),
            APIAction(depends_on=condition, producer_key='result'),
            APIAction(depends_on='ready', producer_key='result'),
        ])
        with self.workflows.conn as conn:
            action_ids = [row[0] for row in conn.execute(
                'SELECT action_id FROM actions WHERE workflow_id = ? ORDER BY workflow_stage', (child,))]
        with patch('amscjobmanager.workflow_data._unser', side_effect=AssertionError('Definitions unpickled')):
            group = self.workflows.listWorkflowDependencyGroups(child)[0]
        self.assertEqual([row['workflow_id'] for row in group], [parent, child])
        self.assertEqual(group[1]['producer_keys'], [
            {'action_id': action_ids[1], 'producer_key': 'result'},
            {'action_id': action_ids[2], 'producer_key': 'result'},
        ])
        self.assertEqual(group[1]['dependency_conditions'], [
            {'action_id': action_ids[1], 'depends_on': condition},
            {'action_id': action_ids[2], 'depends_on': 'ready'},
        ])

        # Returned metadata must be independent of persisted JSON and later reads.
        group[1]['dependency_conditions'][0]['depends_on'][0] = 'edited by caller'
        self.assertEqual(self.workflows.listWorkflowDependencyGroups(child)[0][1]
                         ['dependency_conditions'][0]['depends_on'], condition)

    def test_duplicate_producers_connect_only_once_a_consumer_exists(self):
        """Rebuild groups dynamically as consumers and additional alternative producers arrive."""
        first = self.workflows.enqueueWorkflow(APIAction(producer_key='shared'))
        second = self.workflows.enqueueWorkflow(APIAction(producer_key='shared'))
        self.assertEqual([[row['workflow_id'] for row in group]
                          for group in self.workflows.listWorkflowDependencyGroups()], [[first], [second]])
        consumer = self.workflows.enqueueWorkflow(APIAction(depends_on='shared'))
        self.assertEqual([row['workflow_id'] for row in self.workflows.listWorkflowDependencyGroups(first)[0]],
                         [first, second, consumer])
        late = self.workflows.enqueueWorkflow(APIAction(producer_key='shared'))
        self.assertEqual([row['workflow_id'] for row in self.workflows.listWorkflowDependencyGroups(consumer)[0]],
                         [first, second, consumer, late])

    def test_statuses_do_not_prune_declared_relationships_or_change_state(self):
        """Include every recorded workflow status and avoid progression, polling, or writes.

        Assign workflow statuses directly to isolate graph inspection from
        execution/cancellation machinery. No workers have started, and a full
        SQLite dump must remain identical after the query.
        """
        root = self.workflows.enqueueWorkflow(APIAction(producer_key='historical'))
        states = (ActionStatus.PENDING, ActionStatus.ACTIVE, ActionStatus.COMPLETED,
                  ActionStatus.FAILED, ActionStatus.CANCELING, ActionStatus.CANCELED)
        descendants = [self.workflows.enqueueWorkflow(APIAction(depends_on='historical')) for _ in states]
        with self.workflows.conn as conn:
            for workflow_id, status in zip(descendants, states):
                conn.execute('UPDATE workflows SET workflow_status = ? WHERE workflow_id = ?',
                             (status.name, workflow_id))
            conn.execute('UPDATE workflows SET workflow_status = ? WHERE workflow_id = ?',
                         (ActionStatus.COMPLETED.name, root))
            before = list(conn.iterdump())
        with patch.object(self.workflows, 'getManager', side_effect=AssertionError('Action manager queried')), \
             patch.object(self.workflows, 'progressActiveState', side_effect=AssertionError('Workflow progressed')), \
             patch.object(self.workflows, 'progressActiveActions', side_effect=AssertionError('Polling requested')):
            group = self.workflows.listWorkflowDependencyGroups()[0]
        self.assertEqual([row['workflow_id'] for row in group], [root, *descendants])
        self.assertEqual([row['workflow_status'] for row in group[1:]], list(states))
        with self.workflows.conn as conn:
            self.assertEqual(list(conn.iterdump()), before)

    def test_reopen_reconstructs_groups_without_a_persistent_graph_table(self):
        """Reopen queued normalized records and recover the same groups and stable action IDs."""
        root = self.workflows.enqueueWorkflow(APIAction(producer_key='ready'))
        self.workflows.enqueueWorkflow(APIAction(depends_on='ready'))
        expected = self.workflows.listWorkflowDependencyGroups(root)
        with self.workflows.conn as conn:
            tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")]
        self.workflows.stopWorkers()
        self.workflows.conn.close()
        reopened = WorkflowData(self.database, max_workflows_active=0)
        self.addCleanup(reopened.conn.close)
        self.addCleanup(reopened.stopWorkers)
        self.assertEqual(reopened.listWorkflowDependencyGroups(), expected)
        with reopened.conn as conn:
            self.assertEqual([row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")], tables)

    def test_invalid_and_unknown_ids_raise_without_changing_the_graph(self):
        """Reject booleans, nonintegers, nonpositive IDs, and missing explicit workflows."""
        root = self.workflows.enqueueWorkflow(APIAction())
        expected = self.workflows.listWorkflowDependencyGroups()
        for workflow_id in (True, False, '1', 1.0, 0, -1, root + 100):
            with self.subTest(workflow_id=workflow_id), self.assertRaises(ValueError):
                self.workflows.listWorkflowDependencyGroups(workflow_id)
        self.assertEqual(self.workflows.listWorkflowDependencyGroups(), expected)


if __name__ == '__main__':
    unittest.main()
