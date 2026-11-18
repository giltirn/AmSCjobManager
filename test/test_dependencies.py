"""Nested-list dependency validation and controlled Python Boolean evaluation."""

from itertools import product
import unittest
from unittest.mock import Mock, patch

from pydantic import ValidationError

from amscjobmanager.actions_base import evaluateDependencies
from amscjobmanager.api_actions import APIAction
from amscjobmanager.compute_actions import ExecuteBatchScriptComputeAction, ExecuteJobComputeAction
from amscjobmanager.globus_transfer_action import GlobusTransfer
from amscjobmanager.workflow import workflow_adapter


class DependencyExpressionTests(unittest.TestCase):
    def test_normal_precedence_and_explicit_grouping(self):
        """Check all truth assignments against Python precedence and grouped results.

        A flat mixed expression must use AND precedence rather than left-to-right
        reduction. Nested lists override that precedence as parentheses would.
        """
        for a, b, c in product((False, True), repeat=3):
            completed = {'a': a, 'b': b, 'c': c}.__getitem__
            with self.subTest(a=a, b=b, c=c):
                self.assertEqual(evaluateDependencies(['a', 'OR', 'b', 'AND', 'c'], completed), a or b and c)
                self.assertEqual(evaluateDependencies([['a', 'OR', 'b'], 'AND', 'c'], completed), (a or b) and c)
                self.assertEqual(evaluateDependencies(['a', 'AND', ['b', 'OR', 'c']], completed), a and (b or c))
                self.assertEqual(evaluateDependencies([[['a']], 'OR', ['b', 'AND', ['c']]], completed), a or b and c)

    def test_single_keys_and_absent_conditions(self):
        """Resolve standalone keys and redundant grouping; None requires no lookup."""
        completed = Mock(return_value=False)
        self.assertIs(evaluateDependencies(None, completed), True)
        completed.assert_not_called()
        self.assertIs(evaluateDependencies('prepared', completed), False)
        self.assertIs(evaluateDependencies(['prepared'], completed), False)
        self.assertEqual(completed.call_count, 2)

    def test_invalid_structure_is_rejected_before_completion_lookups(self):
        """Reject malformed trees consistently at action construction and evaluation.

        Include invalid nested groups, token types, operator positions, and the
        superseded object/prefix forms. No invalid expression may query state.
        """
        completed = Mock(return_value=True)
        cases = ['', [], ['a', 'AND'], ['a', 'b'], ['AND', 'a', 'b'], ['a', 'AND', 'OR'],
                 ['a', 'and', 'b'], ['a', 'NOT', 'b'], ['a', 'OR', []],
                 ['a', 'AND', ['b', 'OR']], ['a', 'OR', True], ['a', 'AND', 1],
                 ['a', ['AND'], 'b'], {'all_of': ['a']}, ('a', 'AND', 'b')]
        for condition in cases:
            with self.subTest(condition=condition):
                with self.assertRaises(ValidationError):
                    APIAction(depends_on=condition)
                with self.assertRaises(ValidationError):
                    evaluateDependencies(condition, completed)
        completed.assert_not_called()

    def test_keys_never_enter_the_generated_python_expression(self):
        """Treat arbitrary key text as lookup data and emit only Boolean literals.

        Inspect the actual eval call while still executing it, including its
        empty builtins environment. Python-like keys must never become code.
        """
        key = "__import__('builtins').print('must not execute')"
        completed = Mock(side_effect={key: True, 'a key with spaces': False}.__getitem__)
        with patch('builtins.eval', wraps=eval) as evaluate:
            self.assertIs(evaluateDependencies([key, 'AND', ['a key with spaces', 'OR', key]], completed), True)
        evaluate.assert_called_once_with('(True and (False or True))', {'__builtins__': {}}, {})
        self.assertEqual(completed.call_count, 3)

    def test_completion_lookup_errors_propagate(self):
        """Leave database/provider failure handling to callers rather than return False."""
        with self.assertRaisesRegex(RuntimeError, 'lookup failed'):
            evaluateDependencies(['prepared', 'OR', 'other'], Mock(side_effect=RuntimeError('lookup failed')))

    def test_all_stage_schemas_roundtrip_nested_lists(self):
        """Preserve Python/JSON list expressions for every supported action category.

        Existing positional action arguments remain unchanged. Serialized schemas
        must expose recursive arrays instead of the removed object models.
        """
        condition = ['prepared', 'AND', ['solver_a', 'OR', 'solver_b']]
        stages = [APIAction([], depends_on=condition),
                  ExecuteBatchScriptComputeAction('local', '', '', '60', 'date', '/tmp', 1, 0,
                                                  depends_on=condition, nodes=1),
                  ExecuteJobComputeAction('local', '', '', '60', '/bin/true', [], '/tmp', 1, 0,
                                          depends_on=condition, nodes=1),
                  GlobusTransfer('dest', '/output', 'source', '/input', depends_on=condition)]
        restored = workflow_adapter.validate_json(workflow_adapter.dump_json(stages))
        self.assertEqual(restored, stages)
        self.assertTrue(all(stage.depends_on == condition for stage in restored))
        schema = workflow_adapter.json_schema()
        self.assertIn('DependencyCondition', schema['$defs'])
        self.assertNotIn('AllDependencies', schema['$defs'])
        self.assertNotIn('AnyDependencies', schema['$defs'])


if __name__ == '__main__':
    unittest.main()
