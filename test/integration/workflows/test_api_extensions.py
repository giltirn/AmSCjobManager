"""Python extension operations preserve JSON behavior and durable checkpoints."""

from dataclasses import dataclass
import json
import os
from pathlib import Path
import tempfile
from threading import get_ident
import time
import unittest

from pydantic import ValidationError
from pydantic.dataclasses import dataclass as validated_dataclass

from amscjobmanager.actions_base import ActionClass
from amscjobmanager.action_manager import ActionStatus, SimulatedCrash
from amscjobmanager.api_actions import APIAction, APIActionStageBase, WriteFile
from amscjobmanager.workflow_data import WorkflowData
from amscjobmanager.workflow import workflow_adapter
from support.action_manager_helpers import DirectAPIActions

OBSERVED = []


@dataclass
class RecordOperation(APIActionStageBase):
    """Persist text and observe process state using importable, pickleable fields."""

    path: str
    text: str
    fail: bool = False

    def execute(self, workflow_id):
        """Record workflow_id/process/thread in shared state and write captured text."""
        OBSERVED.append((workflow_id, os.getpid(), get_ident()))
        if self.fail:
            raise RuntimeError('extension failure')
        Path(self.path).write_text(self.text)


@validated_dataclass
class ValidatedOperation(APIActionStageBase):
    """An application dataclass with Pydantic validation and JSON serialization."""

    message: str

    def execute(self, workflow_id):
        """Accept workflow_id without producing external effects."""


class APIExtensionTests(unittest.TestCase):
    def test_python_instances_and_builtin_json_schema(self):
        """Accept extension instances, retaining built-in JSON and binary encodings."""
        operation = ValidatedOperation('captured')
        action = APIAction([operation, WriteFile('local', '/tmp/example', b'\xff')])
        self.assertIs(action.operations[0], operation)
        self.assertIs(workflow_adapter.validate_python([action])[0].operations[0], operation)
        encoded = json.loads(workflow_adapter.dump_json([action]))
        self.assertEqual(encoded[0]['operations'][0], {'message': 'captured'})
        self.assertEqual(encoded[0]['operations'][1]['content'], {'base64': '/w=='})
        builtin = APIAction([{'type': 'write_file', 'machine': 'local', 'path': '/tmp/example', 'content': 'x'}])
        self.assertIsInstance(builtin.operations[0], WriteFile)
        for mode in ('validation', 'serialization'):
            schema = workflow_adapter.json_schema(mode=mode)
            mapping = schema['$defs']['APIAction']['properties']['operations']['items']['discriminator']['mapping']
            self.assertEqual(set(mapping), {'make_directory', 'write_file', 'upload_file', 'download_file', 'copy_file', 'run_local'})
        for value in (object(), lambda workflow_id: None):
            with self.assertRaises(ValidationError):
                APIAction([value])
        with self.assertRaises(ValidationError):
            workflow_adapter.validate_json('[{"type":"api","operations":[{"type":"application_operation"}]}]')
        with self.assertRaises(TypeError):
            APIActionStageBase()

    def test_custom_operations_run_on_worker_in_same_process(self):
        """Real workers see module globals in this process, on a different thread."""
        with tempfile.TemporaryDirectory() as root:
            workflows = WorkflowData(Path(root) / 'workflows.sqlite')
            try:
                workflow_id = workflows.enqueueWorkflow(APIAction([RecordOperation(str(Path(root) / 'result'), 'saved')]))
                workflows.startWorkflows([workflow_id])
                deadline = time.monotonic() + 5
                while workflows.headActionStatus(workflow_id) != ActionStatus.COMPLETED and time.monotonic() < deadline:
                    workflows.progressActiveState()
                    time.sleep(.01)
                self.assertEqual(workflows.headActionStatus(workflow_id), ActionStatus.COMPLETED)
                self.assertEqual(OBSERVED[-1][:2], (workflow_id, os.getpid()))
                self.assertNotEqual(OBSERVED[-1][2], get_ident())
            finally:
                workflows.stopWorkers()
                workflows.conn.close()

    def test_recovery_replays_only_uncheckpointed_operation(self):
        """Reopen SQLite after effects/checkpoints and inspect replayed file effects."""
        for point in ('api_operation_0_checkpoint_persisted', 'api_operation_1_completed'):
            with self.subTest(point=point), tempfile.TemporaryDirectory() as root:
                first, second = Path(root) / 'first', Path(root) / 'second'
                database = Path(root) / 'workflows.sqlite'
                factories = {ActionClass.API: DirectAPIActions}
                workflows = WorkflowData(database, _test_crash_at=point, _test_action_manager_factories=factories)
                operation = RecordOperation(str(second), 'captured')
                workflow_id = workflows.enqueueWorkflow(APIAction([WriteFile('local', str(first), 'first'), operation]))
                # Mutating the caller after enqueue must not change persisted inputs.
                operation.text = 'changed after submission'
                with self.assertRaises(SimulatedCrash):
                    workflows.startWorkflows([workflow_id])
                workflows.conn.close()
                first.write_text('checkpoint preserved')
                second.write_text('partial')
                recovered = WorkflowData(database, _test_action_manager_factories=factories)
                try:
                    recovered.submitSchedulingActions()
                    self.assertEqual(first.read_text(), 'checkpoint preserved')
                    self.assertEqual(second.read_text(), 'captured')
                    self.assertEqual(recovered.headActionStatus(workflow_id), ActionStatus.COMPLETED)
                finally:
                    recovered.conn.close()

    def test_dependencies_and_custom_failure(self):
        """An extension waits for all keys, then records its ordinary exception."""
        with tempfile.TemporaryDirectory() as root:
            workflows = WorkflowData(Path(root) / 'workflows.sqlite', _test_action_manager_factories={ActionClass.API: DirectAPIActions})
            try:
                first = workflows.enqueueWorkflow(APIAction(producer_key='run/first'))
                second = workflows.enqueueWorkflow(APIAction(producer_key='run/second'))
                output = str(Path(root) / 'never')
                consumer = workflows.enqueueWorkflow(APIAction([RecordOperation(output, '', fail=True)],
                                                     depends_on=['run/first', 'AND', 'run/second']))
                workflows.startWorkflows([consumer, first])
                workflows.progressActiveState()
                self.assertEqual(workflows.workflowStatus(consumer), ActionStatus.PENDING)
                workflows.startWorkflows([second])
                workflows.progressActiveState()
                workflows.startWorkflows([consumer])
                workflows.progressActiveState()
                self.assertEqual(workflows.workflowStatus(consumer), ActionStatus.FAILED)
                self.assertFalse(Path(output).exists())
            finally:
                workflows.conn.close()
