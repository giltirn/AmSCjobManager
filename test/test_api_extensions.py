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
from amscjobmanager.job_data import JobData
from amscjobmanager.workflow import workflow_adapter
from action_manager_helpers import DirectAPIActions

OBSERVED = []


@dataclass
class RecordOperation(APIActionStageBase):
    """Persist text and observe process state using importable, pickleable fields."""

    path: str
    text: str
    fail: bool = False

    def execute(self, job_id):
        """Record job_id/process/thread in shared state and write captured text."""
        OBSERVED.append((job_id, os.getpid(), get_ident()))
        if self.fail:
            raise RuntimeError('extension failure')
        Path(self.path).write_text(self.text)


@validated_dataclass
class ValidatedOperation(APIActionStageBase):
    """An application dataclass with Pydantic validation and JSON serialization."""

    message: str

    def execute(self, job_id):
        """Accept job_id without producing external effects."""


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
        for value in (object(), lambda job_id: None):
            with self.assertRaises(ValidationError):
                APIAction([value])
        with self.assertRaises(ValidationError):
            workflow_adapter.validate_json('[{"type":"api","operations":[{"type":"application_operation"}]}]')
        with self.assertRaises(TypeError):
            APIActionStageBase()

    def test_custom_operations_run_on_worker_in_same_process(self):
        """Real workers see module globals in this process, on a different thread."""
        with tempfile.TemporaryDirectory() as root:
            jobs = JobData(Path(root) / 'jobs.sqlite')
            try:
                job_id = jobs.enqueueJob(APIAction([RecordOperation(str(Path(root) / 'result'), 'saved')]))
                jobs.startWorkflows([job_id])
                deadline = time.monotonic() + 5
                while jobs.headActionStatus(job_id) != ActionStatus.COMPLETED and time.monotonic() < deadline:
                    jobs.progressActiveState()
                    time.sleep(.01)
                self.assertEqual(jobs.headActionStatus(job_id), ActionStatus.COMPLETED)
                self.assertEqual(OBSERVED[-1][:2], (job_id, os.getpid()))
                self.assertNotEqual(OBSERVED[-1][2], get_ident())
            finally:
                jobs.stopWorkers()
                jobs.conn.close()

    def test_recovery_replays_only_uncheckpointed_operation(self):
        """Reopen SQLite after effects/checkpoints and inspect replayed file effects."""
        for point in ('api_operation_0_checkpoint_persisted', 'api_operation_1_completed'):
            with self.subTest(point=point), tempfile.TemporaryDirectory() as root:
                first, second = Path(root) / 'first', Path(root) / 'second'
                database = Path(root) / 'jobs.sqlite'
                factories = {ActionClass.API: DirectAPIActions}
                jobs = JobData(database, _test_crash_at=point, _test_action_manager_factories=factories)
                operation = RecordOperation(str(second), 'captured')
                job_id = jobs.enqueueJob(APIAction([WriteFile('local', str(first), 'first'), operation]))
                # Mutating the caller after enqueue must not change persisted inputs.
                operation.text = 'changed after submission'
                with self.assertRaises(SimulatedCrash):
                    jobs.startWorkflows([job_id])
                jobs.conn.close()
                first.write_text('checkpoint preserved')
                second.write_text('partial')
                recovered = JobData(database, _test_action_manager_factories=factories)
                try:
                    recovered.submitSchedulingActions()
                    self.assertEqual(first.read_text(), 'checkpoint preserved')
                    self.assertEqual(second.read_text(), 'captured')
                    self.assertEqual(recovered.headActionStatus(job_id), ActionStatus.COMPLETED)
                finally:
                    recovered.conn.close()

    def test_dependencies_and_custom_failure(self):
        """An extension waits for all keys, then records its ordinary exception."""
        with tempfile.TemporaryDirectory() as root:
            jobs = JobData(Path(root) / 'jobs.sqlite', _test_action_manager_factories={ActionClass.API: DirectAPIActions})
            try:
                first = jobs.enqueueJob(APIAction(producer_key='run/first'))
                second = jobs.enqueueJob(APIAction(producer_key='run/second'))
                output = str(Path(root) / 'never')
                consumer = jobs.enqueueJob(APIAction([RecordOperation(output, '', fail=True)],
                                                     depends_on=['run/first', 'AND', 'run/second']))
                jobs.startWorkflows([consumer, first])
                jobs.progressActiveState()
                self.assertEqual(jobs.jobStatus(consumer), ActionStatus.PENDING)
                jobs.startWorkflows([second])
                jobs.progressActiveState()
                jobs.startWorkflows([consumer])
                jobs.progressActiveState()
                self.assertEqual(jobs.jobStatus(consumer), ActionStatus.FAILED)
                self.assertFalse(Path(output).exists())
            finally:
                jobs.conn.close()
