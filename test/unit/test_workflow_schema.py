"""Native Pydantic workflow JSON, typed stage dispatch and persisted action objects."""

import json
import pickle
import unittest

from pydantic import ValidationError

from amscjobmanager.api_actions import APIAction, MakeDirectory, WriteFile, UploadFile, DownloadFile, CopyFile, RunLocal
from amscjobmanager.compute_actions import ExecuteBatchScriptComputeAction, ExecuteJobComputeAction
from amscjobmanager.globus_transfer_action import GlobusTransfer
from amscjobmanager.workflow import workflow_adapter


class WorkflowSchemaTests(unittest.TestCase):
    def test_all_builtin_actions_roundtrip_json_and_pickle(self):
        """Serialize every stage/operation, preserving positional constructors and raw bytes.

        JSON type tags must restore executable action objects, while the existing
        trusted-pickle database representation must still restore the same data.
        Binary content uses a tagged Base64 object to distinguish it from text.
        """
        stages = [
            APIAction([MakeDirectory('local', '/tmp/work'),
                       WriteFile('local', '/tmp/work/data', b'\x00\xff'),
                       WriteFile('local', '/tmp/work/text', 'text \u03c0'),
                       UploadFile('local', '/tmp/work/upload', '/tmp/source'),
                       DownloadFile('local', '/tmp/download', '/tmp/work/data'),
                       CopyFile('/tmp/copy', '/tmp/download'),
                       RunLocal('/bin/true', [], '/tmp')]),
            ExecuteBatchScriptComputeAction('local', '', '', '60', 'date', '/tmp', 1, 0, nodes=1),
            ExecuteJobComputeAction('local', '', '', '60', '/bin/true', [], '/tmp', 1, 0, nodes=1),
            GlobusTransfer('dest', '/result', 'source', '/input', 'filesystem', 'globus'),
        ]
        encoded = workflow_adapter.dump_json(stages)
        wire = json.loads(encoded)
        self.assertEqual([stage['type'] for stage in wire], ['api', 'batch_script', 'execute_job', 'globus_transfer'])
        self.assertEqual(wire[0]['operations'][1]['content'], {'base64': 'AP8='})
        restored = workflow_adapter.validate_json(encoded)
        self.assertEqual(restored, stages)
        self.assertEqual(restored[0].operations[1].content, b'\x00\xff')
        self.assertEqual(pickle.loads(pickle.dumps(stages)), stages)

    def test_invalid_stages_are_rejected_before_execution(self):
        """Reject unrecognized kinds, unsafe overrides, malformed binary data and bad types."""
        compute = {'type': 'execute_job', 'machine': 'local', 'account': '', 'queue': '', 'time': '60',
                   'executable': '/bin/true', 'arguments': [], 'rundir': '/tmp',
                   'nodes': 1, 'ranks_per_node': 1, 'gpus_per_rank': 0}
        cases = [[], [{'operations': []}], [{'type': 'arbitrary_class'}], [dict(compute, allow_unsafe=True)],
                 [dict(compute, nodes=True)], [dict(compute, nodes=0)],
                 [dict(compute, arguments=[1])],
                 [{'type': 'api', 'operations': [{'type': 'write_file', 'machine': 'local',
                    'path': '/tmp/file', 'content': {'base64': 'not valid base64!'}}]}],
                 [{'type': 'api', 'operations': [{'type': 'delete_everything'}]}]]
        for stages in cases:
            with self.subTest(stages=stages), self.assertRaises(ValidationError):
                workflow_adapter.validate_python(stages)


if __name__ == '__main__':
    unittest.main()
