"""Workflow lifecycle tools with native action schemas, local execution and HTTP CLI.

Run: python -m unittest discover -s test -p test_mcp_workflows.py -v
Requires .[server]; HTTP/asyncio tests need local socket access. Remote Globus
calls alone are mocked. Local files, processes, SQLite and MCP transport are real.
"""

import asyncio
import json
from pathlib import Path
import shlex
import signal
import socket
import shutil
import subprocess
import sys
import tempfile
from threading import Event
import time
import unittest
from unittest.mock import Mock, patch

from amscjobmanager import logging as manager_logging, local_api
from amscjobmanager.api_actions import APIAction, MakeDirectory, WriteFile, UploadFile, DownloadFile, CopyFile, RunLocal
from amscjobmanager.compute_actions import ExecuteBatchScriptComputeAction, ExecuteJobComputeAction
from amscjobmanager.globus_transfer_action import GlobusTransfer
from amscjobmanager.manager import JobManager
from amscjobmanager.workflow import workflow_adapter
from test_mcp_server import Client, call_mcp, run_client


@unittest.skipIf(Client is None, 'Install amsc-job-manager[server]')
class MCPWorkflowTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        """Isolate workflow files and logging; reject unexpected terminal prompts."""
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        for hook in ('wfman_log_func', 'api_log_func'):
            self.enterContext(patch.object(manager_logging, hook, Mock()))
        self.enterContext(patch('builtins.input', side_effect=AssertionError('Unexpected terminal input')))
        status = local_api.LocalJobStatusWrapper(None)
        self.addCleanup(status._status_man.conn.close)
        self.enterContext(patch.object(local_api, 'status_man', status))
        self.enterContext(patch.object(local_api, '_mpi_launcher', None))

    async def _startServer(self, max_workflows_active=0):
        """Create and start an isolated server runtime without binding HTTP sockets.

        max_workflows_active (int, default 0) controls automatic execution.
        Return ManagerHTTPServer with fake-free local JobManager, query routing
        and cleanup installed. Each call uses a different SQLite database.
        """
        from amscjobmanager.server.runner import ManagerHTTPServer

        path = self.root / f'{time.monotonic_ns()}.sqlite'
        server = ManagerHTTPServer(lambda: JobManager(path, poll_freq=.01,
                                                      max_workflows_active=max_workflows_active),
                                   port=0, log_path=self.root / 'server.log')
        self.enterContext(patch.object(manager_logging, 'api_query_user_func', server.runtime.queries.ask))
        self.addCleanup(server.runtime.stop)
        server.runtime.start()
        await asyncio.to_thread(server.runtime._startup_worker.future.result, 3)
        return server

    async def _waitWorkflow(self, client, workflow_id, status):
        """Poll recorded MCP workflow state until status or a twenty-second deadline.

        client (mcp.Client) is connected, workflow_id (int) is the database ID,
        status (str) is the expected overall status. Return the matching summary.
        Fail promptly if execution instead reaches FAILED.
        """
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            response = await asyncio.wait_for(client.call_tool('list_workflows', {'workflow_id': workflow_id}), 3)
            self.assertFalse(response.is_error, response)
            record = response.structured_content['result'][0]
            if record['workflow_status'] == status:
                return record
            self.assertNotEqual(record['workflow_status'], 'FAILED', record)
            await asyncio.sleep(.02)
        listing = await client.call_tool('list_workflows', {})
        self.fail(f'Workflow {workflow_id} did not reach {status}; recorded workflows: {listing.structured_content}')

    async def test_native_stage_submission_idempotency_and_cli_schema(self):
        """Enqueue every built-in stage through MCP without executing any remote work.

        Retry a changed payload under the same key, inspect workflow metadata
        through public state, and verify discovered schemas drive CLI
        parsing/help for nested API operations and all stage alternatives.
        """
        from amscjobmanager.server.cli import parse_function_arguments

        server = await self._startServer()
        native = [APIAction([WriteFile('local', str(self.root / 'data'), b'\xff')]),
                  ExecuteBatchScriptComputeAction('local', '', '', '60', 'date', str(self.root), 1, 0, nodes=1),
                  ExecuteJobComputeAction('local', '', '', '60', '/bin/true', [], str(self.root), 1, 0, nodes=1),
                  GlobusTransfer('dest', '/out', 'source', '/in')]
        stages = json.loads(workflow_adapter.dump_json(native))
        async with Client(server.mcp_server, cache=None) as client:
            result = await call_mcp(client, 'submit_workflow', {'stages': stages, 'job_group': 'original', 'submission_key': 'request'})
            self.assertFalse(result.is_error)
            workflow_id = result.structured_content['workflow_id']
            self.assertEqual(result.structured_content['workflow_status'], 'PENDING')
            original = server.runtime.manager(lambda jobs: jobs.jobState(workflow_id))
            self.assertNotIn('workflow', original)
            retry = await call_mcp(client, 'submit_workflow', {'stages': stages[:1], 'job_group': 'changed', 'submission_key': 'request'})
            self.assertEqual(retry.structured_content['workflow_id'], workflow_id)
            self.assertEqual(server.runtime.manager(lambda jobs: jobs.jobState(workflow_id)), original)
            unkeyed = await client.call_tool('submit_workflow', {'stages': stages[:1]})
            self.assertNotEqual(unkeyed.structured_content['workflow_id'], workflow_id)

            tools = await client.list_tools()
            tool = next(tool for tool in tools.tools if tool.name == 'submit_workflow')
            parsed = parse_function_arguments(tool, ['--stages', *[json.dumps(stage) for stage in stages], '--submission-key', 'cli-key'])
            self.assertEqual(parsed, {'stages': stages, 'submission_key': 'cli-key'})
            with self.assertRaises(ValueError):
                parse_function_arguments(tool, ['--stages', '{"operations":[]}'])
            help_text = parse_function_arguments(tool, ['--help'])
            for field in ('batch_script', 'execute_job', 'globus_transfer', 'run_local', 'base64', 'post_launch', 'operations'):
                self.assertIn(field, help_text)

    async def test_dependency_split_and_join_runs_local_branches_concurrently(self):
        """Submit a dependency graph through real MCP using two real local processes.

        Each branch creates a start marker and waits for the other's marker,
        so sequential execution would fail. A joined API stage may write its
        result only after both compute producers have completed successfully.
        """
        server = await self._startServer(10)
        root = self.root
        stages = [[APIAction([MakeDirectory('local', str(root / name)) for name in ('a', 'b')] +
                            [WriteFile('local', str(root / 'prepared'), 'prepared')], producer_key='ready')]]
        for name, other in (('a', 'b'), ('b', 'a')):
            script = (f'printf started > ../{name}.started\n'
                      'count=0\n'
                      f'while [ ! -f ../{other}.started ]; do\n'
                      '  count=$((count + 1))\n'
                      '  [ "$count" -lt 100 ] || exit 1\n'
                      '  sleep .05\n'
                      'done\n'
                      f'printf done > ../{name}.result\n')
            stages.append([ExecuteJobComputeAction(
                'local', '', '', '60', '/bin/sh', ['-c', script], str(root / name), 1, 0,
                depends_on='ready', producer_key=name, nodes=1)])
        stages.append([APIAction([WriteFile('local', str(root / 'joined'), 'joined')],
                                 depends_on=['a', 'AND', 'b'])])
        async with Client(server.mcp_server, cache=None) as client:
            workflows = []
            for workflow in stages:
                response = await client.call_tool('submit_workflow', {
                    'stages': json.loads(workflow_adapter.dump_json(workflow))})
                self.assertFalse(response.is_error, response)
                workflows.append(response.structured_content['workflow_id'])
            await self._waitWorkflow(client, workflows[-1], 'COMPLETED')
            for workflow_id in workflows:
                await self._waitWorkflow(client, workflow_id, 'COMPLETED')
        self.assertEqual((root / 'prepared').read_text(), 'prepared')
        self.assertEqual((root / 'joined').read_text(), 'joined')
        self.assertTrue(all((root / f'{name}.result').read_text() == 'done' for name in ('a', 'b')))

    async def test_dependency_validation_cancellation_and_submission_retries(self):
        """Reject unknown/canceled references and cascade cancellation through tool submissions.

        Automatic execution is disabled so all workflows remain queued. Invalid
        submissions must leave no records, and retries must retain the original
        canceled workflow even when the replacement payload has unknown keys.
        """
        server = await self._startServer()
        async with Client(server.mcp_server, cache=None) as client:
            invalid = await client.call_tool('submit_workflow', {
                'stages': [{'type': 'api', 'depends_on': 'unknown'}]})
            self.assertTrue(invalid.is_error)
            self.assertEqual(invalid.structured_content['error']['code'], 'invalid_input')
            self.assertIn('Unknown dependency key', str(invalid.content))
            self.assertEqual(server.runtime.manager(lambda jobs: jobs.listWorkflows()), [])
            root = await client.call_tool('submit_workflow', {
                'stages': [{'type': 'api', 'producer_key': 'root'}]})
            dependent = await client.call_tool('submit_workflow', {
                'stages': [{'type': 'api', 'depends_on': 'root', 'producer_key': 'tail'}],
                'submission_key': 'dependent'})
            last = await client.call_tool('submit_workflow', {
                'stages': [{'type': 'api', 'depends_on': 'tail'}]})
            self.assertFalse(root.is_error or dependent.is_error or last.is_error)
            canceled = await client.call_tool('cancel_workflow', {
                'workflow_id': root.structured_content['workflow_id']})
            self.assertFalse(canceled.is_error)
            listing = await client.call_tool('list_workflows', {'statuses': ['CANCELED']})
            self.assertEqual(len(listing.structured_content['result']), 3)
            rejected = await client.call_tool('submit_workflow', {
                'stages': [{'type': 'api', 'depends_on': 'tail'}]})
            self.assertTrue(rejected.is_error)
            self.assertEqual(rejected.structured_content['error']['code'], 'invalid_input')
            retry = await client.call_tool('submit_workflow', {
                'stages': [{'type': 'api', 'depends_on': 'replacement-unknown'}],
                'submission_key': 'dependent'})
            self.assertFalse(retry.is_error)
            self.assertEqual(retry.structured_content, {
                'workflow_id': dependent.structured_content['workflow_id'], 'workflow_status': 'CANCELED'})
            self.assertEqual(len(server.runtime.manager(lambda jobs: jobs.listWorkflows())), 3)

    async def test_invalid_requests_enqueue_nothing(self):
        """Reject invalid later stages and API operations before persisting a workflow."""
        server = await self._startServer()
        good = {'type': 'api', 'operations': []}
        cases = [[], [good, {'type': 'unknown'}],
                 [{'type': 'api', 'operations': [], 'allow_unsafe': True}],
                 [{'type': 'api', 'operations': [{'type': 'write_file', 'machine': 'local',
                    'path': str(self.root / 'bad'), 'content': {'base64': '!'}}]}],
                 [{'type': 'api', 'operations': [{'type': 'run_local', 'executable': '/bin/true',
                    'arguments': [], 'cwd': str(self.root), 'timeout': 0}]}]]
        async with Client(server.mcp_server, cache=None) as client:
            for stages in cases:
                with self.subTest(stages=stages):
                    response = await client.call_tool('submit_workflow', {'stages': stages})
                    self.assertTrue(response.is_error)
            self.assertEqual(server.runtime.manager(lambda jobs: jobs.listWorkflows()), [])

    async def test_cancel_pending_retains_workflow_and_submission_key(self):
        """Cancel via MCP twice and retain the record/key without submitting compute work."""
        server = await self._startServer()
        stage = json.loads(workflow_adapter.dump_json([
            ExecuteJobComputeAction('local', '', '', '60', '/bin/sleep', ['30'], str(self.root), 1, 0, nodes=1)]))
        async with Client(server.mcp_server, cache=None) as client:
            submitted = await client.call_tool('submit_workflow', {'stages': stage, 'submission_key': 'cancel-me'})
            workflow_id = submitted.structured_content['workflow_id']
            for _ in range(2):
                canceled = await call_mcp(client, 'cancel_workflow', {'workflow_id': workflow_id})
                self.assertEqual(canceled.structured_content, {'workflow_id': workflow_id, 'workflow_status': 'CANCELED'})
            again = await client.call_tool('submit_workflow', {'stages': stage, 'submission_key': 'cancel-me'})
            self.assertEqual(again.structured_content, canceled.structured_content)
            missing = await client.call_tool('cancel_workflow', {'workflow_id': workflow_id + 99})
            self.assertEqual(missing.structured_content['error']['code'], 'workflow_not_found')
            for invalid in (0, True):
                self.assertTrue((await client.call_tool('cancel_workflow', {'workflow_id': invalid})).is_error)
            self.assertEqual(len(server.runtime.manager(lambda jobs: jobs.listWorkflows())), 1)

    async def test_lifecycle_tools_report_unavailable_manager(self):
        """Return structured errors before startup, after initialization failure and shutdown."""
        from amscjobmanager.server.runner import ManagerHTTPServer

        for state in ('created', 'failed', 'stopped'):
            with self.subTest(state=state):
                def initialize():
                    """Return a manager or simulate a bootstrap failure for this subtest."""
                    if state == 'failed':
                        raise RuntimeError('Simulated bootstrap failure')
                    return JobManager(max_workflows_active=0)

                server = ManagerHTTPServer(initialize, log_path=self.root / 'server.log')
                self.addCleanup(server.runtime.stop)
                if state != 'created':
                    server.runtime.start()
                    await asyncio.to_thread(server.runtime._startup_worker.future.result, 3)
                if state == 'stopped':
                    server.runtime.stop()
                async with Client(server.mcp_server, cache=None) as client:
                    for tool, arguments in (('submit_workflow', {'stages': [{'type': 'api', 'operations': []}]}),
                                            ('cancel_workflow', {'workflow_id': 1})):
                        response = await client.call_tool(tool, arguments)
                        self.assertEqual(response.structured_content['error']['code'],
                                         'server_initializing' if state == 'created' else 'manager_unavailable')

    async def test_mixed_workflow_executes_in_order(self):
        """Run setup, both compute forms, asynchronous transfer and local processing.

        All API files and compute processes are real. Replace only the Globus
        provider with a small file-copy service, checking asynchronous and safe
        submission options. The final API stage verifies transferred binary data.
        """
        batch, job, transferred = self.root / 'batch', self.root / 'job', self.root / 'transferred'
        payload = b'\x00\xffMPI data\n'
        source = self.root / 'source'
        server = await self._startServer(10)
        native = [APIAction([MakeDirectory('local', str(path)) for path in (batch, job, transferred)] +
                            [WriteFile('local', str(source), payload),
                             UploadFile('local', str(batch / 'input'), str(source))]),
                  ExecuteBatchScriptComputeAction('local', '', '', '60', 'cp input batch-result', str(batch), 1, 0, nodes=1),
                  ExecuteJobComputeAction('local', '', '', '60', '/bin/sh',
                    ['-c', f'cp {shlex.quote(str(batch / "batch-result"))} result'], str(job), 1, 0,
                    pre_launch='printf pre > pre-marker', post_launch='printf post > post-marker', nodes=1),
                  GlobusTransfer('destination', str(transferred), 'source', str(job / 'result'), 'filesystem', 'filesystem'),
                  APIAction([DownloadFile('local', str(self.root / 'download'), str(transferred / 'result')),
                             CopyFile(str(self.root / 'copy'), str(self.root / 'download')),
                             RunLocal(sys.executable, ['-c', "from pathlib import Path; assert Path('copy').read_bytes() == " + repr(payload) + "; Path('processed').write_text('done')"], str(self.root))])]

        def transfer(dest_endpoint, dest_path, source_endpoint, source_path, **options):
            """Copy the submitted file locally and return a fake asynchronous Globus ID.

            Endpoints/paths (str) and options (dict) match globusCopy's interface.
            Assert safety/asynchrony flags; return str without any network call.
            """
            self.assertFalse(options['allow_unsafe'])
            self.assertFalse(options['block_until_complete'])
            shutil.copyfile(source_path, Path(dest_path) / Path(source_path).name)
            return 'fake-transfer-id'

        with patch('amscjobmanager.globus_transfer_action.globusCopy', side_effect=transfer) as submitted, \
             patch('amscjobmanager.globus_transfer_action.globusTransferStatus', return_value='SUCCEEDED'), \
             patch('amscjobmanager.globus_transfer_action.findGlobusTransferByLabel', return_value=None):
            async with Client(server.mcp_server, cache=None) as client:
                response = await call_mcp(client, 'submit_workflow', {'stages': json.loads(workflow_adapter.dump_json(native))})
                self.assertFalse(response.is_error)
                await self._waitWorkflow(client, response.structured_content['workflow_id'], 'COMPLETED')
            submitted.assert_called_once()
        self.assertEqual((self.root / 'copy').read_bytes(), payload)
        self.assertEqual((self.root / 'processed').read_text(), 'done')
        self.assertEqual((job / 'pre-marker').read_text(), 'pre')
        self.assertEqual((job / 'post-marker').read_text(), 'post')

    async def test_cancel_submitted_globus_transfer(self):
        """Cancel a tool-submitted transfer through its real manager/reconciliation path.

        Model only the remote Globus service: submission returns an ID, polling
        reports INACTIVE, and cancellation changes it to FAILED (Globus represents
        canceled tasks as failed). Listing must expose a PROVIDER_HELD head in an
        ACTIVE workflow, and cancellation must eventually produce CANCELED.
        """
        server = await self._startServer(10)
        submitted, canceled = Event(), Event()

        def transfer(*args, **kwargs):
            """Record fake provider acceptance; args/kwargs are globusCopy inputs; return its ID."""
            submitted.set()
            return 'cancel-transfer-id'

        def status(task_id):
            """Return the fake provider state for task_id (str)."""
            return 'FAILED' if canceled.is_set() else 'INACTIVE'

        with patch('amscjobmanager.globus_transfer_action.globusCopy', side_effect=transfer), \
             patch('amscjobmanager.globus_transfer_action.globusTransferStatus', side_effect=status), \
             patch('amscjobmanager.globus_transfer_action.findGlobusTransferByLabel', return_value=None), \
             patch('amscjobmanager.globus_transfer_action.cancelGlobusTransfer', side_effect=lambda task_id: canceled.set()) as cancel:
            async with Client(server.mcp_server, cache=None) as client:
                response = await client.call_tool('submit_workflow', {'stages': [{
                    'type': 'globus_transfer', 'source_endpoint': 'source', 'source_path': '/in',
                    'dest_endpoint': 'destination', 'dest_path': '/out'}]})
                self.assertFalse(response.is_error)
                workflow_id = response.structured_content['workflow_id']
                self.assertTrue(await asyncio.to_thread(submitted.wait, 3))
                # Wait for the head projection as well as provider acceptance;
                # workflow ACTIVE also covers the preceding SCHEDULING interval.
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    record = await self._waitWorkflow(client, workflow_id, 'ACTIVE')
                    if record['head_action_status'] == 'PROVIDER_HELD':
                        break
                    await asyncio.sleep(.02)
                else:
                    self.fail('Held transfer was not exposed by workflow listing')
                self.assertEqual(record['api_status'], 'INACTIVE')
                receipt = await client.call_tool('cancel_workflow', {'workflow_id': workflow_id})
                self.assertFalse(receipt.is_error)
                await self._waitWorkflow(client, workflow_id, 'CANCELED')
                cancel.assert_called_once_with('cancel-transfer-id')

    async def test_cancel_queried_api_action_releases_its_worker(self):
        """Cancel a tool-submitted API action awaiting a response and reject stale replies."""
        server = await self._startServer(10)
        original = WriteFile.execute

        def write(operation, job_id):
            """Ask before writing the selected output, then perform normal API execution."""
            manager_logging.wfapiUserQuery('write', 'Allow output?')
            original(operation, job_id)

        with patch.object(WriteFile, 'execute', write):
            async with Client(server.mcp_server, cache=None) as client:
                submitted = await client.call_tool('submit_workflow', {'stages': [{'type': 'api', 'operations': [
                    {'type': 'write_file', 'machine': 'local', 'path': str(self.root / 'never'), 'content': 'no'}]}]})
                workflow_id = submitted.structured_content['workflow_id']
                deadline = time.monotonic() + 4
                while True:
                    pending = (await client.call_tool('list_user_queries', {})).structured_content['result']
                    if pending:
                        break
                    if time.monotonic() > deadline:
                        self.fail('API worker did not ask its question')
                    await asyncio.sleep(.01)
                canceled = await client.call_tool('cancel_workflow', {'workflow_id': workflow_id})
                self.assertIn(canceled.structured_content['workflow_status'], ('CANCELING', 'CANCELED'))
                await self._waitWorkflow(client, workflow_id, 'CANCELED')
                stale = await client.call_tool('respond_to_user_query', {
                    'session_id': pending[0]['session_id'], 'query_id': pending[0]['query_id'], 'response': 'late'})
                self.assertTrue(stale.is_error)
                self.assertFalse((self.root / 'never').exists())


@unittest.skipIf(Client is None, 'Install amsc-job-manager[server]')
class MCPWorkflowCLITests(unittest.TestCase):
    def test_real_cli_submits_cancels_and_reuses_keys_after_server_restart(self):
        """Exercise actual HTTP tools and both CLI input modes against the server utility.

        Submit API setup followed by both local compute forms, verify their
        filesystem effects, then cancel running sleep jobs in both forms. Restart
        the server on the same database and retry the completed submission key.
        Child cwd is temporary so local status files cannot touch repository data.
        """
        repository = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(prefix='amsc-mcp-workflows-') as directory:
            root = Path(directory)
            config = root / 'config.json'
            config.write_text(json.dumps({'iri_implementations': {}, 'sandbox_directories': {'local': [directory]}}))
            with socket.socket() as reserved:
                reserved.bind(('127.0.0.1', 0))
                port = reserved.getsockname()[1]
            base = [sys.executable, str(repository / 'utilities' / 'manager_client.py'),
                    '--url', f'http://127.0.0.1:{port}/mcp', '--timeout', '5']
            batch, job = root / 'batch', root / 'job'
            native = [APIAction([MakeDirectory('local', str(batch)), MakeDirectory('local', str(job))]),
                      ExecuteBatchScriptComputeAction('local', '', '', '60', 'printf batch > marker', str(batch), 1, 0, nodes=1),
                      ExecuteJobComputeAction('local', '', '', '60', '/bin/sh',
                        ['-c', f'test -f {shlex.quote(str(batch / "marker"))} && printf done > result'],
                        str(job), 1, 0, pre_launch='printf pre > before', post_launch='printf post > after', nodes=1)]
            stages = json.loads(workflow_adapter.dump_json(native))
            document = {'tool': 'submit_workflow', 'arguments': {
                'stages': stages, 'job_group': 'cli-test', 'submission_key': 'cli-workflow'}}
            workflow_id = None
            for session in range(2):
                with (root / 'admin.log').open('w+') as admin, (root / 'stderr.log').open('w+') as errors:
                    process = subprocess.Popen([sys.executable, str(repository / 'utilities' / 'manager_server.py'),
                        str(config), '--database', str(root / 'workflows.sqlite'), '--port', str(port),
                        '--poll-interval', '.02'], cwd=root, stdout=admin, stderr=errors)
                    try:
                        deadline = time.monotonic() + 10
                        while True:
                            self.assertIsNone(process.poll(), 'Server exited during startup')
                            admin.seek(0)
                            if 'Server started:' in admin.read():
                                break
                            if time.monotonic() > deadline:
                                self.fail('Server did not announce startup')
                            time.sleep(.05)
                        # Listener readiness precedes manager readiness; wait for
                        # listing to succeed before attempting workflow mutation.
                        while True:
                            result = run_client(base + ['list_workflows'], capture_output=True, text=True, timeout=10)
                            response = json.loads(result.stdout)
                            if response['success']:
                                break
                            if time.monotonic() > deadline:
                                self.fail('Workflow manager did not become ready')
                            time.sleep(.05)
                        result = run_client(base + ['--file', '-'], input=json.dumps(document),
                                            capture_output=True, text=True, timeout=10)
                        self.assertEqual(result.returncode, 0, result.stderr)
                        receipt = json.loads(result.stdout)['results'][0]['result']
                        if session:
                            self.assertEqual(receipt, {'workflow_id': workflow_id, 'workflow_status': 'COMPLETED'})
                            continue
                        workflow_id = receipt['workflow_id']

                        # Traditional options are derived from the same native
                        # schema. Retrying the key must return the same workflow.
                        result = run_client(base + ['submit_workflow', '--stages',
                            *[json.dumps(stage) for stage in stages], '--submission-key', 'cli-workflow'],
                            capture_output=True, text=True, timeout=10)
                        self.assertEqual(result.returncode, 0, result.stderr)
                        self.assertEqual(json.loads(result.stdout)['results'][0]['result']['workflow_id'], workflow_id)
                        deadline = time.monotonic() + 20
                        while True:
                            result = run_client(base + ['list_workflows', '--workflow-id', str(workflow_id)],
                                                capture_output=True, text=True, timeout=10)
                            self.assertEqual(result.returncode, 0, result.stderr)
                            record = json.loads(result.stdout)['results'][0]['result'][0]
                            if record['workflow_status'] == 'COMPLETED':
                                break
                            self.assertNotEqual(record['workflow_status'], 'FAILED', record)
                            if time.monotonic() > deadline:
                                self.fail('CLI-submitted workflow did not complete')
                            time.sleep(.1)
                        self.assertEqual((job / 'result').read_text(), 'done')
                        self.assertEqual((job / 'before').read_text(), 'pre')
                        self.assertEqual((job / 'after').read_text(), 'post')

                        # Cancel active local processes through the remote tool,
                        # checking provider ACTIVE rather than merely queued state.
                        for kind in ('execute_job', 'batch_script'):
                            run_dir = root / kind
                            run_dir.mkdir()
                            stage = {'type': kind, 'machine': 'local', 'account': '', 'queue': '', 'time': '60',
                                     'rundir': str(run_dir), 'nodes': 1, 'ranks_per_node': 1, 'gpus_per_rank': 0}
                            stage.update({'executable': '/bin/sleep', 'arguments': ['30']} if kind == 'execute_job'
                                         else {'script_body': 'sleep 30'})
                            result = run_client(base + ['submit_workflow', '--stages', json.dumps(stage)],
                                                capture_output=True, text=True, timeout=10)
                            self.assertEqual(result.returncode, 0, result.stderr)
                            sleeping = json.loads(result.stdout)['results'][0]['result']['workflow_id']
                            deadline = time.monotonic() + 10
                            while True:
                                result = run_client(base + ['list_workflows', '--workflow-id', str(sleeping)],
                                                    capture_output=True, text=True, timeout=10)
                                self.assertEqual(result.returncode, 0, result.stderr)
                                record = json.loads(result.stdout)['results'][0]['result'][0]
                                if record['api_status'] == 'active':
                                    break
                                self.assertNotEqual(record['workflow_status'], 'FAILED', record)
                                if time.monotonic() > deadline:
                                    self.fail('Sleep process did not become active')
                                time.sleep(.1)
                            started = time.monotonic()
                            result = run_client(base + ['cancel_workflow', '--workflow-id', str(sleeping)],
                                                capture_output=True, text=True, timeout=10)
                            self.assertEqual(result.returncode, 0, result.stderr)
                            while True:
                                result = run_client(base + ['list_workflows', '--workflow-id', str(sleeping)],
                                                    capture_output=True, text=True, timeout=10)
                                self.assertEqual(result.returncode, 0, result.stderr)
                                record = json.loads(result.stdout)['results'][0]['result'][0]
                                if record['workflow_status'] == 'CANCELED':
                                    break
                                if time.monotonic() - started > 10:
                                    self.fail('Cancellation waited for the sleep process to finish')
                                time.sleep(.1)

                        # Help must expose fields from both stage and operation
                        # variants without any hand-maintained CLI argument list.
                        result = run_client(base + ['submit_workflow', '--help'],
                                            capture_output=True, text=True, timeout=10)
                        self.assertEqual(result.returncode, 0, result.stderr)
                        for text in ('batch_script', 'execute_job', 'globus_transfer', 'write_file', 'submission-key'):
                            self.assertIn(text, result.stdout)
                    finally:
                        process.send_signal(signal.SIGTERM)
                        try:
                            process.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait(timeout=3)
                        errors.seek(0)
                        self.assertNotIn('Traceback', errors.read())


if __name__ == '__main__':
    unittest.main()
