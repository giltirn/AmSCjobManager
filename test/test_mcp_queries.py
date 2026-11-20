"""MCP/CLI query delivery and real server-process restart recovery.

Run: python -m unittest discover -s test -p test_mcp_queries.py -v
Requires .[server]. HTTP subprocess tests need loopback socket access. Provider
calls are simulated; no external credentials, MPI installation or site is used.
"""

import asyncio
from functools import partial
import json
import multiprocessing
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock, patch

from amscjobmanager import logging as manager_logging
from amscjobmanager.api_actions import APIAction, WriteFile
from amscjobmanager.compute_actions import ExecuteJobComputeAction
from amscjobmanager.manager import WorkflowManager
from test_mcp_server import Client, call_mcp


def serve_question_workflow(directory, ready, stop):
    """Run a real HTTP runtime with a startup question and one queried API workflow.

    directory (str) contains persistent SQLite/work files. ready (pipe sender)
    receives {'port': int} after HTTP readiness, or {'error': str}; stop (Event)
    requests cleanup. Return None. Reopening the directory recovers its stored
    workflow and asks a fresh question. Parent tests may kill this process to
    exercise actual crash recovery rather than orderly in-memory continuation.
    """
    from contextlib import redirect_stdout, redirect_stderr
    from amscjobmanager.manager_config import readManagerConfigFile, setupManager
    from amscjobmanager.server.runner import ManagerHTTPServer

    root = Path(directory)
    config = root / 'config.json'
    config.write_text(json.dumps({'iri_implementations': {}, 'sandbox_directories': {'local': [directory]}}))
    original = WriteFile.execute

    def write(operation, workflow_id):
        """Ask before the recoverable blocked write, then execute it for workflow_id."""
        if operation.path == str(root / 'blocked'):
            manager_logging.wfapiUserQuery('Workflow question', 'Authorize this write')
        original(operation, workflow_id)

    def initialize():
        """Ask during bootstrap, configure local API and create/recover the manager."""
        manager_logging.wfapiUserQuery('Startup question', 'Confirm initialization')
        setupManager(readManagerConfigFile(config))
        manager = WorkflowManager(root / 'workflow.sqlite', poll_freq=.05)
        if not manager.workflow_data.listWorkflows():
            manager.workflow_data.enqueueWorkflow(APIAction([WriteFile('local', str(root / 'blocked'), 'done')]), workflow_group='blocked')
            manager.workflow_data.enqueueWorkflow(APIAction([WriteFile('local', str(root / 'independent'), 'done')]), workflow_group='independent')
        return manager

    server = ManagerHTTPServer(initialize, port=0, log_path=root / 'server.log')
    runtime = server.runtime
    thread = None
    with (root / 'server.log').open('a', buffering=1) as log, redirect_stdout(log), redirect_stderr(log), \
         patch.object(manager_logging, 'wfman_log_func', partial(print, file=log, flush=True)), \
         patch.object(manager_logging, 'api_log_func', partial(print, file=log, flush=True)), \
         patch.object(manager_logging, 'api_query_user_func', runtime.queries.ask), \
         patch.object(WriteFile, 'execute', write):
        try:
            thread = threading.Thread(target=server.run, daemon=True)
            thread.start()
            deadline = time.monotonic() + 8
            while not server.started or server.endpoint.endswith(':0/mcp'):
                if not thread.is_alive() or time.monotonic() > deadline:
                    raise TimeoutError('Listener did not become ready')
                time.sleep(.01)
            ready.send({'port': int(server.endpoint.split(':')[-1].split('/')[0])})
            stop.wait(60)
        except Exception as exc:
            ready.send({'error': str(exc)})
            raise
        finally:
            server.should_exit = True
            if thread is not None:
                thread.join(5)
            runtime.stop()
            ready.close()


@unittest.skipIf(Client is None, 'Install amsc-job-manager[server]')
class MCPQueryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        """Isolate artifacts/logs and fail immediately if server code uses terminal input."""
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.logs = Mock()
        for hook in (patch.object(manager_logging, 'wfman_log_func', self.logs),
                     patch.object(manager_logging, 'api_log_func', self.logs),
                     patch('builtins.input', side_effect=AssertionError('Server used terminal input'))):
            hook.start()
            self.addCleanup(hook.stop)

    async def _pending(self, client):
        """Poll MCP client until one question is visible; return its public dictionary.

        client (mcp.Client) must be connected. Bound waiting to four seconds;
        polling does not consume the question or acquire manager locks.
        """
        deadline = time.monotonic() + 4
        while time.monotonic() < deadline:
            response = await asyncio.wait_for(client.call_tool('list_user_queries', {}), timeout=2)
            self.assertFalse(response.is_error)
            if response.structured_content['result']:
                return response.structured_content['result'][0]
            await asyncio.sleep(.01)
        self.fail('No pending MCP query')

    async def _workflow(self, client, workflow_id, status):
        """Wait for workflow_id (int) to reach workflow status (str); return its record.

        client (mcp.Client) is connected. Bound waiting to five seconds, keeping
        reads independent of provider polling and response delivery.
        """
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            response = await asyncio.wait_for(client.call_tool('list_workflows', {'workflow_id': workflow_id}), timeout=2)
            self.assertFalse(response.is_error)
            record = response.structured_content['result'][0]
            if record['workflow_status'] == status:
                return record
            await asyncio.sleep(.02)
        self.fail(f'Workflow {workflow_id} did not reach {status}')

    async def test_startup_questions_can_be_answered_before_manager_readiness(self):
        """Inspect/reply through MCP while initialization has no manager to query.

        Verify tool discovery, unavailable workflow response, stale/duplicate
        rejection and strict input validation. Reply acknowledgment excludes the
        answer and means delivery only; startup completes on its own worker.
        """
        from amscjobmanager.server.runner import ManagerHTTPServer

        def initialize():
            """Ask during initialization, then return a memory-only WorkflowManager."""
            self.assertEqual(manager_logging.wfapiUserQuery('startup', 'Approve setup'), 'private-code')
            return WorkflowManager(max_workflows_active=0)

        server = ManagerHTTPServer(initialize, port=0, log_path=self.root / 'server.log')
        runtime = server.runtime
        self.enterContext(patch.object(manager_logging, 'api_query_user_func', runtime.queries.ask))
        self.addCleanup(runtime.stop)
        runtime.start()
        async with Client(server.mcp_server, cache=None) as client:
            tools = await call_mcp(client)
            self.assertEqual({tool.name for tool in tools.tools},
                             {'list_user_queries', 'respond_to_user_query'})
            response = await call_mcp(client, 'list_workflows')
            self.assertEqual(response.structured_content['error']['code'], 'server_initializing')
            question = await self._pending(client)
            reply = {'session_id': 'previous-session', 'query_id': question['query_id'], 'response': 'private-code'}
            response = await client.call_tool('respond_to_user_query', reply)
            self.assertEqual(response.structured_content['error']['code'], 'stale_session')
            reply['session_id'] = question['session_id']
            response = await client.call_tool('respond_to_user_query', reply)
            self.assertEqual(response.structured_content, {'accepted': True})
            self.assertNotIn('private-code', response.model_dump_json())
            await asyncio.to_thread(runtime._startup_worker.future.result, 2)
            self.assertEqual(runtime.state, 'running')
            response = await call_mcp(client, 'list_workflows')
            self.assertEqual(response.structured_content, {'result': []})
            response = await client.call_tool('respond_to_user_query', reply)
            self.assertEqual(response.structured_content['error']['code'], 'query_not_pending')
            for invalid in (0, True):
                response = await client.call_tool('respond_to_user_query', dict(reply, query_id=invalid))
                self.assertTrue(response.is_error)
        self.assertNotIn('private-code', str(self.logs.call_args_list))

    async def test_api_and_compute_questions_leave_other_workflows_responsive(self):
        """Answer queried API and compute submission workers while other workflows finish.

        Replace only the provider and a selected file-operation question. The
        real managers, SQLite checkpoints, MCP SDK and thread handoff remain.
        """
        from amscjobmanager.server.runner import ManagerHTTPServer

        for kind in ('api', 'compute'):
            with self.subTest(kind=kind):
                blocked, other = self.root / f'blocked-{kind}', self.root / f'other-{kind}'
                ids = {}
                original = WriteFile.execute

                def write(operation, workflow_id):
                    """Ask for this variant's selected file write, then execute normally."""
                    if operation.path == str(blocked):
                        manager_logging.wfapiUserQuery('API operation', 'Authorize file write')
                    original(operation, workflow_id)

                def submit(*args, **kwargs):
                    """Ask during fake compute submission, returning a provider ID string."""
                    manager_logging.wfapiUserQuery('Compute submission', 'Authenticate')
                    return 'remote-1'

                def initialize():
                    """Prepare a queried workflow and independent API work; return WorkflowManager."""
                    manager = WorkflowManager(self.root / f'{kind}.sqlite', poll_freq=.01)
                    action = (APIAction([WriteFile('local', str(blocked), 'done')]) if kind == 'api' else
                              ExecuteJobComputeAction('local', '', '', '60', '/bin/true', [], str(self.root), 1, 0, nodes=1))
                    ids['waiting'] = manager.workflow_data.enqueueWorkflow(action)
                    ids['other'] = manager.workflow_data.enqueueWorkflow(APIAction([WriteFile('local', str(other), 'done')]))
                    return manager

                server = ManagerHTTPServer(initialize, port=0, log_path=self.root / 'server.log')
                runtime = server.runtime
                self.enterContext(patch.object(manager_logging, 'api_query_user_func', runtime.queries.ask))
                try:
                    with patch.object(WriteFile, 'execute', write), \
                         patch('amscjobmanager.compute_actions.executeJob', side_effect=submit) as submitted, \
                         patch('amscjobmanager.compute_action_manager.getJobState', return_value='completed'):
                        runtime.start()
                        async with Client(server.mcp_server, cache=None) as client:
                            question = await self._pending(client)
                            await self._workflow(client, ids['other'], 'COMPLETED')
                            self.assertFalse(blocked.exists())
                            response = await client.call_tool('respond_to_user_query',
                                {'session_id': question['session_id'], 'query_id': question['query_id'], 'response': 'yes'})
                            self.assertFalse(response.is_error)
                            await self._workflow(client, ids['waiting'], 'COMPLETED')
                            if kind == 'compute':
                                submitted.assert_called_once()
                            else:
                                self.assertEqual(blocked.read_text(), 'done')
                finally:
                    runtime.stop()

    async def test_canceled_action_rejects_its_pending_response(self):
        """Cancel a queried API worker and reject its now-stale MCP response."""
        from amscjobmanager.server.runner import ManagerHTTPServer
        original = WriteFile.execute
        ids = {}

        def write(operation, workflow_id):
            """Wait for authorization before performing the file write."""
            manager_logging.wfapiUserQuery('write', 'Allow write?')
            original(operation, workflow_id)

        def initialize():
            """Prepare the queried API workflow and return its WorkflowManager."""
            manager = WorkflowManager()
            ids['workflow'] = manager.workflow_data.enqueueWorkflow(APIAction([WriteFile('local', str(self.root / 'never'), 'never')]))
            return manager

        server = ManagerHTTPServer(initialize, port=0, log_path=self.root / 'server.log')
        runtime = server.runtime
        self.enterContext(patch.object(manager_logging, 'api_query_user_func', runtime.queries.ask))
        try:
            with patch.object(WriteFile, 'execute', write):
                runtime.start()
                async with Client(server.mcp_server, cache=None) as client:
                    question = await self._pending(client)
                    runtime.manager(lambda workflows: workflows.cancelWorkflow(ids['workflow']))
                    response = await client.call_tool('respond_to_user_query',
                        {'session_id': question['session_id'], 'query_id': question['query_id'], 'response': 'too late'})
                    self.assertEqual(response.structured_content['error']['code'], 'query_not_pending')
                    await self._workflow(client, ids['workflow'], 'CANCELED')
                    self.assertFalse((self.root / 'never').exists())
        finally:
            runtime.stop()

    async def test_query_tools_remain_available_after_initialization_failure(self):
        """Keep the MCP listener usable when initialization fails before publication."""
        from amscjobmanager.server.runner import ManagerHTTPServer

        def initialize():
            """Raise a setup failure without creating a manager."""
            raise RuntimeError('Simulated setup failure')

        server = ManagerHTTPServer(initialize, port=0, log_path=self.root / 'server.log')
        runtime = server.runtime
        self.enterContext(patch.object(manager_logging, 'api_query_user_func', runtime.queries.ask))
        self.addCleanup(runtime.stop)
        runtime.start()
        await asyncio.to_thread(runtime._startup_worker.future.result, 2)
        async with Client(server.mcp_server, cache=None) as client:
            response = await call_mcp(client, 'list_workflows')
            self.assertEqual(response.structured_content['error']['code'], 'manager_unavailable')
            response = await call_mcp(client, 'list_user_queries')
            self.assertEqual(response.structured_content, {'result': []})
            self.assertEqual(runtime.state, 'failed')


@unittest.skipIf(Client is None, 'Install amsc-job-manager[server]')
class MCPQuerySubprocessTests(unittest.TestCase):
    def test_cli_queries_survive_process_recovery_with_new_session_ids(self):
        """Use real CLI processes to answer startup/recovery questions over HTTP.

        Start a server, answer its bootstrap query, and kill it while a workflow
        waits. A new server against the same SQLite file must expose fresh
        session/query IDs and reject the original reply. Answer the new requests
        through stdin, confirm workflow completion and verify no response values
        appear in server logs. CLI help must discover the new tools automatically.
        """
        context = multiprocessing.get_context('spawn')
        root = Path(__file__).resolve().parents[1]
        children = []
        with tempfile.TemporaryDirectory(prefix='amsc-mcp-queries-') as directory:
            def start_server():
                """Start a new child for directory; return (Process, stop Event, CLI argv)."""
                receive, send = context.Pipe(duplex=False)
                stop = context.Event()
                process = context.Process(target=serve_question_workflow, args=(directory, send, stop))
                process.start()
                send.close()
                children.append((process, stop))
                try:
                    self.assertTrue(receive.poll(10), 'Query server did not start')
                    ready = receive.recv()
                finally:
                    receive.close()
                self.assertNotIn('error', ready, ready)
                base = [sys.executable, str(root / 'utilities' / 'manager_client.py'),
                        '--url', f"http://127.0.0.1:{ready['port']}/mcp"]
                return process, stop, base

            def query(base, expected_title):
                """Poll CLI argv base for expected_title (str); return the pending query dict."""
                deadline = time.monotonic() + 12
                while time.monotonic() < deadline:
                    result = subprocess.run(base + ['list_user_queries'], capture_output=True, text=True, timeout=10)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    questions = json.loads(result.stdout)['results'][0]['result']
                    matching = [item for item in questions if item['title'] == expected_title]
                    if matching:
                        manager_logging.wfmanLog('CLI pending query: ' + json.dumps(matching[0]), file=sys.stderr)
                        return matching[0]
                    time.sleep(.02)
                self.fail(f'CLI did not report {expected_title}')

            def reply(base, question, answer):
                """Deliver answer privately over stdin; return the subprocess result.

                base (list[str]) is CLI argv; question (dict) supplies session/ID;
                answer (str) is never printed in test diagnostics or CLI output.
                """
                request = {'tool': 'respond_to_user_query', 'arguments': {
                    'session_id': question['session_id'], 'query_id': question['query_id'], 'response': answer}}
                result = subprocess.run(base + ['--file', '-'], input=json.dumps(request), capture_output=True, text=True, timeout=10)
                manager_logging.wfmanLog('CLI reply result (answer omitted): ' + result.stdout, file=sys.stderr)
                self.assertNotIn(answer, result.stdout + result.stderr)
                return result

            try:
                first, _, base = start_server()
                bootstrap = query(base, 'Startup question')
                self.assertEqual(reply(base, bootstrap, 'private-bootstrap-one').returncode, 0)
                stale = query(base, 'Workflow question')
                first.kill()
                first.join(5)
                self.assertFalse(first.is_alive())

                _, _, base = start_server()
                fresh_bootstrap = query(base, 'Startup question')
                rejected = reply(base, stale, 'private-stale-response')
                self.assertNotEqual(rejected.returncode, 0)
                self.assertEqual(json.loads(rejected.stdout)['results'][0]['error']['code'], 'stale_session')
                self.assertEqual(reply(base, fresh_bootstrap, 'private-bootstrap-two').returncode, 0)
                fresh = query(base, 'Workflow question')
                self.assertNotEqual(fresh['session_id'], stale['session_id'])
                self.assertEqual(reply(base, fresh, 'private-live-response').returncode, 0)
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    result = subprocess.run(base + ['list_workflows'], capture_output=True, text=True, timeout=10)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    records = json.loads(result.stdout)['results'][0]['result']
                    if all(item['workflow_status'] == 'COMPLETED' for item in records):
                        break
                else:
                    self.fail('Recovered CLI workflow did not finish')
                self.assertEqual(len(records), 2)
                self.assertEqual((Path(directory) / 'blocked').read_text(), 'done')
                self.assertEqual((Path(directory) / 'independent').read_text(), 'done')
                help_result = subprocess.run(base + ['-h'], capture_output=True, text=True, timeout=10)
                self.assertEqual(help_result.returncode, 0, help_result.stderr)
                self.assertIn('list_user_queries', help_result.stdout)
                self.assertIn('respond_to_user_query', help_result.stdout)
                log = (Path(directory) / 'server.log').read_text()
                for secret in ('private-bootstrap-one', 'private-bootstrap-two', 'private-stale-response', 'private-live-response'):
                    self.assertNotIn(secret, log)
            finally:
                for process, stop in children:
                    if process.is_alive():
                        stop.set()
                        process.join(6)
                        if process.is_alive():
                            process.kill()
                            process.join(3)


if __name__ == '__main__':
    if Client is None:
        raise SystemExit(1)
    unittest.main()
