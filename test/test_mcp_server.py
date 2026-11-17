"""MCP workflow inspection and a real local workflow queried by a CLI process.

Run from the repository root using the same Python environment for installation
and execution:

    source ~/miniconda3/bin/activate
    conda activate ai
    python -m pip install -e '.[server]'
    python -m unittest discover -s test -p test_mcp_server.py -v

Alternatively: python test/test_mcp_server.py -v
No separate uvicorn executable or prestarted server is needed: the test calls
uvicorn.Server.run() internally and launches its own server/client processes.
Missing/incompatible server dependencies print the import error, interpreter,
and installation command. Direct execution then exits with status 1; unittest
discovery retains skips but prints the same diagnostic instead of silent skips.
Request/response diagnostics are always printed to stderr, including the full
CLI command, file/stdin payload, JSON response, exit code, and child stderr.
In-process SDK exchanges are labeled separately from subprocess HTTP calls.

Requires the optional server dependencies. No site credentials or remote service
is used. The subprocess tests need permission to bind/connect to loopback sockets.
The in-process listing test uses the real SDK and manager with automatic
workflow starts disabled, so queued commands never execute. Client failure tests substitute
only the MCP connection. The subprocess tests run the actual HTTP transport and
utility scripts, including real local processes in the workflow-status test.
"""

from contextlib import redirect_stderr, redirect_stdout
from functools import partial
import io
import asyncio
import json
import multiprocessing
import runpy
import shlex
import signal
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import AsyncMock, patch

from amscjobmanager.logging import wfmanLog
from amscjobmanager import logging as manager_logging
from amscjobmanager import globals as manager_globals

try:
    from mcp import Client
    from mcp.server import MCPServer
    import uvicorn
except ImportError as exc:
    Client = None
    wfmanLog(
        '\nMCP SERVER TESTS CANNOT RUN: optional server dependencies are missing or incompatible.\n'
        f'Import error: {exc}\n'
        f'Python interpreter: {sys.executable}\n'
        'Install the server extra (official MCP SDK >=2.3,<3 and its dependencies):\n  '
        + shlex.join([sys.executable, '-m', 'pip', 'install', '-e',
                      str(Path(__file__).resolve().parents[1]) + '[server]'])
        + '\nThen rerun: python test/test_mcp_server.py\n',
        file=sys.stderr, flush=True,
    )


async def call_mcp(client, tool: str | None = None, arguments: dict | None = None):
    """Log an actual SDK request/response, returning the original result model.

    Args:
        client (mcp.Client): Connected in-process MCP client used by the test.
        tool (str | None): Tool name for tools/call; None sends tools/list.
        arguments (dict | None): Tool parameters; None means an empty object.

    Returns:
        CallToolResult | ListToolsResult: Unmodified SDK response, also logged
            in full as JSON. Exceptions are logged and re-raised for assertions.
    """
    request = {'method': 'tools/list'} if tool is None else {
        'method': 'tools/call', 'params': {'name': tool, 'arguments': arguments or {}},
    }
    wfmanLog('\nMCP REQUEST (in-process):\n' + json.dumps(request, indent=2), file=sys.stderr, flush=True)
    try:
        response = await client.list_tools() if tool is None else await client.call_tool(tool, arguments or {})
    except Exception as exc:
        wfmanLog(f'MCP REQUEST EXCEPTION: {type(exc).__name__}: {exc}', file=sys.stderr, flush=True)
        raise
    wfmanLog('MCP RESPONSE:\n' + response.model_dump_json(indent=2, by_alias=True), file=sys.stderr, flush=True)
    return response


def run_client(command: list[str], **options) -> subprocess.CompletedProcess:
    """Run a CLI subprocess and log its command, request, exit code, and output.

    Args:
        command (list[str]): Full argument vector, beginning with the Python
            executable and client script. Printed using shell-safe quoting.
        **options: subprocess.run keyword arguments, forwarded unchanged. Tests
            use capture_output/text (bool), timeout (seconds), and optional
            input (str) for stdin. A --file request is read for display only.

    Returns:
        subprocess.CompletedProcess: Original captured process result. JSON
            stdout is pretty-printed separately from stderr; timeout or launch
            errors are logged and re-raised. Test diagnostics use stderr and
            do not alter the child's stdout or the assertions against it.
    """
    wfmanLog('\nCLI COMMAND:\n' + shlex.join(command), file=sys.stderr, flush=True)
    if '--request' in command:
        payload = command[command.index('--request') + 1]
    elif '--file' in command:
        source = command[command.index('--file') + 1]
        payload = options.get('input', '') if source == '-' else Path(source).read_text()
    elif '--list-tools' in command:
        payload = '(tool discovery: tools/list)'
    else:
        payload = '(traditional tool command; options are parsed using the server schema)'
    wfmanLog('CLI INPUT (sent if validation succeeds):\n' + payload, file=sys.stderr, flush=True)
    try:
        completed = subprocess.run(command, **options)
    except Exception as exc:
        wfmanLog(f'CLI EXCEPTION: {type(exc).__name__}: {exc}', file=sys.stderr, flush=True)
        raise
    wfmanLog(f'CLI EXIT CODE: {completed.returncode}', file=sys.stderr, flush=True)
    try:
        response = json.dumps(json.loads(completed.stdout), indent=2)
    except (ValueError, TypeError):
        response = completed.stdout or '(empty stdout)'
    wfmanLog('CLI RESPONSE:\n' + response, file=sys.stderr, flush=True)
    if completed.stderr:
        wfmanLog('CLI STDERR:\n' + completed.stderr, file=sys.stderr, flush=True)
    return completed


def serve_local_workflow(directory, ready, stop):
    """Host running, successful, and failed local workflows in a child process.

    Args:
        directory (str): Existing temporary filesystem directory for isolated
            configuration, run files, logs, and SQLite databases.
        ready (multiprocessing.connection.Connection): Sending end of a
            one-way pipe. Send a dict with port (int), workflow_id (positive
            int for sleep), completed (positive workflow int ID), and failed
            (positive workflow int ID), or {'error': str} on startup failure.
            These IDs are database keys, not zero-based list positions.
        stop (multiprocessing.synchronize.Event): Parent-owned signal asking
            the listener to stop. Wait at most 60 seconds for it.

    Returns:
        None: Results are sent through ready, not returned. Startup or shutdown
            failures raise and cause a nonzero child-process exit status.

    The sleep job remains active while /bin/true and /bin/false produce terminal
    workflows for filtering checks. Send the assigned port and all workflow IDs
    through ready only after those states and the HTTP listener are ready.
    stop requests shutdown; a deadline also bounds orphaned test servers.
    Always request cancellation of the sleep workflow and close the manager
    on exit. The parent test removes the temporary directory after this returns.
    """
    from amscjobmanager.compute_actions import ExecuteJobComputeAction
    from amscjobmanager.local_api import setStatusManagerDatabasePath
    from amscjobmanager.manager import JobManager
    from amscjobmanager.manager_config import readManagerConfigFile, setupManager
    from amscjobmanager.server.runner import ManagerHTTPServer
    from amscjobmanager import logging as manager_logging

    root = Path(directory)
    with (root / 'server.log').open('w') as log, redirect_stderr(log), \
         patch.object(manager_logging, 'wfman_log_func', partial(print, file=log, flush=True)), \
         patch.object(manager_logging, 'api_log_func', partial(print, file=log, flush=True)), \
         patch.object(manager_logging, 'api_query_user_func'):
        config = root / 'config.json'
        config.write_text(json.dumps({'iri_implementations': {}, 'sandbox_directories': {'local': [directory]}}))
        setupManager(readManagerConfigFile(config))
        setStatusManagerDatabasePath(str(root / 'local.sqlite'))
        manager = JobManager(str(root / 'workflows.sqlite'), poll_freq=1)
        server = ManagerHTTPServer(lambda: manager, port=0, log_path=root / 'server.log')
        runtime = server.runtime
        thread = None
        job_id = None
        try:
            # Submit through the Python API, not through an MCP submission tool.
            with manager as jobs:
                job_id = jobs.enqueueJob(ExecuteJobComputeAction(
                    machine='local', executable='/bin/sleep', arguments=['30'],
                    account='', queue='', time='60', rundir=directory,
                    nodes=1, ranks_per_node=1, gpus_per_rank=0,
                ), job_group='mcp-test')
                # Successful and failing commands provide real terminal states;
                # separate run directories prevent their log files colliding.
                terminal_ids = {}
                for name, executable in (('completed', '/bin/true'), ('failed', '/bin/false')):
                    run_dir = root / name
                    run_dir.mkdir()
                    terminal_ids[name] = jobs.enqueueJob(ExecuteJobComputeAction(
                        machine='local', executable=executable, arguments=[], account='', queue='', time='60',
                        rundir=str(run_dir), nodes=1, ranks_per_node=1, gpus_per_rank=0,
                    ))
            manager_logging.api_query_user_func = runtime.queries.ask
            runtime.start()
            runtime._startup_worker.future.result(timeout=3)
            # Wait for observable states instead of assuming a fixed startup
            # delay is enough for background submission and progression.
            deadline = time.monotonic() + 15
            while manager(lambda jobs: jobs.headActionStatus(job_id)).name != 'ACTIVE':
                if time.monotonic() >= deadline:
                    raise TimeoutError('Local workflow did not become active')
                time.sleep(0.02)
            while any(manager(lambda jobs: jobs.jobStatus(value)).name != name.upper()
                      for name, value in terminal_ids.items()):
                if time.monotonic() >= deadline:
                    raise TimeoutError('Short local workflows did not finish')
                time.sleep(0.02)

            # Bind port zero once and pass the actual socket to Uvicorn, avoiding
            # the race between finding a free port and subsequently binding it.
            with socket.socket() as listener:
                listener.bind(('127.0.0.1', 0))
                listener.listen()
                thread = threading.Thread(target=server.run, kwargs={'sockets': [listener]})
                thread.start()
                while not server.started:
                    if not thread.is_alive() or time.monotonic() >= deadline:
                        raise RuntimeError('MCP listener did not start')
                    time.sleep(0.02)
                ready.send({'port': listener.getsockname()[1], 'workflow_id': job_id, **terminal_ids})
                stop.wait(60)
                # Cancel while storage is open; listener shutdown owns manager
                # cleanup and closes the workflow database.
                manager(lambda jobs: jobs.cancelJob(job_id))
                job_id = None
                server.should_exit = True
                thread.join(timeout=10)
                if thread.is_alive():
                    raise RuntimeError('MCP listener did not stop')
        except Exception as exc:
            ready.send({'error': f'{type(exc).__name__}: {exc}'})
            raise
        finally:
            try:
                if job_id is not None and manager.isAlive():
                    manager(lambda jobs: jobs.cancelJob(job_id))
            finally:
                server.should_exit = True
                if thread is not None:
                    thread.join(timeout=10)
                runtime.stop()
                ready.close()


@unittest.skipIf(Client is None, 'Install amsc-job-manager[server]')
class MCPToolTests(unittest.IsolatedAsyncioTestCase):
    async def test_schema_driven_command_argument_types_and_help(self):
        """Generate CLI options from a synthetic server schema without network calls.

        Args:
            self (MCPToolTests): unittest-created instance; no caller inputs.

        Returns:
            None when awaited: Subtests check omitted defaults, explicit false
            and zero, scalar/list conversion, JSON objects, help, required
            fields, enums, bounds, and malformed/unknown options.
        """
        from mcp.types import Tool
        from amscjobmanager.server.cli import parse_tool_arguments

        schema = {'type': 'object', 'properties': {
            'count': {'type': 'integer', 'minimum': 0, 'default': 3},
            'enabled': {'type': 'boolean', 'default': True},
            'label': {'type': 'string'},
            'offset': {'type': 'number'},
            'statuses': {'anyOf': [{'type': 'array', 'items': {'type': 'string', 'enum': ['FAILED', 'COMPLETED']}}, {'type': 'null'}]},
            'metadata': {'type': 'object', 'properties': {'mode': {'type': 'string'}}, 'required': ['mode']},
        }}
        tool = Tool(name='example', description='Synthetic tool for schema conversion', input_schema=schema,
                    output_schema={'type': 'object', 'properties': {'items': {'type': 'array', 'items': {'$ref': '#/$defs/Item'}}},
                                   '$defs': {'Item': {'type': 'object', 'properties': {
                                       'value': {'type': 'integer', 'description': 'Returned example value.'},
                                   }}}})
        cases = [([], {}), (['--no-enabled'], {'enabled': False}), (['--enabled'], {'enabled': True}),
                 (['--count', '0'], {'count': 0}), (['--offset', '0.5'], {'offset': 0.5}),
                 (['--label', 'hello world'], {'label': 'hello world'}),
                 (['--statuses', 'FAILED', 'COMPLETED'], {'statuses': ['FAILED', 'COMPLETED']}),
                 (['--metadata', '{"mode":"test"}'], {'metadata': {'mode': 'test'}})]
        for tokens, expected in cases:
            with self.subTest(tokens=tokens):
                request = parse_tool_arguments(tool, tokens)
                self.assertEqual(request.tool, 'example')
                self.assertEqual(request.arguments, expected)
        help_text = parse_tool_arguments(tool, ['--help'])
        self.assertIn('--no-enabled', help_text)
        self.assertIn('FAILED', help_text)
        self.assertIn('Outputs (server schema)', help_text)
        self.assertIn('[0..]', help_text)
        self.assertIn('value: integer', help_text)
        self.assertIn('Returned example value.', help_text)
        self.assertNotIn('show this help message and exit', help_text)
        self.assertIn('manager_client.py -h example', help_text)
        self.assertIn('manager_client.py --help example', help_text)
        for tokens in (['--count', '-1'], ['--count'], ['--statuses', 'INVALID'],
                       ['--unknown'], ['--offset', 'nan'], ['--metadata', '{broken']):
            with self.subTest(invalid_tokens=tokens), self.assertRaises(ValueError):
                parse_tool_arguments(tool, tokens)
        required = tool.model_copy(update={'input_schema': {**schema, 'required': ['label']}})
        with self.assertRaises(ValueError):
            parse_tool_arguments(required, [])

    async def test_traditional_command_discovers_and_executes_on_one_connection(self):
        """Use one mocked SDK session for discovery and the subsequent typed call.

        Args:
            self (MCPToolTests): unittest-created instance; no caller inputs.

        Returns:
            None when awaited: Assertions prove one connection, correct option
            conversion, unchanged result envelopes, and no execution for help,
            an unknown tool, or invalid arguments.
        """
        from mcp.types import Tool, ListToolsResult, CallToolResult
        from amscjobmanager.server.client import execute_command

        tool = Tool(name='example', description='A human-friendly example command.', input_schema={'type': 'object', 'properties': {
            'count': {'type': 'integer', 'minimum': 0}, 'enabled': {'type': 'boolean'},
        }})
        cases = [('example', ['--count', '0', '--no-enabled'], True),
                 (None, ['-h'], False),
                 ('example', ['--help'], False), ('example', ['--count', '-1'], False),
                 ('missing', [], False)]
        for name, tokens, executes in cases:
            with self.subTest(name=name, tokens=tokens):
                session = AsyncMock()
                session.list_tools.return_value = ListToolsResult(tools=[tool])
                session.call_tool.return_value = CallToolResult(content=[], structured_content={'result': []})
                with patch('amscjobmanager.server.client.Client') as connect:
                    connect.return_value.__aenter__.return_value = session
                    response = await execute_command('http://unused/mcp', name, tokens)
                    connect.assert_called_once()
                session.list_tools.assert_awaited_once_with(cursor=None)
                if executes:
                    session.call_tool.assert_awaited_once_with('example', {'count': 0, 'enabled': False})
                    self.assertTrue(response['success'])
                    self.assertEqual(response['results'][0]['result'], [])
                else:
                    session.call_tool.assert_not_awaited()
                    if name is None:
                        self.assertIn('example', response['help'])
                        self.assertIn('A human-friendly example command.', response['help'])
                    elif '--help' in tokens:
                        self.assertIn('--count', response['help'])
                    else:
                        self.assertEqual(response['error']['code'], 'invalid_input')

    async def test_general_help_discovers_commands_and_input_modes_are_exclusive(self):
        """CLI help uses discovery; mixed input modes are rejected before execution.

        Args:
            self (MCPToolTests): unittest-created instance; no caller inputs.

        Returns:
            None when awaited: Verify general help invokes discovery and
            -h TOOL requests tool help. Mixed input modes remain usage errors.
        """
        main = runpy.run_path(str(Path(__file__).resolve().parents[1] / 'utilities' / 'manager_client.py'))['main']
        with patch('amscjobmanager.server.client.execute_command', new_callable=AsyncMock) as execute:
            execute.return_value = {'success': True, 'results': [], 'help': 'Commands (from server):\n  example  Example command.\n'}
            for option in ('-h', '--help'):
                execute.reset_mock()
                output = io.StringIO()
                with redirect_stdout(output):
                    self.assertEqual(await asyncio.to_thread(main, [option]), 0)
                self.assertIn('Commands (from server)', output.getvalue())
                self.assertIn('Example command.', output.getvalue())
                self.assertIn('Show the main help page', output.getvalue())
                execute.assert_awaited_once_with('http://127.0.0.1:8765/mcp', None, ['--help'], timeout=30)
                execute.reset_mock()
                with redirect_stdout(io.StringIO()):
                    self.assertEqual(await asyncio.to_thread(main, [option, 'example']), 0)
                execute.assert_awaited_once_with('http://127.0.0.1:8765/mcp', 'example', ['--help'], timeout=30)
            execute.reset_mock()
            with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as invalid:
                main(['--request', '{"tool":"list_workflows"}', 'list_workflows'])
            self.assertEqual(invalid.exception.code, 2)
            execute.assert_not_called()

    async def test_manager_failure_returns_error_and_listener_survives(self):
        """An actual worker crash returns manager_unavailable instead of stale state.

        Use the real SDK and manager. Confirm a never-started manager is
        unavailable, then fail its progression function inside the worker and
        wait for the thread to exit. Status calls must promptly return the
        structured error, while tool discovery still works on the same MCP
        connection. The failed single-use manager must reject restart while the
        listener continues serving tool discovery and unavailable-manager errors.

        Args:
            self (MCPToolTests): unittest-created test instance; no
                caller-supplied parameters.

        Returns:
            None when awaited: Checks report failures through unittest assertions.
        """
        from amscjobmanager.manager import JobManager
        from amscjobmanager.server.runner import ManagerHTTPServer

        manager = JobManager(max_workflows_active=0)
        server = ManagerHTTPServer(lambda: manager, port=0, log_path=Path(tempfile.gettempdir()) / 'amsc-mcp-test.log')
        runtime = server.runtime
        self.enterContext(patch.object(manager_logging, 'api_query_user_func', runtime.queries.ask))
        self.addCleanup(runtime.stop)
        async with Client(server.mcp_server, cache=None) as client:
            response = await asyncio.wait_for(call_mcp(client, 'list_workflows', {}), timeout=3)
            self.assertEqual(response.structured_content['error']['code'], 'manager_unavailable')
            with patch.object(manager.job_data, 'progressActiveState', side_effect=RuntimeError('injected manager crash')), \
                 patch('amscjobmanager.manager.wfmanLog') as log:
                runtime.start()
                await asyncio.to_thread(runtime._startup_worker.future.result, 3)
                await asyncio.to_thread(manager._thread.join, 3)
                self.assertFalse(manager._thread.is_alive())
                self.assertIn('injected manager crash', log.call_args.args[0])
            response = await asyncio.wait_for(call_mcp(client, 'list_workflows', {'refresh': True}), timeout=3)
            self.assertTrue(response.is_error)
            self.assertEqual(response.structured_content['error']['code'], 'manager_unavailable')
            self.assertEqual({tool.name for tool in (await call_mcp(client)).tools},
                             {'list_workflows', 'list_sandbox_directories', 'list_user_queries', 'respond_to_user_query', 'submit_workflow', 'cancel_workflow'})
            with self.assertRaisesRegex(RuntimeError, 'cannot be restarted'):
                manager.start()
            response = await asyncio.wait_for(call_mcp(client, 'list_workflows', {}), timeout=3)
            self.assertEqual(response.structured_content['error']['code'], 'manager_unavailable')
            manager.stop(wait_until_done=False)
            response = await asyncio.wait_for(call_mcp(client, 'list_workflows', {}), timeout=3)
            self.assertEqual(response.structured_content['error']['code'], 'manager_unavailable')

    async def test_manager_health_is_rechecked_after_acquiring_lock(self):
        """If health changes while waiting for the lock, do not query cached records.

        Allow the runtime state and pre-lock health checks to succeed, then
        fail the post-lock health check.
        The tool must return manager_unavailable without calling listWorkflows.
        This complements the real crash test with a deterministic race check.

        Args:
            self (MCPToolTests): unittest-created test instance; no
                caller-supplied parameters.

        Returns:
            None when awaited: Checks report failures through unittest assertions.
        """
        from amscjobmanager.manager import JobManager
        from amscjobmanager.server.runner import ManagerHTTPServer

        manager = JobManager(max_workflows_active=0)
        server = ManagerHTTPServer(lambda: manager, port=0, log_path=Path(tempfile.gettempdir()) / 'amsc-mcp-test.log')
        runtime = server.runtime
        self.enterContext(patch.object(manager_logging, 'api_query_user_func', runtime.queries.ask))
        self.addCleanup(runtime.stop)
        runtime.start()
        await asyncio.to_thread(runtime._startup_worker.future.result, 3)
        async with Client(server.mcp_server, cache=None) as client:
            with patch.object(manager, 'isAlive', side_effect=[True, True, False]), \
                 patch.object(manager.job_data, 'listWorkflows') as listing:
                response = await call_mcp(client, 'list_workflows', {})
                self.assertEqual(response.structured_content['error']['code'], 'manager_unavailable')
                listing.assert_not_called()

    async def test_server_frontend_routes_hooks_to_file_without_redirecting_stdout(self):
        """Server/API hooks log to a file, preserve stdout, and restore prior callbacks.

        Run the actual utility main with configuration and the listener mocked
        so no socket or job is started. Setup emits an API log; the listener
        emits a manager log and an unrelated print. Only the log messages
        belong in the append-only log file. Capture stdout solely to assert
        that the unrelated print was not intercepted by the server. Repeat
        with listener failure to check shutdown and hook restoration on errors.

        Args:
            self (MCPToolTests): unittest-created test instance; no
                caller-supplied parameters.

        Returns:
            None when awaited: Checks report failures through unittest assertions.
        """
        from amscjobmanager.server.runner import ManagerHTTPServer

        script = Path(__file__).resolve().parents[1] / 'utilities' / 'manager_server.py'
        main = runpy.run_path(str(script))['main']
        previous_hooks = (manager_logging.wfman_log_func, manager_logging.api_log_func, manager_logging.api_query_user_func)
        for fail in (False, True):
            with self.subTest(listener_fails=fail), tempfile.TemporaryDirectory() as directory:
                log_path = Path(directory) / 'manager.log'
                log_path.write_text('existing log\n')
                output = io.StringIO()

                def listener(server):
                    """Simulate a listener run for log-routing assertions.

                    server (ManagerHTTPServer) owns the runtime; use the enclosing
                    fail (bool) subtest flag.
                    Return None normally; raise RuntimeError when the enclosing
                    fail (bool) selects the listener-failure subtest. Emit a
                    manager log and an independent stdout message in both cases,
                    and mark the mocked listener started only on success.
                    """
                    runtime = server.runtime
                    runtime.start()
                    runtime._startup_worker.future.result(timeout=2)
                    manager_logging.wfmanLog('listener manager message')
                    print('ordinary stdout')
                    if fail:
                        raise RuntimeError('listener failure')
                    server.started = True

                with patch('amscjobmanager.manager_config.readManagerConfigFile'), \
                     patch('amscjobmanager.manager_config.setupManager', side_effect=lambda config: manager_logging.wfapiLog('setup API message')), \
                     patch('amscjobmanager.manager.JobManager') as manager, \
                     patch.object(ManagerHTTPServer, 'run', listener), redirect_stdout(output):
                    # Exercise the default without creating state in the working
                    # directory: the manager is mocked. Also retain override coverage.
                    database = str(Path(directory) / 'jobs.sqlite') if fail else 'workflows.sqlite'
                    arguments = ['unused.json', '--log-file', str(log_path)]
                    if fail:
                        arguments += ['--database', database]
                    if fail:
                        with self.assertRaisesRegex(RuntimeError, 'listener failure'):
                            main(arguments)
                    else:
                        self.assertEqual(main(arguments), 0)
                    manager.assert_called_once_with(database, poll_freq=20)
                    manager.return_value.stop.assert_called_once_with(wait_until_done=False, close_database=True)
                log = log_path.read_text()
                self.assertTrue(log.startswith('existing log\n'))
                self.assertIn('setup API message', log)
                self.assertIn('listener manager message', log)
                self.assertIn('Stopped AmSC Job Manager MCP server', log)
                self.assertNotIn('ordinary stdout', log)
                self.assertEqual(output.getvalue(), 'ordinary stdout\n')
                self.assertEqual((manager_logging.wfman_log_func, manager_logging.api_log_func, manager_logging.api_query_user_func), previous_hooks)

    async def test_sandbox_listing_groups_filters_and_never_validates_directories(self):
        """Query the real MCP tool without probes, exposing paths rather than internal flags.

        Cover readiness, all/filtered/empty results, case-insensitive names,
        strict input types, generated CLI arguments, live registration changes,
        and copied Python results that cannot mutate the owner's records.
        """
        from amscjobmanager.manager import JobManager
        from amscjobmanager.server.cli import parse_tool_arguments
        from amscjobmanager.server.runner import ManagerHTTPServer

        self.enterContext(patch.object(manager_globals, 'remote_workdir', {
            'frontier': [manager_globals.SandboxDirectory('/safe/one'),
                         manager_globals.SandboxDirectory('/safe/two', True)],
            'local': [],
        }))
        probe = self.enterContext(patch('amscjobmanager.api_general.isDirectory',
                                       side_effect=AssertionError('Listing probed a directory')))
        query = self.enterContext(patch.object(manager_logging, 'api_query_user_func',
                                               side_effect=AssertionError('Listing asked a question')))
        manager = JobManager(max_workflows_active=0)
        server = ManagerHTTPServer(lambda: manager, port=0,
                                   log_path=Path(tempfile.gettempdir()) / 'amsc-sandbox-list.log')
        self.addCleanup(server.runtime.stop)
        expected = {'frontier': ['/safe/one', '/safe/two'], 'local': []}

        async with Client(server.mcp_server, cache=None) as client:
            # Before readiness, do not expose potentially incomplete setup state.
            unavailable = await call_mcp(client, 'list_sandbox_directories')
            self.assertTrue(unavailable.is_error)
            self.assertEqual(unavailable.structured_content['error']['code'], 'manager_unavailable')
            server.runtime.start()
            await asyncio.to_thread(server.runtime._startup_worker.future.result, 3)

            tools = await client.list_tools()
            tool = next(tool for tool in tools.tools if tool.name == 'list_sandbox_directories')
            self.assertTrue(tool.annotations.read_only_hint)
            self.assertIn('result', tool.output_schema['properties'])
            request = parse_tool_arguments(tool, ['--machine', 'FRONTIER'])
            self.assertEqual(request.arguments, {'machine': 'FRONTIER'})
            for arguments, paths in (({}, expected), ({'machine': None}, expected),
                                     ({'machine': 'FrOnTiEr'}, {'frontier': expected['frontier']}),
                                     ({'machine': 'local'}, {'local': []}), ({'machine': 'unknown'}, {})):
                result = await call_mcp(client, 'list_sandbox_directories', arguments)
                self.assertFalse(result.is_error, result)
                self.assertEqual(result.structured_content, {'result': paths})
            invalid = await call_mcp(client, 'list_sandbox_directories', {'machine': 42})
            self.assertTrue(invalid.is_error)

            # Read current registrations, including additions not yet validated,
            # and keep returned lists separate from the owner's records.
            copied = manager_globals.getSandboxDirs()
            copied['frontier'].clear()
            self.assertEqual(manager_globals.getSandboxDirs(), expected)
            manager_globals.addSandboxDirs({'local': '/new/local'})
            added = await call_mcp(client, 'list_sandbox_directories', {'machine': 'LOCAL'})
            self.assertEqual(added.structured_content, {'result': {'local': ['/new/local']}})
            manager_globals.removeSandboxDirs({'local': '/new/local'})
            removed = await call_mcp(client, 'list_sandbox_directories', {'machine': 'local'})
            self.assertEqual(removed.structured_content, {'result': {'local': []}})
            with patch.object(manager_globals, 'remote_workdir', None):
                empty = await call_mcp(client, 'list_sandbox_directories')
                self.assertEqual(empty.structured_content, {'result': {}})
        self.assertFalse(manager_globals.remote_workdir['frontier'][0].validated)
        probe.assert_not_called()
        query.assert_not_called()

    async def test_listing_statuses_errors_and_public_fields(self):
        """Exercise tool discovery, listing, filters, and validation through real MCP.

        Connect the SDK client directly to a server backed by an in-memory
        manager. Disable automatic starts, enqueue two workflows,
        and cancel one before submission, so neither executable actually runs.
        Check empty/ordered listings, distinct workflow/head statuses, null
        provider status, and exclusion of serialized action definitions.
        Check that status filtering selects the canceled record, combining ID
        and status filters can return no match, and unknown IDs or malformed
        arguments produce MCP tool errors rather than successful responses.

        Args:
            self (MCPToolTests): unittest-created test instance; no
                caller-supplied parameters.

        Returns:
            None when awaited: Checks report failures through unittest assertions.
        """
        from amscjobmanager.compute_actions import ExecuteJobComputeAction
        from amscjobmanager.manager import JobManager
        from amscjobmanager.server.runner import ManagerHTTPServer

        manager = JobManager(max_workflows_active=0)
        server = ManagerHTTPServer(lambda: manager, port=0, log_path=Path(tempfile.gettempdir()) / 'amsc-mcp-test.log')
        runtime = server.runtime
        self.enterContext(patch.object(manager_logging, 'api_query_user_func', runtime.queries.ask))
        self.addCleanup(runtime.stop)
        runtime.start()
        await asyncio.to_thread(runtime._startup_worker.future.result, 3)
        async with Client(server.mcp_server, cache=None) as client:
            tools = await call_mcp(client)
            self.assertEqual({tool.name for tool in tools.tools},
                             {'list_workflows', 'list_sandbox_directories', 'list_user_queries', 'respond_to_user_query', 'submit_workflow', 'cancel_workflow'})
            result = await call_mcp(client, 'list_workflows', {})
            self.assertEqual(result.structured_content, {'result': []})
            action = ExecuteJobComputeAction(
                machine='local', executable='/bin/true', arguments=[], account='', queue='', time='60',
                rundir='/tmp', nodes=1, ranks_per_node=1, gpus_per_rank=0,
            )
            first = manager(lambda jobs: jobs.enqueueJob(action))
            second = manager(lambda jobs: jobs.enqueueJob(action))
            manager(lambda jobs: jobs.cancelJob(second))
            result = await call_mcp(client, 'list_workflows', {})
            records = result.structured_content['result']
            self.assertEqual([row['workflow_id'] for row in records], [first, second])
            self.assertEqual([row['workflow_status'] for row in records], ['PENDING', 'CANCELED'])
            self.assertEqual(records[0]['head_action_status'], 'PENDING')
            self.assertIsNone(records[0]['api_status'])
            self.assertNotIn('workflow', records[0])
            result = await call_mcp(client, 'list_workflows', {'statuses': ['CANCELED']})
            self.assertEqual([row['workflow_id'] for row in result.structured_content['result']], [second])
            result = await call_mcp(client, 'list_workflows', {'workflow_id': first, 'statuses': ['FAILED']})
            self.assertEqual(result.structured_content['result'], [])
            # Include booleans and strings that Python could otherwise coerce
            # into integers/booleans, plus an invalid enum name.
            for arguments in ({'workflow_id': 999}, {'workflow_id': -1}, {'workflow_id': True}, {'refresh': 'yes'}, {'statuses': ['INVALID']}):
                result = await call_mcp(client, 'list_workflows', arguments)
                self.assertTrue(result.is_error, arguments)

    async def test_whole_document_is_validated_before_connecting(self):
        """Reject malformed request documents before opening any MCP connection.

        Invoke the CLI main with the executor mocked and supply an empty batch,
        null input, and a batch whose valid first request precedes a request
        with non-object arguments. Each must return invalid_input with no
        results. Assert that the executor was never called: even the valid
        first request must not execute before the whole batch is validated.

        Args:
            self (MCPToolTests): unittest-created test instance; no
                caller-supplied parameters.

        Returns:
            None when awaited: Checks report failures through unittest assertions.
        """
        script = Path(__file__).resolve().parents[1] / 'utilities' / 'manager_client.py'
        main = runpy.run_path(str(script))['main']
        with patch('amscjobmanager.server.client.execute_requests') as execute:
            for document in ([], None, [{'tool': 'list_workflows'}, {'tool': 'list_workflows', 'arguments': []}],
                             {'tool': 'list_workflows', 'unexpected': True},
                             {'tool': 'list_workflows', 'arguments': {'value': float('nan')}}):
                output = io.StringIO()
                with redirect_stdout(output):
                    self.assertEqual(main(['--request', json.dumps(document)]), 1)
                result = json.loads(output.getvalue())
                wfmanLog('\nLOCAL VALIDATION INPUT (not sent):\n' + json.dumps(document, indent=2), file=sys.stderr, flush=True)
                wfmanLog('LOCAL VALIDATION RESPONSE:\n' + json.dumps(result, indent=2), file=sys.stderr, flush=True)
                self.assertFalse(result['success'])
                self.assertEqual(result['results'], [])
                self.assertEqual(result['error']['code'], 'invalid_input')
            execute.assert_not_called()

    async def test_executor_accepts_single_and_multiple_typed_requests(self):
        """Execute validated models directly, without converting raw dictionaries.

        Args:
            self (MCPToolTests): unittest-created instance, with no caller inputs.

        Returns:
            None when awaited: Assertions verify single/batch success envelopes
            and exact forwarding of model fields to a mocked MCP session.
        """
        from mcp.types import CallToolResult
        from amscjobmanager.server.client import ToolRequest, execute_requests

        request = ToolRequest(tool='list_workflows', arguments={'statuses': ['FAILED']})
        for requests, count in ((request, 1), ([request, request], 2)):
            session = AsyncMock()
            session.call_tool.return_value = CallToolResult(content=[], structured_content={'result': []})
            with patch('amscjobmanager.server.client.Client') as connect:
                connect.return_value.__aenter__.return_value = session
                result = await execute_requests('http://unused/mcp', requests)
            wfmanLog('\nMOCKED MCP INPUT:\n' + json.dumps([
                request.model_dump() for request in ([requests] if isinstance(requests, ToolRequest) else requests)
            ], indent=2), file=sys.stderr, flush=True)
            wfmanLog('MOCKED MCP RESPONSE:\n' + json.dumps(result, indent=2), file=sys.stderr, flush=True)
            self.assertTrue(result['success'])
            self.assertEqual(len(result['results']), count)
            self.assertEqual(session.call_tool.await_count, count)
            session.call_tool.assert_awaited_with('list_workflows', {'statuses': ['FAILED']})

    async def test_interrupted_call_has_unknown_outcome_and_skips_remaining_requests(self):
        """Preserve earlier results and stop after a simulated connection loss.

        Substitute an async MCP session that succeeds on its first tool call
        and raises ConnectionError on its second. Submit three operations with
        continue_on_error enabled. Expect success/failure/skipped in that order,
        the original failure reason, and outcome_unknown on the interrupted
        call. Exactly two calls proves there was neither a retry nor execution
        of the third request. This tests client behavior, not a physical socket
        interruption or a server-side operation's eventual outcome.

        Args:
            self (MCPToolTests): unittest-created test instance; no
                caller-supplied parameters.

        Returns:
            None when awaited: Checks report failures through unittest assertions.
        """
        from mcp.types import CallToolResult
        from amscjobmanager.server.client import ToolRequest, execute_requests

        session = AsyncMock()
        session.call_tool.side_effect = [
            CallToolResult(content=[], structured_content={'result': []}),
            ConnectionError('connection dropped during request'),
        ]
        with patch('amscjobmanager.server.client.Client') as connect:
            connect.return_value.__aenter__.return_value = session
            result = await execute_requests('http://unused/mcp', [
                ToolRequest(tool='list_workflows'), ToolRequest(tool='list_workflows'), ToolRequest(tool='list_workflows'),
            ], continue_on_error=True)
        wfmanLog('\nMOCKED CONNECTION LOSS: three list_workflows requests; the second loses its connection', file=sys.stderr, flush=True)
        wfmanLog('MOCKED MCP RESPONSE:\n' + json.dumps(result, indent=2), file=sys.stderr, flush=True)
        self.assertEqual([entry['status'] for entry in result['results']], ['succeeded', 'failed', 'skipped'])
        self.assertTrue(result['results'][1]['error']['outcome_unknown'])
        self.assertIn('connection dropped during request', result['results'][1]['error']['message'])
        self.assertEqual(session.call_tool.await_count, 2)


@unittest.skipIf(Client is None, 'Install amsc-job-manager[server]')
class MCPSubprocessTests(unittest.TestCase):
    def test_standalone_server_frontend_and_connection_failure(self):
        """Exercise the actual server launcher and initial connection-error envelope.

        Reserve a loopback port without listening and invoke the CLI there;
        connection refusal must yield a nonzero exit, connection_error with
        its underlying network reason, and no operation results. Then release
        the port and launch utilities/manager_server.py with temporary config
        and an empty persistent database. Wait for socket readiness, invoke a
        separate CLI process, and expect a successful empty workflow listing.
        Capture server stdout separately: it must announce readiness with the
        endpoint/PID/log file, add nothing for HTTP requests, and announce
        termination on SIGTERM and SIGINT, with no traceback. Ctrl-C must exit
        successfully and finish manager cleanup. An occupied port must not
        announce success.
        Terminate the launcher in finally, with a kill fallback if it stalls.
        No compute job is submitted in this test.

        Args:
            self (MCPSubprocessTests): unittest-created test instance; no
                caller-supplied parameters.

        Returns:
            None: Checks report failures through unittest assertions.
        """
        root = Path(__file__).resolve().parents[1]
        client = [sys.executable, str(root / 'utilities' / 'manager_client.py'), '--timeout', '2']
        with tempfile.TemporaryDirectory(prefix='amsc-mcp-launcher-') as directory:
            directory = Path(directory)
            config = directory / 'config.json'
            config.write_text(json.dumps({'iri_implementations': {}, 'sandbox_directories': {'local': [str(directory)]}}))
            with socket.socket() as reserved:
                reserved.bind(('127.0.0.1', 0))
                port = reserved.getsockname()[1]
                completed = run_client(
                    client + ['--url', f'http://127.0.0.1:{port}/mcp', '--request', '{"tool":"list_workflows"}'],
                    capture_output=True, text=True, timeout=10,
                )
                self.assertEqual(completed.returncode, 1)
                response = json.loads(completed.stdout)
                self.assertEqual(response['error']['code'], 'connection_error')
                self.assertEqual(response['results'], [])
                self.assertIn('ConnectError', response['error']['message'])
                completed = run_client(
                    client + ['--url', f'http://127.0.0.1:{port}/mcp', 'list_workflows'],
                    capture_output=True, text=True, timeout=10,
                )
                self.assertEqual(completed.returncode, 1)
                self.assertEqual(json.loads(completed.stdout)['error']['code'], 'connection_error')

                # A failed bind must not misleadingly claim the server started.
                blocked = subprocess.run([
                    sys.executable, str(root / 'utilities' / 'manager_server.py'), str(config),
                    '--database', str(directory / 'blocked.sqlite'), '--port', str(port),
                ], capture_output=True, text=True, timeout=10)
                self.assertNotEqual(blocked.returncode, 0)
                self.assertNotIn('Server started:', blocked.stdout)
                self.assertNotIn('Server terminating:', blocked.stdout)

            # Exercise the actual signal path: Ctrl-C is SIGINT and must not
            # expose Uvicorn/asyncio's re-raised KeyboardInterrupt traceback.
            for stop_signal in (signal.SIGTERM, signal.SIGINT):
                with (directory / 'server.log').open('w+') as log, \
                     (directory / 'server.stdout').open('w+') as server_stdout:
                    process = subprocess.Popen([
                        sys.executable, str(root / 'utilities' / 'manager_server.py'), str(config),
                        '--database', str(directory / 'workflows.sqlite'), '--port', str(port),
                    ], stdout=server_stdout, stderr=log)
                    try:
                        deadline = time.monotonic() + 10
                        while True:
                            self.assertIsNone(process.poll(), 'Server frontend exited before accepting connections')
                            server_stdout.seek(0)
                            notices = server_stdout.read().splitlines()
                            if notices:
                                self.assertTrue(notices[0].startswith(f'Server started: http://127.0.0.1:{port}/mcp'))
                                self.assertIn(f'PID {process.pid}', notices[0])
                                self.assertIn('workflows.sqlite.log', notices[0])
                                break
                            if time.monotonic() >= deadline:
                                self.fail('Standalone server readiness announcement timed out')
                            time.sleep(0.05)
                        completed = run_client(
                            client + ['--url', f'http://127.0.0.1:{port}/mcp', '--request', '{"tool":"list_workflows"}'],
                            capture_output=True, text=True, timeout=10,
                        )
                        self.assertEqual(completed.returncode, 0, completed.stderr)
                        self.assertEqual(json.loads(completed.stdout)['results'][0]['result'], [])
                        server_stdout.seek(0)
                        self.assertEqual(len(server_stdout.read().splitlines()), 1, 'HTTP requests produced extra stdout logs')
                    finally:
                        process.send_signal(stop_signal)
                        try:
                            process.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait(timeout=5)
                    server_stdout.seek(0)
                    notices = server_stdout.read().splitlines()
                    self.assertEqual(len(notices), 2, notices)
                    self.assertEqual(notices[1], f'Server terminating: http://127.0.0.1:{port}/mcp')
                    log.seek(0)
                    diagnostics = log.read()
                    self.assertNotIn('Traceback', diagnostics)
                    self.assertNotIn('CancelledError', diagnostics)
                    if stop_signal == signal.SIGINT:
                        self.assertEqual(process.returncode, 0, diagnostics)
                        self.assertIn('Stopped AmSC Job Manager MCP server',
                                      (directory / 'workflows.sqlite.log').read_text())

    def test_local_job_status_through_json_cli(self):
        """Query real local workflows over HTTP from the JSON CLI subprocess.

        Spawn serve_local_workflow, which directly submits sleep/true/false
        actions through Python and starts the MCP listener. Wait for its
        readiness message, then invoke the actual client utility in separate
        processes. A JSON batch must return both manager statuses and provider
        status for the running sleep job, and select exactly the successful
        and failed workflows using individual or combined status filters.

        Further calls exercise JSON-file input, stop-on-error versus explicit
        continuation, stable workflow_not_found errors, nonzero failure exit
        codes, tool discovery, stdin input, and malformed-JSON rejection.
        Parsing stdout directly as JSON also checks that library diagnostics
        have not contaminated the result channel. Finally signal the child to
        cancel its sleep job and stop cleanly, with bounded termination fallback.

        Args:
            self (MCPSubprocessTests): unittest-created test instance; no
                caller-supplied parameters.

        Returns:
            None: Checks report failures through unittest assertions.
        """
        context = multiprocessing.get_context('spawn')
        with tempfile.TemporaryDirectory(prefix='amsc-mcp-test-') as directory:
            parent, child = context.Pipe(duplex=False)
            stop = context.Event()
            process = context.Process(target=serve_local_workflow, args=(directory, child, stop))
            process.start()
            child.close()
            try:
                self.assertTrue(parent.poll(25), 'Server readiness timed out')
                info = parent.recv()
                self.assertNotIn('error', info, info)
                url = f"http://127.0.0.1:{info['port']}/mcp"
                job_id = info['workflow_id']
                script = Path(__file__).resolve().parents[1] / 'utilities' / 'manager_client.py'
                base = [sys.executable, str(script), '--url', url, '--timeout', '5']

                # One connection executes all requests in order. Refresh must
                # return a provider status as well as both manager statuses.
                requests = [{'tool': 'list_workflows', 'arguments': {}},
                            {'tool': 'list_workflows', 'arguments': {'workflow_id': job_id, 'refresh': True}},
                            {'tool': 'list_workflows', 'arguments': {'statuses': ['FAILED']}},
                            {'tool': 'list_workflows', 'arguments': {'statuses': ['COMPLETED']}},
                            {'tool': 'list_workflows', 'arguments': {'statuses': ['FAILED', 'COMPLETED']}}]
                completed = run_client(base + ['--request', json.dumps(requests)], capture_output=True, text=True, timeout=15)
                self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
                result = json.loads(completed.stdout)
                self.assertTrue(result['success'])
                # The sleep is still executing, while true/false have already
                # produced final workflow states before the listener was ready.
                row = result['results'][1]['result'][0]
                self.assertEqual(row['workflow_id'], job_id)
                self.assertEqual(row['workflow_status'], 'ACTIVE')
                self.assertEqual(row['head_action_status'], 'ACTIVE')
                self.assertEqual(row['api_status'], 'active')
                self.assertEqual(row['job_group'], 'mcp-test')
                self.assertEqual([row['workflow_id'] for row in result['results'][2]['result']], [info['failed']])
                self.assertEqual([row['workflow_id'] for row in result['results'][3]['result']], [info['completed']])
                self.assertEqual(len(result['results'][4]['result']), 2)

                # The traditional command must produce the same workflow record
                # as JSON input, while translating list and boolean options.
                completed = run_client(base + ['list_workflows', '--workflow-id', str(job_id),
                                              '--refresh', '--statuses', 'ACTIVE'],
                                       capture_output=True, text=True, timeout=15)
                self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
                traditional = json.loads(completed.stdout)
                self.assertEqual(traditional['results'][0]['result'][0], row)
                completed = run_client(base + ['list_workflows', '--statuses', 'FAILED', 'COMPLETED', '--no-refresh'],
                                       capture_output=True, text=True, timeout=15)
                self.assertEqual(completed.returncode, 0, completed.stderr)
                self.assertEqual(len(json.loads(completed.stdout)['results'][0]['result']), 2)
                completed = run_client(base + ['list_workflows', '--help'], capture_output=True, text=True, timeout=15)
                self.assertEqual(completed.returncode, 0, completed.stderr)
                self.assertIn('--workflow-id', completed.stdout)
                self.assertIn('--no-refresh', completed.stdout)
                self.assertIn('Outputs (server schema)', completed.stdout)
                self.assertIn('workflow_stage', completed.stdout)
                self.assertIn('Zero-based', completed.stdout)
                completed = run_client(base + ['-h', 'list_workflows'], capture_output=True, text=True, timeout=15)
                self.assertEqual(completed.returncode, 0, completed.stderr)
                self.assertIn('--workflow-id', completed.stdout)
                self.assertIn('api_status', completed.stdout)
                completed = run_client(base + ['-h'], capture_output=True, text=True, timeout=15)
                self.assertEqual(completed.returncode, 0, completed.stderr)
                self.assertIn('Commands (from server)', completed.stdout)
                self.assertIn('list_workflows', completed.stdout)
                self.assertIn('List all workflows', completed.stdout)
                completed = run_client(base + ['list_workflows', '--workflow-id', '0'], capture_output=True, text=True, timeout=15)
                self.assertEqual(completed.returncode, 1)
                self.assertEqual(json.loads(completed.stdout)['error']['code'], 'invalid_input')

                # A missing workflow stops the default sequence; opt-in
                # continuation runs the following operation on the same server.
                requests = [{'tool': 'list_workflows', 'arguments': {'workflow_id': 999}}, requests[0]]
                request_file = Path(directory) / 'requests.json'
                request_file.write_text(json.dumps(requests))
                for extra, statuses in (([], ['failed', 'skipped']), (['--continue-on-error'], ['failed', 'succeeded'])):
                    completed = run_client(base + ['--file', str(request_file)] + extra, capture_output=True, text=True, timeout=15)
                    self.assertEqual(completed.returncode, 1, completed.stderr)
                    result = json.loads(completed.stdout)
                    self.assertEqual([row['status'] for row in result['results']], statuses)
                    self.assertEqual(result['results'][0]['error']['code'], 'workflow_not_found')

                # Tool discovery must use the same endpoint and expose only
                # workflow inspection and user interaction tools.
                completed = run_client(base + ['--list-tools'], capture_output=True, text=True, timeout=15)
                self.assertEqual(completed.returncode, 0, completed.stderr)
                tools = json.loads(completed.stdout)['results'][0]['result']
                self.assertEqual({tool['name'] for tool in tools},
                                 {'list_workflows', 'list_sandbox_directories', 'list_user_queries', 'respond_to_user_query', 'submit_workflow', 'cancel_workflow'})

                # stdin accepts the same request format as an inline string
                # or file. Broken JSON must instead return a JSON error envelope.
                completed = run_client(base + ['--file', '-'], input=json.dumps({'tool': 'list_workflows'}), capture_output=True, text=True, timeout=15)
                self.assertEqual(completed.returncode, 0, completed.stderr)
                self.assertTrue(json.loads(completed.stdout)['success'])

                completed = run_client(base + ['--request', '{broken'], capture_output=True, text=True, timeout=15)
                self.assertEqual(completed.returncode, 1)
                self.assertEqual(json.loads(completed.stdout)['error']['code'], 'invalid_input')
            finally:
                stop.set()
                process.join(timeout=20)
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=5)
                parent.close()
            self.assertEqual(process.exitcode, 0, (Path(directory) / 'server.log').read_text())
            log = (Path(directory) / 'server.log').read_text()
            self.assertIn('Executing local job', log)
            self.assertIn('Initiating action of type COMPUTE', log)


if __name__ == '__main__':
    if Client is None:
        raise SystemExit(1)
    unittest.main()
