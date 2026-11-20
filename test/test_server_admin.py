"""Local password administration, sandbox ownership, and actual utility integration.

Run: PYTHONPATH=test python -m unittest test_server_admin -v
Socket tests require permission to bind/connect to local Unix sockets. The final
test additionally requires .[server] and loopback sockets; it starts both real
utilities and enters passwords through pseudo-terminals, never MCP or argv.
No remote service, site credential, or compute job is used.
"""

import asyncio
import errno
import json
import os
from pathlib import Path
import pty
import select
import socket
import stat
import subprocess
import sys
import tempfile
from threading import Event, Thread
import time
import unittest
from unittest.mock import Mock, patch

from amscjobmanager import globals as sandbox
from amscjobmanager import logging as manager_logging
from amscjobmanager.actions_base import ActionClass
from amscjobmanager.server.admin import AdminCallRequest, AdminDiscoveryRequest, AdminResponse, AdminServer, execute_admin_request
from amscjobmanager.server.runtime import ServerRuntime
from amscjobmanager.manager import WorkflowManager


class AdminSocketTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        """Start a real private socket with isolated roots and a fake healthy manager."""
        temporary = tempfile.TemporaryDirectory(prefix='amsc-admin-')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.address = self.root / 'private' / 'admin.sock'
        self.password = 'private-admin-credential'
        self.enterContext(patch.object(sandbox, 'remote_workdir', {'local': []}))
        self.enterContext(patch('amscjobmanager.api_general.getKnownMachines', return_value=['local']))
        self.logs = Mock()
        self.enterContext(patch.object(manager_logging, 'wfman_log_func', self.logs))
        self.queries = Mock(side_effect=AssertionError('Password reached a user-query hook'))
        self.enterContext(patch.object(manager_logging, 'api_query_user_func', self.queries))
        self.runtime = ServerRuntime(lambda: WorkflowManager(max_workflows_active=0))
        self.addCleanup(self.runtime.stop)
        self.runtime.start()
        await asyncio.to_thread(self.runtime._startup_worker.future.result, 3)
        self.server = AdminServer(self.address, self.password, self.runtime)
        await self.server.start()
        self.addAsyncCleanup(self.server.stop)

    async def _call(self, command='add_sandbox', path=None, password=None):
        """Send command/path/password (str or None defaults) through the real client.

        Return AdminResponse. None path uses self.root; None password uses the
        correct credential. The blocking client runs outside the server loop.
        """
        return await asyncio.to_thread(execute_admin_request, self.address, AdminCallRequest(command=command, arguments={'machine': 'local', 'path': str(self.root) if path is None else str(path)}, password=self.password if password is None else password))

    async def _send(self, payload):
        """Send payload (bytes), end its write stream and return the JSON response dict."""
        reader, writer = await asyncio.open_unix_connection(self.address)
        try:
            writer.write(payload)
            await writer.drain()
            writer.write_eof()
            return json.loads(await reader.readline())
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_wallet_commands_require_password_and_persist(self):
        """Use the Unix protocol to budget compute, reject unauthorized grants, and reopen.

        Wallet changes go through a real WorkflowManager/WorkflowData, while the socket
        fixture supplies password protection. Unlike sandbox roots, the balance
        and enable flag must remain after the manager database is reopened.
        """
        from amscjobmanager.manager import WorkflowManager
        from amscjobmanager.workflow_data import WorkflowData

        database = self.root / 'wallet.sqlite'
        manager = WorkflowManager(database, max_workflows_active=0)
        self.runtime.stop()
        self.runtime = ServerRuntime(lambda: manager)
        self.server.runtime = self.runtime
        self.addCleanup(self.runtime.stop)
        self.runtime.start()
        await asyncio.to_thread(self.runtime._startup_worker.future.result, 3)
        for command, hours in (('enable_wallet', None), ('recharge_wallet', 2.5)):
            denied = await asyncio.to_thread(execute_admin_request, self.address, AdminCallRequest(command=command, arguments={'machine': 'local', **({} if hours is None else {'node_hours': hours})}, password='wrong'))
            self.assertEqual(denied.code, 'authentication_failed')
            self.assertEqual(manager(lambda workflows: workflows.getManager(ActionClass.COMPUTE).getWallets()), {})
        for command, hours in (('enable_wallet', None), ('recharge_wallet', 2.5),
                               ('disable_wallet', None)):
            result = await asyncio.to_thread(execute_admin_request, self.address, AdminCallRequest(command=command, arguments={'machine': 'local', **({} if hours is None else {'node_hours': hours})}, password=self.password))
            self.assertTrue(result.success, result)
        snapshot = manager(lambda workflows: workflows.getManager(ActionClass.COMPUTE).getWallets())
        self.assertEqual(snapshot['local']['available_node_hours'], 2.5)
        self.assertFalse(snapshot['local']['enabled'])
        # Invalid command-specific fields must fail before changing any state.
        invalid = await self._send(json.dumps(dict(type='call', command='enable_wallet', password=self.password, arguments=dict(machine='local', path='/unexpected'))).encode() + b'\n')
        self.assertFalse(invalid['success'])
        self.assertEqual(manager(lambda workflows: workflows.getManager(ActionClass.COMPUTE).getWallets()), snapshot)
        manager.stop(False, close_database=True)
        reopened = WorkflowData(database)
        self.addCleanup(reopened.conn.close)
        self.assertEqual(reopened.getManager(ActionClass.COMPUTE).getWallets(), snapshot)
        self.queries.assert_not_called()

    async def test_password_is_required_for_each_call_and_never_echoed(self):
        """Authenticate each connection and prove failures have no sandbox/API effects."""
        with patch.object(sandbox, 'addSandboxDirs', wraps=sandbox.addSandboxDirs) as add:
            for password in ('wrong-credential', ''):
                failed = await self._call(password=password)
                self.assertEqual(failed.code, 'authentication_failed')
            missing = await self._send(b'{"command":"add_sandbox","machine":"local","path":"/unused"}\n')
            self.assertEqual(missing['code'], 'authentication_failed')
            add.assert_not_called()
            self.assertEqual(sandbox.remote_workdir, {'local': []})
            self.assertTrue((await self._call()).success)
            failed_again = await self._call('remove_sandbox', password='wrong-credential')
            self.assertEqual(failed_again.code, 'authentication_failed')
            self.assertEqual([root.path for root in sandbox.remote_workdir['local']], [str(self.root)])
        self.queries.assert_not_called()
        self.assertNotIn(self.password, str(self.logs.call_args_list))
        self.assertNotIn(self.password, failed.model_dump_json())
        self.assertNotIn(self.password, repr(AdminCallRequest(command='add_sandbox', password=self.password, arguments=dict(machine='local', path=str(self.root)))))

    async def test_add_remove_changes_later_checks_without_deleting_files(self):
        """Add and remove a real directory; explicitly exclude /tmp's implicit allowance.

        Existing files remain and repeated add/removal is idempotent. Ordinary
        /tmp access stays permitted independently of the registered roots.
        """
        artifact = self.root / 'retained'
        artifact.write_text('keep')
        self.assertFalse(sandbox.checkSafePath('local', artifact, allow_tmp=False))
        self.assertTrue((await self._call()).success)
        self.assertTrue((await self._call()).success)
        self.assertEqual([root.path for root in sandbox.remote_workdir['local']], [str(self.root)])
        self.assertTrue(sandbox.checkSafePath('local', artifact, allow_tmp=False))
        for _ in range(2):
            self.assertTrue((await self._call('remove_sandbox')).success)
        self.assertFalse(sandbox.checkSafePath('local', artifact, allow_tmp=False))
        self.assertTrue(sandbox.checkSafePath('local', artifact))
        self.assertEqual(artifact.read_text(), 'keep')

    async def test_invalid_inputs_and_unavailable_manager_do_not_mutate_roots(self):
        """Reject unsupported commands/fields and unhealthy runtimes without side effects."""
        for extra in ({'command': '__getattribute__'}, {'unexpected': self.password}, {'path': ''}):
            request = dict(type='call', command='add_sandbox', password=self.password, arguments=dict(machine='local', path=str(self.root)))
            request.update(extra)
            result = await self._send(json.dumps(request).encode() + b'\n')
            self.assertEqual(result['code'], 'invalid_request')
            self.assertNotIn(self.password, json.dumps(result))
        rejected = await self._call(path='~/unsupported')
        self.assertEqual(rejected.code, 'invalid_request')
        self.assertIn("'~'", rejected.message)
        self.runtime.stop()
        self.assertEqual((await self._call()).code, 'manager_unavailable')
        self.assertEqual(sandbox.remote_workdir, {'local': []})

    async def test_fragmented_malformed_oversized_and_incomplete_messages(self):
        """Exercise stream framing and recover after malformed or oversized clients."""
        for payload in (b'not json\n', b'{}', b'x' * 20000 + b'\n', b'{"password": "x"}\n', b'\xff\n'):
            result = await self._send(payload)
            self.assertFalse(result['success'])
        reader, writer = await asyncio.open_unix_connection(self.address)
        try:
            payload = json.dumps(dict(type='call', command='add_sandbox', password=self.password, arguments=dict(machine='local', path=str(self.root)))).encode() + b'\n'
            # A stream may split at any byte; the server waits for the full frame.
            writer.write(payload[:12])
            await writer.drain()
            await asyncio.sleep(.01)
            writer.write(payload[12:])
            await writer.drain()
            self.assertTrue(json.loads(await reader.readline())['success'])
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_incomplete_client_times_out_without_affecting_next_request(self):
        """Bound an idle reader, then verify a fresh authenticated connection still works."""
        with patch('amscjobmanager.server.admin._READ_TIMEOUT', .02):
            reader, writer = await asyncio.open_unix_connection(self.address)
            try:
                self.assertEqual(json.loads(await reader.readline())['code'], 'invalid_request')
            finally:
                writer.close()
                await writer.wait_closed()
        self.assertTrue((await self._call()).success)

    async def test_slow_validation_keeps_other_clients_responsive(self):
        """Registration never probes; a later blocking check does not block the admin listener."""
        entered, release = Event(), Event()

        def probe(machine, path):
            """Pause a sandbox directory probe for machine/path (str), then return True."""
            entered.set()
            if not release.wait(3):
                raise TimeoutError('Test did not release probe')
            return True

        with patch('amscjobmanager.api_general.isDirectory', side_effect=probe) as directory_probe:
            self.assertTrue((await self._call()).success)
            directory_probe.assert_not_called()
            pending = asyncio.create_task(asyncio.to_thread(
                sandbox.checkSafePath, 'local', self.root / 'file', allow_tmp=False))
            try:
                self.assertTrue(await asyncio.to_thread(entered.wait, 2))
                result = await asyncio.wait_for(self._call(password='wrong'), 1)
                self.assertEqual(result.code, 'authentication_failed')
            finally:
                release.set()
            self.assertTrue(await pending)

    async def test_private_permissions_conflicts_and_shutdown_cleanup(self):
        """Check private modes, refusal to overwrite, and cleanup of only the owned socket."""
        self.assertEqual(stat.S_IMODE(self.address.parent.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE(self.address.stat().st_mode), 0o600)
        duplicate = AdminServer(self.address, 'other-password', self.runtime)
        with self.assertRaises(FileExistsError):
            await duplicate.start()
        await duplicate.stop()
        self.assertTrue(self.address.is_socket())
        # A replaced entry is not ours to remove, even during shutdown.
        self.address.unlink()
        self.address.write_text('replacement')
        await self.server.stop()
        self.assertEqual(self.address.read_text(), 'replacement')

    async def test_relative_socket_paths_connect_and_literal_tilde_remains_rejected(self):
        """Bind and connect using a relative path while preserving literal-tilde rejection."""
        absolute = self.root / 'relative' / 'admin.sock'
        relative = os.path.relpath(absolute)
        server = AdminServer(relative, self.password, self.runtime)
        self.addAsyncCleanup(server.stop)
        self.assertEqual(server.socket_path, absolute)
        await server.start()
        result = await asyncio.to_thread(execute_admin_request, relative, AdminCallRequest(command='add_sandbox', arguments={'machine': 'local', 'path': str(self.root)}, password=self.password))
        self.assertTrue(result.success)
        with self.assertRaisesRegex(ValueError, "containing '~'"):
            AdminServer('./socket~', self.password, self.runtime)
        with self.assertRaisesRegex(ValueError, "containing '~'"):
            await asyncio.to_thread(execute_admin_request, './socket~', AdminCallRequest(command='add_sandbox', arguments={'machine': 'local', 'path': str(self.root)}, password=self.password))

    async def test_shared_parent_warns_but_commands_work_and_existing_entries_are_rejected(self):
        """Allow a mode-0755 parent with advice, keeping per-call auth and socket permissions."""
        shared = self.root / 'shared'
        shared.mkdir(mode=0o755)
        server = AdminServer(shared / 'admin.sock', self.password, self.runtime)
        self.addAsyncCleanup(server.stop)
        self.logs.reset_mock()
        await server.start()
        self.assertEqual(stat.S_IMODE(server.socket_path.stat().st_mode), 0o600)
        self.assertIn('0700', str(self.logs.call_args_list))
        result = await asyncio.to_thread(execute_admin_request, server.socket_path, AdminCallRequest(command='add_sandbox', arguments={'machine': 'local', 'path': str(self.root)}, password=self.password))
        self.assertTrue(result.success)
        failed = await asyncio.to_thread(execute_admin_request, server.socket_path, AdminCallRequest(command='remove_sandbox', arguments={'machine': 'local', 'path': str(self.root)}, password='wrong-password'))
        self.assertEqual(failed.code, 'authentication_failed')
        for name in ('file', 'link'):
            address = self.address.parent / name
            if name == 'file':
                address.write_text('retain')
            else:
                address.symlink_to(self.root / 'absent')
            with self.assertRaises(FileExistsError):
                await AdminServer(address, self.password, self.runtime).start()
            self.assertTrue(os.path.lexists(address))

    async def test_other_owned_parent_warns_instead_of_preventing_startup(self):
        """Exercise the advisory ownership check without requiring root or changing ownership."""
        directory = self.root / 'other-parent'
        directory.mkdir(mode=0o700)
        server = AdminServer(directory / 'admin.sock', self.password, self.runtime)
        self.addAsyncCleanup(server.stop)
        self.logs.reset_mock()
        with patch('amscjobmanager.server.admin.os.geteuid', return_value=os.geteuid() + 1):
            await server.start()
        self.assertTrue(server.socket_path.is_socket())
        self.assertIn('owned by another user', str(self.logs.call_args_list))

    async def test_stop_closes_incomplete_connections_and_removes_socket(self):
        """Shut down with a client awaiting input; no connection or socket survives."""
        reader, writer = await asyncio.open_unix_connection(self.address)
        await asyncio.sleep(.01)
        await self.server.stop()
        self.assertEqual(await reader.read(), b'')
        self.assertFalse(self.address.exists())
        writer.close()
        await writer.wait_closed()

    async def test_connection_limit_rejects_extra_clients_before_authentication(self):
        """Fill all four reader slots and reject an extra connection without hashing input."""
        clients = [await asyncio.open_unix_connection(self.address) for _ in range(4)]
        try:
            await asyncio.sleep(.02)
            with patch('amscjobmanager.server.admin.hashlib.scrypt') as derive:
                result = await self._send(b'{"password":"ignored"}\n')
                self.assertEqual(result['code'], 'busy')
                derive.assert_not_called()
        finally:
            for _, writer in clients:
                writer.close()
            await asyncio.gather(*(writer.wait_closed() for _, writer in clients))

    async def test_client_rejects_nonprivate_socket_before_sending_credentials(self):
        """Refuse a socket accessible to other users; restore its permissions afterward."""
        self.address.chmod(0o666)
        try:
            with self.assertRaisesRegex(ValueError, 'private'):
                await self._call()
            self.assertEqual(self.server._connections, set())
        finally:
            self.address.chmod(0o600)

    async def test_admin_bind_failure_closes_partial_http_startup(self):
        """Keep an existing admin socket intact and close HTTP when admin startup fails."""
        try:
            from amscjobmanager.server.runner import ManagerHTTPServer
        except ImportError as exc:
            self.skipTest(f'Install amsc-job-manager[server]: {exc}')
        initialize = Mock(side_effect=AssertionError('Manager must not start after admin bind failure'))
        server = ManagerHTTPServer(initialize, port=0, log_path=self.root / 'http.log',
                                   admin_socket=self.address, admin_password='another-password')
        with self.assertRaises(FileExistsError):
            await server.serve()
        initialize.assert_not_called()
        self.assertTrue(self.address.is_socket())
        self.assertTrue(all(not listener.is_serving() for listener in server.servers))


class SandboxRemovalTests(unittest.TestCase):
    def test_successful_records_are_retained_when_a_later_directory_fails(self):
        """Cache each successful probe and retry only failed roots; duplicate add retains flags."""
        roots = [sandbox.SandboxDirectory('/safe/first'), sandbox.SandboxDirectory('/safe/second')]
        with patch.object(sandbox, 'remote_workdir', {'local': roots}), \
             patch('amscjobmanager.api_general.isDirectory', side_effect=[True, False, True]) as probe, \
             patch('amscjobmanager.utils.queryYesNo', return_value=False):
            with self.assertRaisesRegex(Exception, 'does not exist'):
                sandbox.validateSandboxDirs('local')
            self.assertEqual([root.validated for root in sandbox.remote_workdir['local']], [True, False])
            first = sandbox.remote_workdir['local'][0]
            sandbox.addSandboxDirs({'local': '/safe/first'})
            self.assertIs(sandbox.remote_workdir['local'][0], first)
            sandbox.validateSandboxDirs('local')
            self.assertTrue(all(root.validated for root in sandbox.remote_workdir['local']))
            self.assertEqual([call.args for call in probe.call_args_list], [
                ('local', '/safe/first'), ('local', '/safe/second'), ('local', '/safe/second')])

    def test_late_success_cannot_validate_a_removed_and_readded_path(self):
        """Replace a registration during its probe; the new record needs its own successful probe."""
        original = sandbox.SandboxDirectory('/safe')
        calls = 0

        def probe(machine, path):
            """Replace machine/path (str) on the first probe; reject the new registration."""
            nonlocal calls
            calls += 1
            if calls == 1:
                sandbox.removeSandboxDirs({machine: path})
                sandbox.addSandboxDirs({machine: path})
                return True
            return False

        with patch.object(sandbox, 'remote_workdir', {'local': [original]}), \
             patch('amscjobmanager.api_general.isDirectory', side_effect=probe) as directory_probe, \
             patch('amscjobmanager.utils.queryYesNo', return_value=False):
            with self.assertRaisesRegex(Exception, 'does not exist'):
                sandbox.checkSafePath('local', '/safe/output')
            self.assertEqual(calls, 2)
            self.assertIsNot(sandbox.remote_workdir['local'][0], original)
            self.assertFalse(sandbox.remote_workdir['local'][0].validated)
            directory_probe.side_effect = None
            directory_probe.return_value = True
            self.assertTrue(sandbox.checkSafePath('local', '/safe/output'))

    def test_removal_preserves_overlaps_and_clears_only_matching_pending_checks(self):
        """Remove exact records and their flags, preserving overlapping registrations."""
        roots = [sandbox.SandboxDirectory('/safe', True), sandbox.SandboxDirectory('/safe/child'),
                 sandbox.SandboxDirectory('/safe/child/'), sandbox.SandboxDirectory('/other')]
        with patch.object(sandbox, 'remote_workdir', {'local': roots}):
            sandbox.removeSandboxDirs({'LOCAL': '/safe/child'})
            self.assertEqual(sandbox.remote_workdir['local'], [roots[0], roots[2], roots[3]])
            sandbox.removeSandboxDirs({'local': '/other'})
            self.assertEqual(sandbox.remote_workdir['local'], [roots[0], roots[2]])
            with self.assertRaises(ValueError):
                sandbox.removeSandboxDirs({'local': ['/safe', '~/bad']})
            self.assertIn(roots[0], sandbox.remote_workdir['local'])

    def test_removal_during_validation_does_not_restore_pending_roots(self):
        """Remove a root while validation waits; the late result cannot register it again."""
        entered, release = Event(), Event()

        def probe(machine, path):
            """Block the machine/path (str) probe until the root is removed; return True."""
            entered.set()
            return release.wait(2)

        with patch.object(sandbox, 'remote_workdir', {'local': [sandbox.SandboxDirectory('/safe')]}), \
             patch('amscjobmanager.api_general.isDirectory', side_effect=probe):
            thread = Thread(target=sandbox.validateSandboxDirs, args=('local',))
            thread.start()
            try:
                self.assertTrue(entered.wait(2))
                sandbox.removeSandboxDirs({'local': '/safe'})
            finally:
                release.set()
                thread.join(3)
            self.assertFalse(thread.is_alive())
            self.assertEqual(sandbox.remote_workdir['local'], [])


class AdminUtilityIntegrationTests(unittest.TestCase):
    def test_admin_help_uses_discovered_schemas_without_password(self):
        """Generate every admin help page from actual registry descriptions without authentication.

        Stub only socket I/O; command definitions come from a real WorkflowData.
        Offline help shows connection instructions, while detailed help uses
        SOCKET -h COMMAND and never prompts or submits an execution request.
        """
        import io
        import runpy
        from contextlib import redirect_stdout
        from amscjobmanager.workflow_data import WorkflowData
        from amscjobmanager.server.functions import FunctionRegistry

        workflows = WorkflowData()
        self.addCleanup(workflows.conn.close)
        descriptions = FunctionRegistry(workflows).describe('admin')
        main = runpy.run_path('utilities/manager_admin.py')['main']
        for flag in ('-h', '--help'):
            with patch.dict(main.__globals__, {'execute_admin_request': Mock(side_effect=AssertionError('Offline help connected'))}), redirect_stdout(io.StringIO()):
                self.assertEqual(main([flag]), 0)
            for function in descriptions:
                output = io.StringIO()
                discover = Mock(return_value=AdminResponse(success=True, code='ok', message='Available.', functions=descriptions))
                with patch.dict(main.__globals__, {'execute_admin_request': discover}), \
                     patch('getpass.getpass', side_effect=AssertionError('Help asked for password')), redirect_stdout(output):
                    self.assertEqual(main(['/unused/admin.sock', flag, function.name]), 0)
                self.assertEqual(discover.call_count, 1)
                self.assertIsInstance(discover.call_args.args[1], AdminDiscoveryRequest)
                self.assertIn('Inputs (server schema)', output.getvalue())
                self.assertIn('Outputs (server schema)', output.getvalue())
                self.assertIn(function.name, output.getvalue())

    def test_frontends_reject_exposed_password_input(self):
        """Run both real frontends without a terminal; neither may fall back to visible stdin.

        A separate session prevents accidental use of the test runner's terminal.
        Argument validation also rejects a startup password without a socket and
        must not echo that literal credential in its error message.
        """
        commands = [
            [sys.executable, 'utilities/manager_admin.py', '/unused/admin.sock',
             'add_sandbox', 'local', '/unused'],
            [sys.executable, 'utilities/manager_server.py', '/unused/config.json',
             '--database', '/unused/db', '--admin-socket', '/unused/admin.sock'],
        ]
        for command in commands:
            with self.subTest(command=command[1]):
                result = subprocess.run(command, stdin=subprocess.DEVNULL, capture_output=True,
                                        text=True, start_new_session=True, timeout=10)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('Admin connection failed' if command[1].endswith('manager_admin.py') else 'hidden password input', result.stderr)
        secret = 'do-not-echo-this-credential'
        result = subprocess.run([sys.executable, 'utilities/manager_server.py', '/unused/config.json',
                                 '--database', '/unused/db', '--admin-password', secret],
                                capture_output=True, text=True, timeout=10)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('requires --admin-socket', result.stderr)
        self.assertNotIn(secret, result.stdout + result.stderr)

    def _runAdminUtility(self, address, command, argument, password):
        """Run the real CLI with address/command/password (str) and argument (str | None).

        Return (exit code int, output str). argument supplies a sandbox path or
        recharge amount; None is used for enable/disable commands.

        Enter the secret only after the pseudo-terminal prompt, when echo is
        disabled. Bound the child to ten seconds. Never put it in argv or logs.
        """
        master, slave = pty.openpty()
        process = subprocess.Popen([sys.executable, 'utilities/manager_admin.py', str(address),
                                    command, 'local', *([] if argument is None else [str(argument)])], stdin=slave, stdout=slave,
                                   stderr=slave, start_new_session=True)
        os.close(slave)
        output = b''
        sent = False
        deadline = time.monotonic() + 10
        try:
            while time.monotonic() < deadline:
                if select.select([master], [], [], .05)[0]:
                    try:
                        chunk = os.read(master, 4096)
                    except OSError as exc:
                        if exc.errno == errno.EIO:
                            break
                        raise
                    if not chunk:
                        break
                    output += chunk
                    if b'Admin password:' in output and not sent:
                        os.write(master, password.encode() + b'\n')
                        sent = True
                if process.poll() is not None:
                    break
            code = process.wait(timeout=2)
            self.assertTrue(sent, 'CLI did not prompt for a password')
            self.assertNotIn(password.encode(), output)
            return code, output.decode()
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
            os.close(master)

    def test_actual_server_and_admin_cli_keep_privileges_out_of_mcp(self):
        """Start the real server, enter passwords privately, and check API writes through MCP.

        The target directory is outside /tmp, so implicit temporary access cannot
        mask missing enforcement. Writes fail before addition, succeed after
        addition, and fail after removal. Wrong credentials leave the root granted.
        MCP discovery/direct calls expose no admin operations. Shutdown removes
        the socket; restart restores configured roots without retaining grants.
        """
        try:
            from amscjobmanager.server.client import execute_requests, ToolRequest
        except ImportError as exc:
            self.skipTest(f'Install amsc-job-manager[server]: {exc}')
        with tempfile.TemporaryDirectory(prefix='amsc-live-admin-') as temporary, \
             tempfile.TemporaryDirectory(prefix='amsc-admin-target-', dir=Path.cwd()) as target:
            root, directory = Path(temporary), Path(target)
            address = root / 'private' / 'admin.sock'
            socket_argument = os.path.relpath(address)
            config = root / 'config.json'
            config.write_text(json.dumps({'iri_implementations': {}, 'sandbox_directories': {'local': []}}))
            with socket.socket() as reserved:
                reserved.bind(('127.0.0.1', 0))
                port = reserved.getsockname()[1]
            password = 'integration-admin-credential'
            master, slave = pty.openpty()
            process = subprocess.Popen([sys.executable, 'utilities/manager_server.py', str(config),
                                        '--database', str(root / 'manager.sqlite'), '--port', str(port),
                                        '--poll-interval', '.05', '--admin-socket', socket_argument],
                                       stdin=slave, stdout=slave, stderr=slave, start_new_session=True)
            os.close(slave)
            output = b''
            sent = False
            url = f'http://127.0.0.1:{port}/mcp'

            def call(tool, arguments=None):
                """Call tool (str) with arguments (dict or None); return the CLI-style result dict."""
                if tool == 'list_tools':
                    return asyncio.run(execute_requests(url, None, list_tools=True, timeout=5))
                return asyncio.run(execute_requests(url, ToolRequest(tool=tool, arguments=arguments or {}), timeout=5))

            def write(name):
                """Submit a local write to name (str), return its terminal workflow-status name."""
                submitted = call('submit_workflow', {'stages': [{'type': 'api', 'operations': [
                    {'type': 'write_file', 'machine': 'local', 'path': str(directory / name), 'content': 'test'}]}]})
                self.assertTrue(submitted['success'], submitted)
                workflow_id = submitted['results'][0]['result']['workflow_id']
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    listing = call('list_workflows', {'workflow_id': workflow_id})
                    state = listing['results'][0]['result'][0]['workflow_status']
                    if state in ('COMPLETED', 'FAILED'):
                        return state
                    time.sleep(.05)
                self.fail('Workflow did not finish')

            try:
                deadline = time.monotonic() + 15
                while time.monotonic() < deadline:
                    if select.select([master], [], [], .05)[0]:
                        output += os.read(master, 4096)
                    if b'Set admin password:' in output and not sent:
                        os.write(master, password.encode() + b'\n')
                        sent = True
                    log = root / 'manager.sqlite.log'
                    if address.exists() and log.exists() and 'Workflow manager ready' in log.read_text():
                        break
                    if process.poll() is not None:
                        self.fail('Server exited before readiness: ' + output.decode())
                else:
                    self.fail('Server did not become ready: ' + output.decode())
                self.assertNotIn(password.encode(), output)
                discovery = call('list_tools')
                self.assertNotIn('add_sandbox', json.dumps(discovery))
                self.assertFalse(call('add_sandbox', {'machine': 'local', 'path': target})['success'])
                self.assertEqual(call('list_sandbox_directories')['results'][0]['result'], {'local': []})
                # Wallet mutations travel only over the password-protected CLI;
                # read-only MCP inspection reflects the same durable balances.
                self.assertNotIn('recharge_wallet', json.dumps(discovery))
                self.assertEqual(self._runAdminUtility(socket_argument, 'enable_wallet', None, password)[0], 0)
                self.assertEqual(self._runAdminUtility(socket_argument, 'recharge_wallet', '1.5', password)[0], 0)
                wallet = call('list_compute_wallets')['results'][0]['result']['local']
                self.assertTrue(wallet['enabled'])
                self.assertEqual(wallet['available_node_hours'], 1.5)
                denied = self._runAdminUtility(socket_argument, 'recharge_wallet', '5', 'wrong')
                self.assertNotEqual(denied[0], 0)
                self.assertEqual(call('list_compute_wallets')['results'][0]['result']['local'], wallet)
                self.assertEqual(self._runAdminUtility(socket_argument, 'disable_wallet', None, password)[0], 0)
                self.assertEqual(write('denied-before'), 'FAILED')
                self.assertEqual(self._runAdminUtility(socket_argument, 'add_sandbox', directory, password)[0], 0)
                listed = call('list_sandbox_directories', {'machine': 'LOCAL'})
                self.assertEqual(listed['results'][0]['result'], {'local': [target]})
                self.assertEqual(write('allowed'), 'COMPLETED')
                self.assertTrue((directory / 'allowed').is_file())
                self.assertNotEqual(self._runAdminUtility(socket_argument, 'remove_sandbox', directory, 'incorrect-password')[0], 0)
                self.assertEqual(write('still-allowed'), 'COMPLETED')
                self.assertEqual(self._runAdminUtility(socket_argument, 'remove_sandbox', directory, password)[0], 0)
                self.assertEqual(call('list_sandbox_directories')['results'][0]['result'], {'local': []})
                self.assertEqual(write('denied-after'), 'FAILED')
                self.assertEqual(call('list_user_queries')['results'][0]['result'], [])
                # Leave a live grant at shutdown so restart genuinely tests that
                # admin registration changes are not persisted with workflows.
                self.assertEqual(self._runAdminUtility(socket_argument, 'add_sandbox', directory, password)[0], 0)
            finally:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                os.close(master)
            self.assertFalse(address.exists())
            self.assertNotIn(password, (root / 'manager.sqlite.log').read_text())
            self.assertNotIn(password, config.read_text())

            # Reopen the same workflow database with the unchanged configuration.
            # No admin flags means no admin listener or password prompt at all.
            restarted_log = root / 'restarted.log'
            with (root / 'restart-output').open('w') as log:
                restarted = subprocess.Popen([sys.executable, 'utilities/manager_server.py', str(config),
                                               '--database', str(root / 'manager.sqlite'), '--port', str(port),
                                               '--poll-interval', '.05', '--log-file', str(restarted_log)],
                                              stdin=subprocess.DEVNULL, stdout=log, stderr=log)
                try:
                    deadline = time.monotonic() + 10
                    while time.monotonic() < deadline:
                        if restarted_log.exists() and 'Workflow manager ready' in restarted_log.read_text():
                            break
                        if restarted.poll() is not None:
                            self.fail('Restart failed; see restart-output')
                        time.sleep(.05)
                    else:
                        self.fail('Restart did not become ready')
                    self.assertFalse(address.exists())
                    response = call('list_compute_wallets')
                    self.assertTrue(response['success'], f'{response}\n{restarted_log.read_text()}')
                    persisted_wallet = response['results'][0]['result']['local']
                    self.assertFalse(persisted_wallet['enabled'])
                    self.assertEqual(persisted_wallet['available_node_hours'], 1.5)
                    self.assertEqual(write('denied-after-restart'), 'FAILED')
                    self.assertTrue((directory / 'allowed').exists())
                finally:
                    restarted.terminate()
                    try:
                        restarted.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        restarted.kill()
                        restarted.wait()


if __name__ == '__main__':
    unittest.main()
