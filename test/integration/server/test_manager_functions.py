"""Extensible manager capabilities, typed dispatch, and transport-independent schemas.

Run: python test/run_tests.py integration --pattern test_manager_functions.py -v
The transport test needs optional server dependencies and permission for a local
Unix socket. No credentials, remote API, or actual compute job is used.
"""

from pathlib import Path
import tempfile
import sys
import subprocess
import unittest
from unittest.mock import patch

from pydantic import Field, RootModel, ValidationError

from amscjobmanager.actions_base import ActionClass
from amscjobmanager.compute_action_manager import ComputeActions
from amscjobmanager.workflow_data import WorkflowData
from amscjobmanager.manager import WorkflowManager
from amscjobmanager.manager_functions import FunctionArguments, FunctionDefinition
from amscjobmanager.server.functions import FunctionError, FunctionRegistry
from amscjobmanager.server.runtime import ServerRuntime


class ExtensionArguments(FunctionArguments):
    """Required value (nonnegative strict int) supplies an example manager input."""

    value: int = Field(ge=0, description='Nonnegative example input.')


class ExtensionResult(FunctionArguments):
    """result (int) is the implementation's computed example value."""

    result: int


class ExtensionComputeActions(ComputeActions):
    """Test implementation advertising new methods without transport modifications."""

    invocations = 0

    def inspectExtension(self, value: int, multiplier: int = 2) -> int:
        """Return value * multiplier (int inputs), counting invocations for test observability."""
        self.invocations += 1
        return value * multiplier

    def getMCPFunctions(self):
        """Describe inherited inspection and one annotated extension operation."""
        def _inspect(manager: ExtensionComputeActions, args: ExtensionArguments) -> ExtensionResult:
            """Return manager's computed result for validated args.value."""
            return ExtensionResult(result=manager.inspectExtension(args.value))

        return super().getMCPFunctions() + (FunctionDefinition(
            'inspect_extension', 'Inspect an extension-owned result.', _inspect, read_only=True, idempotent=True,
        ),)

    def getAdminFunctions(self):
        """Describe inherited admin commands and an adapter choosing its own argument binding."""
        def _apply(manager: ExtensionComputeActions, args: ExtensionArguments) -> ExtensionResult:
            """Call manager with args.value and a fixed multiplier; return the output model."""
            return ExtensionResult(result=manager.inspectExtension(args.value, multiplier=3))

        return super().getAdminFunctions() + (FunctionDefinition(
            'apply_extension', 'Apply an extension operation.', _apply, success_message='Extension applied.',
        ),)


class ExtensionWorkflowData(WorkflowData):
    """Advertise workflow-data operations without modifying transport code."""

    def getMCPFunctions(self):
        """Describe standard operations and a WorkflowData-owned extension."""
        def _inspect(workflows: ExtensionWorkflowData, args: ExtensionArguments) -> ExtensionResult:
            """Return args.value plus ten using the supplied WorkflowData owner."""
            return ExtensionResult(result=args.value + 10)

        return super().getMCPFunctions() + (FunctionDefinition(
            'inspect_workflow_extension', 'Inspect a WorkflowData extension.', _inspect, read_only=True,
        ),)

    def getAdminFunctions(self):
        """Describe standard administration and a WorkflowData-owned extension."""
        def _apply(workflows: ExtensionWorkflowData, args: ExtensionArguments) -> ExtensionResult:
            """Return args.value plus twenty using the supplied WorkflowData owner."""
            return ExtensionResult(result=args.value + 20)

        return super().getAdminFunctions() + (FunctionDefinition(
            'apply_workflow_extension', 'Apply a WorkflowData extension.', _apply,
        ),)


class ManagerFunctionTests(unittest.TestCase):
    """Test the public registration contract and dispatch against real WorkflowData owners."""

    def setUp(self):
        """Construct an empty database with the extension manager; register close cleanup."""
        self.workflows = ExtensionWorkflowData(_test_action_manager_factories={ActionClass.COMPUTE: ExtensionComputeActions})
        self.addCleanup(self.workflows.conn.close)

    def test_discovery_and_invocation_resolve_the_supplied_workflow_data(self):
        """Use one registry against two databases to prove manager instances are not captured."""
        registry = FunctionRegistry(self.workflows)
        other = ExtensionWorkflowData(_test_action_manager_factories={ActionClass.COMPUTE: ExtensionComputeActions})
        self.addCleanup(other.conn.close)
        descriptions = {item.name: item for item in registry.describe('mcp')}
        self.assertIn('inspect_extension', descriptions)
        self.assertNotIn('apply_extension', descriptions)
        description = descriptions['inspect_extension']
        self.assertEqual(description.parameter_order, ['value'])
        self.assertEqual(description.input_schema['required'], ['value'])
        self.assertTrue(description.read_only)
        result, _ = registry.invoke(other, 'mcp', 'inspect_extension', {'value': 4})
        self.assertEqual(result, {'result': 8})
        self.assertEqual(self.workflows.getManager(ActionClass.COMPUTE).invocations, 0)
        self.assertEqual(other.getManager(ActionClass.COMPUTE).invocations, 1)
        result, message = registry.invoke(other, 'admin', 'apply_extension', {'value': 4})
        self.assertEqual(result, {'result': 12})
        self.assertEqual(message, 'Extension applied.')

        # Workflow-owned operations resolve from the invocation's database,
        # just like action-manager operations, rather than capturing discovery state.
        receipt, _ = registry.invoke(other, 'mcp', 'submit_workflow',
                                    {'stages': [{'type': 'api', 'operations': []}]})
        self.assertEqual(self.workflows.listWorkflows(), [])
        self.assertEqual(other.listWorkflows()[0]['workflow_id'], receipt['workflow_id'])

    def test_invalid_arguments_and_wrong_channels_never_invoke_methods(self):
        """Reject extra fields, type coercions, invalid bounds, and admin calls through MCP."""
        registry = FunctionRegistry(self.workflows)
        for arguments in ({'value': -1}, {'value': True}, {'value': '2'}, {'value': 2, 'multiplier': 99}):
            with self.subTest(arguments=arguments), self.assertRaises(ValidationError):
                registry.invoke(self.workflows, 'admin', 'apply_extension', arguments)
        with self.assertRaises(FunctionError):
            registry.invoke(self.workflows, 'mcp', 'apply_extension', {'value': 2})
        self.assertEqual(self.workflows.getManager(ActionClass.COMPUTE).invocations, 0)

    def test_invalid_output_is_reported_after_exactly_one_invocation(self):
        """Distinguish a faulty result contract from invalid input, without retrying the operation."""
        registry = FunctionRegistry(self.workflows)
        with patch.object(ExtensionComputeActions, 'inspectExtension', return_value='not an integer') as operation:
            with self.assertRaises(FunctionError) as error:
                registry.invoke(self.workflows, 'admin', 'apply_extension', {'value': 3})
            self.assertEqual(error.exception.code, 'operation_failed')
            operation.assert_called_once_with(3, multiplier=3)

    def test_invalid_registrations_fail_before_publication(self):
        """Reject duplicate names, missing model hints, invalid arity, and non-model returns."""
        definition = self.workflows.getManager(ActionClass.COMPUTE).getMCPFunctions()[-1]
        from dataclasses import replace

        def _unannotated(manager, args):
            """Provide an intentionally unannotated registration without executing work."""
            raise AssertionError('Registration must not execute this function')

        def _wrong_arity(args: ExtensionArguments) -> ExtensionResult:
            """Provide a deliberately unsupported one-argument registration."""
            raise AssertionError('Registration must not execute this function')

        def _wrong_return(manager, args: ExtensionArguments) -> int:
            """Provide an annotated but non-model result contract."""
            raise AssertionError('Registration must not execute this function')

        def _root_result(manager, args: ExtensionArguments) -> RootModel[list[int]]:
            """Provide a root-array output that cannot be advertised as a structured object."""
            raise AssertionError('Registration must not execute this function')

        for definitions in ((definition, definition),
                            (self.workflows.getMCPFunctions()[0],),
                            (replace(definition, invoke=_unannotated),),
                            (replace(definition, invoke=_wrong_arity),),
                            (replace(definition, invoke=_wrong_return),),
                            (replace(definition, invoke=_root_result),)):
            with self.subTest(definitions=definitions), \
                 patch.object(ExtensionComputeActions, 'getMCPFunctions', return_value=definitions), \
                 self.assertRaises(ValueError):
                FunctionRegistry(self.workflows)

    def test_backend_definitions_do_not_import_server_or_mcp(self):
        """Describe all backend functions in a fresh process with both transport imports blocked."""
        script = """
import sys
sys.modules['mcp'] = None
sys.modules['amscjobmanager.server'] = None
from amscjobmanager.workflow_data import WorkflowData
from amscjobmanager.actions_base import ActionClass
from amscjobmanager.user_queries import UserQueryQueue
workflows = WorkflowData(max_workflows_active=0)
try:
    providers = [workflows, UserQueryQueue(), workflows.getManager(ActionClass.COMPUTE)]
    for provider in providers:
        for definition in provider.getMCPFunctions() + provider.getAdminFunctions():
            definition.describe()
finally:
    workflows.stopWorkers()
    workflows.conn.close()
"""
        result = subprocess.run([sys.executable, '-c', script], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_annotations_support_named_and_positional_only_parameters(self):
        """Infer models by parameter position rather than name, including positional-only callables."""
        def _call(owner, query: ExtensionArguments, /) -> ExtensionResult:
            """Return query.value in a result model; owner is accepted explicitly and unused."""
            return ExtensionResult(result=query.value)

        definition = FunctionDefinition('example', 'Example.', _call)
        self.assertEqual(definition.models(), (ExtensionArguments, ExtensionResult))
        self.assertEqual(definition.describe().parameter_order, ['value'])
        self.assertEqual(definition.invoke(None, ExtensionArguments(value=7)), ExtensionResult(result=7))

    def test_workflow_data_functions_work_without_a_background_manager(self):
        """Dispatch workflow callbacks against plain storage, preserving public response shapes.

        No WorkflowManager is constructed or started. Submit/list/cancel and
        extension operations all resolve their owner from the supplied WorkflowData.
        """
        registry = FunctionRegistry(self.workflows)
        receipt, _ = registry.invoke(self.workflows, 'mcp', 'submit_workflow',
                                    {'stages': [{'type': 'api', 'operations': []}]})
        self.assertEqual(set(receipt), {'workflow_id', 'workflow_status'})
        self.assertEqual(receipt['workflow_status'], 'PENDING')
        listing, _ = registry.invoke(self.workflows, 'mcp', 'list_workflows', {})
        self.assertEqual(listing['result'][0]['workflow_id'], receipt['workflow_id'])
        extended, _ = registry.invoke(self.workflows, 'mcp', 'inspect_workflow_extension', {'value': 4})
        self.assertEqual(extended, {'result': 14})
        extended, _ = registry.invoke(self.workflows, 'admin', 'apply_workflow_extension', {'value': 4})
        self.assertEqual(extended, {'result': 24})
        canceled, _ = registry.invoke(self.workflows, 'mcp', 'cancel_workflow',
                                     {'workflow_id': receipt['workflow_id']})
        self.assertEqual(canceled['workflow_status'], 'CANCELED')
        with self.assertRaises(FunctionError) as error:
            registry.invoke(self.workflows, 'mcp', 'cancel_workflow', {'workflow_id': 999})
        self.assertEqual(error.exception.code, 'workflow_not_found')

    def test_manager_dispatch_checks_health_under_the_lock_for_all_owners(self):
        """Use one lock/health path for workflow, action-manager, and admin operations.

        Inspect the real manager lock inside a health probe. Healthy callbacks
        run while it is held; unhealthy managers must reject every owner/channel
        before invoking its callback or changing workflow records.
        """
        manager = WorkflowManager(max_workflows_active=0)
        self.addCleanup(manager.stop, False, close_database=True)
        with manager as workflows:
            registry = FunctionRegistry(workflows)

        def health():
            """Assert the captured manager lock is held and report healthy without starting a thread."""
            self.assertTrue(manager._lock.locked())
            return True

        calls = (('mcp', 'list_workflows'), ('mcp', 'list_compute_wallets'),
                 ('admin', 'add_sandbox'))
        # Even a never-started manager can exercise the boundary using a health
        # probe; no production thread competes with this deterministic lock test.
        with patch.object(manager, 'isAlive', side_effect=health) as healthy:
            for channel, name in calls[:2]:
                result, _ = registry.invoke(None, channel, name, {}, manager=manager)
                self.assertIn('result', result)
            self.assertEqual(healthy.call_count, 2)
        with patch.object(manager, 'isAlive', return_value=False), \
             patch('amscjobmanager.api_general.getKnownMachines', return_value=['local']), \
             patch.object(manager.workflow_data, 'listWorkflows', side_effect=AssertionError('Unhealthy read')), \
             patch.object(manager.workflow_data.getManager(ActionClass.COMPUTE), 'getWallets',
                          side_effect=AssertionError('Unhealthy action read')), \
             patch('amscjobmanager.workflow_data.addSandboxDirs', side_effect=AssertionError('Unhealthy mutation')):
            for channel, name in calls:
                arguments = {'machine': 'local', 'path': '/tmp/blocked'} if channel == 'admin' else {}
                with self.subTest(name=name), self.assertRaises(FunctionError) as error:
                    registry.invoke(None, channel, name, arguments, manager=manager)
                self.assertEqual(error.exception.code, 'manager_unavailable')
        self.assertFalse(manager._lock.locked())

    def test_wallet_adapters_keep_enabled_out_of_public_inputs(self):
        """Retain separate enable/disable commands with typed machine-only inputs."""
        registry = FunctionRegistry(self.workflows)
        with patch('amscjobmanager.api_general.getKnownMachines', return_value=['local']):
            descriptions = {item.name: item for item in registry.describe('admin')}
            self.assertEqual(descriptions['enable_wallet'].parameter_order, ['machine'])
            self.assertNotIn('enabled', descriptions['enable_wallet'].input_schema['properties'])
            registry.invoke(self.workflows, 'admin', 'enable_wallet', {'machine': 'LOCAL'})
            registry.invoke(self.workflows, 'admin', 'recharge_wallet', {'machine': 'local', 'node_hours': 2})
            with self.assertRaises(ValidationError):
                registry.invoke(self.workflows, 'admin', 'disable_wallet', {'machine': 'local', 'enabled': True})
        wallet = self.workflows.getManager(ActionClass.COMPUTE).getWallets()['local']
        self.assertTrue(wallet['enabled'])
        self.assertEqual(wallet['available_node_hours'], 2)

    def test_runtime_reports_initialization_and_failure_without_publishing_functions(self):
        """A malformed registration fails initialization rather than exposing a partial catalog."""
        runtime = ServerRuntime(lambda: WorkflowManager(max_workflows_active=0))
        self.addCleanup(runtime.stop)
        with self.assertRaises(FunctionError) as error:
            runtime.describeFunctions('admin')
        self.assertEqual(error.exception.code, 'server_initializing')
        with patch.object(ComputeActions, 'getMCPFunctions', return_value=(
                self.workflows.getManager(ActionClass.COMPUTE).getMCPFunctions()[-1],) * 2):
            runtime.start()
            runtime._startup_worker.future.result(3)
        self.assertEqual(runtime.state, 'failed')
        self.assertIsNone(runtime.manager)
        with self.assertRaises(FunctionError) as error:
            runtime.describeFunctions('mcp')
        self.assertEqual(error.exception.code, 'manager_unavailable')


try:
    from mcp import Client
except ImportError:
    Client = None


@unittest.skipIf(Client is None, 'Install amsc-job-manager[server]')
class FunctionTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_extension_discovery_help_and_execution_on_both_transports(self):
        """Advertise a new implementation through MCP and a real admin socket, with no adapters.

        Before readiness metadata reports initialization and auth tools remain
        available. After readiness each transport sees only its own commands;
        shared parsing supports flags for MCP and positional inputs for admin.
        """
        import asyncio
        from amscjobmanager.server.admin import AdminCallRequest, AdminDiscoveryRequest, AdminServer, execute_admin_request
        from amscjobmanager.server.cli import parse_function_arguments
        from amscjobmanager.server.runner import ManagerHTTPServer

        temporary = self.enterContext(tempfile.TemporaryDirectory())
        with patch('amscjobmanager.workflow_data.ComputeActions', ExtensionComputeActions), \
             patch('amscjobmanager.manager.WorkflowData', ExtensionWorkflowData):
            manager = WorkflowManager(max_workflows_active=0)
        server = ManagerHTTPServer(lambda: manager, port=0, log_path=Path(temporary) / 'server.log')
        self.addCleanup(server.runtime.stop)
        admin = AdminServer(Path(temporary) / 'private' / 'admin.sock', 'secret', server.runtime)
        await admin.start()
        self.addAsyncCleanup(admin.stop)
        async with Client(server.mcp_server, cache=None) as client:
            tools = await client.list_tools()
            self.assertEqual(tools.meta['initialization_error']['code'], 'server_initializing')
            self.assertEqual({tool.name for tool in tools.tools}, {'list_user_queries', 'respond_to_user_query'})
            response_tool = next(tool for tool in tools.tools if tool.name == 'respond_to_user_query')
            self.assertFalse(response_tool.annotations.destructive_hint)
            auth_queries = await client.call_tool('list_user_queries', {})
            self.assertFalse(auth_queries.is_error)
            pending = await asyncio.to_thread(execute_admin_request, admin.socket_path, AdminDiscoveryRequest())
            self.assertEqual(pending.code, 'server_initializing')
            server.runtime.start()
            await asyncio.to_thread(server.runtime._startup_worker.future.result, 3)
            tools = await client.list_tools()
            names = {tool.name for tool in tools.tools}
            self.assertIn('inspect_extension', names)
            self.assertIn('inspect_workflow_extension', names)
            self.assertNotIn('apply_extension', names)
            response = await client.call_tool('inspect_extension', {'value': 5})
            self.assertEqual(response.structured_content, {'result': 10})
            response = await client.call_tool('inspect_workflow_extension', {'value': 5})
            self.assertEqual(response.structured_content, {'result': 15})
            rejected = await client.call_tool('apply_extension', {'value': 5})
            self.assertTrue(rejected.is_error)
            discovery = await asyncio.to_thread(execute_admin_request, admin.socket_path, AdminDiscoveryRequest())
            function = next(item for item in discovery.functions if item.name == 'apply_extension')
            self.assertNotIn('inspect_extension', {item.name for item in discovery.functions})
            self.assertNotIn('secret', discovery.model_dump_json())
            # The actual CLI must discover this previously unknown command,
            # with no local registration and no password required for help.
            process = await asyncio.create_subprocess_exec(
                sys.executable, 'utilities/manager_admin.py', str(admin.socket_path), '-h', 'apply_extension',
                stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            output, errors = await asyncio.wait_for(process.communicate(), timeout=10)
            self.assertEqual(process.returncode, 0, errors.decode())
            self.assertIn('Nonnegative example input', output.decode())
            self.assertNotIn('Admin password:', output.decode() + errors.decode())
            arguments = parse_function_arguments(function, ['5'], required_positionals=True)
            self.assertEqual(arguments, {'value': 5})
            help_text = parse_function_arguments(function, ['--help'], required_positionals=True)
            self.assertIn('Nonnegative example input', help_text)
            failed = await asyncio.to_thread(execute_admin_request, admin.socket_path,
                AdminCallRequest(command='apply_extension', arguments=arguments, password='wrong'))
            self.assertEqual(failed.code, 'authentication_failed')
            accepted = await asyncio.to_thread(execute_admin_request, admin.socket_path,
                AdminCallRequest(command='apply_extension', arguments=arguments, password='secret'))
            self.assertEqual(accepted.result, 15)
            self.assertEqual(accepted.message, 'Extension applied.')
            accepted = await asyncio.to_thread(execute_admin_request, admin.socket_path,
                AdminCallRequest(command='apply_workflow_extension', arguments=arguments, password='secret'))
            self.assertEqual(accepted.result, 25)


if __name__ == '__main__':
    unittest.main()
