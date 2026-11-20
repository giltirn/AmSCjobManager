"""Offline checks for lazy authentication and deferred sandbox validation."""

from concurrent.futures import ThreadPoolExecutor, TimeoutError
from contextlib import ExitStack
from threading import Event
import unittest
from unittest.mock import call, patch

import httpx

from amscjobmanager import globals as manager_globals
from amscjobmanager.iri_api import auth, globus, resources, rest, state


class IRILazySetupTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        for registry in (state.iri_compute_clients, state._implementation_credentials):
            self.stack.enter_context(patch.dict(registry, {}, clear=True))
        self.stack.enter_context(patch.object(manager_globals, "remote_workdir", None))
        self.stack.enter_context(patch.object(state, "_transfer_token_path", None))
        self.stack.enter_context(patch.object(state, "_configuration_cache_directory", None))
        self.stack.enter_context(patch.dict(state.tokens, {key: None for key in state.tokens}))
        self.requests = []

        def respond(request):
            self.requests.append(request)
            return httpx.Response(200, json={"ok": True})

        def authenticate(implementation, token_path, token_type):
            client = state.IRIComputeClient(implementation, "fixture-token", http_client=httpx.Client(
                transport=httpx.MockTransport(respond)))
            self.addCleanup(client.close)
            return client

        self.login = self.stack.enter_context(patch.object(state, "setupIRIapiCompute", side_effect=authenticate))
        self.transfer_login = self.stack.enter_context(patch.object(state, "setupIRIapiGlobusTransfer"))
        self.is_directory = self.stack.enter_context(patch("amscjobmanager.api_general.isDirectory", return_value=True))
        self.work_dirs = state.setupWorkflowAgent(
            {name: {"token_path": f"{name}.json", "token_type": "amsc_pat"} for name in ("nersc", "alcf")},
            "transfer.json", {"perlmutter": ["/sandbox/nersc"], "aurora": ["/sandbox/alcf"]},
        )

    def test_setup_and_configuration_queries_do_not_authenticate_or_probe(self):
        self.assertEqual(set(state.getConfiguredImplementations()), {"nersc", "alcf"})
        self.assertIn("aurora", resources.getKnownMachines())
        self.assertIn("perlmutter", resources.getKnownMachines())
        self.assertTrue(resources.getMachineQueues("perlmutter"))
        self.assertEqual(manager_globals.remote_workdir["aurora"], [manager_globals.SandboxDirectory("/sandbox/alcf")])
        self.assertEqual(self.work_dirs, {'perlmutter': ['/sandbox/nersc'], 'aurora': ['/sandbox/alcf']})
        self.login.assert_not_called()
        self.transfer_login.assert_not_called()
        self.is_directory.assert_not_called()

    def test_sandbox_check_can_obtain_a_client_without_recursive_validation(self):
        """Validate one target through real client retrieval without a recursion guard."""
        def check_directory(machine, directory):
            """Probe machine/directory (str) through REST, returning True without recursion."""
            rest.get(machine, "sandbox-probe")
            return True

        self.is_directory.side_effect = check_directory
        self.assertTrue(manager_globals.checkSafePath("perlmutter", "/sandbox/nersc/file"))
        rest.get("perlmutter", "operation")
        rest.get("perlmutter", "operation")
        self.login.assert_called_once_with("nersc", "nersc.json", token_type="amsc_pat")
        self.is_directory.assert_called_once_with("perlmutter", "/sandbox/nersc")
        self.assertEqual([request.url.path for request in self.requests], [
            "/api/v1/sandbox-probe", "/api/v1/operation", "/api/v1/operation",
        ])
        self.assertNotIn("alcf", state.iri_compute_clients)
        self.assertFalse(manager_globals.remote_workdir['aurora'][0].validated)

    def test_failed_sandbox_validation_blocks_authorization_and_remains_retryable(self):
        """Keep failed roots pending and retry without repeating successful authentication."""
        state.getImplementationClient("nersc")
        self.is_directory.return_value = False
        with patch("amscjobmanager.utils.queryYesNo", return_value=False):
            with self.assertRaisesRegex(Exception, "does not exist"):
                manager_globals.checkSafePath("perlmutter", "/sandbox/nersc/file")
        self.assertEqual(self.requests, [])
        self.is_directory.return_value = True
        self.assertTrue(manager_globals.checkSafePath("perlmutter", "/sandbox/nersc/file"))
        self.login.assert_called_once()
        self.assertEqual(self.is_directory.call_count, 2)

    def test_reads_and_client_creation_do_not_validate_sandboxes(self):
        """Defer probes until the first sandbox check, including a path rejected afterward."""
        state.getImplementationClient("nersc")
        rest.get("perlmutter", "read-only-operation")
        self.is_directory.assert_not_called()
        self.assertFalse(manager_globals.remote_workdir['perlmutter'][0].validated)
        self.assertFalse(manager_globals.checkSafePath("perlmutter", "/outside/file.txt"))
        self.is_directory.assert_called_once_with("perlmutter", "/sandbox/nersc")
        self.assertTrue(manager_globals.checkSafePath("perlmutter", "/sandbox/nersc/file.txt"))
        self.is_directory.assert_called_once_with("perlmutter", "/sandbox/nersc")
        self.assertTrue(manager_globals.checkSafePath("perlmutter", "/sandbox/nersc/another.txt"))
        self.assertEqual(self.is_directory.call_count, 1)

    def test_first_sandbox_check_validates_only_its_machine(self):
        """Machines sharing credentials validate independently, reusing their common client."""
        manager_globals.addSandboxDirs({"perlmutter_login": ["/sandbox/login"]})

        def check_directory(machine, directory):
            """Retrieve machine/directory's shared client (str paths); return True."""
            state.getImplementationClient("nersc")
            return True

        self.is_directory.side_effect = check_directory
        self.assertTrue(manager_globals.checkSafePath("perlmutter", "/sandbox/nersc/file"))
        self.is_directory.assert_called_once_with("perlmutter", "/sandbox/nersc")
        self.assertFalse(manager_globals.remote_workdir['perlmutter_login'][0].validated)
        self.assertFalse(manager_globals.remote_workdir['aurora'][0].validated)
        self.assertTrue(manager_globals.checkSafePath("perlmutter_login", "/sandbox/login/file"))
        self.assertEqual(self.is_directory.call_args_list, [
            call("perlmutter", "/sandbox/nersc"), call("perlmutter_login", "/sandbox/login")])
        self.login.assert_called_once()

    def test_direct_client_creation_only_authenticates(self):
        """Explicit client initialization must not probe configured sandbox directories."""
        state.setupImplementationClient("nersc", "direct.json", "amsc_pat")
        self.is_directory.assert_not_called()
        self.assertFalse(manager_globals.remote_workdir['perlmutter'][0].validated)

    def test_local_sandboxes_do_not_require_an_iri_client(self):
        """Register a local root without probes; its first path check validates without IRI."""
        manager_globals.addSandboxDirs({"local": ["/sandbox/local"]})
        self.is_directory.assert_not_called()
        self.assertFalse(manager_globals.remote_workdir['local'][0].validated)
        self.assertTrue(manager_globals.checkSafePath("local", "/sandbox/local/file", allow_tmp=False))
        self.is_directory.assert_called_once_with("local", "/sandbox/local")
        self.login.assert_not_called()

    def test_sandbox_registration_derives_available_machines_from_current_setup(self):
        state.setupWorkflowAgent({"alcf": {"token_path": "alcf.json"}}, None, {})
        with self.assertWarnsRegex(RuntimeWarning, "not configured"):
            manager_globals.addSandboxDirs({
                "perlmutter": ["/sandbox/nersc"], "aurora": ["/sandbox/alcf"],
            }, replace=True)
        self.assertEqual(manager_globals.remote_workdir, {"aurora": [manager_globals.SandboxDirectory("/sandbox/alcf")]})
        self.login.assert_not_called()
        self.is_directory.assert_not_called()

    def test_missing_sandbox_can_still_be_created(self):
        """First authorization can create an approved missing root without reentering checks."""
        self.is_directory.return_value = False
        with patch("amscjobmanager.utils.queryYesNo", return_value=True), patch("amscjobmanager.api_general.remoteMkdir") as mkdir:
            self.assertTrue(manager_globals.checkSafePath("perlmutter", "/sandbox/nersc/file"))
            mkdir.assert_called_once_with("perlmutter", "/sandbox/nersc", allow_unsafe=True)
        self.assertTrue(all(root.validated for root in manager_globals.remote_workdir.get('perlmutter', [])))

    def test_concurrent_checks_wait_for_validation_but_other_machines_proceed(self):
        """Pause the first probe; a same-machine caller waits while another target completes."""
        entered, release, second_entered = Event(), Event(), Event()

        def probe(machine, directory):
            """Block perlmutter's machine/directory probe (str), returning True after release."""
            if machine == 'perlmutter':
                entered.set()
                if not release.wait(3):
                    raise TimeoutError('Probe was not released')
            return True

        def second_check():
            """Mark entry and return the second perlmutter path authorization (bool)."""
            second_entered.set()
            return manager_globals.checkSafePath('perlmutter', '/sandbox/nersc/second')

        self.is_directory.side_effect = probe
        with ThreadPoolExecutor(max_workers=3) as threads:
            first = threads.submit(manager_globals.checkSafePath, 'perlmutter', '/sandbox/nersc/first')
            try:
                self.assertTrue(entered.wait(2))
                second = threads.submit(second_check)
                self.assertTrue(second_entered.wait(2))
                with self.assertRaises(TimeoutError):
                    second.result(timeout=.05)
                other = threads.submit(manager_globals.checkSafePath, 'aurora', '/sandbox/alcf/file')
                self.assertTrue(other.result(timeout=1))
            finally:
                release.set()
            self.assertTrue(first.result(timeout=2))
            self.assertTrue(second.result(timeout=2))
        self.assertEqual(self.is_directory.call_args_list.count(call('perlmutter', '/sandbox/nersc')), 1)

    def test_additions_during_validation_are_checked_and_removed_roots_cannot_authorize(self):
        """Change registrations while a probe blocks; drain new roots and revoke the old grant."""
        entered, release = Event(), Event()

        def probe(machine, directory):
            """Pause the old machine/directory probe (str); accept all checked directories."""
            if directory == '/sandbox/nersc':
                entered.set()
                if not release.wait(3):
                    raise TimeoutError('Probe was not released')
            return True

        self.is_directory.side_effect = probe
        with ThreadPoolExecutor(max_workers=1) as threads:
            check = threads.submit(manager_globals.checkSafePath, 'perlmutter', '/sandbox/nersc/file')
            try:
                self.assertTrue(entered.wait(2))
                manager_globals.removeSandboxDirs({'perlmutter': '/sandbox/nersc'})
                manager_globals.addSandboxDirs({'perlmutter': '/sandbox/new'})
            finally:
                release.set()
            self.assertFalse(check.result(timeout=2))
        self.assertEqual(self.is_directory.call_args_list, [
            call('perlmutter', '/sandbox/nersc'), call('perlmutter', '/sandbox/new')])
        self.assertTrue(all(root.validated for root in manager_globals.remote_workdir.get('perlmutter', [])))
        self.assertTrue(manager_globals.checkSafePath('perlmutter', '/sandbox/new/file'))
        self.assertEqual(self.is_directory.call_count, 2)

    def test_authentication_failures_are_retryable(self):
        self.login.side_effect = [RuntimeError("authentication failed"), state.IRIComputeClient("nersc", "retry-token")]
        with self.assertRaisesRegex(RuntimeError, "authentication failed"):
            state.getImplementationClient("nersc")
        self.assertNotIn("nersc", state.iri_compute_clients)
        client = state.getImplementationClient("nersc")
        self.addCleanup(client.close)
        self.assertEqual(client.access_token, "retry-token")
        self.assertEqual(self.login.call_count, 2)

    def test_reinitialization_closes_clients_and_replaces_credentials_and_sandboxes(self):
        client = state.getImplementationClient("nersc")
        state.setupWorkflowAgent({"alcf": {"token_path": "new.json"}}, None, {"aurora": ["/new/sandbox"]})
        self.assertTrue(client.http_client.is_closed)
        self.assertEqual(state.getConfiguredImplementations(), ("alcf",))
        self.assertEqual(manager_globals.remote_workdir, {"aurora": [manager_globals.SandboxDirectory("/new/sandbox")]})
        with self.assertRaisesRegex(RuntimeError, "not configured"):
            state.getImplementationClient("nersc")
        self.login.assert_called_once()

    def test_transfer_authentication_is_lazy_and_scopes_include_unused_providers(self):
        self.assertIn("9032dd3a-e841-4687-a163-2720da731b5b", auth._globus_data_access_collections())
        self.login.assert_not_called()

        def authenticate(path):
            state.tokens.update(iriapi_transfer_base="iri-transfer-token", globus_transfer="globus-token")

        self.transfer_login.side_effect = authenticate
        with patch.object(state.iri_transfer_client, "get", return_value=httpx.Response(200, json={"status": "SUCCEEDED"})):
            self.assertEqual(globus.globusTransferStatus("test-task"), "SUCCEEDED")
            self.assertEqual(globus.globusTransferStatus("test-task"), "SUCCEEDED")
        self.assertEqual(state.getTransferToken("globus_transfer"), "globus-token")
        self.transfer_login.assert_called_once_with("transfer.json")
        self.login.assert_not_called()


if __name__ == "__main__":
    unittest.main()
