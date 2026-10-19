"""Offline checks for lazy authentication and deferred sandbox validation."""

from contextlib import ExitStack
import unittest
from unittest.mock import call, patch

import httpx

from amscjobmanager import globals as manager_globals
from amscjobmanager.iri_api import auth, globus, resources, rest, state


class IRILazySetupTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        for registry in (state.iri_compute_clients, state._implementation_credentials,
                         manager_globals._pending_sandbox_checks):
            self.stack.enter_context(patch.dict(registry, {}, clear=True))
        self.stack.enter_context(patch.object(manager_globals, "_validating_sandboxes", set()))
        self.stack.enter_context(patch.object(manager_globals, "remote_workdir", None))
        self.stack.enter_context(patch.object(state, "_transfer_token_path", None))
        self.stack.enter_context(patch.object(state, "_configuration_cache_directory", None))
        self.stack.enter_context(patch.object(state, "_validating_clients", set()))
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
        state.setupWorkflowAgent(
            {name: {"token_path": f"{name}.json", "token_type": "amsc_pat"} for name in ("nersc", "alcf")},
            "transfer.json", {"perlmutter": ["/sandbox/nersc"], "aurora": ["/sandbox/alcf"]},
        )

    def test_setup_and_configuration_queries_do_not_authenticate_or_probe(self):
        self.assertEqual(set(state.getConfiguredImplementations()), {"nersc", "alcf"})
        self.assertIn("aurora", resources.getKnownMachines())
        self.assertIn("perlmutter", resources.getKnownMachines())
        self.assertTrue(resources.getMachineQueues("perlmutter"))
        self.assertEqual(manager_globals.remote_workdir["aurora"], ["/sandbox/alcf"])
        self.login.assert_not_called()
        self.transfer_login.assert_not_called()
        self.is_directory.assert_not_called()

    def test_first_machine_request_authenticates_and_validates_only_that_machine(self):
        def check_directory(machine, directory):
            # Model the REST calls used by pathType without restarting checks.
            rest.get(machine, "sandbox-probe")
            return True

        self.is_directory.side_effect = check_directory
        rest.get("perlmutter", "operation")
        rest.get("perlmutter", "operation")
        self.login.assert_called_once_with("nersc", "nersc.json", token_type="amsc_pat")
        self.is_directory.assert_called_once_with("perlmutter", "/sandbox/nersc")
        self.assertEqual([request.url.path for request in self.requests], [
            "/api/v1/sandbox-probe", "/api/v1/operation", "/api/v1/operation",
        ])
        self.assertNotIn("alcf", state.iri_compute_clients)
        self.assertIn("aurora", manager_globals._pending_sandbox_checks)

    def test_failed_sandbox_validation_blocks_request_and_remains_retryable(self):
        self.is_directory.return_value = False
        with patch("amscjobmanager.utils.queryYesNo", return_value=False):
            with self.assertRaisesRegex(Exception, "does not exist"):
                rest.post("perlmutter", "operation")
        self.assertEqual(self.requests, [])
        self.is_directory.return_value = True
        rest.get("perlmutter", "operation")
        self.login.assert_called_once()
        self.assertEqual(self.is_directory.call_count, 2)

    def test_client_access_validates_sandboxes_not_safe_path_checks(self):
        self.assertTrue(manager_globals.checkSafePath("perlmutter", "/sandbox/nersc/file.txt"))
        self.is_directory.assert_not_called()
        state.getImplementationClient("nersc")
        self.is_directory.assert_called_once_with("perlmutter", "/sandbox/nersc")
        self.assertFalse(manager_globals.checkSafePath("perlmutter", "/outside/file.txt"))
        self.is_directory.assert_called_once()

    def test_client_creation_checks_all_machines_for_its_implementation(self):
        manager_globals.addSandboxDirs({"perlmutter_login": ["/sandbox/login"]}, defer_checks=True)

        def check_directory(machine, directory):
            state.getImplementationClient("nersc")
            return True

        self.is_directory.side_effect = check_directory
        client = state.getImplementationClient("nersc")
        self.assertEqual(self.is_directory.call_args_list, [
            call("perlmutter", "/sandbox/nersc"),
            call("perlmutter_login", "/sandbox/login"),
        ])
        self.assertIs(state.getImplementationClient("nersc"), client)
        self.assertEqual(self.is_directory.call_count, 2)
        self.login.assert_called_once()

    def test_direct_client_creation_validates_registered_sandboxes(self):
        state.setupImplementationClient("nersc", "direct.json", "amsc_pat")
        self.is_directory.assert_called_once_with("perlmutter", "/sandbox/nersc")

    def test_local_sandboxes_do_not_require_an_iri_client(self):
        manager_globals.addSandboxDirs({"local": ["/sandbox/local"]}, defer_checks=True)
        self.is_directory.assert_called_once_with("local", "/sandbox/local")
        self.login.assert_not_called()

    def test_sandbox_registration_derives_available_machines_from_current_setup(self):
        state.setupWorkflowAgent({"alcf": {"token_path": "alcf.json"}}, None, {})
        with self.assertWarnsRegex(RuntimeWarning, "not configured"):
            manager_globals.addSandboxDirs({
                "perlmutter": ["/sandbox/nersc"], "aurora": ["/sandbox/alcf"],
            }, defer_checks=True, replace=True)
        self.assertEqual(manager_globals.remote_workdir, {"aurora": ["/sandbox/alcf"]})
        self.login.assert_not_called()
        self.is_directory.assert_not_called()

    def test_missing_sandbox_can_still_be_created(self):
        self.is_directory.return_value = False
        with patch("amscjobmanager.utils.queryYesNo", return_value=True), patch("amscjobmanager.api_general.remoteMkdir") as mkdir:
            rest.get("perlmutter", "operation")
            mkdir.assert_called_once_with("perlmutter", "/sandbox/nersc", allow_unsafe=True)
        self.assertNotIn("perlmutter", manager_globals._pending_sandbox_checks)

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
        self.assertEqual(manager_globals.remote_workdir, {"aurora": ["/new/sandbox"]})
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
