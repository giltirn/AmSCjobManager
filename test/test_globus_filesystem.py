"""Offline tests for Globus metadata, directory operations and IRI fallbacks."""

import unittest
from unittest.mock import Mock, call, patch

import globus_sdk
import requests

from amscjobmanager import api_general, globals as manager_globals
from amscjobmanager.iri_api import filesystem, globus, state


class GlobusFilesystemTests(unittest.TestCase):
    def test_failed_transfer_logs_server_reason(self):
        """Failed or suspended transfers report the server's details without changing the status."""
        for status in ("FAILED", "INACTIVE"):
            with self.subTest(status=status):
                response = {"status": status, "details": "Endpoint credentials expired"}
                with patch.object(globus, "_transfer_get", return_value=response), \
                     patch.object(globus, "wfapiLog") as log:
                    self.assertEqual(globus.globusTransferStatus("transfer-1"), status)
                message = log.call_args.args[0]
                self.assertIn("transfer-1", message)
                self.assertIn(status, message)
                self.assertIn("Endpoint credentials expired", message)

    def setUp(self):
        self.client = Mock()
        client = patch.object(globus.globus_sdk, "TransferClient", return_value=self.client)
        client.start()
        self.addCleanup(client.stop)
        token = patch.object(globus, "getTransferToken", return_value="test-token")
        self.token = token.start()
        self.addCleanup(token.stop)
        self.metadata = {"type": "file", "size": 7, "permissions": "0640",
                         "user": "alice", "group": "project",
                         "last_modified": "2026-01-02 03:04:05+00:00", "link_target": None}
        self.client.operation_stat.return_value = self.metadata
        self.errors = {}
        for code, status in (("ClientError.NotFound", 404), ("NotFound", 404), ("EndpointPermissionDenied", 403),
                             ("ServiceUnavailable", 503), ("ExternalError.MkdirFailed.Exists", 502)):
            response = requests.Response()
            response.status_code = status
            response.request = requests.Request("GET", "https://transfer.example/operation").prepare()
            response._content = ('{"code": "' + code + '", "message": "test failure"}').encode()
            response.headers["Content-Type"] = "application/json"
            self.errors[code] = globus_sdk.TransferAPIError(response)

    def test_ls_uses_mapped_collection_and_preserves_hidden_names(self):
        """ALCF home paths translate to collection paths; reads need no staging."""
        self.client.operation_ls.return_value = {"DATA": [{"name": ".hidden"}, {"name": "file"}]}
        self.assertEqual(globus.remoteLsGlobus("polaris", "/home/alice"), [".hidden", "file"])
        self.client.operation_ls.assert_called_once_with(
            "9032dd3a-e841-4687-a163-2720da731b5b", path="/alice", show_hidden=True)
        self.token.assert_called_once_with("globus_transfer")

    def test_stat_shared_fields_and_link_semantics(self):
        """Stat returns only shared fields and rejects metadata for a link itself."""
        result = globus.pathStatGlobus("frontier", "/safe/file")
        self.assertEqual(result, {"type": "file", "size": 7, "permissions": "0640",
                                  "user": "alice", "group": "project",
                                  "last_modified": "2026-01-02T03:04:05+00:00"})
        self.metadata["link_target"] = "target"
        with self.assertRaises(NotImplementedError):
            globus.pathStatGlobus("frontier", "/safe/link", dereference=False)
        self.assertEqual(globus.pathStatGlobus("frontier", "/safe/link")["size"], 7)

    def test_globus_missing_paths_remain_compatible_with_general_checks(self):
        """Directory/existence helpers use basic Globus types and missing-path errors."""
        self.client.operation_stat.return_value = {**self.metadata, "type": "dir"}
        self.assertTrue(api_general.isDirectory("frontier", "/safe", check_exists=True))
        for code in ("ClientError.NotFound", "NotFound"):
            with self.subTest(code=code):
                self.client.operation_stat.side_effect = self.errors[code]
                self.assertFalse(api_general.pathExists("frontier", "/missing"))
                self.assertFalse(api_general.isDirectory("frontier", "/missing", check_exists=True))

    def test_pending_olcf_sandbox_validation_uses_globus_without_native_filesystem(self):
        """Client setup can validate OLCF sandboxes without any native filesystem call."""
        self.client.operation_stat.return_value = {**self.metadata, "type": "dir"}
        compute_client = object()
        with patch.dict(state.iri_compute_clients, {"olcf": compute_client}, clear=True), \
             patch.object(state, "_validating_clients", set()), \
             patch.dict(manager_globals._pending_sandbox_checks, {"frontier": ["/safe"]}, clear=True), \
             patch.object(manager_globals, "_validating_sandboxes", set()), \
             patch.object(filesystem, "getResourceID", side_effect=AssertionError("Native lookup attempted")):
            self.assertIs(state.getImplementationClient("olcf"), compute_client)
            self.assertNotIn("frontier", manager_globals._pending_sandbox_checks)
        self.token.assert_called_once_with("globus_transfer")

    def test_invalid_paths_are_rejected_before_globus_authentication(self):
        """Direct Globus filesystem functions retain explicit-path validation."""
        for function in (globus.remoteLsGlobus, globus.pathStatGlobus, globus.pathTypeGlobus, globus.remoteMkdirGlobus):
            for path in ("relative", "/safe/../outside", "/home/~user"):
                with self.subTest(function=function.__name__, path=path), self.assertRaises(ValueError):
                    function("frontier", path)
        self.token.assert_not_called()

    def test_missing_path_is_distinct_from_missing_collection(self):
        """A stat 404 means a missing path only after collection access succeeds."""
        for code in ("ClientError.NotFound", "NotFound"):
            with self.subTest(code=code):
                self.client.get_endpoint.reset_mock(side_effect=True)
                self.client.operation_stat.side_effect = self.errors[code]
                with self.assertRaises(FileNotFoundError):
                    globus.pathStatGlobus("frontier", "/missing")
                self.client.get_endpoint.assert_called_once_with("36d521b3-c182-4071-b7d5-91db5d380d42")
                self.assertIn("cannot open", globus.pathTypeGlobus("frontier", "/missing"))
                self.client.get_endpoint.side_effect = self.errors[code]
                with self.assertRaises(globus_sdk.TransferAPIError):
                    globus.pathTypeGlobus("frontier", "/missing")

    def test_type_does_not_hide_permission_or_outage_errors(self):
        """Unknown state must propagate rather than masquerade as a missing path."""
        for code in ("EndpointPermissionDenied", "ServiceUnavailable"):
            with self.subTest(code=code):
                self.client.operation_stat.side_effect = self.errors[code]
                with self.assertRaises(globus_sdk.TransferAPIError):
                    globus.pathTypeGlobus("frontier", "/safe/file")
        self.client.get_endpoint.assert_not_called()

    def test_mkdir_creates_missing_parents_in_order(self):
        """Only missing, sandboxed ancestors are created, starting at the top."""
        for code in ("ClientError.NotFound", "NotFound"):
            with self.subTest(code=code):
                # Reproduce the live test: neither the unique parent nor its
                # inputs child exists, while the configured sandbox does.
                self.client.operation_mkdir.reset_mock()
                self.client.operation_stat.side_effect = [self.errors[code], self.errors[code],
                                                         {**self.metadata, "type": "dir"}]
                with patch.object(globus, "checkSafePath", side_effect=lambda machine, path: path.startswith("/safe")):
                    globus.remoteMkdirGlobus("frontier", "/safe/parent/child")
                endpoint = "36d521b3-c182-4071-b7d5-91db5d380d42"
                self.assertEqual(self.client.operation_mkdir.call_args_list,
                                 [call(endpoint, path="/safe/parent"), call(endpoint, path="/safe/parent/child")])

    def test_mkdir_rejects_missing_ancestor_outside_sandbox_before_writing(self):
        """An allowed child cannot authorize creation of its unapproved parent."""
        self.client.operation_stat.side_effect = self.errors["ClientError.NotFound"]
        with patch.object(globus, "checkSafePath", side_effect=lambda machine, path: path.startswith("/safe/child")):
            with self.assertRaisesRegex(ValueError, "Parent"):
                globus.remoteMkdirGlobus("frontier", "/safe/child")
        self.client.operation_mkdir.assert_not_called()

    def test_mkdir_rejects_disallowed_destination_before_authentication(self):
        """Sandbox denial occurs before network I/O or credential acquisition."""
        with patch.object(globus, "checkSafePath", return_value=False):
            with self.assertRaises(ValueError):
                globus.remoteMkdirGlobus("frontier", "/outside")
        self.token.assert_not_called()

    def test_mkdir_without_parents_and_explicit_unsafe_override(self):
        """The nonrecursive variant submits only the requested mkdir operation."""
        with patch.object(globus, "checkSafePath", return_value=False):
            globus.remoteMkdirGlobus("frontier", "/outside/new", create_parents=False, allow_unsafe=True)
        self.client.operation_stat.assert_not_called()
        self.client.operation_mkdir.assert_called_once_with(
            "36d521b3-c182-4071-b7d5-91db5d380d42", path="/outside/new")

    def test_mkdir_existing_file_is_not_success(self):
        """An existing file cannot satisfy a request to create a directory."""
        with patch.object(globus, "checkSafePath", return_value=True):
            with self.assertRaises(NotADirectoryError):
                globus.remoteMkdirGlobus("frontier", "/safe/file")
        self.client.operation_mkdir.assert_not_called()

    def test_mkdir_handles_concurrent_directory_creation(self):
        """An Exists race succeeds only when a subsequent stat confirms a directory."""
        self.client.operation_stat.side_effect = [self.errors["ClientError.NotFound"],
                                                 {**self.metadata, "type": "dir"}, {**self.metadata, "type": "dir"}]
        self.client.operation_mkdir.side_effect = self.errors["ExternalError.MkdirFailed.Exists"]
        with patch.object(globus, "checkSafePath", return_value=True):
            globus.remoteMkdirGlobus("frontier", "/safe/new")

    def test_fallbacks_dispatch_before_native_storage_lookup(self):
        """OLCF metadata/directory calls and ALCF stat bypass native IRI endpoints."""
        self.client.operation_ls.return_value = {"DATA": [{"name": "file"}]}
        with patch.object(filesystem, "getResourceID", side_effect=AssertionError("Native lookup attempted")):
            self.assertEqual(filesystem.remoteLs("frontier", "/safe"), ["file"])
            self.assertEqual(filesystem.pathType("frontier", "/safe/file"), "regular file")
            self.assertEqual(filesystem.pathStat("polaris", "/home/alice/file")["size"], 7)
            with patch.object(globus, "checkSafePath", return_value=True):
                filesystem.remoteMkdir("frontier", "/safe/new", create_parents=False)
            with self.assertRaises(NotImplementedError):
                filesystem.pathStat("frontier", "/safe/file", raw=True)
            with self.assertRaises(NotImplementedError):
                filesystem.remoteChmod("frontier", "/safe/file", "700")

    def test_native_stat_normalizes_shared_metadata_and_can_return_raw(self):
        """Native text and Globus metadata produce the same shared dictionary."""
        output = """  File: /safe/file
  Size: 7 Blocks: 8 IO Block: 4096 regular file
Access: (0640/-rw-r-----) Uid: (1000/alice) Gid: (1001/project)
Modify: 2026-01-02 03:04:05.000000000 +0000"""
        task = {"status": "completed", "result": {"output": output}}
        with patch.object(filesystem, "getResourceID", return_value="login"), patch.object(filesystem, "submitTask", return_value=task):
            self.assertEqual(filesystem.pathStat("perlmutter", "/safe/file"),
                             globus.pathStatGlobus("frontier", "/safe/file"))
            self.assertEqual(filesystem.pathStat("perlmutter", "/safe/file", raw=True), output)
            task["result"]["output"] = "unknown stat format"
            with self.assertRaisesRegex(ValueError, "raw=True"):
                filesystem.pathStat("perlmutter", "/safe/file")


if __name__ == "__main__":
    unittest.main()
