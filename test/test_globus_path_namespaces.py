"""Offline checks for explicit Globus namespaces, routing and sandbox safety."""

from contextlib import ExitStack
from pathlib import Path, PurePosixPath
import tempfile
import unittest
from unittest.mock import patch

from amscjobmanager import globals as manager_globals
from amscjobmanager.iri_api import globus, state


HOME_ENDPOINT = "9032dd3a-e841-4687-a163-2720da731b5b"
EAGLE_ENDPOINT = "05d2c76a-e867-4f67-aa57-76edeb0beda0"
FLARE_ENDPOINT = "f39a7a0f-5bfc-46ce-9615-ba9f8592814f"


class GlobusPathNamespaceTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.directory = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        self.staging = self.directory / "local-staging"
        self.stack.enter_context(patch.object(state, "globus_personal_endpoint", {
            "endpoint_uuid": "personal-uuid", "staging_directory_local": str(self.staging),
            "staging_directory_globus": "/windows/staging",
        }))
        self.stack.enter_context(patch.dict(state.special_globus_endpoints, {"personal": "personal-uuid"}))
        self.stack.enter_context(patch.object(manager_globals, "remote_workdir", {
            "polaris": ["/home/alice/safe", "/eagle/project/safe"],
            "aurora": ["/flare/project/safe"],
            "perlmutter": ["/global/safe"], "perlmutter_login": ["/global/login-safe"],
        }))
        self.stack.enter_context(patch.object(state, "setupIRIapiCompute", side_effect=AssertionError("Must not authenticate with IRI")))
        self.submit = self.stack.enter_context(patch.object(globus, "_transfer_post", return_value=({"transfer_uuid": "transfer-id"}, 200)))
        self.permission = self.stack.enter_context(patch.object(globus, "queryYesNo", return_value=False))

    def test_copy_checks_translated_sandbox_and_preserves_globus_namespace(self):
        self.assertEqual(globus.globusCopy(
            dest_endpoint=HOME_ENDPOINT, dest_globus_path="/alice/safe/nested",
            source_endpoint="external-uuid", source_globus_path="/input.txt",
        ), "transfer-id")
        request = self.submit.call_args.kwargs["data"]
        self.assertEqual(request["destination_uuid"], HOME_ENDPOINT)
        self.assertEqual(request["destination_path"], "/alice/safe/nested/")
        self.assertEqual(request["source_path"], "/input.txt")
        self.permission.assert_not_called()
        self.submit.reset_mock()
        # A filesystem path must not be compared against the filesystem sandbox
        # or translated silently when supplied to this Globus-path-only API.
        with self.assertRaisesRegex(Exception, "disallowed Globus"):
            globus.globusCopy(HOME_ENDPOINT, "/home/alice/safe/", "external-uuid", "/input.txt")
        self.submit.assert_not_called()

    def test_machine_tags_resolve_collection_without_aliasing_sandboxes(self):
        globus.globusCopy("perlmutter", "/global/safe/", "dtn", "/global/source.txt")
        request = self.submit.call_args.kwargs["data"]
        self.assertEqual(request["destination_uuid"], state.replaceSpecialGlobusEndpoint("dtn"))
        self.assertEqual(request["source_uuid"], state.replaceSpecialGlobusEndpoint("dtn"))
        with self.assertRaisesRegex(Exception, "disallowed Globus"):
            globus.globusCopy("perlmutter", "/global/login-safe/", "external-uuid", "/input.txt")
        with self.assertRaisesRegex(ValueError, "collection UUID"):
            globus.globusCopy("polaris", "/alice/safe/", "external-uuid", "/input.txt")

    def test_known_roots_reject_sibling_prefixes_and_parent_traversal(self):
        for destination in ("/project/safe-elsewhere/", "/project/safe/../outside/", "relative/"):
            with self.subTest(destination=destination), self.assertRaises(Exception):
                globus.globusCopy(EAGLE_ENDPOINT, destination, "external-uuid", "/input.txt")
        self.submit.assert_not_called()
        self.permission.assert_not_called()

    def test_personal_sandbox_uses_globus_staging_root_not_local_os_path(self):
        globus.globusCopy("personal", "/windows/staging/results/", "external-uuid", "/input.txt")
        self.assertEqual(self.submit.call_args.kwargs["data"]["destination_uuid"], "personal-uuid")
        with self.assertRaisesRegex(Exception, "disallowed Globus"):
            globus.globusCopy("personal", str(self.staging), "external-uuid", "/input.txt")
        self.permission.assert_not_called()

    def test_arbitrary_external_collections_keep_explicit_permission_prompt(self):
        self.permission.return_value = True
        globus.globusCopy("unknown-destination", "/external/output/", "unknown-source", "/external/input.txt")
        self.permission.assert_called_once()
        self.assertEqual(self.submit.call_args.kwargs["data"]["destination_path"], "/external/output/")

    def test_upload_supports_both_namespaces_and_renames_staged_source(self):
        local_source = self.directory / "input.bin"
        local_source.write_bytes(b"payload")

        def transfer(dest_endpoint, dest_globus_path, source_endpoint, source_globus_path, **kwargs):
            self.assertEqual(state.replaceSpecialGlobusEndpoint(dest_endpoint), HOME_ENDPOINT)
            self.assertEqual(dest_globus_path, "/alice/safe/")
            relative = PurePosixPath(source_globus_path).relative_to("/windows/staging")
            staged_file = self.staging / str(relative)
            self.assertEqual(staged_file.name, "result.bin")
            self.assertEqual(staged_file.read_bytes(), b"payload")
            return "transfer-id"

        with patch.object(globus, "globusCopy", side_effect=transfer):
            self.assertEqual(globus.uploadFileGlobus("polaris", "/home/alice/safe/result.bin", str(local_source)), "transfer-id")
            self.assertEqual(globus.uploadFileGlobus(
                "polaris", "/alice/safe/result.bin", str(local_source),
                remote_path_type="globus", remote_endpoint=HOME_ENDPOINT,
            ), "transfer-id")
        self.assertEqual(list(self.staging.iterdir()), [])
        with self.assertRaisesRegex(Exception, "disallowed"):
            globus.uploadFileGlobus("polaris", "/alice/outside/result.bin", str(local_source), remote_path_type="globus", remote_endpoint=HOME_ENDPOINT)
        with self.assertRaisesRegex(ValueError, "remote_endpoint"):
            globus.uploadFileGlobus("polaris", "/alice/safe/result.bin", str(local_source), remote_path_type="globus")

    def test_download_supports_both_namespaces_and_local_destination_rename(self):
        output = self.directory / "renamed.bin"

        def transfer(dest_endpoint, dest_globus_path, source_endpoint, source_globus_path, **kwargs):
            self.assertEqual(source_endpoint, FLARE_ENDPOINT)
            self.assertEqual(source_globus_path, "/project/safe/result.bin")
            relative = PurePosixPath(dest_globus_path).relative_to("/windows/staging")
            (self.staging / str(relative) / "result.bin").write_bytes(b"downloaded")
            return "transfer-id"

        with patch.object(globus, "globusCopy", side_effect=transfer):
            for remote_path, mode in (("/lus/flare/projects/project/safe/result.bin", "filesystem"), ("/project/safe/result.bin", "globus")):
                with self.subTest(mode=mode):
                    self.assertEqual(globus.downloadFileGlobus("aurora", remote_path, str(output), remote_path_type=mode), str(output))
                    self.assertEqual(output.read_bytes(), b"downloaded")
                    self.assertEqual(list(self.staging.iterdir()), [])

    def test_namespace_and_collection_selection_are_validated(self):
        for options in (
            {"remote_path_type": "invalid"},
            {"remote_path_type": "globus", "remote_endpoint": FLARE_ENDPOINT},
            {"remote_path_type": "filesystem", "remote_endpoint": HOME_ENDPOINT},
        ):
            with self.subTest(options=options), self.assertRaises(ValueError):
                globus.downloadFileGlobus("polaris", "/alice/safe/result.bin", str(self.directory / "output.bin"), **options)
        self.submit.assert_not_called()


if __name__ == "__main__":
    unittest.main()
