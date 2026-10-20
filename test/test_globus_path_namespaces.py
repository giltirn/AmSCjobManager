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
            "local": [str(self.directory)],
            "polaris": ["/home/alice/safe", "/eagle/project/safe"],
            "aurora": ["/flare/project/safe"],
            "perlmutter": ["/global/safe"], "perlmutter_login": ["/global/login-safe"],
        }))
        self.stack.enter_context(patch.object(state, "setupIRIapiCompute", side_effect=AssertionError("Must not authenticate with IRI")))
        self.submit = self.stack.enter_context(patch.object(globus, "_transfer_post", return_value=({"transfer_uuid": "transfer-id"}, 200)))

    def test_copy_checks_translated_sandbox_and_preserves_globus_namespace(self):
        self.assertEqual(globus.globusCopy(
            dest_endpoint=HOME_ENDPOINT, dest_path="/alice/safe/nested",
            source_endpoint="external-uuid", source_path="/input.txt",
        ), "transfer-id")
        request = self.submit.call_args.kwargs["data"]
        self.assertEqual(request["destination_uuid"], HOME_ENDPOINT)
        self.assertEqual(request["destination_path"], "/alice/safe/nested/")
        self.assertEqual(request["source_path"], "/input.txt")
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

    def test_copy_translates_filesystem_paths_for_machine_and_collection(self):
        self.assertEqual(globus.globusCopy(
            "polaris", "/home/alice/safe/output", FLARE_ENDPOINT,
            "/lus/flare/projects/project/input.bin",
            dest_path_type="filesystem", source_path_type="filesystem",
        ), "transfer-id")
        request = self.submit.call_args.kwargs["data"]
        self.assertEqual(request["destination_uuid"], HOME_ENDPOINT)
        self.assertEqual(request["destination_path"], "/alice/safe/output/")
        self.assertEqual(request["source_uuid"], FLARE_ENDPOINT)
        self.assertEqual(request["source_path"], "/project/input.bin")

        globus.globusCopy(
            HOME_ENDPOINT, "/home/alice/safe/", "dtn", "/global/source.bin",
            dest_path_type="filesystem", source_path_type="filesystem",
        )
        request = self.submit.call_args.kwargs["data"]
        self.assertEqual(request["destination_path"], "/alice/safe/")
        self.assertEqual(request["source_uuid"], state.replaceSpecialGlobusEndpoint("dtn"))

    def test_filesystem_paths_require_a_known_unambiguous_route(self):
        for endpoint, path, options in (
            ("external-uuid", "/anywhere/file", {"source_path_type": "filesystem"}),
            (HOME_ENDPOINT, "/outside/file", {"source_path_type": "filesystem"}),
            ("aurora", "/home/alice/file", {"source_path_type": "filesystem"}),
            (HOME_ENDPOINT, "/home/alice/../file", {"source_path_type": "filesystem"}),
            (HOME_ENDPOINT, "/home/alice/file", {"source_path_type": "invalid"}),
        ):
            with self.subTest(endpoint=endpoint, path=path), self.assertRaises(ValueError):
                globus.globusCopy(HOME_ENDPOINT, "/alice/safe/", endpoint, path, **options)
        self.submit.assert_not_called()
        with self.assertRaisesRegex(ValueError, "No configured filesystem-to-Globus route"):
            globus.globusCopy(
                "external-uuid", "/home/alice/safe/", "dtn", "/global/input.bin",
                dest_path_type="filesystem",
            )
        self.submit.assert_not_called()

        ambiguous_routes = {"one": {"globus_filesystems": [
            {"endpoint": HOME_ENDPOINT, "filesystem_roots": ["/home"], "globus_root": "/first"},
            {"endpoint": HOME_ENDPOINT, "filesystem_roots": ["/home"], "globus_root": "/second"},
        ]}}
        with patch.object(globus, "getMountedFilesystems", return_value=ambiguous_routes):
            with self.assertRaisesRegex(ValueError, "Ambiguous filesystem-to-Globus routes"):
                globus.globusCopy(
                    HOME_ENDPOINT, "/alice/safe/", HOME_ENDPOINT, "/home/alice/input.bin",
                    source_path_type="filesystem",
                )
        self.submit.assert_not_called()

    def test_known_roots_reject_sibling_prefixes_and_parent_traversal(self):
        for destination in ("/project/safe-elsewhere/", "/project/safe/../outside/", "relative/"):
            with self.subTest(destination=destination), self.assertRaises(Exception):
                globus.globusCopy(EAGLE_ENDPOINT, destination, "external-uuid", "/input.txt")
        self.submit.assert_not_called()

    def test_personal_paths_use_only_the_configured_staging_roots(self):
        with patch.object(globus, "createGlobusStagingDirectory", side_effect=AssertionError("No staging creation expected")), \
             patch.object(globus, "globusTransferStatus", side_effect=AssertionError("Nonblocking transfer must not poll")):
            globus.globusCopy("personal", "/windows/staging/results/", "external-uuid", "/input.txt")
            self.assertEqual(self.submit.call_args.kwargs["data"]["destination_uuid"], "personal-uuid")
            globus.globusCopy(
                "personal-uuid", str(self.staging / "results"), "external-uuid", "/input.txt",
                dest_path_type="filesystem",
            )
            self.assertEqual(self.submit.call_args.kwargs["data"]["destination_path"], "/windows/staging/results/")
            globus.globusCopy(
                "external-uuid", "/output/", "personal", str(self.staging / "input.txt"),
                source_path_type="filesystem",
            )
            self.assertEqual(self.submit.call_args.kwargs["data"]["source_path"], "/windows/staging/input.txt")
        self.assertFalse(self.staging.exists())

    def test_personal_paths_outside_staging_are_rejected_on_both_sides(self):
        for endpoint, path, options in (
            ("personal", "/other/visible/results/", {}),
            ("personal-uuid", str(self.directory / "output"), {"dest_path_type": "filesystem"}),
            ("personal", "/outside/output", {"allow_unsafe": True}),
        ):
            with self.subTest(endpoint=endpoint, path=path), self.assertRaisesRegex(ValueError, "outside the staging directory"):
                globus.globusCopy(endpoint, path, "external-uuid", "/input.txt", **options)
        for path, options in (
            ("/outside/input.txt", {}),
            (str(self.directory / "input.txt"), {"source_path_type": "filesystem"}),
        ):
            with self.subTest(path=path), self.assertRaisesRegex(ValueError, "outside the staging directory"):
                globus.globusCopy("external-uuid", "/output/", "personal", path, **options)
        self.submit.assert_not_called()

    def test_personal_filesystem_path_cannot_escape_staging_through_symlink(self):
        self.staging.mkdir()
        (self.staging / "escape").symlink_to(self.directory, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "outside the staging directory"):
            globus.globusCopy(
                "personal", str(self.staging / "escape" / "output"),
                "external-uuid", "/input.txt", dest_path_type="filesystem",
            )
        self.submit.assert_not_called()

    def test_arbitrary_external_collections_transfer_without_permission_prompt(self):
        globus.globusCopy("unknown-destination", "/external/output/", "unknown-source", "/external/input.txt")
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
                    self.assertEqual(globus.downloadFileGlobus("aurora", str(output), remote_path, remote_path_type=mode), str(output))
                    self.assertEqual(output.read_bytes(), b"downloaded")
                    self.assertEqual(list(self.staging.iterdir()), [])

    def test_download_rejects_destinations_outside_configured_local_sandbox(self):
        outside = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        escaped = self.directory / "escape"
        escaped.symlink_to(outside, target_is_directory=True)
        for destination in (
            outside / "output.bin",
            outside,
            escaped / "output.bin",
        ):
            with self.subTest(destination=destination):
                with self.assertRaisesRegex(ValueError, "outside the configured local sandbox"):
                    globus.downloadFileGlobus(
                        "aurora", str(destination), "/project/safe/result.bin",
                        remote_path_type="globus",
                    )
        self.submit.assert_not_called()
        self.assertFalse((outside / "output.bin").exists())

    def test_download_accepts_directory_inside_configured_local_sandbox(self):
        output_directory = self.directory / "downloads"
        output_directory.mkdir()

        def transfer(dest_endpoint, dest_path, source_endpoint, source_path, **kwargs):
            relative = PurePosixPath(dest_path).relative_to("/windows/staging")
            (self.staging / str(relative) / "result.bin").write_bytes(b"downloaded")

        with patch.object(globus, "globusCopy", side_effect=transfer):
            result = globus.downloadFileGlobus(
                "aurora", str(output_directory), "/project/safe/result.bin",
                remote_path_type="globus",
            )
        self.assertEqual(result, str(output_directory / "result.bin"))
        self.assertEqual((output_directory / "result.bin").read_bytes(), b"downloaded")

    def test_download_allows_configured_staging_outside_local_sandbox(self):
        outside = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        with patch.object(state, "globus_personal_endpoint", {
            "endpoint_uuid": "personal-uuid",
            "staging_directory_local": str(outside / "staging"),
            "staging_directory_globus": "/windows/staging",
        }):
            def transfer(dest_endpoint, dest_path, source_endpoint, source_path, **kwargs):
                relative = PurePosixPath(dest_path).relative_to("/windows/staging")
                (outside / "staging" / str(relative) / "result.bin").write_bytes(b"downloaded")

            with patch.object(globus, "globusCopy", side_effect=transfer):
                result = globus.downloadFileGlobus(
                    "aurora", str(self.directory / "output.bin"), "/project/safe/result.bin",
                    remote_path_type="globus",
                )
                staged_result = globus.downloadFileGlobus(
                    "aurora", str(outside / "staging" / "result.bin"),
                    "/project/safe/result.bin", remote_path_type="globus",
                )
        self.assertEqual(result, str(self.directory / "output.bin"))
        self.assertEqual((self.directory / "output.bin").read_bytes(), b"downloaded")
        self.assertEqual(staged_result, str(outside / "staging" / "result.bin"))
        self.assertEqual((outside / "staging" / "result.bin").read_bytes(), b"downloaded")
        self.submit.assert_not_called()
        self.assertEqual(list((outside / "staging").iterdir()), [outside / "staging" / "result.bin"])

    def test_namespace_and_collection_selection_are_validated(self):
        for options in (
            {"remote_path_type": "invalid"},
            {"remote_path_type": "globus", "remote_endpoint": FLARE_ENDPOINT},
            {"remote_path_type": "filesystem", "remote_endpoint": HOME_ENDPOINT},
        ):
            with self.subTest(options=options), self.assertRaises(ValueError):
                globus.downloadFileGlobus("polaris", str(self.directory / "output.bin"), "/alice/safe/result.bin", **options)
        self.submit.assert_not_called()


if __name__ == "__main__":
    unittest.main()
