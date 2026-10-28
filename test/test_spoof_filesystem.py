"""Stateless filesystem examples exposed through the SPOOF API facade."""

import os

os.environ["AMSC_JOB_MANAGER_API_IMPL"] = "SPOOF"

from pathlib import Path, PurePosixPath
import tempfile
import unittest
from unittest.mock import patch

from amscjobmanager import api_general as api
from amscjobmanager import globals as manager_globals


class SpoofFilesystemTests(unittest.TestCase):
    def test_directory_operations_return_fake_names_without_creating_files(self):
        with tempfile.TemporaryDirectory() as directory:
            remote_directory = str(Path(directory) / "fake-remote-directory")
            api.remoteMkdir("perlmutter", remote_directory)
            api.remoteChmod("perlmutter", remote_directory, "700")
            files = api.remoteLs("perlmutter", remote_directory)
            self.assertIn(str(PurePosixPath(remote_directory) / "result.txt"), files)
            self.assertTrue(all(PurePosixPath(name).is_absolute() for name in files))
            self.assertFalse(Path(remote_directory).exists())

    def test_metadata_and_path_helpers_use_fake_remote_responses(self):
        self.assertIn("Size:", api.pathStat("perlmutter", "/fake/result.txt"))
        self.assertIn("ASCII text", api.pathType("perlmutter", "/fake/result.txt"))
        self.assertTrue(api.isDirectory("perlmutter", "/fake/output"))
        self.assertFalse(api.isDirectory("perlmutter", "/fake/result.txt"))
        self.assertTrue(api.pathExists("perlmutter", "/fake/result.txt"))
        with patch("amscjobmanager.local_api.localPathType", return_value="local result") as identify:
            self.assertEqual(api.pathType("local", "/local/file.txt"), "local result")
            identify.assert_called_once_with("/local/file.txt")

    def test_downloads_write_only_placeholder_contents_locally(self):
        with tempfile.TemporaryDirectory() as directory:
            expected = api.downloadFileContents("perlmutter", "/fake/result.txt")
            result = api.downloadFile("perlmutter", directory, "/fake/result.txt")
            self.assertEqual(result, str(Path(directory) / "result.txt"))
            self.assertEqual(Path(result).read_text(), expected)
            explicit = api.downloadFile("perlmutter", str(Path(directory) / "renamed.txt"), "/fake/result.txt")
            self.assertEqual(Path(explicit).read_text(), expected)

    def test_trailing_slash_directory_check_can_query_existence(self):
        # Preserve the default request-free shortcut, but let callers opt into
        # checking whether a purported remote directory actually exists.
        with patch.object(api, "pathType") as query:
            self.assertTrue(api.isDirectory("perlmutter", "/remote/output/"))
            query.assert_not_called()
            for response, expected in (
                ("cannot open /remote/output/: No such file or directory", False),
                ("/remote/output/: ASCII text", False),
                ("/remote/output/: directory", True),
            ):
                with self.subTest(response=response):
                    query.return_value = response
                    self.assertEqual(api.isDirectory(
                        "perlmutter", "/remote/output/", check_exists=True,
                    ), expected)
                    query.assert_called_with("perlmutter", "/remote/output/")

    def test_checked_local_directories_follow_actual_filesystem_state(self):
        # Local callers use the same interface and retain symlink-following
        # behavior when distinguishing a directory from a destination filename.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "file.txt").write_text("contents")
            (root / "directory-link").symlink_to(root, target_is_directory=True)
            (root / "broken-link").symlink_to(root / "missing")
            for path, expected in (
                (root, True), (root / "directory-link", True),
                (root / "missing", False), (root / "file.txt", False),
                (root / "broken-link", False),
            ):
                with self.subTest(path=path):
                    self.assertEqual(api.isDirectory(
                        "local", f"{path}/", check_exists=True,
                    ), expected)

    def test_real_local_download_checks_sandbox_and_accepts_explicit_override(self):
        # SPOOF simulates remote activity, but its actual local write must use
        # the same destination policy as the native download implementation.
        with patch.object(manager_globals, "remote_workdir", {"local": []}), \
             patch.object(Path, "write_text") as write:
            with self.assertRaisesRegex(ValueError, "sandbox"):
                api.downloadFile("perlmutter", "/unconfigured-local-output/result", "/fake/result")
            write.assert_not_called()
            api.downloadFile(
                "perlmutter", "/unconfigured-local-output/result", "/fake/result", allow_unsafe=True,
            )
            write.assert_called_once_with("FAKE CONTENTS\n", encoding="utf-8")

    def test_spoof_paths_reject_tilde_even_for_unsafe_operations(self):
        with tempfile.TemporaryDirectory() as directory:
            bad = str(Path(directory) / "output~")
            with self.assertRaisesRegex(ValueError, "~"):
                api.downloadFile("perlmutter", bad, "/source", allow_unsafe=True)
            with self.assertRaisesRegex(ValueError, "~"):
                api.remoteMkdir("perlmutter", bad, allow_unsafe=True)
            with self.assertRaisesRegex(ValueError, "~"):
                api.globusCopy("external", "/output", "external", "~/source", allow_unsafe=True)
            with self.assertRaisesRegex(ValueError, "~"):
                api.pathType("perlmutter", "~/source")
            self.assertFalse(Path(bad).exists())


if __name__ == "__main__":
    unittest.main()
