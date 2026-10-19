"""Stateless filesystem examples exposed through the SPOOF API facade."""

import os

os.environ["AMSC_JOB_MANAGER_API_IMPL"] = "SPOOF"

from pathlib import Path, PurePosixPath
import tempfile
import unittest
from unittest.mock import patch

from amscjobmanager import api_general as api


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


if __name__ == "__main__":
    unittest.main()
