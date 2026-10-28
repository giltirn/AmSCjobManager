"""Offline checks for local data writes, resolved output paths, and overrides."""

import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import mock_open, patch

from amscjobmanager import globals as manager_globals, local_api
from amscjobmanager.iri_api import filesystem


class LocalWriteSandboxTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        config = patch.object(manager_globals, "remote_workdir", {
            "local": ["/configured-local-sandbox"],
        })
        config.start()
        self.addCleanup(config.stop)

    def test_tmp_writes_work_without_local_configuration(self):
        # /tmp is a built-in writable root, even without registered sandboxes.
        with patch.object(manager_globals, "remote_workdir", None):
            local_api.localMkdir(str(self.directory / "output"))
            output = self.directory / "output" / "data.bin"
            local_api.writeBytes(str(output), io.BytesIO(b"payload"))
        self.assertEqual(output.read_bytes(), b"payload")
        with patch.object(manager_globals, "remote_workdir", {"local": []}):
            self.assertFalse(manager_globals.checkSafePath("local", output, allow_tmp=False))

    def test_local_copy_checks_final_filename_and_preserves_contents(self):
        source = self.directory / "source.bin"
        source.write_bytes(b"payload")
        destination = self.directory / "output"
        destination.mkdir()
        local_api.localCopyFile(str(destination), str(source))
        self.assertEqual((destination / source.name).read_bytes(), b"payload")

        # An allowed directory does not authorize a child symlink outside it.
        (destination / source.name).unlink()
        (destination / source.name).symlink_to("/unconfigured-local-output/data.bin")
        with patch.object(local_api.subprocess, "run") as copy:
            with self.assertRaisesRegex(Exception, "sandbox"):
                local_api.localCopyFile(str(destination), str(source))
            copy.assert_not_called()

    def test_local_writes_reject_escaped_paths_and_allow_explicit_override(self):
        escaped = self.directory / "escape"
        escaped.symlink_to("/unconfigured-local-output", target_is_directory=True)
        with patch.object(local_api.subprocess, "run") as mutation, \
             patch("builtins.open", mock_open()) as output:
            for operation in (
                lambda **kwargs: local_api.localMkdir(str(escaped / "dir"), **kwargs),
                lambda **kwargs: local_api.writeBytes(str(escaped / "data"), io.BytesIO(b"x"), **kwargs),
                lambda **kwargs: local_api.localCopyFile(str(escaped / "copy"), str(self.directory / "source"), **kwargs),
            ):
                with self.subTest(operation=operation):
                    with self.assertRaisesRegex(Exception, "sandbox"):
                        operation()
                    mutation.assert_not_called()
                    output.assert_not_called()
                    mutation.return_value.returncode = 0
                    operation(allow_unsafe=True)
                    mutation.reset_mock()
                    output.reset_mock()

    def test_native_download_checks_output_before_fetching_contents(self):
        with patch.object(filesystem, "getFilesystemOperationHandling", return_value=None), \
             patch.object(filesystem, "downloadFileContents", return_value="hello world\n") as read, \
             patch("builtins.open", mock_open()) as output:
            with self.assertRaisesRegex(ValueError, "sandbox"):
                filesystem.downloadFile("perlmutter", "/unconfigured-local-output/result", "/source")
            read.assert_not_called()
            output.assert_not_called()

            # Configured roots and explicit overrides both permit the write.
            for destination, options in (
                ("/configured-local-sandbox/result", {}),
                ("/unconfigured-local-output/result", {"allow_unsafe": True}),
            ):
                with self.subTest(destination=destination):
                    self.assertEqual(filesystem.downloadFile(
                        "perlmutter", destination, "/source", **options,
                    ), destination)
                    output.assert_called_with(destination, "w")
                    output().write.assert_called_with("hello world\n")

    def test_native_download_rejects_directory_child_symlink_before_read(self):
        (self.directory / "result.txt").symlink_to("/unconfigured-local-output/result.txt")
        with patch.object(filesystem, "getFilesystemOperationHandling", return_value=None), \
             patch.object(filesystem, "downloadFileContents") as read:
            with self.assertRaisesRegex(ValueError, "sandbox"):
                filesystem.downloadFile("perlmutter", str(self.directory), "/remote/result.txt")
            read.assert_not_called()

    def test_local_download_copies_into_directory_and_forwards_override(self):
        source = self.directory / "source.bin"
        source.write_bytes(b"payload")
        destination = self.directory / "output"
        destination.mkdir()
        with patch.object(local_api, "localCopyFile", wraps=local_api.localCopyFile) as copy:
            for allow_unsafe in (False, True):
                result = filesystem.downloadFile(
                    "local", str(destination), str(source), allow_unsafe=allow_unsafe,
                )
                self.assertEqual(Path(result).read_bytes(), b"payload")
                copy.assert_called_with(
                    str(destination / source.name), str(source), allow_unsafe=allow_unsafe,
                )

    def test_native_download_forwards_override_to_globus_fallback(self):
        with patch.object(filesystem, "getFilesystemOperationHandling", return_value="globus"), \
             patch.object(filesystem, "requireGlobusFallback"), \
             patch.object(filesystem, "downloadFileGlobus", return_value="/output") as download:
            for allow_unsafe in (False, True):
                self.assertEqual(filesystem.downloadFile(
                    "aurora", "/output", "/source", allow_unsafe=allow_unsafe,
                ), "/output")
                download.assert_called_with("aurora", "/output", "/source", allow_unsafe=allow_unsafe)

    def test_local_job_rejects_unsafe_directory_and_wrapper_before_start(self):
        (self.directory / "exec_wrap.sh").symlink_to("/unconfigured-local-output/wrapper")
        with patch.object(local_api, "statusMan") as status, \
             patch.object(local_api.subprocess, "run") as run, \
             patch.object(local_api.threading, "Thread") as worker:
            for directory in ("/unconfigured-local-output", str(self.directory)):
                with self.subTest(directory=directory), self.assertRaisesRegex(ValueError, "sandbox"):
                    local_api.executeJobScript("true", directory)
            status.assert_not_called()
            run.assert_not_called()
            worker.assert_not_called()

            # A safe directory also cannot authorize an escaping log symlink.
            (self.directory / "exec_wrap.sh").unlink()
            run.return_value.stdout = "local-id"
            (self.directory / "run.local-id.log").symlink_to("/unconfigured-local-output/log")
            with self.assertRaisesRegex(ValueError, "sandbox"):
                local_api.executeJobScript("true", str(self.directory))
            status.assert_not_called()
            worker.assert_not_called()

            local_api.executeJobScript("true", str(self.directory), allow_unsafe=True)
            worker.return_value.start.assert_called_once()


if __name__ == "__main__":
    unittest.main()
