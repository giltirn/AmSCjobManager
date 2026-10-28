"""Exercise local filesystem API dispatch without remote credentials or services."""

import base64
from pathlib import Path
import stat
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from amscjobmanager import api_general as api, globals as manager_globals, local_api
from amscjobmanager.iri_api import filesystem


class LocalFilesystemTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        # Local dispatch must work without discovering an IRI resource/provider.
        remote = patch.object(filesystem, "getResourceID", side_effect=AssertionError("Remote lookup"))
        remote.start()
        self.addCleanup(remote.stop)
        provider = patch.object(filesystem, "getMachineImplementation", side_effect=AssertionError("Provider lookup"))
        provider.start()
        self.addCleanup(provider.stop)

    def test_listing_includes_hidden_entries_without_recursing(self):
        (self.directory / "subdirectory").mkdir()
        (self.directory / "subdirectory" / "nested").touch()
        (self.directory / "a file.txt").touch()
        (self.directory / ".hidden").touch()
        expected = sorted(str(self.directory / name) for name in ("subdirectory", "a file.txt", ".hidden"))
        self.assertEqual(api.remoteLs("LOCAL", str(self.directory) + "/"), expected)
        with self.assertRaises(FileNotFoundError):
            api.remoteLs("local", str(self.directory / "missing"))
        with self.assertRaises(NotADirectoryError):
            api.remoteLs("local", str(self.directory / "a file.txt"))

    def test_chmod_applies_numeric_and_symbolic_permissions(self):
        target = self.directory / "file"
        target.touch()
        self.assertIsNone(api.remoteChmod("LOCAL", str(target), "600"))
        self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o600)
        api.remoteChmod("local", str(target), "u+x")
        self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o700)
        with self.assertRaises(subprocess.CalledProcessError):
            api.remoteChmod("local", str(target), "invalid")
        self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o700)

    def test_chmod_checks_symlink_target_and_honors_override(self):
        sandbox = self.directory / "sandbox"
        sandbox.mkdir()
        target = self.directory / "outside"
        target.touch(mode=0o600)
        link = sandbox / "link"
        link.symlink_to(target)
        # Disable /tmp's built-in exemption to exercise a real sandbox escape.
        with patch.object(manager_globals, "remote_workdir", {"local": [str(sandbox)]}), \
             patch.object(local_api, "checkSafePath", side_effect=lambda machine, path:
                          manager_globals.checkSafePath(machine, path, allow_tmp=False)):
            with self.assertRaisesRegex(ValueError, "sandbox"):
                api.remoteChmod("local", str(link), "700")
            self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o600)
            api.remoteChmod("local", str(link), "700", allow_unsafe=True)
            self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o700)

    def test_stat_distinguishes_links_and_targets(self):
        target = self.directory / "target"
        target.write_text("payload")
        link = self.directory / "link"
        link.symlink_to(target)
        self.assertIn("regular file", api.pathStat("LOCAL", str(link)))
        self.assertIn("symbolic link", api.pathStat("local", str(link), dereference=False))
        target.unlink()
        self.assertIn("symbolic link", api.pathStat("local", str(link), dereference=False))
        with self.assertRaises(subprocess.CalledProcessError):
            api.pathStat("local", str(link))

    def test_download_contents_preserves_text_and_binary_contract(self):
        for name, contents, expected in (
            ("text", "Hello, café\n".encode(), "Hello, café\n"),
            ("empty", b"", ""),
            ("binary", b"\x00\xff\x80payload", base64.b64encode(b"\x00\xff\x80payload").decode("ascii")),
        ):
            with self.subTest(name=name):
                target = self.directory / name
                target.write_bytes(contents)
                self.assertEqual(local_api.readFileBinary(str(target)), contents)
                self.assertEqual(api.downloadFileContents("LOCAL", str(target)), expected)
                link = self.directory / (name + "-link")
                link.symlink_to(target)
                self.assertEqual(api.downloadFileContents("local", str(link)), expected)
        with self.assertRaises(FileNotFoundError):
            api.downloadFileContents("local", str(self.directory / "missing"))

    def test_globus_fallback_uses_local_read_and_cleans_staging(self):
        for contents, expected in ((b"plain text\n", "plain text\n"), (b"\x00\xff", "AP8=")):
            with self.subTest(contents=contents):
                staging = self.directory / "staging"
                staging.mkdir()
                with patch.object(filesystem, "getFilesystemOperationHandling", return_value="globus"), \
                     patch.object(filesystem, "requireGlobusFallback"), \
                     patch.object(filesystem, "createGlobusStagingDirectory", return_value=(staging, "/staging")), \
                     patch.object(filesystem, "downloadFileGlobus", side_effect=lambda machine, dest, source:
                                  Path(dest).write_bytes(contents)), \
                     patch.object(local_api, "readFileBinary", wraps=local_api.readFileBinary) as read:
                    self.assertEqual(filesystem.downloadFileContents("aurora", "/remote/file"), expected)
                    read.assert_called_once_with(str(staging / "file"))
                self.assertFalse(staging.exists())

    def test_new_operations_reject_tilde_paths(self):
        bad = str(self.directory / "ambiguous~")
        for operation in (
            lambda: api.remoteLs("local", bad),
            lambda: api.remoteChmod("local", bad, "700", allow_unsafe=True),
            lambda: api.pathStat("local", bad, dereference=False),
            lambda: api.downloadFileContents("local", bad),
            lambda: local_api.readFileBinary(bad),
        ):
            with self.subTest(operation=operation), self.assertRaisesRegex(ValueError, "~"):
                operation()


if __name__ == "__main__":
    unittest.main()
