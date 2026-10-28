"""Reject ambiguous home syntax before local or remote filesystem activity."""

from copy import deepcopy
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from pydantic import ValidationError

from amscjobmanager import api_general, globals as manager_globals, local_api
from amscjobmanager.iri_api import auth, compute, discovery, filesystem, globus, resources, state
from amscjobmanager.job_data import JobData
from amscjobmanager.manager_config import readManagerConfigFile
from amscjobmanager.manager_config_models import ManagerConfig


class ExplicitPathTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        roots = patch.object(manager_globals, "remote_workdir", {
            "local": ["/"], "perlmutter": ["/"],
        })
        roots.start()
        self.addCleanup(roots.stop)

    def test_sandbox_and_directory_queries_reject_tilde_before_shortcuts(self):
        # Even a root authorizing every path and the trailing-slash shortcut
        # must not turn a tilde into the manager host's username.
        with patch.object(api_general, "pathType") as query:
            for path in ("~/output/", "~other/output", "/tmp/file~", "/home/~other/output/"):
                for machine in ("local", "perlmutter"):
                    with self.subTest(path=path, machine=machine):
                        with self.assertRaisesRegex(ValueError, "~"):
                            manager_globals.checkSafePath(machine, Path(path))
                        for check_exists in (False, True):
                            with self.assertRaisesRegex(ValueError, "~"):
                                api_general.isDirectory(machine, path, check_exists=check_exists)
                        with self.assertRaisesRegex(ValueError, "~"):
                            api_general.pathExists(machine, path)
            query.assert_not_called()

    def test_local_operations_reject_tilde_even_with_unsafe_override(self):
        # Rejection is path validation, independent of sandbox bypasses.
        bad = str(self.directory / "ambiguous~")
        operations = (
            lambda unsafe: local_api.localMkdir(bad, allow_unsafe=unsafe),
            lambda unsafe: local_api.writeBytes(bad, io.BytesIO(b"x"), allow_unsafe=unsafe),
            lambda unsafe: local_api.localCopyFile(bad, "/source", allow_unsafe=unsafe),
            lambda unsafe: local_api.localCopyFile("/output", bad, allow_unsafe=unsafe),
            lambda unsafe: local_api.executeJobScript("true", bad, allow_unsafe=unsafe),
            lambda unsafe: filesystem.downloadFile("local", bad, "/source", allow_unsafe=unsafe),
            lambda unsafe: filesystem.downloadFile("local", "/output", bad, allow_unsafe=unsafe),
        )
        with patch.object(local_api.subprocess, "run") as run, \
             patch.object(local_api.threading, "Thread") as worker:
            for operation in operations:
                for unsafe in (False, True):
                    with self.subTest(operation=operation, unsafe=unsafe):
                        with self.assertRaisesRegex(ValueError, "~"):
                            operation(unsafe)
            with self.assertRaisesRegex(ValueError, "~"):
                local_api.localPathType(bad)
            run.assert_not_called()
            worker.assert_not_called()
        self.assertFalse(Path(bad).exists())

    def test_remote_filesystem_paths_rejected_before_authentication(self):
        path = "/home/~remote-user/output"
        operations = (
            lambda: filesystem.remoteLs("perlmutter", path),
            lambda: filesystem.remoteMkdir("perlmutter", path, allow_unsafe=True),
            lambda: filesystem.remoteChmod("perlmutter", path, "700", allow_unsafe=True),
            lambda: filesystem.pathStat("perlmutter", path),
            lambda: filesystem.pathType("perlmutter", path),
            lambda: filesystem.uploadBytes("perlmutter", path, io.BytesIO(b"x"), allow_unsafe=True),
            lambda: filesystem.downloadFileContents("perlmutter", path),
        )
        with patch.object(resources, "getImplementationClient") as client:
            for operation in operations:
                with self.subTest(operation=operation), self.assertRaisesRegex(ValueError, "~"):
                    operation()
            client.assert_not_called()

    def test_remote_compute_and_upload_paths_reject_tilde_before_submission(self):
        options = dict(
            nodes=1, ranks_per_node=1, gpus_per_rank=0, time="60",
            queue="debug", account="account", job_run_dir="/home/~user/run", allow_unsafe=True,
        )
        with patch.object(compute, "post") as submit, \
             patch.object(api_general, "uploadBytes") as upload:
            with self.assertRaisesRegex(ValueError, "~"):
                compute.executeJob("perlmutter", "/bin/true", [], **options)
            with self.assertRaisesRegex(ValueError, "~"):
                compute.executeBatchJobCompat("perlmutter", "true", **options)
            with self.assertRaisesRegex(ValueError, "~"):
                api_general.uploadSmallFile("perlmutter", "/output", "~/input", allow_unsafe=True)
            submit.assert_not_called()
            upload.assert_not_called()

    def test_globus_paths_reject_tilde_before_staging_or_submission(self):
        staging = {
            "endpoint_uuid": "personal-uuid", "staging_directory_local": str(self.directory / "staging"),
            "staging_directory_globus": "/staging",
        }
        with patch.object(state, "globus_personal_endpoint", staging), \
             patch.object(globus, "_transfer_post") as submit:
            for bad in ("~/file", "/data/~user/file", "/data/file~"):
                with self.subTest(path=bad):
                    with self.assertRaisesRegex(ValueError, "~"):
                        globus.globusCopy("external", bad, "external", "/source", allow_unsafe=True)
                    with self.assertRaisesRegex(ValueError, "~"):
                        globus.globusCopy("external", "/output", "external", bad, allow_unsafe=True)
                    with self.assertRaisesRegex(ValueError, "~"):
                        globus.downloadFileGlobus("perlmutter", bad, "/source", allow_unsafe=True)
                    with self.assertRaisesRegex(ValueError, "~"):
                        globus.uploadFileGlobus("perlmutter", "/output", bad)
            submit.assert_not_called()
        self.assertFalse((self.directory / "staging").exists())

    def test_configuration_rejects_ambiguous_path_fields_and_roots(self):
        base = {
            "iri_implementations": {"nersc": {"token_path": "/tokens/nersc.json"}},
            "sandbox_directories": {"local": ["/safe"]},
            "globus_personal_endpoint": {
                "endpoint_uuid": "uuid", "staging_directory_local": "/staging",
                "staging_directory_globus": "/staging",
            },
        }
        for keys in (
            ("iri_implementations", "nersc", "token_path"),
            ("iri_config_cache_directory",), ("transferapi_key_path",),
            ("globus_personal_endpoint", "staging_directory_local"),
            ("globus_personal_endpoint", "staging_directory_globus"),
        ):
            config = deepcopy(base)
            target = config
            for key in keys[:-1]:
                target = target[key]
            target[keys[-1]] = "~/ambiguous"
            with self.subTest(field=keys), self.assertRaises(ValidationError):
                ManagerConfig.model_validate(config)
        base["sandbox_directories"]["local"] = ["/tmp/root~"]
        with self.assertRaises(ValidationError):
            ManagerConfig.model_validate(base)
        with self.assertRaisesRegex(ValueError, "~"):
            manager_globals.addSandboxDirs({"local": ["~/sandbox"]})

    def test_state_and_credential_paths_do_not_expand_home_directories(self):
        # Use a literal tilde inside a temporary filename so a regression cannot
        # accidentally create credentials or a database in the user's home.
        bad = self.directory / "state~"
        operations = (
            lambda: JobData(str(bad)),
            lambda: local_api.LocalJobStatus(str(bad)),
            lambda: ManagerConfig.write_template_json(bad),
            lambda: readManagerConfigFile(str(bad)),
            lambda: auth.load_globus_tokens(bad),
            lambda: auth.save_globus_tokens(bad, {}),
            lambda: auth._load_amsc_pat(str(bad)),
            lambda: discovery.loadCachedImplementation("nersc", cache_directory=bad),
            lambda: discovery.loadCachedImplementations(bad),
        )
        for operation in operations:
            with self.subTest(operation=operation), self.assertRaisesRegex(ValueError, "~"):
                operation()
        self.assertFalse(bad.exists())


if __name__ == "__main__":
    unittest.main()
