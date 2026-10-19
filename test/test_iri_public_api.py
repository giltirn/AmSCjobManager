"""Checks for explicit public exports and configuration-free operation inputs."""

from pathlib import Path, PurePosixPath
import tempfile
import unittest
from unittest.mock import patch

from amscjobmanager import iri_api, spoof_api
from amscjobmanager.iri_api import state
from amscjobmanager.manager_config_models import ManagerConfig


class IRIPublicAPITests(unittest.TestCase):
    def test_config_setup_forwards_all_fields_and_returns_sandboxes(self):
        config = ManagerConfig(
            iri_implementations={"nersc": {"token_path": "nersc.json"}},
            transferapi_key_path="transfer.json",
            sandbox_directories={"perlmutter": ["/sandbox"]},
            globus_personal_endpoint={
                "endpoint_uuid": "fixture-endpoint", "staging_directory_local": "/local/staging",
                "staging_directory_globus": "/globus/staging",
            },
            iri_config_cache_directory="/config/cache",
        )
        for module in (state, spoof_api):
            with self.subTest(module=module.__name__), patch.object(module, "setupWorkflowAgent", return_value=config.sandbox_directories) as setup:
                self.assertIs(module.setupWorkflowAgentFromConfig(config), config.sandbox_directories)
                setup.assert_called_once_with(
                    config.iri_implementations, config.transferapi_key_path,
                    config.sandbox_directories, config.globus_personal_endpoint,
                    iri_config_cache_directory=config.iri_config_cache_directory,
                )
        self.assertIs(iri_api.setupWorkflowAgentFromConfig, state.setupWorkflowAgentFromConfig)

    def test_star_import_exposes_only_the_declared_public_api(self):
        namespace = {}
        exec("from amscjobmanager.iri_api import *", namespace)
        self.assertEqual(set(namespace) - {"__builtins__"}, set(iri_api.__all__))
        for name in ("json", "Path", "httpx", "tokens", "iri_compute_clients",
                     "setupImplementationClient", "setupIRIapiCompute",
                     "interactive_globus_login", "refresh_globus_tokens"):
            with self.subTest(name=name):
                self.assertFalse(hasattr(iri_api, name))
        self.assertIs(iri_api.executeJob, iri_api.compute.executeJob)

    def test_staging_directory_uses_configured_local_and_globus_paths(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(state, "globus_personal_endpoint", {
            "endpoint_uuid": "fixture-endpoint",
            "staging_directory_local": directory,
            "staging_directory_globus": "/windows/staging",
        }):
            local_path, globus_path = iri_api.createGlobusStagingDirectory()
            try:
                self.assertTrue(local_path.is_dir())
                self.assertEqual(local_path.parent, Path(directory))
                self.assertEqual(globus_path, str(PurePosixPath("/windows/staging") / local_path.name))
            finally:
                local_path.rmdir()

    def test_staging_directory_reports_missing_personal_endpoint(self):
        with patch.object(state, "globus_personal_endpoint", None):
            with self.assertRaisesRegex(RuntimeError, "globus_personal_endpoint"):
                iri_api.createGlobusStagingDirectory()


if __name__ == "__main__":
    unittest.main()
