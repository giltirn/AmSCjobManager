"""Offline tests for configured accounts and optional IRI project discovery."""

from contextlib import ExitStack
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from pydantic import ValidationError

from amscjobmanager import globals as manager_globals
from amscjobmanager.iri_api import resources, state
from amscjobmanager.manager_config import readManagerConfigFile
from amscjobmanager.manager_config_models import ManagerConfig, IRIImplementationConfig


class IRIAccountTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        for registry in (state._implementation_accounts, state._implementation_credentials,
                         state.iri_compute_clients,
                         state.special_globus_endpoints):
            self.stack.enter_context(patch.dict(registry, {}, clear=True))
        for name in ("_configuration_cache_directory", "_transfer_token_path", "globus_personal_endpoint"):
            self.stack.enter_context(patch.object(state, name, None))
        self.stack.enter_context(patch.dict(state.tokens))
        self.stack.enter_context(patch.object(manager_globals, "remote_workdir", {}))
        self.client = SimpleNamespace(project_map={})
        self.client_lookup = self.stack.enter_context(patch.object(resources, "getImplementationClient", return_value=self.client))
        self.get = self.stack.enter_context(patch.object(resources, "get", return_value=[]))
        self.login = self.stack.enter_context(patch.object(state, "setupIRIapiCompute", side_effect=AssertionError("Unexpected login")))

    def test_olcf_configured_accounts_work_without_discovery_or_authentication(self):
        """Unsupported discovery uses configured names, with no account API request."""
        config = ManagerConfig(iri_implementations={"OLCF": {
            "token_path": "/fake/pat.json", "token_type": "amsc_pat", "accounts": ["phy157", "phy157", " other "],
        }}, sandbox_directories={})
        state.setupWorkflowAgentFromConfig(config)
        self.assertEqual(resources.getUserAccountProjects("FRONTIER"), ["phy157", "other"])
        self.assertEqual(resources.getUserAccountProjects(implementation="OLCF"), ["phy157", "other"])
        self.get.assert_not_called()
        self.client_lookup.assert_not_called()
        self.login.assert_not_called()

    def test_missing_olcf_accounts_explain_the_required_config_field(self):
        """A site without discovery must explain how to supply account names."""
        state.setupWorkflowAgent({"olcf": {"token_path": "/fake/pat.json"}}, None, {})
        with self.assertRaisesRegex(RuntimeError, r"iri_implementations\.olcf\.accounts") as error:
            resources.getUserAccountProjects("frontier")
        self.assertIn("discovery is unavailable", str(error.exception))
        self.get.assert_not_called()

    def test_discovery_appends_new_names_without_losing_configured_accounts(self):
        """Discovered IDs add unique names; cached discovery preserves merged order."""
        state._implementation_accounts["nersc"] = ["manual", "known"]
        self.get.return_value = [{"name": "known", "id": "id-known"}, {"name": "new", "id": "id-new"}]
        self.assertEqual(resources.getUserAccountProjects(implementation="nersc"), ["manual", "known", "new"])
        self.assertEqual(resources.getUserProjectIDmap(implementation="nersc"), {"known": "id-known", "new": "id-new"})
        self.assertEqual(state._implementation_accounts["nersc"], ["manual", "known", "new"])
        self.get.assert_called_once_with(suburl="account/projects", implementation="nersc")
        # Returning the list must not let callers alter the registry accidentally.
        result = resources.getUserAccountProjects(implementation="nersc")
        result.append("caller-only")
        self.assertNotIn("caller-only", state._implementation_accounts["nersc"])
        # The state accessor also returns an isolated copy; additions and
        # duplicate handling belong to state rather than the caller.
        stored = state.getImplementationAccounts("NERSC")
        stored.append("getter-only")
        state.addImplementationAccount("NERSC", "new")
        self.assertEqual(state.getImplementationAccounts("nersc"), ["manual", "known", "new"])

    def test_401_discovery_failure_uses_configured_accounts(self):
        """A restricted project endpoint cannot prevent using an explicit account."""
        state._implementation_accounts["nersc"] = ["manual"]
        self.get.side_effect = RuntimeError("HTTP 401 unauthorized")
        self.assertEqual(resources.getUserAccountProjects(implementation="nersc"), ["manual"])
        self.assertEqual(state._implementation_accounts["nersc"], ["manual"])

    def test_failed_discovery_without_accounts_preserves_context_and_guidance(self):
        """An account API failure with no fallback includes the original cause."""
        failure = RuntimeError("HTTP 401 unauthorized")
        self.get.side_effect = failure
        with self.assertRaisesRegex(RuntimeError, r"iri_implementations\.nersc\.accounts") as error:
            resources.getUserAccountProjects(implementation="nersc")
        self.assertIs(error.exception.__cause__, failure)
        self.assertIn("401", str(error.exception))

    def test_empty_discovery_uses_manual_names_or_explains_how_to_add_them(self):
        """A successful empty response is distinct from a failed account query."""
        with self.assertRaisesRegex(RuntimeError, "returned no accounts"):
            resources.getUserAccountProjects(implementation="nersc")
        state._implementation_accounts["nersc"] = ["manual"]
        self.assertEqual(resources.getUserAccountProjects(implementation="nersc"), ["manual"])

    def test_perlmutter_account_suffix_is_applied_once_and_deduplicated(self):
        """Manual GPU account names and discovered base names resolve consistently."""
        state._implementation_accounts["nersc"] = ["example_g", "other"]
        self.get.return_value = [{"name": "example", "id": "id-example"}]
        self.assertEqual(resources.getUserAccountProjects("perlmutter"), ["example_g", "other_g"])
        self.assertEqual(resources.getUserProjectIDmap("perlmutter"), {"example": "id-example"})

    def test_configured_names_do_not_fabricate_ids_for_allocations(self):
        """Submission account names cannot stand in for undiscovered project IDs."""
        state._implementation_accounts["olcf"] = ["phy157"]
        with self.assertRaisesRegex(NotImplementedError, "cannot supply project IDs"):
            resources.getProjectAllocation("frontier", "phy157")
        self.get.assert_not_called()
        state._implementation_accounts["nersc"] = ["manual"]
        with self.assertRaisesRegex(ValueError, "No discovered project ID"):
            resources.getProjectAllocation("perlmutter", "manual_g")

    def test_discovery_does_not_modify_config_file_and_setup_resets_account_state(self):
        """Discovery merges only runtime state; another setup discards previous names."""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            text = json.dumps({"iri_implementations": {"nersc": {"token_path": "/fake/token.json", "accounts": ["manual"]}},
                               "sandbox_directories": {}})
            path.write_text(text)
            config = readManagerConfigFile(path)
            state.setupWorkflowAgentFromConfig(config)
            self.get.return_value = [{"name": "discovered", "id": "id"}]
            self.assertEqual(resources.getUserAccountProjects(implementation="nersc"), ["manual", "discovered"])
            self.assertEqual(config.iri_implementations["nersc"].accounts, ["manual"])
            self.assertEqual(path.read_text(), text)
            state.setupWorkflowAgent({"olcf": {"token_path": "/fake/pat.json", "accounts": ["different"]}}, None, {})
            self.assertEqual(state._implementation_accounts, {"olcf": ["different"]})

    def test_account_configuration_rejects_blank_or_non_list_values(self):
        """Account names must be nonempty strings rather than a scalar or numbers."""
        for accounts in ([""], ["   "], [123], "project", None):
            with self.subTest(accounts=accounts), self.assertRaises(ValidationError):
                IRIImplementationConfig(token_path="/fake/token.json", accounts=accounts)


if __name__ == "__main__":
    unittest.main()
