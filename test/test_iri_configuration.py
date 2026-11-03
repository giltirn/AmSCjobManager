"""Offline coverage of packaged defaults and user configuration precedence."""

import json
import tempfile
import unittest
from importlib.resources import files
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from amscjobmanager.iri_api import config, discovery, state
from amscjobmanager.iri_api.configuration_models import ImplementationCache


class IRIConfigurationTests(unittest.TestCase):
    def setUp(self):
        for registry in (config.IRI_IMPLEMENTATIONS, config.MACHINE_IMPLEMENTATIONS):
            registry_patch = patch.dict(registry)
            registry_patch.start()
            self.addCleanup(registry_patch.stop)
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        # A deliberately sparse v2 replacement must not inherit v1 routes/scopes.
        self.replacement = {
            "schema_version": 1, "implementation": "nersc",
            "api_base": "https://replacement.example/api/v2",
            "configuration": {
                "api_base": "https://replacement.example/api/v2", "api_version": 2,
                "status_resources_supports_filters": False,
                "machines": {"replacement": {"iriapi_resource_id": "new-id"}},
            },
        }
        self.path = self.directory / "nersc.json"
        self.path.write_text(json.dumps(self.replacement))

    def test_packaged_defaults_preserve_operational_settings(self):
        """Load packaged JSON with the shared schema and retain routing/auth fields."""
        for name in ("nersc", "alcf"):
            resource = files("amscjobmanager.iri_api").joinpath("implementations", f"{name}.json")
            definition = ImplementationCache.fromFile(resource).configuration.model_dump()
            self.assertEqual(config.IRI_IMPLEMENTATIONS[name], definition)
        nersc = config.IRI_IMPLEMENTATIONS["nersc"]
        alcf = config.IRI_IMPLEMENTATIONS["alcf"]
        self.assertEqual(nersc["machines"]["perlmutter"]["launcher"], "srun")
        self.assertIsInstance(nersc["machines"]["perlmutter"]["queues"][0], tuple)
        self.assertTrue(set(nersc["required_scopes"]))
        self.assertEqual(alcf["unsupported_filesystem_operations"]["downloadFile"], "globus")
        self.assertEqual(alcf["session_policy"], "a128e981-c9a5-417a-97ab-8571c9831bff")
        self.assertEqual(alcf["machines"]["aurora"]["custom_attributes"], {"filesystems": "flare"})

    def test_cache_replaces_whole_definition_and_machine_index_in_place(self):
        """User entries override URL/version and remove obsolete packaged mappings."""
        definitions = config.IRI_IMPLEMENTATIONS
        machines = config.MACHINE_IMPLEMENTATIONS
        with patch.object(state, "setupImplementationClient", side_effect=AssertionError("Authentication attempted")), patch("httpx.Client.request", side_effect=AssertionError("Network request attempted")):
            discovery.loadCachedImplementations(self.directory)
        self.assertIs(definitions, config.IRI_IMPLEMENTATIONS)
        self.assertIs(machines, config.MACHINE_IMPLEMENTATIONS)
        self.assertEqual(definitions["nersc"]["api_version"], 2)
        self.assertEqual(definitions["nersc"]["api_base"], self.replacement["api_base"])
        self.assertNotIn("required_scopes", definitions["nersc"])
        self.assertEqual(definitions["nersc"]["filesystems"], {})
        self.assertNotIn("perlmutter", machines)
        self.assertNotIn("perlmutter_login", machines)
        self.assertEqual(machines["replacement"], "nersc")
        self.assertEqual(machines["aurora"], "alcf")

    def test_single_provider_and_discovery_cache_hit_use_cached_url_and_version(self):
        """Direct cache/discovery calls honor overrides without defaults or prompts."""
        with patch("builtins.input", side_effect=AssertionError("Unexpected prompt")), patch.object(discovery, "_query_implementation_configuration", side_effect=AssertionError("Fresh discovery attempted")):
            result = discovery.discoverImplementation("nersc", cache_directory=self.directory)
            self.assertEqual(result["api_version"], 2)
            self.assertEqual(discovery.loadCachedImplementation("nersc", cache_directory=self.directory), result)

    def test_explicit_connection_constraints_reject_conflicting_cache(self):
        """Explicit URL/version conflicts fail without changing registered defaults."""
        original = config.IRI_IMPLEMENTATIONS["nersc"]
        with self.assertRaisesRegex(ValueError, "different implementation or API URL"):
            discovery.loadCachedImplementation("nersc", original["api_base"], self.directory)
        with self.assertRaisesRegex(ValueError, "API version"):
            discovery.discoverImplementation("nersc", cache_directory=self.directory, api_version=1)
        self.assertIs(config.IRI_IMPLEMENTATIONS["nersc"], original)
        discovery.loadCachedImplementations(self.directory)
        with self.assertRaisesRegex(ValueError, "API version"):
            discovery.configureImplementation("nersc", SimpleNamespace(api_url=None, api_version=1), self.directory)

    def test_configure_provider_applies_override_without_bulk_loading(self):
        """Single-provider setup selects its cache before checking packaged defaults."""
        with patch.object(state, "setupImplementationClient", side_effect=AssertionError("Authentication attempted")):
            discovery.configureImplementation("nersc", {"api_version": 2}, self.directory, cache_only=True)
        self.assertEqual(config.IRI_IMPLEMENTATIONS["nersc"]["api_version"], 2)
        self.assertEqual(config.MACHINE_IMPLEMENTATIONS["replacement"], "nersc")

    def test_new_provider_can_be_loaded_alongside_defaults(self):
        """Additional user providers register without removing unrelated defaults."""
        self.path.unlink()
        self.replacement["implementation"] = "new-provider"
        (self.directory / "new-provider.json").write_text(json.dumps(self.replacement))
        discovery.loadCachedImplementations(self.directory)
        self.assertIn("nersc", config.IRI_IMPLEMENTATIONS)
        self.assertEqual(config.MACHINE_IMPLEMENTATIONS["replacement"], "new-provider")

    def test_invalid_files_fail_without_falling_back(self):
        """Malformed JSON, invalid schema, identity mismatches and filenames fail."""
        original = config.IRI_IMPLEMENTATIONS["nersc"]
        cases = ["{", "{}", json.dumps({**self.replacement, "schema_version": 99}),
                 json.dumps({**self.replacement, "implementation": "another"}),
                 json.dumps({**self.replacement, "api_base": "https://wrong.example/api/v2"})]
        for contents in cases:
            with self.subTest(contents=contents):
                self.path.write_text(contents)
                with self.assertRaisesRegex(ValueError, "nersc.json"):
                    discovery.loadCachedImplementations(self.directory)
                self.assertIs(config.IRI_IMPLEMENTATIONS["nersc"], original)

    def test_machine_conflict_does_not_partially_replace_definition(self):
        """An override cannot take a machine owned by another implementation."""
        original = config.IRI_IMPLEMENTATIONS["nersc"]
        self.replacement["configuration"]["machines"]["aurora"] = {"iriapi_resource_id": "conflict"}
        self.path.write_text(json.dumps(self.replacement))
        with self.assertRaisesRegex(ValueError, "already belongs"):
            discovery.loadCachedImplementations(self.directory)
        self.assertIs(config.IRI_IMPLEMENTATIONS["nersc"], original)
        self.assertEqual(config.MACHINE_IMPLEMENTATIONS["perlmutter"], "nersc")

    def test_missing_cache_directory_keeps_packaged_defaults(self):
        """A missing user cache leaves the packaged implementations available."""
        discovery.loadCachedImplementations(self.directory / "missing")
        self.assertEqual(config.MACHINE_IMPLEMENTATIONS["perlmutter"], "nersc")
        self.assertEqual(config.MACHINE_IMPLEMENTATIONS["aurora"], "alcf")


if __name__ == "__main__":
    unittest.main()
