"""Unit tests for named facility filesystems and machine path routing."""

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from amscjobmanager import iri_api
from amscjobmanager.iri_api import resources


class IRIStoragePathTests(unittest.TestCase):
    def test_frontier_paths_map_unchanged_to_olcf_dtn(self):
        """The packaged Frontier route preserves absolute paths on the OLCF DTN."""
        definition = iri_api.IRI_IMPLEMENTATIONS["olcf"]
        self.assertEqual(definition["api_version"], 2)
        self.assertEqual(definition["machines"]["frontier"]["iriapi_resource_id"], "frontier")
        self.assertEqual(iri_api.MACHINE_IMPLEMENTATIONS["frontier"], "olcf")
        for path in ("/", "/some/project/data.bin", "/some/home/user/file.txt"):
            with self.subTest(path=path):
                self.assertEqual(
                    iri_api.getRemoteGlobusLocation("frontier", path),
                    ("36d521b3-c182-4071-b7d5-91db5d380d42", path),
                )

    def test_frontier_native_filesystem_paths_use_compute_resource(self):
        """Native filesystem routing selects Frontier even without storage resources."""
        self.assertEqual(iri_api.getStorageResourcePaths("frontier"), [
            {"filesystem_root": "/", "resource_name": "Frontier", "resource_id": "frontier"},
        ])
        client = SimpleNamespace(implementation="olcf", resource_map={})
        inventory = [{"id": "frontier", "name": "Frontier", "group": "olcf",
                      "resource_type": "urn:doe-iri:resource:compute"}]
        with patch.object(resources, "getImplementationClient", return_value=client), patch.object(resources, "getResources", return_value=inventory):
            for path in ("/", "/some/project/data.bin", "/some/home/user/file.txt"):
                with self.subTest(path=path):
                    self.assertEqual(resources.getResourceID("frontier", rtype="storage", path=path), "frontier")

    def test_alcf_machines_declare_their_mounted_filesystems(self):
        self.assertEqual(
            set(iri_api.getMountedFilesystems("polaris")),
            {"alcf_home", "alcf_eagle"},
        )
        self.assertEqual(
            set(iri_api.getMountedFilesystems("aurora")),
            {"alcf_flare"},
        )

    def test_polaris_home_uses_the_polaris_mapping(self):
        self.assertIn(
            {"filesystem_root": "/home", "resource_name": "Home"},
            iri_api.getStorageResourcePaths("polaris"),
        )
        endpoint, globus_path = iri_api.getRemoteGlobusLocation(
            "polaris", "/home/alice/example.txt"
        )
        self.assertEqual(endpoint, "9032dd3a-e841-4687-a163-2720da731b5b")
        self.assertEqual(globus_path, "/alice/example.txt")

    def test_aurora_home_is_explicitly_unsupported(self):
        with self.assertRaisesRegex(ValueError, "alcf_home"):
            iri_api.getRemoteGlobusLocation("aurora", "/home/alice/example.txt")

    def test_aurora_shared_flare_mapping_remains_available(self):
        endpoint, globus_path = iri_api.getRemoteGlobusLocation(
            "aurora", "/lus/flare/projects/example/data.bin"
        )
        self.assertEqual(endpoint, "f39a7a0f-5bfc-46ce-9615-ba9f8592814f")
        self.assertEqual(globus_path, "/example/data.bin")
        self.assertEqual(
            iri_api.getRemoteGlobusLocation("aurora", "/flare/example/data.bin"),
            ("f39a7a0f-5bfc-46ce-9615-ba9f8592814f", "/example/data.bin"),
        )

    def test_nersc_storage_and_globus_mapping_are_unchanged(self):
        self.assertEqual(
            iri_api.getStorageResourcePaths("perlmutter"),
            [{"filesystem_root": "/", "resource_name": "login"}],
        )
        self.assertEqual(
            iri_api.getRemoteGlobusLocation("perlmutter", "/global/test.txt"),
            ("dtn", "/global/test.txt"),
        )


if __name__ == "__main__":
    unittest.main()
