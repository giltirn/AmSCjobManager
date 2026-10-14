"""Unit tests for named facility filesystems and machine path routing."""

import unittest

from amscjobmanager import iri_api


class IRIStoragePathTests(unittest.TestCase):
    def test_alcf_machines_declare_their_mounted_filesystems(self):
        self.assertEqual(
            set(iri_api._mounted_filesystems("polaris")),
            {"alcf_home", "alcf_eagle"},
        )
        self.assertEqual(
            set(iri_api._mounted_filesystems("aurora")),
            {"alcf_flare"},
        )

    def test_polaris_home_uses_the_polaris_mapping(self):
        self.assertIn(
            {"filesystem_root": "/home", "resource_name": "Home"},
            iri_api._machine_storage_resource_paths("polaris"),
        )
        endpoint, globus_path = iri_api._remote_globus_location(
            "polaris", "/home/alice/example.txt"
        )
        self.assertEqual(endpoint, "9032dd3a-e841-4687-a163-2720da731b5b")
        self.assertEqual(globus_path, "/alice/example.txt")

    def test_aurora_home_is_explicitly_unsupported(self):
        with self.assertRaisesRegex(ValueError, "alcf_home"):
            iri_api._remote_globus_location("aurora", "/home/alice/example.txt")

    def test_aurora_shared_flare_mapping_remains_available(self):
        endpoint, globus_path = iri_api._remote_globus_location(
            "aurora", "/lus/flare/projects/example/data.bin"
        )
        self.assertEqual(endpoint, "f39a7a0f-5bfc-46ce-9615-ba9f8592814f")
        self.assertEqual(globus_path, "/example/data.bin")
        self.assertEqual(
            iri_api._remote_globus_location("aurora", "/flare/example/data.bin"),
            ("f39a7a0f-5bfc-46ce-9615-ba9f8592814f", "/example/data.bin"),
        )

    def test_nersc_storage_and_globus_mapping_are_unchanged(self):
        self.assertEqual(
            iri_api._machine_storage_resource_paths("perlmutter"),
            [{"filesystem_root": "/", "resource_name": "login"}],
        )
        self.assertEqual(
            iri_api._remote_globus_location("perlmutter", "/global/test.txt"),
            ("dtn", "/global/test.txt"),
        )


if __name__ == "__main__":
    unittest.main()
