"""Offline checks for unified IRI query targeting and version handling."""

import unittest
import json
import runpy
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import httpx
from pydantic import ValidationError

from amscjobmanager.iri_api import discovery, filesystem, resources, rest, state
from amscjobmanager.iri_api.config import IRI_IMPLEMENTATIONS, MACHINE_IMPLEMENTATIONS
from amscjobmanager.iri_api.discovery import DiscoveredImplementation


class IRIQueryTests(unittest.TestCase):
    def setUp(self):
        cache_directory = patch.object(state, "_configuration_cache_directory", None)
        cache_directory.start()
        self.addCleanup(cache_directory.stop)
        credentials = patch.dict(state._implementation_credentials, {}, clear=True)
        credentials.start()
        self.addCleanup(credentials.stop)
        definitions = patch.dict(IRI_IMPLEMENTATIONS, {"esnet": {
            "api_version": 2, "api_base": "https://iri.example/api/v2",
            "machines": {},
        }})
        definitions.start()
        self.addCleanup(definitions.stop)
        self.requests = []
        self.response = lambda request: httpx.Response(200, json=[])

        def handle(request):
            self.requests.append(request)
            return self.response(request)

        self.clients = {
            name: state.IRIComputeClient(
                name, "test-token",
                http_client=httpx.Client(transport=httpx.MockTransport(handle)),
            )
            for name in ("nersc", "alcf", "esnet")
        }
        self.patch = patch.dict(state.iri_compute_clients, self.clients, clear=True)
        self.patch.start()
        self.addCleanup(self.patch.stop)
        for client in self.clients.values():
            self.addCleanup(client.close)

    def test_rest_targets_and_existing_positional_calls(self):
        rest.get("PERLMUTTER", "status/resources")
        rest.get(suburl="status/resources", implementation="ESNET")
        rest.get(machine="polaris", implementation="alcf", suburl="status/resources")
        for method in (rest.post, rest.put, rest.delete):
            method(suburl="test", implementation="esnet")
        self.assertEqual(self.requests[0].url.path, "/api/v1/status/resources")
        self.assertEqual(self.requests[1].url.path, "/api/v2/status/resources")
        self.assertEqual([r.method for r in self.requests[-3:]], ["POST", "PUT", "DELETE"])
        self.assertTrue(all(r.headers["Authorization"] == "Bearer test-token" for r in self.requests))

    def test_invalid_or_unconfigured_target_fails_before_http(self):
        for target in ({}, {"machine": "perlmutter", "implementation": "alcf"}, {"implementation": "unknown"}, {"machine": "unknown"}):
            with self.subTest(target=target), self.assertRaises(ValueError):
                rest.get(suburl="status/resources", **target)
        with patch.dict(state.iri_compute_clients, {}, clear=True), self.assertRaises(RuntimeError):
            rest.get(suburl="status/resources", implementation="esnet")
        self.assertEqual(self.requests, [])

    def test_v1_rejects_explicit_v2_options_and_keeps_alcf_filtering(self):
        for option in ({"offset": 0}, {"limit": 10}, {"capability": []}, {"current_status": "up"}, {"modified_since": "2026-01-01T00:00:00Z"}):
            with self.subTest(option=option), self.assertRaises(NotImplementedError):
                resources.getResources(implementation="nersc", **option)
        self.assertEqual(self.requests, [])
        self.response = lambda request: httpx.Response(200, json=[
            {"id": "polaris", "group": "computes", "resource_type": "compute"},
            {"id": "home", "group": "storages", "resource_type": "storage"},
        ])
        result = resources.getResources(implementation="alcf", group="computes", resource_type="compute")
        self.assertEqual([r["id"] for r in result], ["polaris"])
        self.assertFalse(self.requests[-1].url.query)

    def test_v2_paginates_without_discovered_machines(self):
        self.response = lambda request: httpx.Response(200, json=[
            {"id": str(i)} for i in range(100 if request.url.params["offset"] == "0" else 1)
        ])
        self.assertEqual(len(resources.getResources(implementation="esnet")), 101)
        self.assertEqual([r.url.params["offset"] for r in self.requests], ["0", "100"])
        self.requests.clear()
        resources.getResources(implementation="esnet", offset=5, limit=3, current_status="up", capability=["cpu", "gpu"])
        self.assertEqual(len(self.requests), 1)
        self.assertEqual(self.requests[0].url.params.get_list("capability"), ["cpu", "gpu"])

    def test_project_queries_accept_implementation_and_preserve_account_suffix(self):
        self.response = lambda request: httpx.Response(200, json=(
            [{"name": "example", "id": "project-id"}]
            if request.url.path.endswith("/projects") else [{"entries": [{"allocation": 10}]}]
        ))
        self.assertEqual(resources.getUserAccountProjects(implementation="nersc"), ["example"])
        self.assertEqual(resources.getUserAccountProjects("perlmutter"), ["example_g"])
        self.assertEqual(resources.getProjectAllocation(project_name="example", implementation="nersc"), [{"allocation": 10}])
        self.assertEqual(resources.getProjectAllocation("perlmutter", "example_g"), [{"allocation": 10}])

    def test_username_uses_account_whoami_for_machine_and_implementation(self):
        self.response = lambda request: httpx.Response(200, json={"username": "remote-user"})
        self.assertEqual(resources.getUsername(implementation="ESNET"), "remote-user")
        with patch.dict(MACHINE_IMPLEMENTATIONS, {"esnet_compute": "esnet"}):
            self.assertEqual(resources.getUsername("esnet_compute"), "remote-user")
        self.assertEqual([request.url.path for request in self.requests], [
            "/api/v2/account/whoami", "/api/v2/account/whoami",
        ])

    def test_username_rejects_v1_before_authentication_or_http(self):
        with patch.object(state, "setupIRIapiCompute") as authenticate, patch.dict(state.iri_compute_clients, {}, clear=True):
            for target in ({"machine": "perlmutter"}, {"machine": "aurora"}, {"implementation": "nersc"}, {"implementation": "alcf"}):
                with self.subTest(target=target), self.assertRaisesRegex(NotImplementedError, "requires IRI API v2"):
                    resources.getUsername(**target)
            authenticate.assert_not_called()
        self.assertEqual(self.requests, [])

    def test_local_username_needs_no_client_or_remote_request(self):
        with patch.dict(state.iri_compute_clients, {}, clear=True), patch("amscjobmanager.local_api.localUsername", return_value="local-user") as whoami:
            self.assertEqual(resources.getUsername(machine="LOCAL"), "local-user")
            whoami.assert_called_once_with()
            with self.assertRaisesRegex(ValueError, "local"):
                resources.getUsername(machine="local", implementation="esnet")
        self.assertEqual(self.requests, [])

    def test_username_rejects_missing_or_incompatible_targets(self):
        for target in ({}, {"machine": "perlmutter", "implementation": "alcf"}, {"implementation": "unknown"}):
            with self.subTest(target=target), self.assertRaises(ValueError):
                resources.getUsername(**target)
        self.assertEqual(self.requests, [])

    def test_remote_ls_uses_v1_get_and_v2_post_then_polls_task(self):
        self.response = lambda request: httpx.Response(200, json=(
            {"task_id": "listing-task"} if "/filesystem/ls/" in request.url.path else {
                "status": "completed", "result": {"output": [{"name": "/data/a.txt"}, {"name": "/data/b.txt"}]},
            }
        ))
        with patch.dict(MACHINE_IMPLEMENTATIONS, {"esnet_compute": "esnet"}), patch.object(filesystem, "getResourceID", return_value="storage-id") as resource_id:
            for machine, version, method in (("perlmutter", 1, "GET"), ("esnet_compute", 2, "POST")):
                with self.subTest(machine=machine):
                    self.requests.clear()
                    self.assertEqual(filesystem.remoteLs(machine, "/data"), ["/data/a.txt", "/data/b.txt"])
                    resource_id.assert_called_with(machine, rtype="storage", path="/data")
                    self.assertEqual(len(self.requests), 2)
                    submission, poll = self.requests
                    self.assertEqual(submission.method, method)
                    self.assertEqual(submission.url.path, f"/api/v{version}/filesystem/ls/storage-id")
                    if version == 2:
                        self.assertEqual(json.loads(submission.content), {"path": "/data"})
                        self.assertFalse(submission.url.query)
                    else:
                        self.assertEqual(submission.url.params["path"], "/data")
                        self.assertFalse(submission.content)
                    self.assertEqual(poll.method, "GET")
                    self.assertEqual(poll.url.path, f"/api/v{version}/task/listing-task")

    def test_remote_ls_reports_v2_submission_errors_without_polling(self):
        self.response = lambda request: httpx.Response(403, json={"detail": "Directory access denied"})
        with patch.dict(MACHINE_IMPLEMENTATIONS, {"esnet_compute": "esnet"}), patch.object(filesystem, "getResourceID", return_value="storage-id"), patch.object(rest, "IRI_TASK_MAX_ATTEMPTS", 1):
            with self.assertRaisesRegex(RuntimeError, "HTTP 403.*Directory access denied"):
                filesystem.remoteLs("esnet_compute", "/data")
        self.assertEqual(len(self.requests), 1)
        self.assertEqual(self.requests[0].method, "POST")

    def test_v2_resource_selection_uses_ids_without_group_or_name(self):
        specification = {**IRI_IMPLEMENTATIONS["esnet"], "machines": {"test-machine": {"iriapi_resource_id": "system-id"}}}
        self.response = lambda request: httpx.Response(200, json=[
            {"id": "node-id", "resource_type": "urn:doe-iri:resource:compute:node"},
            {"id": "system-id", "resource_type": "urn:doe-iri:resource:compute:system"},
        ])
        with patch.dict(IRI_IMPLEMENTATIONS, {"esnet": specification}), patch.dict(MACHINE_IMPLEMENTATIONS, {"test-machine": "esnet"}):
            self.assertEqual(resources.getResourceID("test-machine"), "system-id")
        self.assertNotIn("group", self.requests[0].url.params)

    def test_resource_detail_and_v1_compute_storage_routing(self):
        self.response = lambda request: httpx.Response(200, json={"id": "resource-id"})
        self.assertEqual(resources.getResource("resource-id", implementation="esnet"), {"id": "resource-id"})
        self.assertEqual(self.requests[-1].url.path, "/api/v2/status/resources/resource-id")
        self.response = lambda request: httpx.Response(200, json=[
            {"id": "compute-id", "name": "compute"},
            {"id": "login-id", "name": "login"},
        ])
        self.assertEqual(resources.getResourceID("perlmutter"), "compute-id")
        self.assertEqual(resources.getResourceID("perlmutter", "storage", "/global/example"), "login-id")

    def test_version_validation_and_implementation_task_retries(self):
        for version in (True, "2", 3):
            with self.subTest(version=version), self.assertRaises(ValidationError):
                DiscoveredImplementation(api_base="https://example.invalid", api_version=version, machines={}, status_resources_supports_filters=True)
        attempts = []

        def action():
            attempts.append(1)
            return "task-id"

        self.response = lambda request: httpx.Response(200, json={"status": "failed" if len(attempts) == 1 else "completed"})
        with patch.object(rest.time, "sleep"):
            result = rest.submitTask(implementation="esnet", action=action)
        self.assertEqual(result["status"], "completed")
        self.assertEqual(len(attempts), 2)

    def test_compute_discovery_filters_inventory_and_reuses_cache(self):
        inventory = [
            {"id": "node-id", "name": "Node", "resource_type": "urn:doe-iri:resource:compute:node"},
            {"id": "gpu-id", "resource_type": "urn:doe-iri:resource:compute:gpu"},
            {"id": "storage-id", "resource_type": "urn:doe-iri:resource:storage:system"},
            {"id": "system-id", "name": "Test-Compute", "group": "test-group", "resource_type": "urn:doe-iri:resource:compute:system", "current_status": "up", "last_modified": "2026-10-15T00:00:00Z", "_links": {"iri:submit-job": {"href": "/jobs/system-id"}}},
            {"id": "unnamed-system", "name": None, "group": None, "resource_type": "urn:doe-iri:resource:compute"},
        ]
        self.response = lambda request: httpx.Response(200, json=inventory)
        with tempfile.TemporaryDirectory() as directory, patch.dict(IRI_IMPLEMENTATIONS), patch.dict(MACHINE_IMPLEMENTATIONS), patch("builtins.input", side_effect=["yes", "yes"]) as prompt:
            result = discovery.discoverImplementation("esnet", IRI_IMPLEMENTATIONS["esnet"]["api_base"], directory)
            self.assertEqual(set(result["machines"]), {"esnet_test-compute", "esnet_unnamed-system"})
            self.assertEqual(result["machines"]["esnet_test-compute"]["iriapi_resource_id"], "system-id")
            cache = json.loads((Path(directory) / "esnet.json").read_text())
            self.assertEqual(cache["configuration"], result)
            self.assertEqual(cache["metadata"]["resources"], inventory)
            self.assertEqual(cache["metadata"]["discovered_sections"], ["compute", "filesystems"])
            self.assertNotIn("discovered_sections", result)
            self.assertNotIn("metadata", result)
            for machine in result["machines"].values():
                self.assertEqual(set(machine), {
                    "iriapi_group", "iriapi_resource_name", "iriapi_resource_id",
                    "mounted_filesystems", "queues",
                })
            self.assertEqual(IRI_IMPLEMENTATIONS["esnet"], result)
            self.assertEqual(MACHINE_IMPLEMENTATIONS["esnet_test-compute"], "esnet")
            self.assertEqual(discovery.loadCachedImplementation("esnet", result["api_base"], directory), result)
            self.assertEqual(discovery.discoverImplementation("esnet", result["api_base"], directory), result)
            self.assertEqual(len(self.requests), 1)
            self.assertEqual(prompt.call_count, 2)
            self.assertNotIn("test-token", (Path(directory) / "esnet.json").read_text())

    def test_filesystem_discovery_routes_user_paths_to_backing_storage(self):
        api_base = IRI_IMPLEMENTATIONS["esnet"]["api_base"]

        def resource(resource_id, kind, **fields):
            return {
                "id": resource_id, "name": resource_id, "resource_type": f"urn:doe-iri:resource:{kind}",
                "self_uri": f"{api_base}/status/resources/{resource_id}", **fields,
            }

        def link(resource_id):
            return {"href": f"{api_base}/status/resources/{resource_id}"}

        inventory = [
            resource("test-compute", "compute:system"),
            resource("other-compute", "compute:system"),
            resource("storage", "storage:system", _links={"iri:provides-filesystem": [link("home"), link("project")]}),
            resource("home", "storage:filesystem", _links={"iri:has-mount": [link("home-mount"), link("other-home-mount")]}),
            resource("project", "storage:filesystem", _links={"iri:has-mount": [link("project-mount")]}),
            resource("home-mount", "storage:mount", attributes={"mount_path": "/data/home/{user_id}"}, _links={"iri:mounted-on": link("test-compute")}),
            resource("project-mount", "storage:mount", attributes={"mount_path": "/data/projects/{project_id}"}, _links={"iri:mounted-on": link("test-compute")}),
            resource("other-home-mount", "storage:mount", attributes={"mount_path": "/home/{user_id}"}, _links={"iri:mounted-on": link("other-compute")}),
        ]
        self.response = lambda request: httpx.Response(200, json=inventory)
        with tempfile.TemporaryDirectory() as directory, patch.dict(IRI_IMPLEMENTATIONS), patch.dict(MACHINE_IMPLEMENTATIONS), patch("builtins.input", side_effect=["yes"] * 5) as prompt:
            result = discovery.discoverImplementation("esnet", api_base, directory)
            self.assertEqual(prompt.call_count, 5)
            self.assertEqual(result["machines"]["esnet_test-compute"]["mounted_filesystems"], ["home-mount", "project-mount"])
            self.assertEqual(result["machines"]["esnet_other-compute"]["mounted_filesystems"], ["other-home-mount"])
            for mount, root in (("home-mount", "/data/home"), ("project-mount", "/data/projects")):
                self.assertEqual(result["filesystems"][mount]["storage_resource_paths"], [{
                    "filesystem_roots": [root], "resource_name": "storage", "resource_id": "storage",
                }])
                self.assertEqual(result["filesystems"][mount]["globus_filesystems"], [])
            for path in ("/data/home/alice/example.txt", "/data/projects/science/example.txt"):
                self.assertEqual(resources.getResourceID("esnet_test-compute", "storage", path), "storage")
            with self.assertRaisesRegex(ValueError, "No storage resource"):
                resources.getResourceID("esnet_test-compute", "storage", "/other/example.txt")
            self.assertEqual(resources.getResourceID("esnet_other-compute", "storage", "/home/bob/example.txt"), "storage")
            with self.assertRaisesRegex(ValueError, "not mounted"):
                resources.getResourceID("esnet_other-compute", "storage", "/data/home/bob/example.txt")
            cache = json.loads((Path(directory) / "esnet.json").read_text())
            self.assertEqual(cache["metadata"]["resources"], inventory)
            self.assertEqual(discovery.loadCachedImplementation("esnet", api_base, directory), result)
            self.assertEqual(discovery.discoverImplementation("esnet", api_base, directory), result)
            self.assertEqual(prompt.call_count, 5)

    def test_mount_path_roots_require_confirmation(self):
        for path, root in (
            ("/data/home/{user_id}", "/data/home"),
            ("/data/projects/{project_id}", "/data/projects"),
            ("/data/{project_id}/{user_id}/scratch", "/data"),
            ("/scratch", "/scratch"),
        ):
            with self.subTest(path=path), patch("builtins.input", return_value="yes") as prompt:
                self.assertEqual(discovery._mount_path_root(path), root)
                self.assertIn(repr(root), prompt.call_args.args[0])
        for path in ("relative/{user_id}", "/{user_id}", "/data/{username}", "/data/user-{user_id}", "/data/../{project_id}"):
            with self.subTest(path=path), patch("builtins.input", return_value="/chosen/root") as prompt:
                self.assertEqual(discovery._mount_path_root(path), "/chosen/root")
                prompt.assert_called_once()
                self.assertNotIn("[y/n]", prompt.call_args.args[0])

    def test_discovered_machine_names_are_prefixed_confirmed_and_overridable(self):
        self.response = lambda request: httpx.Response(200, json=[
            {"id": "low-id", "name": "Low Priority Compute", "resource_type": "urn:doe-iri:resource:compute:system"},
            {"id": "other-id", "name": "Other Compute", "resource_type": "urn:doe-iri:resource:compute:system"},
        ])
        with tempfile.TemporaryDirectory() as directory, patch.dict(IRI_IMPLEMENTATIONS), patch.dict(MACHINE_IMPLEMENTATIONS), patch("builtins.input", side_effect=["yes", "no", "Invalid Name", "custom_compute"]) as prompt:
            result = discovery.discoverImplementation("esnet", IRI_IMPLEMENTATIONS["esnet"]["api_base"], directory)
            self.assertEqual(set(result["machines"]), {"esnet_low_priority_compute", "custom_compute"})
            self.assertEqual(result["machines"]["esnet_low_priority_compute"]["iriapi_resource_name"], "Low Priority Compute")
            self.assertEqual(result["machines"]["custom_compute"]["iriapi_resource_id"], "other-id")
            self.assertEqual(MACHINE_IMPLEMENTATIONS["custom_compute"], "esnet")
            self.assertIn("esnet_low_priority_compute", prompt.call_args_list[0].args[0])
            self.assertEqual(discovery.discoverImplementation("esnet", result["api_base"], directory), result)
            self.assertEqual(prompt.call_count, 4)

    def test_machine_name_confirmation_reports_noninteractive_discovery(self):
        with patch("builtins.input", side_effect=EOFError), self.assertRaisesRegex(ValueError, "interactive machine-name confirmation"):
            discovery._machine_name("esnet", {"id": "resource-id", "name": "Low Priority Compute"})

    def test_mount_path_root_can_be_corrected_and_validates_user_input(self):
        with patch("builtins.input", side_effect=["maybe", "no", "relative", "/root/{user_id}", "/root/../data", "/chosen/root/"]) as prompt:
            self.assertEqual(discovery._mount_path_root("/home/{user_id}"), "/chosen/root")
            self.assertEqual(prompt.call_count, 6)
        with patch("builtins.input", side_effect=EOFError), self.assertRaisesRegex(ValueError, "run discovery interactively"):
            discovery._mount_path_root("/home/{user_id}")

    def test_compute_discovery_rejects_duplicate_names_and_v1(self):
        self.response = lambda request: httpx.Response(200, json=[
            {"id": identifier, "name": "Same-Name", "resource_type": "urn:doe-iri:resource:compute:system"}
            for identifier in ("first", "second")
        ])
        with tempfile.TemporaryDirectory() as directory, patch("builtins.input", return_value="yes"):
            original = IRI_IMPLEMENTATIONS["esnet"]
            with self.assertRaises(ValueError):
                discovery.discoverImplementation("esnet", original["api_base"], directory)
            self.assertEqual(IRI_IMPLEMENTATIONS["esnet"], original)
            self.assertFalse((Path(directory) / "esnet.json").exists())
            with self.assertRaises(NotImplementedError):
                discovery.discoverImplementation("nersc", IRI_IMPLEMENTATIONS["nersc"]["api_base"], directory)

    def test_manager_discovery_authenticates_once_before_querying(self):
        def authenticate(implementation, token_path, token_type):
            return state.IRIComputeClient(implementation, "fixture-pat", http_client=httpx.Client(transport=httpx.MockTransport(
                lambda request: httpx.Response(200, json=[{"id": "manager-system", "resource_type": "urn:doe-iri:resource:compute:system"}])
            )))

        with tempfile.TemporaryDirectory() as directory, patch.dict(IRI_IMPLEMENTATIONS), patch.dict(MACHINE_IMPLEMENTATIONS), patch.object(state, "setupIRIapiCompute", side_effect=authenticate) as login, patch.object(state, "addSandboxDirs"), patch("builtins.input", return_value="yes"):
            state.setupWorkflowAgent({"esnet": {
                "api_url": IRI_IMPLEMENTATIONS["esnet"]["api_base"],
                "token_path": "unused", "token_type": "amsc_pat",
            }}, None, {}, iri_config_cache_directory=directory)
            login.assert_called_once_with("esnet", "unused", token_type="amsc_pat")
            self.assertIn("esnet_manager-system", IRI_IMPLEMENTATIONS["esnet"]["machines"])
            state.getImplementationClient("esnet").close()

    def test_discovery_reuses_configured_cache_and_credentials(self):
        def authenticate(implementation, token_path, token_type):
            client = state.IRIComputeClient(implementation, "fixture-pat", http_client=httpx.Client(
                transport=httpx.MockTransport(lambda request: httpx.Response(200, json=[]))))
            self.addCleanup(client.close)
            return client

        with tempfile.TemporaryDirectory() as directory, patch.dict(IRI_IMPLEMENTATIONS), patch.dict(MACHINE_IMPLEMENTATIONS), patch.object(state, "setupIRIapiCompute", side_effect=authenticate) as login, patch.object(state, "addSandboxDirs"):
            state.setupWorkflowAgent({"esnet": {"token_path": "unused", "token_type": "amsc_pat"}}, None, {}, iri_config_cache_directory=directory)
            login.assert_not_called()
            result = discovery.discoverImplementation("esnet")
            login.assert_called_once_with("esnet", "unused", token_type="amsc_pat")
            self.assertEqual(discovery.loadCachedImplementation("esnet"), result)
            discovery.loadCachedImplementations()
            self.assertEqual(discovery.discoverImplementation("esnet"), result)
            login.assert_called_once()

    def test_unknown_provider_prompts_once_then_startup_loads_all_caches(self):
        settings = {"token_path": "unused", "token_type": "amsc_pat"}

        def authenticate(implementation, token_path, token_type):
            return state.IRIComputeClient(implementation, "fixture-pat", http_client=httpx.Client(
                transport=httpx.MockTransport(lambda request: httpx.Response(200, json=[]))
            ))

        with tempfile.TemporaryDirectory() as directory, patch.dict(IRI_IMPLEMENTATIONS), patch.dict(MACHINE_IMPLEMENTATIONS), patch.object(state, "setupIRIapiCompute", side_effect=authenticate) as login, patch.object(state, "addSandboxDirs"):
            with patch("builtins.input", side_effect=["https://new.example/api/v2#/", "2"]) as prompt:
                state.setupWorkflowAgent({"new-provider": settings}, None, {}, iri_config_cache_directory=directory)
                self.assertEqual(prompt.call_count, 2)
            self.assertEqual(IRI_IMPLEMENTATIONS["new-provider"]["api_base"], "https://new.example/api/v2")
            del IRI_IMPLEMENTATIONS["new-provider"]
            with patch("builtins.input", side_effect=AssertionError("Unexpected prompt")):
                # Load even providers not selected for authentication this run.
                state.setupWorkflowAgent({}, None, {}, iri_config_cache_directory=directory)
                self.assertIn("new-provider", IRI_IMPLEMENTATIONS)
                login.assert_called_once()
                state.setupWorkflowAgent({"new-provider": settings}, None, {}, iri_config_cache_directory=directory)
                self.assertEqual(login.call_count, 1)
                state.getImplementationClient("new-provider")
                self.assertEqual(login.call_count, 2)
            state.getImplementationClient("new-provider").close()

    def test_unknown_provider_accepts_explicit_url_and_version_without_prompting(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(IRI_IMPLEMENTATIONS), patch("builtins.input", side_effect=AssertionError("Unexpected prompt")), patch.object(discovery, "_query_implementation_configuration", return_value=({
            "api_base": "https://new.example/api/v2", "api_version": 2,
            "status_resources_supports_filters": True, "machines": {},
        }, {})):
            discovery.configureImplementation("new-provider", {
                "api_url": "https://new.example/api/v2", "api_version": 2,
                "token_path": None,
            }, directory)
            self.assertIn("new-provider", IRI_IMPLEMENTATIONS)
        with tempfile.TemporaryDirectory() as directory, patch("builtins.input", side_effect=["https://new.example/api/v1", "1"]):
            with self.assertRaisesRegex(NotImplementedError, "requires IRI API v2"):
                discovery.discoverImplementation("v1-provider", None, directory)
            self.assertNotIn("v1-provider", IRI_IMPLEMENTATIONS)

    def test_cache_loading_reports_invalid_file_and_tolerates_missing_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            discovery.loadCachedImplementations(Path(directory) / "missing")
            (Path(directory) / "broken.json").write_text("{}")
            with self.assertRaisesRegex(ValueError, "broken.json"):
                discovery.loadCachedImplementations(directory)

    def test_machine_tests_initialize_the_full_configuration_before_machine_lookup(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(IRI_IMPLEMENTATIONS), patch.dict(MACHINE_IMPLEMENTATIONS):
            cache = discovery.ImplementationCache(
                implementation="cached-provider", api_base="https://cached.example/api/v2",
                configuration=DiscoveredImplementation(
                    api_base="https://cached.example/api/v2", api_version=2,
                    status_resources_supports_filters=True,
                    machines={"cached-machine": {"iriapi_resource_id": "cached-id"}},
                ),
            )
            (Path(directory) / "cached-provider.json").write_text(cache.model_dump_json())
            settings = {"token_path": "unused", "token_type": "amsc_pat"}
            config = SimpleNamespace(
                iri_config_cache_directory=directory,
                iri_implementations={"cached-provider": settings, "nersc": settings},
                transferapi_key_path=None, sandbox_directories={}, globus_personal_endpoint=None,
            )
            for script, arguments in (
                ("test_iri_api_smoke.py", ["cached-machine"]),
                ("test_iri_compute_execution.py", ["perlmutter", "test-account"]),
            ):
                with self.subTest(script=script), patch("sys.argv", [script, "unused.json", *arguments]), patch("amscjobmanager.manager_config.readManagerConfigFile", return_value=config), patch("amscjobmanager.iri_api.setupWorkflowAgentFromConfig", side_effect=RuntimeError("stop before remote operations")) as setup:
                    with self.assertRaisesRegex(RuntimeError, "stop before remote operations"):
                        runpy.run_path(str(Path(__file__).with_name(script)))
                    setup.assert_called_once_with(config)


if __name__ == "__main__":
    unittest.main()
