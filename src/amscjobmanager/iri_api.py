from authlib.integrations.requests_client import OAuth2Session
from authlib.oauth2.rfc7523 import PrivateKeyJWT
import httpx
from . import globals as manager_globals
from .globals import checkSafePath, addSandboxDirs
import json
from typing import Literal, Union, List, Optional, Tuple
import time
import pathlib
import io
import os
import shutil
import stat
import base64
import re
import uuid
from dataclasses import dataclass, field
from pathlib import Path
import globus_sdk
from globus_sdk.exc import GlobusAPIError
from globus_sdk.scopes import TransferScopes
from .utils import queryYesNo
from .logging import wfapiLog, wfapiUserQuery
from . import local_api

IRI_IMPLEMENTATIONS = {
    "nersc": {
        "resource_server": "ed3e577d-f7f3-4639-b96e-ff5a8445d699",
        "api_base": "https://api.iri.nersc.gov/api/v1",
        "status_resources_supports_filters": True,
        "storage_iriapi_group": "perlmutter",
        "storage_resource_type": "compute",
        "storage_resource_paths": {"/": "login"},
        "globus_filesystems": [
            {
                "filesystem_root": "/",
                "endpoint": "dtn",
                "globus_root": "/",
            },
        ],
        "required_scopes": {
            "https://auth.globus.org/scopes/ed3e577d-f7f3-4639-b96e-ff5a8445d699/iri_api",
        },
        "machines": {
            "perlmutter": {
                "iriapi_group": "perlmutter",
                "iriapi_resource_name": "compute",
                "queues": [
                    ("debug", "max time 0.5 hours, max nodes 8"),
                    ("regular", "use for standard, production jobs or those too large for debug"),
                ],
            },
            "perlmutter_login": {
                "iriapi_group": "perlmutter",
                "iriapi_resource_name": "login",
                "queues": [],
            },
        },
    },
    "alcf": {
        "resource_server": "6be511f6-a071-471f-9bc0-02a0d0836723",
        "api_base": "https://api.alcf.anl.gov/api/v1",
        "status_resources_supports_filters": False,
        "storage_iriapi_group": "storages",
        "storage_resource_type": "storage",
        "storage_resource_paths": {
            "/flare": "Flare",
            "/lus/flare": "Flare",
            "/home": "Home",
            "/eagle": "Eagle",
            "/lus/eagle": "Eagle",
        },
        "globus_filesystems": [
            {
                "filesystem_root": "/home",
                "endpoint": "9032dd3a-e841-4687-a163-2720da731b5b",
                "globus_root": "/",
                "requires_data_access": True,
            },
            {
                "filesystem_root": "/lus/flare/projects",
                "endpoint": "f39a7a0f-5bfc-46ce-9615-ba9f8592814f",
                "globus_root": "/",
                "requires_data_access": True,
            },
            {
                "filesystem_root": "/flare",
                "endpoint": "f39a7a0f-5bfc-46ce-9615-ba9f8592814f",
                "globus_root": "/",
                "requires_data_access": True,
            },
            {
                "filesystem_root": "/lus/eagle/projects",
                "endpoint": "05d2c76a-e867-4f67-aa57-76edeb0beda0",
                "globus_root": "/",
                "requires_data_access": True,
            },
            {
                "filesystem_root": "/eagle",
                "endpoint": "05d2c76a-e867-4f67-aa57-76edeb0beda0",
                "globus_root": "/",
                "requires_data_access": True,
            },
        ],
        "required_scopes": {
            "https://auth.globus.org/scopes/6be511f6-a071-471f-9bc0-02a0d0836723/filesystem",
        },
        # ALCF requires the session policy used by its IRI authorization flow.
        "session_policy": "a128e981-c9a5-417a-97ab-8571c9831bff",
        "machines": {
            "polaris": {
                "iriapi_group": "computes",
                "iriapi_resource_name": "Polaris",
                "queues": [
                    ("debug", "1-2 nodes, 5 minutes-1 hour; max 24 nodes in use"),
                    ("debug-scaling", "1-10 nodes, 5 minutes-1 hour; max one job per user"),
                    ("prod", "10-496 nodes, 5 minutes-24 hours; routing queue"),
                    ("preemptable", "1-10 nodes, 5 minutes-72 hours; may be killed for demand jobs; max 20 jobs per project"),
                    ("demand", "1-56 nodes, 5 minutes-1 hour; by request only; max 100 jobs per project"),
                    ("capacity", "1-4 nodes, 5 minutes-7 days; max 32 nodes and one running job per user"),
                ],
            },
            "aurora": {
                "iriapi_group": "computes",
                "iriapi_resource_name": "Aurora",
                "queues": [
                    ("debug", "1-2 nodes, 5 minutes-1 hour; max one job per user"),
                    ("debug-scaling", "2-256 nodes, 5 minutes-1 hour; max one job per user"),
                    ("prod", "256-10,624 nodes, 5 minutes-24 hours; routing queue"),
                    ("capacity", "1-16 nodes, 5 minutes-7 days; max 512 nodes and two running jobs per user"),
                ],
            },
        },
    },
}

MACHINE_IMPLEMENTATIONS = {
    machine: implementation
    for implementation, specification in IRI_IMPLEMENTATIONS.items()
    for machine in specification["machines"]
}

TRANSFER_API_BASE = "https://amsc-data-api.nersc.gov"

#These endpoints require special data access permissions
#We also use this to bake in nicknames
#TODO: figure out how to deal with those that also require a special domain
#TODO: user config should store endpoints for which scopes are required, and token should be regenerated if list is changed (right now requires deletion)
special_globus_endpoints = {
    "dtn": "9d6d994a-6d04-11e5-ba46-22000b92c6ec",
    "bnl": "12782fb1-a599-4f18-b0fb-2e849681e214",
}
globus_personal_endpoint: dict[str, str] | None = None

def replaceSpecialGlobusEndpoint(endpoint : str):
    """Replace a special globus endpoint tag with the actual UUID"""
    endpoint = endpoint.lower()
    if endpoint in special_globus_endpoints:
        return special_globus_endpoints[endpoint]
    else:
        return endpoint


def listSpecialGlobusEndpoints():
    return special_globus_endpoints.keys()


tokens = { "iriapi_transfer_base" : None, "globus_transfer" : None }


@dataclass
class IRIComputeClient:
    """Authenticated compute/filesystem connection for one IRI implementation."""

    implementation: str
    access_token: str
    http_client: httpx.Client = field(default_factory=httpx.Client)
    resource_map: dict[str, dict[str, Union[str, dict[str, str]]]] = field(
        default_factory=dict
    )
    project_map: dict[str, dict[str, str]] = field(default_factory=dict)

    def close(self):
        self.http_client.close()


iri_compute_clients: dict[str, IRIComputeClient] = {}
# The data-transfer API remains a separate, single service.
iri_transfer_client = httpx.Client()

IRI_GET_RETRYABLE_STATUS_CODES = {502, 503, 504}
IRI_GET_MAX_ATTEMPTS = 4
IRI_GET_INITIAL_RETRY_DELAY_SECONDS = 1
               
def parse_scope_string(scope_string: str) -> set[str]:
    return set(scope_string.split()) if scope_string else set()

def ensure_private_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)

def load_tokens(token_file: Path) -> dict | None:
    if not token_file.exists():
        return None
    with token_file.open("r", encoding="utf-8") as f:
        return json.load(f)

def save_tokens(token_file: Path, tokens: dict) -> None:
    ensure_private_parent_dir(token_file)
    tmp = token_file.with_suffix(".tmp")
    with os.fdopen(
        os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600),
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(tokens, f, indent=2)
    os.replace(tmp, token_file)
    os.chmod(token_file, stat.S_IRUSR | stat.S_IWUSR)

def refresh_tokens(client: globus_sdk.NativeAppAuthClient, refresh_token: str, server: str) -> dict | None:
    try:
        token_response = client.oauth2_refresh_token(refresh_token)
        return token_response.by_resource_server[server]
    except GlobusAPIError as exc:
        wfapiLog(
            f"IRI token refresh failed ({exc.http_status}); switching to interactive login."
        )
        return None
    except Exception as exc:
        wfapiLog(
            f"IRI token refresh failed other error (Exception: {exc}, Server response: {token_response}, Resource server: {server}); switching to interactive login."
        )
        return None

############################################################
####IRI API SETUP
#############################################################

GLOBUS_CLIENT_ID = "eb18f0bb-4c76-43b5-88f1-750782be30ad" #Femtomeas, ckelly@bnl.gov

RESOURCE_SERVER = "auth.globus.org"

def _implementation_scopes(implementation: str) -> set[str]:
    return set(IRI_IMPLEMENTATIONS[implementation]["required_scopes"])


def interactive_login(client: globus_sdk.NativeAppAuthClient, implementation: str) -> dict:
    resource_server = IRI_IMPLEMENTATIONS[implementation]["resource_server"]
    required_scopes = _implementation_scopes(implementation)
    client.oauth2_start_flow(
        requested_scopes=" ".join(sorted(required_scopes)),
        refresh_tokens=True,
    )
    query_parameters = {"prompt": "login"}
    session_policy = IRI_IMPLEMENTATIONS[implementation].get("session_policy")
    if session_policy:
        query_parameters["session_required_policies"] = session_policy
    query = f"""Open this URL, login, and consent:
    { client.oauth2_get_authorize_url(query_params=query_parameters) }

    Enter authorization code: """

    accept = False
    while(not accept):
        try:
            code = wfapiUserQuery("IRI API login", query)
            token_response = client.oauth2_exchange_code_for_tokens(code)
            accept = True
        except Exception as e:
            continue

    assert resource_server in token_response.by_resource_server.keys()
        
    return token_response.by_resource_server[resource_server]

def setupIRIapiCompute(implementation: str, key_path: str) -> IRIComputeClient:
    if implementation not in IRI_IMPLEMENTATIONS:
        raise ValueError(f"Unknown IRI implementation: {implementation!r}")

    auth_client = globus_sdk.NativeAppAuthClient(GLOBUS_CLIENT_ID)
    resource_server = IRI_IMPLEMENTATIONS[implementation]["resource_server"]
    required_scopes = _implementation_scopes(implementation)

    wfapiLog(f"setupIRIapi({implementation}) checking stored tokens at", key_path)
    stored = load_tokens(Path(key_path))
    auth_data = None
    if stored and stored.get("refresh_token"):
        auth_data = refresh_tokens(auth_client, stored["refresh_token"], resource_server)

    if auth_data == None:
        auth_data = interactive_login(auth_client, implementation)

    granted = parse_scope_string(auth_data.get("scope", ""))
    missing = required_scopes - granted
    if missing:
        raise RuntimeError(f"Missing required scopes: {sorted(missing)}")

    save_tokens(Path(key_path), auth_data)

    expires_at = auth_data.get("expires_at_seconds")
    if expires_at:
        ttl = int(expires_at - time.time())
        print(f"\nAccess token valid for ~{max(ttl, 0)} seconds.")

    wfapiLog(f"Saved {implementation} token data to {key_path}")
    wfapiLog(f"Granted scopes: {auth_data.get('scope', '')}")

    return IRIComputeClient(implementation, auth_data['access_token'])
    

#############################
#### IRI TRANSFER API
#############################

IRI_TRANSFER_RESOURCE_SERVER = "08da012c-6998-46f9-9375-a6985ebe3f2b"
IRI_TRANSFER_DEFAULT_SCOPE = f"https://auth.globus.org/scopes/{IRI_TRANSFER_RESOURCE_SERVER}/transfer"

def _globus_data_access_collections() -> set[str]:
    """Return every collection for which the transfer login needs access."""
    collections = set()
    for implementation in iri_compute_clients:
        specification = IRI_IMPLEMENTATIONS[implementation]
        for filesystem in specification.get("globus_filesystems", []):
            if not filesystem.get("requires_data_access", False):
                continue
            endpoint = filesystem["endpoint"]
            collections.add(special_globus_endpoints.get(endpoint, endpoint))
    return collections

def interactive_login_transfer(client: globus_sdk.NativeAppAuthClient) -> dict:
    scope = globus_sdk.Scope(IRI_TRANSFER_DEFAULT_SCOPE)
    mapped_collections = _globus_data_access_collections()
    
    data_access = [globus_sdk.scopes.GCSCollectionScopes(mc).data_access for mc in mapped_collections] #data access scope required for mapped collections
    transfer_scope = TransferScopes.all.with_dependencies(data_access)
    scope = scope.with_dependency(transfer_scope)
    print(f"Logging in with scope: {scope}")

    client.oauth2_start_flow(
        # The IRI service needs Transfer as a dependency, while this client
        # also needs a first-party Transfer API token for task-list lookup.
        requested_scopes=[scope, transfer_scope],
        refresh_tokens=True,
    )
    query = f"""Open this URL, login, and consent:
    { client.oauth2_get_authorize_url(query_params={"prompt": "login"}) }

    Enter authorization code: """

    accept = False
    while(not accept):
        try:
            code = wfapiUserQuery("IRI Transfer API login", query)
            token_response = client.oauth2_exchange_code_for_tokens(code)
            accept = True
        except EOFError as exc:
            raise RuntimeError(
                "Globus Transfer API authorization requires an interactive terminal"
            ) from exc
        except Exception as e:
            continue

    return _transfer_tokens_from_response(token_response)

def _transfer_tokens_from_response(token_response):
    """Extract the IRI transfer-service and Globus Transfer API tokens."""
    resource_tokens = token_response.by_resource_server
    if IRI_TRANSFER_RESOURCE_SERVER not in resource_tokens:
        raise RuntimeError(
            "IRI transfer-service token was not granted; received tokens for "
            f"{sorted(resource_tokens)}"
        )
    if "transfer.api.globus.org" not in resource_tokens:
        raise RuntimeError(
            "Globus Transfer API token was not granted; received tokens for "
            f"{sorted(resource_tokens)}"
        )
    return (
        resource_tokens[IRI_TRANSFER_RESOURCE_SERVER],
        resource_tokens["transfer.api.globus.org"],
    )

def refresh_transfer_tokens(client: globus_sdk.NativeAppAuthClient, stored_tokens: dict):
    """Refresh the separately persisted IRI and Globus Transfer API tokens."""
    globus_auth_data = stored_tokens.get("globus_transfer")
    if not globus_auth_data or not globus_auth_data.get("refresh_token"):
        return None

    iri_auth_data = refresh_tokens(
        client, stored_tokens["refresh_token"], IRI_TRANSFER_RESOURCE_SERVER
    )
    globus_auth_data = refresh_tokens(
        client, globus_auth_data["refresh_token"], "transfer.api.globus.org"
    )
    if iri_auth_data is None or globus_auth_data is None:
        return None
    return iri_auth_data, globus_auth_data

def setupIRIapiTransfer(key_path):
    client =  globus_sdk.NativeAppAuthClient(GLOBUS_CLIENT_ID)

    wfapiLog("setupIRIapiTransfer checking stored tokens at",key_path)
    stored = load_tokens(Path(key_path))
    auth_data = None
    globus_auth_data = None
    if stored and stored.get("refresh_token"):
        refreshed = refresh_transfer_tokens(client, stored)
        if refreshed is not None:
            auth_data, globus_auth_data = refreshed

    if auth_data == None:
        auth_data, globus_auth_data = interactive_login_transfer(client)

    stored_tokens = dict(auth_data)
    stored_tokens["globus_transfer"] = globus_auth_data
    save_tokens(Path(key_path), stored_tokens)

    expires_at = auth_data.get("expires_at_seconds")
    if expires_at:
        ttl = int(expires_at - time.time())
        print(f"\nAccess token valid for ~{max(ttl, 0)} seconds.")

    wfapiLog(f"Saved token data to {key_path}")
    wfapiLog(f"Granted scopes: {auth_data.get('scope', '')}")

    tokens['iriapi_transfer_base'] = auth_data['access_token']
    tokens['globus_transfer'] = globus_auth_data['access_token']

    
################################################
##### Public facing API
################################################
        
def _configured_implementation_name(machine: str) -> str:
    machine = machine.lower()
    try:
        return MACHINE_IMPLEMENTATIONS[machine]
    except KeyError as exc:
        raise ValueError(f"Unknown IRI compute/filesystem machine: {machine!r}") from exc


def _get_compute_client(machine: str) -> tuple[IRIComputeClient, dict]:
    machine = machine.lower()
    implementation = _configured_implementation_name(machine)
    try:
        client = iri_compute_clients[implementation]
    except KeyError as exc:
        raise RuntimeError(
            f"IRI implementation {implementation!r}, required for machine "
            f"{machine!r}, is not configured"
        ) from exc
    return client, IRI_IMPLEMENTATIONS[implementation]["machines"][machine]


def _close_compute_clients():
    for client in iri_compute_clients.values():
        client.close()
    iri_compute_clients.clear()


def _configure_personal_globus_endpoint(configuration) -> None:
    """Configure the optional personal Globus endpoint for this process."""
    global globus_personal_endpoint
    special_globus_endpoints.pop("personal", None)
    if configuration is None:
        globus_personal_endpoint = None
        return

    def value(name):
        if hasattr(configuration, name):
            return getattr(configuration, name)
        return configuration[name]

    globus_personal_endpoint = {
        "endpoint_uuid": value("endpoint_uuid"),
        "staging_directory_globus": value("staging_directory_globus"),
        "staging_directory_local": value("staging_directory_local"),
    }
    special_globus_endpoints["personal"] = globus_personal_endpoint["endpoint_uuid"]


def setupWorkflowAgent(iri_implementations: dict, iriapi_transfer_key_path: str,
                       work_dir: dict, globus_personal_endpoint_config=None):
    """
    Setup the workflow agent
    Args:
       iri_implementations: Mapping of implementation names to configuration
           objects containing a token_path. Each implementation stores its
           authentication token independently.
       iriapi_transfer_key_path: The full path to the IRI transfer API key file. This will be generated automatically if it doesn't currently exist.
       work_dir: The remote work directories, by machine as a dict, e.g. { "perlmutter" : "/path/to/dir" }. Use a list of directories if more than one. The API is only allowed to modify the contents of files within this directory or its children
       globus_personal_endpoint_config: Optional personal endpoint UUID and
           staging-directory paths used by blocking Globus file transfers.
    """    
    normalized_implementations = {
        name.lower(): settings for name, settings in iri_implementations.items()
    }
    unknown_implementations = set(normalized_implementations) - set(IRI_IMPLEMENTATIONS)
    if unknown_implementations:
        raise ValueError(f"Unknown IRI implementations: {sorted(unknown_implementations)}")

    _configure_personal_globus_endpoint(globus_personal_endpoint_config)
    _close_compute_clients()
    for implementation, settings in normalized_implementations.items():
        token_path = (
            settings.token_path
            if hasattr(settings, "token_path")
            else settings["token_path"]
        )
        iri_compute_clients[implementation] = setupIRIapiCompute(
            implementation, token_path
        )

    normalized_work_dir = {
        machine.lower(): directory for machine, directory in work_dir.items()
    }
    setupIRIapiTransfer(iriapi_transfer_key_path)
    configured_machines = {"local"}
    for implementation in iri_compute_clients:
        configured_machines.update(
            IRI_IMPLEMENTATIONS[implementation]["machines"].keys()
        )
    manager_globals.configured_machines = configured_machines
    addSandboxDirs(normalized_work_dir, warn_unavailable=False)
    return normalized_work_dir
        

def get(machine, suburl, params = None):
    machine = machine.lower()
    client, _ = _get_compute_client(machine)
    url = IRI_IMPLEMENTATIONS[client.implementation]["api_base"] + '/' + suburl
    retry_errors = []
    for attempt in range(IRI_GET_MAX_ATTEMPTS):
        try:
            resp = client.http_client.get(
                url,
                headers={"accept": "application/json", "Authorization": f"Bearer {client.access_token}"},
                params=params,
                timeout=300,
            )
        except httpx.TransportError as exc:
            retryable_error = exc
        else:
            if resp.status_code == 200:
                return json.loads(resp.text)
            if resp.status_code not in IRI_GET_RETRYABLE_STATUS_CODES:
                raise Exception(
                    f"Get operation failed with code {resp.status_code} and text "
                    f"{resp.text} (full response: {resp})"
                )
            retryable_error = RuntimeError(
                f"HTTP {resp.status_code}: {resp.text}"
            )

        retry_errors.append(f"attempt {attempt + 1}: {retryable_error}")

        if attempt == IRI_GET_MAX_ATTEMPTS - 1:
            raise RuntimeError(
                f"GET {url} failed after {IRI_GET_MAX_ATTEMPTS} attempts: "
                + "; ".join(retry_errors)
            ) from retryable_error

        delay = IRI_GET_INITIAL_RETRY_DELAY_SECONDS * (2 ** attempt)
        wfapiLog(
            f"Transient GET failure for {machine}:{suburl} ({retryable_error}); "
            f"retrying in {delay} seconds"
        )
        time.sleep(delay)
        
def getUserProjectIDmap(machine):
    """
    Obtain the mapping between project name and id
    Return:
       dict name -> id
    """
    machine = machine.lower()
    client, _ = _get_compute_client(machine)

    if machine not in client.project_map:
        j = get(machine, "account/projects")
        pmap = dict()
        for acct in j:
            pmap[acct['name']] = acct['id']
        client.project_map[machine] = pmap
    
    return client.project_map[machine]

def getKnownMachines():
    return [
        machine
        for implementation in iri_compute_clients
        for machine in IRI_IMPLEMENTATIONS[implementation]["machines"]
    ] + ["local"]

def getMachineQueues(machine)->List[ Tuple[str,str] ]:
    """
    Provide a list of queues and associated information for a given machine
    
    Return: a list of string tuples, with the first tuple entry being the queue name and the second relevant information about the queue
    """
    machine = machine.lower()
    _, machine_specification = _get_compute_client(machine)
    return machine_specification["queues"]

def getUserAccountProjects(machine):
    machine = machine.lower()
    _get_compute_client(machine)
    
    out = list(getUserProjectIDmap(machine).keys())
    if machine == "perlmutter": #_g is required for GPU nodes
        for i in range(len(out)):
            out[i] += "_g"
    return out
    
def getResourceID(machine, rtype="compute", path: Optional[str] = None):
    """
    Get the resource ID associated with the resource.

    rtype: "compute" or "storage". A storage path is required to select a
        named storage resource.
    """
    machine = machine.lower()
    if rtype not in ["compute", "storage"]:
        raise Exception("Invalid resource type")
    client, machine_specification = _get_compute_client(machine)

    if machine not in client.resource_map:
        wfapiLog("Obtaining resource information for machine", machine)

        def resources_for(group, resource_type):
            resource_params = {"group": group, "resource_type": resource_type}
            if IRI_IMPLEMENTATIONS[client.implementation]["status_resources_supports_filters"]:
                return get(machine, "status/resources", params=resource_params)
            return [
                resource for resource in get(machine, "status/resources")
                if resource.get("group") == group
                and resource.get("resource_type") == resource_type
            ]

        resources = resources_for(machine_specification["iriapi_group"], "compute")
        client.resource_map[machine] = {}

        resource_name = machine_specification["iriapi_resource_name"]
        matched_resources = [
            resource for resource in resources
            if resource.get("name") == resource_name
        ]
        if len(matched_resources) != 1:
            raise RuntimeError(
                f"Expected exactly one resource named {resource_name!r} for "
                f"machine {machine!r}; found {len(matched_resources)}"
            )
        client.resource_map[machine]["compute"] = matched_resources[0]["id"]
        wfapiLog(
            f"Identified resource {json.dumps(matched_resources[0], indent=2)} "
            f"as machine {machine}"
        )

        implementation_specification = IRI_IMPLEMENTATIONS[client.implementation]
        storage_group = implementation_specification.get("storage_iriapi_group")
        if storage_group:
            storage_resources = resources_for(
                storage_group,
                implementation_specification["storage_resource_type"],
            )
            client.resource_map[machine]["storage"] = {
                resource["name"]: resource["id"] for resource in storage_resources
            }

    resource = client.resource_map[machine][rtype]
    if rtype != "storage" or isinstance(resource, str):
        return resource

    if path is None:
        raise ValueError(
            f"A path is required to select a storage resource for {machine!r}"
        )
    path_object = pathlib.PurePosixPath(path)
    if not path_object.is_absolute():
        raise ValueError(f"Storage path must be absolute: {path!r}")

    storage_paths = IRI_IMPLEMENTATIONS[client.implementation]["storage_resource_paths"]
    matching_roots = [
        pathlib.PurePosixPath(root)
        for root in storage_paths
        if path_object.is_relative_to(pathlib.PurePosixPath(root))
    ]
    if not matching_roots:
        raise ValueError(
            f"No storage resource is configured for {machine}:{path_object}"
        )
    selected_root = max(matching_roots, key=lambda root: len(root.parts))
    resource_name = storage_paths[str(selected_root)]
    if resource_name not in resource:
        raise RuntimeError(
            f"Configured storage resource {resource_name!r} is not available for "
            f"machine {machine!r}"
        )
    return resource[resource_name]


def queryMachineStatus(machine: str, rtype="compute")-> bool:
    """
    Query the status of a machine
    Args:
       machine - The name of the machine
       rtype - The resource type (compute)
    Return:
       A bool indicating whether the machine up (True) or down (False)
    """
    machine = machine.lower()
    if machine == "local":
        return True
    rid = getResourceID(machine, rtype)
    j = get(machine, f"status/resources/{rid}")
    wfapiLog(f"Query status of machine {machine} returned {j['current_status']}")
    return j['current_status'] == 'up'
    

def waitTask(machine, task_id, poll_freq=4):
    machine = machine.lower()
    j = get(machine, f"task/{task_id}")
    
    while(j['status'] == "active" or j['status'] == "pending"):
        time.sleep(poll_freq)
        j = get(machine, f"task/{task_id}")

    if j['status'] != "completed":
        raise Exception(f"Task not completed, response {json.dumps(j,indent=2)}")

    return j

def remoteLs(machine: str, path: str)-> List[str]:
    """
    Query the contents of a path on a given machine
    Args:
       machine - The name of the machine. Valid values are 'Perlmutter'
       path - The absolute path
    Return:
       A list of files in the directory

    TODO: Explore behavior of trailing slashes, and the fact that the directory name itself seems to be listed among the directory content; should we unify the behavior with SFAPI?
    """
    machine = machine.lower()
    _get_compute_client(machine)

    wfapiLog(f"Listing contents of directory {machine}:{path}")
    
    rid = getResourceID(machine, rtype="storage", path=path)

    j = get(machine, f"filesystem/ls/{rid}", params={"path" : path})
    tid = j['task_id']
    j = waitTask(machine, tid)

    files = [ f['name'] for f in j['result']['output'] ]
    return files


def put(machine, suburl, data = None, params=None):
    machine = machine.lower()
    client, _ = _get_compute_client(machine)
    headers={ "accept" : "application/json", "Authorization" : f"Bearer {client.access_token}" }

    resp = client.http_client.put(
        IRI_IMPLEMENTATIONS[client.implementation]["api_base"] + '/' + suburl,
        headers=headers,
        json=data,
        params=params,
        timeout=300,
    )
    return json.loads(resp.text), resp.status_code



def remoteChmod(machine: str, path : str, mode : str, allow_unsafe = False):
    machine = machine.lower()
    wfapiLog(f"Changing permissions of file {machine}:{path} to {mode}")
    
    if not allow_unsafe and not checkSafePath(machine, path):
        raise Exception("Path is not a subdirectory of the sandbox path")

    if not pathlib.Path(path).is_absolute():
        raise Exception("Path must be absolute")

    rid = getResourceID(machine, rtype="storage", path=path)
    j, status = put(machine, f"filesystem/chmod/{rid}", data={"path" : path, "mode" : mode})
    tid = j['task_id']
    j = waitTask(machine, tid)
    
    if j["status"] != "completed":        
        raise Exception(f"Permission change failed: {json.dumps(j,indent=2)}")        
    

def post(machine, suburl, data = None, params=None, files=None, data_is_json=True):
    machine = machine.lower()
    client, _ = _get_compute_client(machine)
    headers={ "accept" : "application/json", "Authorization" : f"Bearer {client.access_token}" }

    resp = client.http_client.post(
        IRI_IMPLEMENTATIONS[client.implementation]["api_base"] + '/' + suburl,
        headers=headers,
        json=data if data_is_json else None,
        data=data if not data_is_json else None,
        params=params,
        files=files,
        timeout=300,
    )
    return json.loads(resp.text), resp.status_code
    
def remoteMkdir(machine: str, path: str, create_parents = True, allow_unsafe = False):
    """
    Create a directory on the remote machine. This is an unsafe action as it is not confined to the sandbox directory, and thus should not be exposed as a tool without safeguards
    Args:
           allow_unsafe - Allow uploading to directories other than within the sandbox
    """
    machine = machine.lower()
    if machine == "local":
        return local_api.localMkdir(path, create_parents, allow_unsafe)


    wfapiLog(f"Creating directory {machine}:{path}")
    
    if not allow_unsafe and not checkSafePath(machine, path):
        raise Exception(f"Path {path} is not a subdirectory of the sandbox path")

    if not pathlib.Path(path).is_absolute():
        raise Exception("Path must be absolute")

    rid = getResourceID(machine, rtype="storage", path=path)
        
    j, status = post(machine, f"filesystem/mkdir/{rid}", data={"path" : path, "parent" : create_parents})
    tid = j['task_id']
    j = waitTask(machine, tid)

    if j["status"] != "completed":
        raise Exception(f"Directory creation failed, status: {status},  response: { json.dumps(j,indent=2) }")        


def pathStat(machine: str, remote_path: str, dereference=True)->str:
    """
    Run stat on the provided path
    Args:
       machine - The name of the machine. Valid values are 'Perlmutter'
       remote_path - The absolute path on the remote machine
    """
    machine = machine.lower()    

    if machine == "local":
        raise NotImplementedError()
       
    if not pathlib.Path(remote_path).is_absolute():
        raise Exception("Path must be absolute")
    
    rid = getResourceID(machine, rtype="storage", path=remote_path)

    j = get(machine, f"filesystem/stat/{rid}", params={'path' : remote_path, 'dereference' : dereference})
    tid = j['task_id']
    j = waitTask(machine, tid)

    if j["status"] == "completed":
        return j["result"]["output"]
    else:
        raise Exception(f"Download failed, response: { json.dumps(j,indent=2)}")        


def pathType(machine: str, remote_path: str)->str:
    """
    Run 'file' on the provided path, identifying whether a file or directory
    Args:
       machine - The name of the machine. Valid values are 'Perlmutter'
       remote_path - The absolute path on the remote machine
    """
    machine = machine.lower()    

    if machine == "local":
        return local_api.localPathType(remote_path)

    if not pathlib.Path(remote_path).is_absolute():
        raise Exception("Path must be absolute")
    
    rid = getResourceID(machine, rtype="storage", path=remote_path)

    j = get(machine, f"filesystem/file/{rid}", params={'path' : remote_path})
    tid = j['task_id']
    j = waitTask(machine, tid)

    if j["status"] == "completed":
        return j["result"]["output"]
    else:
        raise Exception(f"Download failed, response: { json.dumps(j,indent=2)}")        

    


def uploadBytes(machine: str, remote_path: str, content: io.BytesIO, allow_unsafe = False):
    """
    Upload file contents as bytes to a remote path (max 5242880 Bytes)
    Args:
       machine - The name of the machine. Valid values are 'Perlmutter'
       remote_path - The absolute path to the resulting file on the remote machine
       content - The file contents as binary
       allow_unsafe - Allow uploading to directories other than within the sandbox
    """
    machine = machine.lower()
    if machine == "local":
        return local_api.writeBytes(remote_path, content, allow_unsafe)

    wfapiLog(f"Uploading binary data to {machine}:{remote_path}")
    
    if not allow_unsafe and not checkSafePath(machine, remote_path):
        raise Exception("Path is not below the privileged directory")
    
    if not pathlib.Path(remote_path).is_absolute():
        raise Exception("Path must be absolute")
   
    rid = getResourceID(machine, rtype="storage", path=remote_path)

    j, status = post(machine, f"filesystem/upload/{rid}", params={'path' : remote_path}, files={'file': content})

    tid = j['task_id']
    j = waitTask(machine, tid)

    if j["status"] != "completed":
        raise Exception(f"Upload failed, status: {status},  response: {json.dumps(j,indent=2)}")        


def downloadFileContents(machine: str, remote_path: str)->str:
    """
    Download a (small) remote file. Returns the file contents as a string (base64 or plaintext encoded, depending on the file)
    Args:
       machine - The name of the machine. Valid values are 'Perlmutter'
       remote_path - The absolute path on the remote machine
    """
    machine = machine.lower()

    wfapiLog(f"Downloading file contents {machine}:{remote_path}")    

    if not pathlib.Path(remote_path).is_absolute():
        raise Exception("Path must be absolute")
    
    rid = getResourceID(machine, rtype="storage", path=remote_path)

    j = get(machine, f"filesystem/download/{rid}", params={'path' : remote_path})
    tid = j['task_id']
    j = waitTask(machine, tid)
    
    if j["status"] == "completed":
        return j["result"]["output"]
    else:
        raise Exception(f"Download failed, response: { json.dumps(j,indent=2)}")        

def downloadFile(machine: str, local_path_out: str, remote_path_in: str)->str:
    from .api_general import isDirectory

    if isDirectory("local", local_path_out):
        local_path_out = local_api.getAbsoluteLocalPath(local_path_out) + "/" + pathlib.Path(remote_path_in).name

    if machine.lower() == "local":
        wfapiLog(f"Copying local file {remote_path_in} to {local_path_out}")    
        local_api.localCopyFile(local_path_out, remote_path_in, allow_unsafe=True)
    else:
        content = downloadFileContents(machine, remote_path_in)
        #Need to manually identify whether it is text or base64-binary encoded
        if re.match(r'^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)?$', content[:4096]):
            wfapiLog(f"Writing *binary* file contents to {local_path_out}")    
            with open(local_path_out, 'wb') as f:
                f.write(base64.b64decode(content))
        else:
            wfapiLog(f"Writing *plaintext* file contents to {local_path_out}")    
            with open(local_path_out, 'w') as f:
                f.write(content)
    return local_path_out
    
def executeBatchJobCompat(machine: str, script_body: str,
                    nodes : int, ranks_per_node : int, gpus_per_rank : int,
                    time : str, queue : str, account : str,
                    job_run_dir : str, exclusive=True, allow_unsafe=False,
                    name: str | None = None) -> str:
    """
    Execute batch script on the machine
    script_body: The content of the batch script. If you are executing an existing remote script, use "source /path/to/script"    
    Note that any SLURM/PBS headers will be ignored; ensure that SLURM headers that are usually passed to srun are manually passed instead

    time: the job duration. Currently it seems to only accept integers, which my testing indicates is in *seconds*

    NOTE: This relies on hacking the API to bypass the automatic srun command in favor of the (more flexible) script contents. Hopefully they will allow arbitrary batch script submission as part of the API eventually!
    """
    machine = machine.lower()
    if machine == "local":
        return local_api.executeJobScript(
            script_body, job_run_dir, allow_unsafe, name=name
        )

    wfapiLog(f"Executing batch job on machine {machine} with nodes:{nodes}, ranks/node:{ranks_per_node}, gpus/rank:{gpus_per_rank}, time:{time}, queue:{queue}, account:{account}")
    
    if not allow_unsafe and not checkSafePath(machine, job_run_dir):
        raise Exception("Path is not below the privileged directory")

    resources = {
        "node_count" : nodes,
        "processes_per_node" : ranks_per_node,
        "exclusive_node_use": exclusive,
        "gpu_cores_per_process" : gpus_per_rank
        }       
        
    attributes = {
        "duration" : time,
        "account": account,
        "queue_name": queue
        }
    spec = {
        "executable": "date", 
        "directory" : job_run_dir,
        "name": name,
        "inherit_environment": True,
        "stdin_path" : None,
        "stdout_path": f"{job_run_dir}/run.log",
        "stderr_path": f"{job_run_dir}/err.log",
        "resources" : resources,
        "attributes" : attributes,
        "launcher" : "srun",
        "pre_launch" : f"cd {job_run_dir}\n" + script_body,  #hijack the pre_launch to run the actual batch script content to avoid the jobspec restrictions
        "post_launch" : None
        }
    
    rid = getResourceID(machine, rtype="compute")

    j, status = post(machine, f"compute/job/{rid}", data=spec)
    if status == 200:
        return j['id']
    else:        
        raise Exception(f"Job submission failed, status: {status}, reason: { json.dumps(j,indent=2)}")
    
def getJobState(machine: str, jobid: str) -> str:
    machine = machine.lower()
    if machine == "local":
        return local_api.getJobState(jobid)

    rid = getResourceID(machine, rtype="compute")
    j = get(machine, f"compute/status/{rid}/{jobid}", params = { "historical" : True })
    wfapiLog(f"Queried job state {machine}:{jobid}, got {j['status']['state']}")
    return j['status']['state']

def findJobByName(machine: str, name: str) -> str | None:
    """Return the remote job ID with an exact JobSpec name, if present."""
    machine = machine.lower()
    if machine == "local":
        return local_api.findJobByName(name)

    rid = getResourceID(machine, rtype="compute")
    offset = 0
    limit = 1000
    while True:
        jobs, status = post(
            machine,
            f"compute/status/{rid}",
            params={"historical": True, "include_spec": True, "offset": offset, "limit": limit},
        )
        if status != 200:
            raise Exception(f"Job lookup failed, status: {status}, reason: {json.dumps(jobs, indent=2)}")
        for job in jobs:
            if job.get("job_spec", {}).get("name") == name:
                return job["id"]
        if len(jobs) < limit:
            return None
        offset += limit
    
    
def delete(machine, suburl, params = None):
    machine = machine.lower()
    client, _ = _get_compute_client(machine)
    resp = client.http_client.delete(
        IRI_IMPLEMENTATIONS[client.implementation]["api_base"] + '/' + suburl,
        headers={"accept": "*/*", "Authorization": f"Bearer {client.access_token}"},
        params=params,
        timeout=300,
    )
    return {} if resp.text == "" else resp.json(), resp.status_code


def _transfer_get(suburl, params=None):
    token = tokens["iriapi_transfer_base"]
    if token is None:
        raise RuntimeError("IRI transfer API token is not initialized")
    response = iri_transfer_client.get(
        TRANSFER_API_BASE + "/" + suburl,
        headers={"accept": "application/json", "Authorization": f"Bearer {token}"},
        params=params,
        timeout=300,
    )
    if response.status_code != 200:
        raise Exception(
            f"Get operation failed with code {response.status_code} and text "
            f"{response.text} (full response: {response})"
        )
    return json.loads(response.text)


def _transfer_post(suburl, data=None, params=None, files=None, data_is_json=True):
    token = tokens["iriapi_transfer_base"]
    if token is None:
        raise RuntimeError("IRI transfer API token is not initialized")
    response = iri_transfer_client.post(
        TRANSFER_API_BASE + "/" + suburl,
        headers={"accept": "application/json", "Authorization": f"Bearer {token}"},
        json=data if data_is_json else None,
        data=data if not data_is_json else None,
        params=params,
        files=files,
        timeout=300,
    )
    return json.loads(response.text), response.status_code

def cancelJob(machine: str, jobid: str):
    machine = machine.lower()

    wfapiLog(f"Canceling job {machine}:{jobid}")
    rid = getResourceID(machine, rtype="compute")
    j, status = delete(machine, f"compute/cancel/{rid}/{jobid}")
    if status != 204:
        raise Exception("Job cancellation failed:",json.dumps(j))


def globusTransferStatus(transfer_id)-> str:
    """
    Query the status of a Globus transfer with the provided transfer_id
    Returns the status from the following (cf. https://docs.globus.org/api/transfer/task/):
    "ACTIVE"  The task is in progress.
    "INACTIVE" The task has been suspended and will not continue without intervention. Currently, only credential expiration will cause this state.
    "SUCCEEDED"  The task completed successfully.
    "FAILED"  The task or one of its subtasks failed, expired, or was canceled.
    """
    j = _transfer_get(f"movement/transfer/globus/{transfer_id}")
    return j["status"]

def findGlobusTransfersByLabel(label: str) -> str | None:
    """Return the unique Globus task ID bearing an exact task label."""
    token = tokens["globus_transfer"]
    if token is None:
        raise RuntimeError("Globus Transfer API token is not initialized")
    client = globus_sdk.TransferClient(
        authorizer=globus_sdk.AccessTokenAuthorizer(token)
    )
    tasks = list(client.task_list(filter=f"label:={label}", limit=1000))
    if len(tasks) > 1:
        raise RuntimeError(f"Multiple Globus tasks use submission label {label!r}")
    return None if not tasks else tasks[0]["task_id"]

def findGlobusTransferByLabel(label: str) -> str | None:
    """Compatibility wrapper for the singular form of the lookup helper."""
    return findGlobusTransfersByLabel(label)


def _globus_path(root: str, relative_path: pathlib.PurePosixPath) -> str:
    return str(pathlib.PurePosixPath(root).joinpath(relative_path))


def _remote_globus_location(machine: str, remote_path: str) -> tuple[str, str]:
    """Translate a machine filesystem path to its Globus collection and path."""
    path = pathlib.PurePosixPath(remote_path)
    if not path.is_absolute():
        raise ValueError(f"Remote Globus path must be absolute: {remote_path!r}")

    implementation = _configured_implementation_name(machine)
    filesystems = IRI_IMPLEMENTATIONS[implementation].get("globus_filesystems", [])
    matches = [
        filesystem for filesystem in filesystems
        if path.is_relative_to(pathlib.PurePosixPath(filesystem["filesystem_root"]))
    ]
    if not matches:
        raise ValueError(f"No Globus endpoint is configured for {machine}:{path}")
    filesystem = max(
        matches,
        key=lambda item: len(pathlib.PurePosixPath(item["filesystem_root"]).parts),
    )
    root = pathlib.PurePosixPath(filesystem["filesystem_root"])
    return filesystem["endpoint"], _globus_path(
        filesystem["globus_root"], path.relative_to(root)
    )


def _personal_staging_configuration() -> dict[str, str]:
    if globus_personal_endpoint is None:
        raise RuntimeError(
            "Globus file transfers require globus_personal_endpoint in the "
            "manager configuration"
        )
    return globus_personal_endpoint


def _staging_globus_path(local_path: Path, staging: dict[str, str]) -> str | None:
    staging_local = Path(staging["staging_directory_local"]).expanduser().resolve()
    try:
        relative_path = local_path.expanduser().resolve().relative_to(staging_local)
    except ValueError:
        return None
    return _globus_path(staging["staging_directory_globus"], relative_path)


def _temporary_staging_directory(staging: dict[str, str]) -> tuple[Path, str]:
    staging_local = Path(staging["staging_directory_local"]).expanduser().resolve()
    staging_local.mkdir(parents=True, exist_ok=True)
    temporary_directory = staging_local / f".amsc-globus-{uuid.uuid4().hex}"
    temporary_directory.mkdir()
    return temporary_directory, _globus_path(
        staging["staging_directory_globus"],
        temporary_directory.relative_to(staging_local),
    )


def uploadFileGlobus(machine: str, remote_path: str, local_path: str) -> str:
    """Blocking upload of one local file through the configured personal endpoint."""
    source_path = Path(local_path).expanduser().resolve()
    if not source_path.is_file():
        raise ValueError(f"Local upload source is not a file: {local_path!r}")
    if not checkSafePath(machine, remote_path):
        raise Exception(f"Attempting to copy data to a disallowed location on {machine}")

    staging = _personal_staging_configuration()
    remote_endpoint, remote_globus_path = _remote_globus_location(machine, remote_path)
    remote_globus_file = pathlib.PurePosixPath(remote_globus_path)
    remote_filename = remote_globus_file.name
    if not remote_filename:
        raise ValueError(f"Remote upload path must name a file: {remote_path!r}")

    staged_path = _staging_globus_path(source_path, staging)
    temporary_directory = None
    if staged_path is None or source_path.name != remote_filename:
        temporary_directory, staging_directory = _temporary_staging_directory(staging)
        staged_path = _globus_path(staging_directory, pathlib.PurePosixPath(remote_filename))
        shutil.copy2(source_path, temporary_directory / remote_filename)

    try:
        return globusCopy(
            remote_endpoint, f"{remote_globus_file.parent}/", "personal", staged_path,
            allow_unsafe=True, block_until_complete=True,
            label="AmSC job manager file upload",
        )
    finally:
        if temporary_directory is not None:
            shutil.rmtree(temporary_directory, ignore_errors=True)


def downloadFileGlobus(machine: str, remote_path: str, local_path: str) -> str:
    """Blocking download of one remote file through the configured personal endpoint."""
    staging = _personal_staging_configuration()
    output_path = Path(local_path).expanduser()
    if output_path.is_dir():
        output_path /= pathlib.PurePosixPath(remote_path).name
    output_path = output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    remote_endpoint, remote_globus_path = _remote_globus_location(machine, remote_path)
    remote_globus_file = pathlib.PurePosixPath(remote_globus_path)
    remote_filename = remote_globus_file.name
    if not remote_filename:
        raise ValueError(f"Remote download path must name a file: {remote_path!r}")

    staged_path = _staging_globus_path(output_path, staging)
    temporary_directory = None
    if staged_path is not None and output_path.name == remote_filename:
        staging_destination = str(pathlib.PurePosixPath(staged_path).parent)
    else:
        temporary_directory, staging_destination = _temporary_staging_directory(staging)

    try:
        globusCopy(
            "personal", f"{staging_destination}/", remote_endpoint, remote_globus_path,
            allow_unsafe=True, block_until_complete=True,
            label="AmSC job manager file download",
        )
        if temporary_directory is not None:
            downloaded_file = temporary_directory / remote_filename
            if _staging_globus_path(output_path, staging) is not None:
                shutil.move(downloaded_file, output_path)
            else:
                shutil.copy2(downloaded_file, output_path)
        return str(output_path)
    finally:
        if temporary_directory is not None:
            shutil.rmtree(temporary_directory, ignore_errors=True)


def _globusCopy(source_endpoint, dest_endpoint, source_path, dest_path,
                block_until_complete=False, label: str | None = None):
    trans_args = { "source_uuid" : source_endpoint, "source_path": source_path,
                   "destination_uuid" : dest_endpoint, "destination_path" : dest_path,
                   "label" : label or "FemtoMeas transfer" }
    
    j, status = _transfer_post("movement/transfer/globus", data=trans_args)
    if status == 200:
        tid = j["transfer_uuid"]

        if block_until_complete:
            while (status := globusTransferStatus(tid)) == "ACTIVE":
                print(".", end="")
                time.sleep(20)
            print(status)
            if status != "SUCCEEDED":
                raise RuntimeError(f"Globus transfer {tid} finished with status {status}")

        return tid
    else:
        raise Exception("Globus transfer failed, response content: " + json.dumps(j))

def _checkSafePathTagOrUUID(machine_or_uuid: str, path: str)->bool:
    machine_or_uuid = machine_or_uuid.lower()

    # First check whether this is a configured machine name.
    if machine_or_uuid in manager_globals.remote_workdir:
        return checkSafePath(machine_or_uuid, path)

    # Resolve a special endpoint tag or UUID to its configured machine name.
    machine=None
    for m, uuid in special_globus_endpoints.items():
        if m == machine_or_uuid or uuid == machine_or_uuid:
            machine = m
            break

    if machine is not None and machine in manager_globals.remote_workdir:
        return checkSafePath(machine, path)

    #Otherwise we have to ask the user
    query = f"Do you give permission to write to path {path} on UUID {machine_or_uuid}"
    if machine is not None:
        query += f" ({machine})"
    return queryYesNo(query)

def globusCopy(dest_uuid: str, dest_path: str,
               source_uuid: str, source_path: str,
               allow_unsafe=False,
               block_until_complete=False,
               label: str | None = None)-> str:
    """
    Perform a Globus transfer between two endpoints
    Args:
       dest_uuid: The destination UUID (or a known machine name/tag)
       dest_path: The destination path on that machine (directory)
       source_uuid: The source UUID (or a known machine name/tag)
       source_path : The path on that endpoint
       allow_unsafe : Allow movement to paths outside of the sandbox or user permissions
       block_until_complete : Poll the transfer status every 20s until the transfer is complete before returning
    Return:
       The transfer ID as a string

    Notes:
       If source_path is a filename, only that file will be copied. If it is a directory name only the contents of that directory will be copied, not the directory itself (even if there is no trailing /)
    """
       
    wfapiLog(f"Initiating globus copy from {source_uuid}:{source_path} to {dest_uuid}:{dest_path}")    

    if not allow_unsafe and not _checkSafePathTagOrUUID(dest_uuid, dest_path):
        raise Exception(f"Attempting to copy data to a disallowed location on {dest_uuid}")

    #Transform tags into actual endpoints
    if dest_uuid in special_globus_endpoints:
        wfapiLog(f"Destination tag {dest_uuid} replaced by UUID {special_globus_endpoints[dest_uuid]}")
        dest_uuid = special_globus_endpoints[dest_uuid]
    if source_uuid in special_globus_endpoints:
        wfapiLog(f"Source tag {source_uuid} replaced by UUID {special_globus_endpoints[source_uuid]}")
        source_uuid = special_globus_endpoints[source_uuid]        

    return _globusCopy(source_endpoint=source_uuid, dest_endpoint=dest_uuid, source_path=source_path, dest_path=dest_path, block_until_complete=block_until_complete, label=label)
