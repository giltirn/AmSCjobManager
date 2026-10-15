"""Process-wide authenticated IRI connection state."""

from dataclasses import dataclass, field
from typing import Union

import httpx
from .config import MACHINE_IMPLEMENTATIONS,IRI_IMPLEMENTATIONS
from .. import globals as manager_globals
from ..globals import addSandboxDirs

tokens = { "iriapi_transfer_base" : None, "globus_transfer" : None }

special_globus_endpoints = {
    "dtn": "9d6d994a-6d04-11e5-ba46-22000b92c6ec",
    "bnl": "12782fb1-a599-4f18-b0fb-2e849681e214",
}

def replaceSpecialGlobusEndpoint(endpoint : str):
    """Replace a special globus endpoint tag with the actual UUID"""
    endpoint = endpoint.lower()
    if endpoint in special_globus_endpoints:
        return special_globus_endpoints[endpoint]
    else:
        return endpoint
    
def listSpecialGlobusEndpoints():
    return special_globus_endpoints.keys()



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

globus_personal_endpoint: dict[str, str] | None = None

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


def _require_globus_fallback(machine: str, operation: str) -> None:
    if (
        _filesystem_operation_handling(machine, operation) == "globus"
        and globus_personal_endpoint is None
    ):
        implementation = _configured_implementation_name(machine)
        raise RuntimeError(
            f"{operation} on {implementation!r} requires the Globus fallback; "
            "configure globus_personal_endpoint in the manager configuration"
        )


def _personal_staging_configuration() -> dict[str, str]:
    if globus_personal_endpoint is None:
        raise RuntimeError(
            "Globus file transfers require globus_personal_endpoint in the "
            "manager configuration"
        )
    return globus_personal_endpoint


# Import after the connection state is defined: auth uses that shared state.
from .auth import setupIRIapiCompute, setupIRIapiTransfer
from .filesystem import _filesystem_operation_handling
