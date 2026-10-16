"""Process-wide authenticated IRI connection state."""

from dataclasses import dataclass, field
from typing import Union

import httpx
from .config import MACHINE_IMPLEMENTATIONS,IRI_IMPLEMENTATIONS
from .discovery import configureImplementation, loadCachedImplementations
from ..globals import addSandboxDirs, validateSandboxDirs

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
    access_token: str = field(repr=False)
    http_client: httpx.Client = field(default_factory=httpx.Client)
    resource_map: dict[str, dict[str, Union[str, dict[str, str]]]] = field(
        default_factory=dict
    )
    project_map: dict[str, dict[str, str]] = field(default_factory=dict)
    token_type: str = "globus"
    project: str | None = None
    description: str | None = None
    token_id: str | None = None

    def close(self):
        self.http_client.close()


iri_compute_clients: dict[str, IRIComputeClient] = {}
_implementation_credentials: dict[str, dict] = {}
_validating_clients: set[str] = set()
_transfer_token_path: str | None = None
_configuration_cache_directory: str | None = None
# The data-transfer API remains a separate, single service.
iri_transfer_client = httpx.Client()


def getMachineImplementation(machine: str) -> str:
    """Return a machine's implementation tag, rejecting unknown machines.

    Machine names are case-insensitive. This reads configuration and does not
    require an authenticated client.
    """
    machine = machine.lower()
    try:
        return MACHINE_IMPLEMENTATIONS[machine]
    except KeyError as exc:
        raise ValueError(f"Unknown IRI compute/filesystem machine: {machine!r}") from exc


def resolveImplementation(*, machine: str | None = None, implementation: str | None = None) -> str:
    """Return the implementation selected by a query's target arguments.

    At least one target is required. If both are given, the machine must belong
    to that implementation. Names are case-insensitive; authentication is
    checked separately when a client is retrieved.
    """
    if machine is None and implementation is None:
        raise ValueError("Specify machine or implementation")
    if implementation is not None:
        implementation = implementation.lower()
        if implementation not in IRI_IMPLEMENTATIONS:
            raise ValueError(f"Unknown IRI implementation: {implementation!r}")
    if machine is not None:
        machine_implementation = getMachineImplementation(machine)
        if implementation is not None and machine_implementation != implementation:
            raise ValueError(
                f"Machine {machine!r} belongs to {machine_implementation!r}, "
                f"not implementation {implementation!r}"
            )
        implementation = machine_implementation
    return implementation


def getImplementationClient(implementation: str) -> IRIComputeClient:
    """Return an authenticated client by implementation tag.

    Authenticate on first use and reuse the client thereafter. A known API
    definition alone is insufficient: credentials must have been configured.
    Validate pending sandboxes for this implementation before returning the
    client. Failed validation is retried on the next access without repeating
    authentication. Reentrant lookups from the checks reuse the registered
    client without restarting validation.
    """
    implementation = implementation.lower()
    if implementation not in iri_compute_clients:
        settings = _implementation_credentials.get(implementation)
        if settings is None:
            raise RuntimeError(f"IRI implementation {implementation!r} is not configured")
        setupImplementationClient(implementation, **settings)
    if implementation not in _validating_clients:
        _validating_clients.add(implementation)
        try:
            for machine in IRI_IMPLEMENTATIONS[implementation]["machines"]:
                validateSandboxDirs(machine)
        finally:
            _validating_clients.remove(implementation)
    return iri_compute_clients[implementation]


def getConfiguredImplementations() -> tuple[str, ...]:
    """Return providers selected by the user, without authenticating them."""
    return tuple(_implementation_credentials)


def getConfigurationCacheDirectory() -> str:
    """Return the configured discovery cache directory, or explain how to set it."""
    if _configuration_cache_directory is None:
        raise ValueError("Configure iri_config_cache_directory or supply cache_directory explicitly")
    return _configuration_cache_directory


def getTransferToken(service: str) -> str:
    """Authenticate the transfer service on first use and return its token.

    service is 'iriapi_transfer_base' or 'globus_transfer'. Merely configuring
    transfer credentials does not trigger Globus login during manager setup.
    """
    if service not in tokens:
        raise ValueError(f"Unknown transfer service: {service!r}")
    if tokens[service] is None:
        if _transfer_token_path is None:
            raise RuntimeError("Globus transfers require transferapi_key_path in the manager configuration")
        setupIRIapiGlobusTransfer(_transfer_token_path)
    return tokens[service]


def setupImplementationClient(
    implementation: str, token_path: str, token_type: str = "globus"
) -> IRIComputeClient:
    """Immediately authenticate, register a client, and validate pending sandboxes.

    The implementation must already have an API definition. Replaces and closes
    any previous client only after authentication succeeds. This supports
    discovery before machine and filesystem mappings are known. Registering
    before validation allows sandbox probes to reuse the same client.
    """
    implementation = implementation.lower()
    client = setupIRIapiCompute(implementation, token_path, token_type=token_type)
    previous = iri_compute_clients.get(implementation)
    if previous is not None:
        previous.close()
    iri_compute_clients[implementation] = client
    _implementation_credentials[implementation] = {"token_path": token_path, "token_type": token_type}
    return getImplementationClient(implementation)


def _close_compute_clients():
    for client in iri_compute_clients.values():
        client.close()
    iri_compute_clients.clear()
    _validating_clients.clear()

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

def setupWorkflowAgent(iri_implementations: dict, iriapi_transfer_key_path: str | None,
                       work_dir: dict, globus_personal_endpoint_config=None,
                       iri_config_cache_directory: str | None = None):
    """
    Configure the workflow agent without eager authentication or remote sandbox probes.

    Clients authenticate when first requested, then validate all registered
    sandboxes belonging to that implementation. Fresh discovery authenticates when it must
    query a provider; the explicit authorization test also remains eager.
    Local sandbox paths are checked during setup, without an IRI client.
    Args:
       iri_implementations: Mapping of implementation names to configuration
           objects containing a token_path and an optional token_type
           (globus by default, or amsc_pat). Implementations may share a PAT.
       iriapi_transfer_key_path: Optional IRI transfer token file path. None
           disables transfer authentication; login and token generation are
           deferred until the first transfer operation.
       work_dir: The remote work directories, by machine as a dict, e.g. { "perlmutter" : "/path/to/dir" }. Use a list of directories if more than one. The API is only allowed to modify the contents of files within this directory or its children
       globus_personal_endpoint_config: Optional personal endpoint UUID and
           staging-directory paths used by blocking Globus file transfers.
       iri_config_cache_directory: Cache directory for implementations whose
           configuration must be discovered from an API URL. All provider
           caches are loaded before configuring lazy clients.
           Unknown providers prompt for missing API URL and version.
    """    
    global _transfer_token_path, _configuration_cache_directory
    _configuration_cache_directory = iri_config_cache_directory
    normalized_implementations = {
        name.lower(): settings for name, settings in iri_implementations.items()
    }
    _configure_personal_globus_endpoint(globus_personal_endpoint_config)
    _close_compute_clients()
    # Discovery may create a client before new machine mappings are available;
    # discard previous sandbox checks before it can make any client requests.
    addSandboxDirs({}, warn_unavailable=False, defer_checks=True, replace=True)
    _implementation_credentials.clear()
    for implementation, settings in normalized_implementations.items():
        _implementation_credentials[implementation] = {
            "token_path": settings["token_path"] if isinstance(settings, dict) else settings.token_path,
            "token_type": settings.get("token_type", "globus") if isinstance(settings, dict) else settings.token_type,
        }
    if iri_config_cache_directory is not None:
        loadCachedImplementations(iri_config_cache_directory)
    for implementation, settings in normalized_implementations.items():
        configureImplementation(implementation, settings, iri_config_cache_directory)
    unknown_implementations = set(normalized_implementations) - set(IRI_IMPLEMENTATIONS)
    if unknown_implementations:
        raise ValueError(f"Unknown IRI implementations: {sorted(unknown_implementations)}")

    normalized_work_dir = {
        machine.lower(): directory for machine, directory in work_dir.items()
    }
    tokens.update(iriapi_transfer_base=None, globus_transfer=None)
    _transfer_token_path = iriapi_transfer_key_path
    addSandboxDirs(normalized_work_dir, warn_unavailable=False, defer_checks=True,
                   replace=True)
    return normalized_work_dir


def requireGlobusFallback(machine: str, operation: str) -> None:
    """Reject an operation whose Globus fallback lacks a personal endpoint.

    Operations handled natively or without a Globus fallback are unaffected.
    The check reads the endpoint configured in this module at call time.
    """
    if (
        getFilesystemOperationHandling(machine, operation) == "globus"
        and globus_personal_endpoint is None
    ):
        implementation = getMachineImplementation(machine)
        raise RuntimeError(
            f"{operation} on {implementation!r} requires the Globus fallback; "
            "configure globus_personal_endpoint in the manager configuration"
        )


def getPersonalStagingConfiguration() -> dict[str, str]:
    """Return the configured personal endpoint and its two staging paths.

    Raises RuntimeError if no endpoint is configured. The returned dictionary
    is shared configuration and should be treated as read-only by callers.
    """
    if globus_personal_endpoint is None:
        raise RuntimeError(
            "Globus file transfers require globus_personal_endpoint in the "
            "manager configuration"
        )
    return globus_personal_endpoint


# Import after the connection state is defined: auth uses that shared state.
from .auth import setupIRIapiCompute, setupIRIapiGlobusTransfer
from .filesystem import getFilesystemOperationHandling
