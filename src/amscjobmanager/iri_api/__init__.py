"""IRI API compatibility facade.

The public API remains available from ``amscjobmanager.iri_api`` while the
implementation is split into focused modules within this package.
"""

from . import state
from .config import *
from .state import (
    IRIComputeClient,
    iri_compute_clients,
    iri_transfer_client,
    listSpecialGlobusEndpoints,
    replaceSpecialGlobusEndpoint,
    setupWorkflowAgent,
    special_globus_endpoints,
    tokens,
)
from .auth import *
from .rest import *
from .resources import *
from .filesystem import *
from .globus import *
from .compute import *

# Retain the routing helpers used by the integration scripts and unit tests.
from .filesystem import _machine_storage_resource_paths, _mounted_filesystems
from .globus import _machine_globus_filesystems, _remote_globus_location


def __getattr__(name):
    # Read reassigned configuration from its owner rather than caching a copy.
    if name == "globus_personal_endpoint":
        return state.globus_personal_endpoint
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
