"""IRI resource discovery, machine status, and project queries."""

import json
import pathlib
from typing import List, Optional, Tuple
from urllib.parse import quote

from ..logging import wfapiLog
from .. import local_api
from .config import IRI_IMPLEMENTATIONS, getImplementationAPIVersion
from .rest import get
from .state import (
    getMachineImplementation, getImplementationClient, resolveImplementation,
    getConfiguredImplementations,
)


def getResources(
    *, machine: str | None = None, implementation: str | None = None,
    name: str | None = None, description: str | None = None,
    group: str | None = None, resource_type: str | None = None,
    offset: int | None = None, limit: int | None = None,
    modified_since: str | None = None, current_status: str | None = None,
    capability: list[str] | None = None,
) -> list[dict]:
    """List resources without requiring a known machine or resource group.

    V2 returns all pages by default. Explicit offset or limit requests a single
    page. Pagination, modified_since, current_status, and capability are
    implemented here for v2 only; supplying them to v1 raises NotImplementedError. Resource types
    use the deployment's vocabulary (short strings in v1, URNs in v2).
    ALCF's existing lack of server-side filtering is handled locally.
    """
    implementation = resolveImplementation(machine=machine, implementation=implementation)
    version = getImplementationAPIVersion(implementation)
    v2_options = {
        "offset": offset, "limit": limit, "modified_since": modified_since,
        "current_status": current_status, "capability": capability,
    }
    supplied = [key for key, value in v2_options.items() if value is not None]
    if version == 1 and supplied:
        raise NotImplementedError(
            f"These getResources options are not implemented for IRI API v1 "
            f"implementation {implementation!r}: {', '.join(supplied)}"
        )
    if offset is not None and (type(offset) is not int or offset < 0):
        raise ValueError("offset must be a nonnegative integer")
    if limit is not None and (type(limit) is not int or not 0 <= limit <= 1000):
        raise ValueError("limit must be an integer between 0 and 1000")
    filters = {
        key: value for key, value in {
            "name": name, "description": description,
            "group": group, "resource_type": resource_type,
        }.items() if value is not None
    }
    server_filters = IRI_IMPLEMENTATIONS[implementation].get("status_resources_supports_filters", True)
    params = dict(filters) if server_filters else {}
    params.update({key: value for key, value in v2_options.items() if value is not None})
    all_pages = version == 2 and offset is None and limit is None
    if version == 2:
        params.setdefault("offset", 0)
        params.setdefault("limit", 100)
    resources = []
    while True:
        page = get(suburl="status/resources", params=params or None, implementation=implementation)
        if not isinstance(page, list):
            raise RuntimeError(f"Resource listing from {implementation!r} must be a JSON array")
        resources.extend(page)
        if not all_pages or len(page) < params["limit"]:
            break
        params["offset"] += len(page)
    if not server_filters:
        resources = [
            resource for resource in resources
            if all(resource.get(key) == value for key, value in filters.items())
        ]
    return resources


def getResource(
    resource_id: str, *, machine: str | None = None, implementation: str | None = None,
) -> dict:
    """Retrieve one resource by ID through either a machine or implementation."""
    implementation = resolveImplementation(machine=machine, implementation=implementation)
    if not isinstance(resource_id, str) or not resource_id:
        raise ValueError("A nonempty resource ID is required")
    return get(
        suburl=f"status/resources/{quote(resource_id, safe='')}",
        implementation=implementation,
    )


def getUsername(machine: str | None = None, *, implementation: str | None = None) -> str:
    """Return the authenticated caller's username for a machine or implementation.

    Remote queries require API v2 and use GET account/whoami with the configured
    client. API v1 raises NotImplementedError before authentication or HTTP I/O.
    machine='local' runs local whoami without IRI setup or authentication;
    an implementation cannot also be specified for the local machine.
    """
    if machine is not None and machine.lower() == "local":
        if implementation is not None:
            raise ValueError("Cannot specify an IRI implementation for machine='local'")
        return local_api.localUsername()
    implementation = resolveImplementation(machine=machine, implementation=implementation)
    if getImplementationAPIVersion(implementation) != 2:
        raise NotImplementedError(f"getUsername requires IRI API v2; {implementation!r} uses API v1")
    return get(implementation=implementation, suburl="account/whoami")["username"]


def getUserProjectIDmap(machine=None, *, implementation=None):
    """
    Obtain the mapping between project name and id
    Return:
       dict name -> id
    """
    implementation = resolveImplementation(machine=machine, implementation=implementation)
    client = getImplementationClient(implementation)

    if implementation not in client.project_map:
        j = get(suburl="account/projects", implementation=implementation)
        pmap = dict()
        for acct in j:
            pmap[acct['name']] = acct['id']
        client.project_map[implementation] = pmap
    
    return client.project_map[implementation]

def getKnownMachines():
    return [
        machine
        for implementation in getConfiguredImplementations()
        for machine in IRI_IMPLEMENTATIONS[implementation]["machines"]
    ] + ["local"]

def getMachineQueues(machine)->List[ Tuple[str,str] ]:
    """
    Provide a list of queues and associated information for a given machine
    
    Return: a list of string tuples, with the first tuple entry being the queue name and the second relevant information about the queue
    """
    machine = machine.lower()
    implementation = getMachineImplementation(machine)
    machine_specification = IRI_IMPLEMENTATIONS[implementation]["machines"][machine]
    return machine_specification["queues"]



def getUserAccountProjects(machine=None, *, implementation=None):
    """List site projects; a machine additionally applies machine account naming."""
    if machine is not None:
        machine = machine.lower()
    
    out = list(getUserProjectIDmap(machine=machine, implementation=implementation).keys())
    if machine == "perlmutter": #_g is required for GPU nodes
        for i in range(len(out)):
            out[i] += "_g"
    return out


def getProjectAllocation(machine=None, project_name: str | None = None, *, implementation=None) -> List[dict]:
    """Return all allocation entries associated with ``project_name``.

    The IRI endpoint returns a list of project-allocation records, each of
    which contains an ``entries`` list.  Flattening those lists preserves the
    useful allocation records even if a project has more than one allocation.
    The internal project ID required by the endpoint is resolved from the
    user-facing project name.
    """
    implementation = resolveImplementation(machine=machine, implementation=implementation)
    if not isinstance(project_name, str) or not project_name:
        raise ValueError("A project name is required")
    if machine is not None:
        machine = machine.lower()
    project_map = getUserProjectIDmap(implementation=implementation)
    project_id = project_map.get(project_name)
    if project_id is None and machine == "perlmutter" and project_name.endswith("_g"):
        # GPU queue accounts carry this suffix, while the IRI project map uses
        # the underlying project name.
        project_id = project_map.get(project_name.removesuffix("_g"))
    if project_id is None:
        raise ValueError(
            f"Unknown project {project_name!r} for implementation {implementation!r}"
        )
    allocations = get(
        suburl=f"account/projects/{quote(project_id, safe='')}/project_allocations",
        implementation=implementation,
    )
    return [
        entry
        for allocation in allocations
        for entry in allocation.get("entries", [])
    ]
    
def getResourceID(machine, rtype="compute", path: Optional[str] = None):
    """
    Get the resource ID associated with the resource.

    rtype: "compute" or "storage". A storage path is required to select a
        named storage resource.
    """
    machine = machine.lower()
    if rtype not in ["compute", "storage"]:
        raise Exception("Invalid resource type")
    implementation = getMachineImplementation(machine)
    client = getImplementationClient(implementation)
    machine_specification = IRI_IMPLEMENTATIONS[implementation]["machines"][machine]

    if machine not in client.resource_map:
        wfapiLog("Obtaining resource information for machine", machine)

        def resources_for(group, resource_type):
            if getImplementationAPIVersion(client.implementation) == 1:
                return getResources(machine=machine, group=group, resource_type=resource_type)
            # Parent-URN filtering is not guaranteed to include subtypes.
            # Fetch the group and recognize systems locally, excluding nodes/GPUs.
            resources = getResources(machine=machine, group=group)
            if resource_type.startswith("urn:"):
                return [resource for resource in resources if resource.get("resource_type") == resource_type]
            root = f"urn:doe-iri:resource:{resource_type}"
            return [
                resource for resource in resources
                if resource.get("resource_type") in {root, root + ":system"}
                or resource.get("resource_type", "").startswith(root + ":system:")
            ]

        resources = resources_for(machine_specification.get("iriapi_group"), "compute")

        resource_name = machine_specification.get("iriapi_resource_name")
        resource_id = machine_specification.get("iriapi_resource_id")
        if resource_id is None and resource_name is None:
            raise ValueError(f"Machine {machine!r} must specify iriapi_resource_id or iriapi_resource_name")
        matched_resources = [
            resource for resource in resources
            if (
                resource.get("id") == resource_id if resource_id is not None
                else resource.get("name") == resource_name
            )
        ]
        if len(matched_resources) != 1:
            raise RuntimeError(
                f"Expected exactly one resource matching {resource_id or resource_name!r} for "
                f"machine {machine!r}; found {len(matched_resources)}"
            )
        resource_map = {"compute": matched_resources[0]["id"]}
        wfapiLog(
            f"Identified resource {json.dumps(matched_resources[0], indent=2)} "
            f"as machine {machine}"
        )

        implementation_specification = IRI_IMPLEMENTATIONS[client.implementation]
        storage_group = implementation_specification.get("storage_iriapi_group")
        if storage_group or getImplementationAPIVersion(client.implementation) == 2:
            storage_resources = resources_for(
                storage_group,
                implementation_specification.get("storage_resource_type", "storage"),
            )
            resource_map["storage"] = {
                resource["name"]: resource["id"] for resource in storage_resources
                if resource.get("name") is not None
            }
            if getImplementationAPIVersion(client.implementation) == 2:
                resource_map["storage"].update({resource["id"]: resource["id"] for resource in storage_resources})
        client.resource_map[machine] = resource_map

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

    storage_paths = getStorageResourcePaths(machine)
    matching_storage_paths = [
        storage_mapping
        for storage_mapping in storage_paths
        if path_object.is_relative_to(
            pathlib.PurePosixPath(storage_mapping["filesystem_root"])
        )
    ]
    if not matching_storage_paths:
        if error := getUnmountedFilesystemError(
            machine, path_object, "storage_resource_paths"
        ):
            raise error
        raise ValueError(
            f"No storage resource is configured for {machine}:{path_object}"
        )
    selected_storage_path = max(
        matching_storage_paths,
        key=lambda mapping: len(
            pathlib.PurePosixPath(mapping["filesystem_root"]).parts
        ),
    )
    resource_name = selected_storage_path.get("resource_id") or selected_storage_path["resource_name"]
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
    j = getResource(rid, machine=machine)
    wfapiLog(f"Query status of machine {machine} returned {j['current_status']}")
    return j['current_status'] == 'up'


# These routing helpers are defined before filesystem imports this module.
from .filesystem import getStorageResourcePaths, getUnmountedFilesystemError
