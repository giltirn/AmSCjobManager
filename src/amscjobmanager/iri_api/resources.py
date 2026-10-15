"""IRI resource discovery, machine status, and project queries."""

import json
import pathlib
from typing import List, Optional, Tuple

from ..logging import wfapiLog
from .config import IRI_IMPLEMENTATIONS
from .rest import get
from .state import _get_compute_client, iri_compute_clients


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


def getProjectAllocation(machine: str, project_name: str) -> List[dict]:
    """Return all allocation entries associated with ``project_name``.

    The IRI endpoint returns a list of project-allocation records, each of
    which contains an ``entries`` list.  Flattening those lists preserves the
    useful allocation records even if a project has more than one allocation.
    The internal project ID required by the endpoint is resolved from the
    user-facing project name.
    """
    machine = machine.lower()
    _get_compute_client(machine)
    project_map = getUserProjectIDmap(machine)
    project_id = project_map.get(project_name)
    if project_id is None and machine == "perlmutter" and project_name.endswith("_g"):
        # GPU queue accounts carry this suffix, while the IRI project map uses
        # the underlying project name.
        project_id = project_map.get(project_name.removesuffix("_g"))
    if project_id is None:
        raise ValueError(
            f"Unknown project {project_name!r} for machine {machine!r}"
        )
    allocations = get(
        machine, f"account/projects/{project_id}/project_allocations"
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

    storage_paths = _machine_storage_resource_paths(machine)
    matching_storage_paths = [
        storage_mapping
        for storage_mapping in storage_paths
        if path_object.is_relative_to(
            pathlib.PurePosixPath(storage_mapping["filesystem_root"])
        )
    ]
    if not matching_storage_paths:
        if error := _unmounted_filesystem_error(
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
    resource_name = selected_storage_path["resource_name"]
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


# These routing helpers are defined before filesystem imports this module.
from .filesystem import _machine_storage_resource_paths, _unmounted_filesystem_error
