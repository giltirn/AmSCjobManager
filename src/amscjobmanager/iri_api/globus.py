"""Globus transfer operations, endpoint routing, and local staging."""

import json
import pathlib
import shutil
import time
import uuid
from pathlib import Path
from typing import Literal

import globus_sdk

from .. import globals as manager_globals
from ..globals import checkSafePath
from ..logging import wfapiLog
from .config import MACHINE_IMPLEMENTATIONS, TRANSFER_API_BASE
from .filesystem import getMountedFilesystems, getUnmountedFilesystemError
from .state import (
    getPersonalStagingConfiguration,
    getTransferToken,
    iri_transfer_client,
    special_globus_endpoints,
    replaceSpecialGlobusEndpoint,
)

def _transfer_get(suburl, params=None):
    token = getTransferToken("iriapi_transfer_base")
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
    token = getTransferToken("iriapi_transfer_base")
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
    token = getTransferToken("globus_transfer")
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


def _absolute_remote_path(remote_path: str) -> pathlib.PurePosixPath:
    """Validate a remote POSIX path without resolving it on the manager's OS."""
    path = pathlib.PurePosixPath(remote_path)
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError(f"Remote paths must be absolute and contain no '..': {remote_path!r}")
    return path


def getRemoteGlobusLocation(machine: str, remote_filesystem_path: str) -> tuple[str, str]:
    """Translate an absolute machine path to a Globus endpoint tag/UUID and path.

    remote_filesystem_path is visible on the machine, NOT a Globus path.
    Returns (endpoint tag/UUID, Globus-visible path), using the longest
    matching root alias among mounted filesystems. Raises
    ValueError if the path is relative, unmounted, or has no configured route.
    """
    path = _absolute_remote_path(remote_filesystem_path)

    filesystems = [
        {**route, "filesystem_root": root}
        for filesystem in getMountedFilesystems(machine).values()
        for route in filesystem.get("globus_filesystems", [])
        for root in route["filesystem_roots"]
    ]
    matches = [
        filesystem for filesystem in filesystems
        if path.is_relative_to(pathlib.PurePosixPath(filesystem["filesystem_root"]))
    ]
    if not matches:
        if error := getUnmountedFilesystemError(
            machine, path, "globus_filesystems"
        ):
            raise error
        raise ValueError(f"No Globus endpoint is configured for {machine}:{path}")
    filesystem = max(
        matches,
        key=lambda item: len(pathlib.PurePosixPath(item["filesystem_root"]).parts),
    )
    root = pathlib.PurePosixPath(filesystem["filesystem_root"])
    return filesystem["endpoint"], str(pathlib.PurePosixPath(filesystem["globus_root"]) / path.relative_to(root))


def _file_transfer_location(machine, remote_path, remote_path_type, remote_endpoint):
    """Resolve a file operation's explicit namespace and collection selection."""
    if remote_path_type == "filesystem":
        if remote_endpoint is not None:
            raise ValueError("remote_endpoint is only needed with remote_path_type='globus'")
        return getRemoteGlobusLocation(machine, remote_path)
    if remote_path_type != "globus":
        raise ValueError("remote_path_type must be 'filesystem' or 'globus'")
    globus_path = _absolute_remote_path(remote_path)
    endpoints = {
        replaceSpecialGlobusEndpoint(route["endpoint"])
        for filesystem in getMountedFilesystems(machine).values()
        for route in filesystem.get("globus_filesystems", [])
        if globus_path.is_relative_to(pathlib.PurePosixPath(route["globus_root"]))
    }
    if remote_endpoint is not None:
        endpoint_uuid = replaceSpecialGlobusEndpoint(remote_endpoint)
        if endpoint_uuid not in endpoints:
            raise ValueError(f"Endpoint {remote_endpoint!r} has no configured Globus route for {machine}:{globus_path}")
        return endpoint_uuid, str(globus_path)
    if len(endpoints) != 1:
        raise ValueError(
            f"Expected one Globus endpoint for {machine}:{globus_path}; found {len(endpoints)}. "
            "Specify remote_endpoint with its collection UUID or endpoint tag."
        )
    return endpoints.pop(), str(globus_path)


def _staging_globus_path(local_filesystem_path: Path, staging: dict[str, str]) -> str | None:
    """Translate an OS-accessible staging path into the personal Globus namespace."""
    staging_local = Path(staging["staging_directory_local"]).expanduser().resolve()
    try:
        relative_path = local_filesystem_path.expanduser().resolve().relative_to(staging_local)
    except ValueError:
        return None
    return str(_absolute_remote_path(staging["staging_directory_globus"]) / relative_path)


def createGlobusStagingDirectory() -> tuple[Path, str]:
    """Create a unique directory inside the personal endpoint's staging area.

    Uses the configured personal endpoint. Return its local Path and
    corresponding Globus path. The caller owns the
    temporary directory and must remove it after the transfer finishes.
    """
    staging = getPersonalStagingConfiguration()
    staging_local = Path(staging["staging_directory_local"]).expanduser().resolve()
    staging_local.mkdir(parents=True, exist_ok=True)
    temporary_directory = staging_local / f".amsc-globus-{uuid.uuid4().hex}"
    temporary_directory.mkdir()
    return temporary_directory, str(
        pathlib.PurePosixPath(staging["staging_directory_globus"])
        / temporary_directory.relative_to(staging_local)
    )


def uploadFileGlobus(machine: str, remote_path: str, local_filesystem_path: str, *,
                     remote_path_type: Literal["filesystem", "globus"] = "filesystem",
                     remote_endpoint: str | None = None) -> str:
    """Upload a local file, blocking until the personal-endpoint transfer finishes.

    local_filesystem_path is accessible to the manager's OS. remote_path names
    the destination FILE, in the machine filesystem by default. Set
    remote_path_type='globus' to provide a Globus-visible file path instead;
    remote_endpoint selects its collection UUID/tag when the machine has
    overlapping Globus namespaces. Filesystem mode selects it automatically.
    Uploads must fall within a configured sandbox in the selected namespace.
    Returns the transfer ID. Local staging and filename changes are automatic.
    """
    source_filesystem_path = Path(local_filesystem_path).expanduser().resolve()
    if not source_filesystem_path.is_file():
        raise ValueError(f"Local upload source is not a file: {local_filesystem_path!r}")
    remote_endpoint, remote_globus_path = _file_transfer_location(
        machine, remote_path, remote_path_type, remote_endpoint
    )
    if remote_path_type == "filesystem":
        safe = checkSafePath(machine, remote_path)
    else:
        safe = checkSafeGlobusPath(remote_endpoint, remote_globus_path, machine=machine)
    if not safe:
        raise Exception(f"Attempting to copy data to a disallowed location on {machine}")

    staging = getPersonalStagingConfiguration()
    remote_globus_file = pathlib.PurePosixPath(remote_globus_path)
    remote_filename = remote_globus_file.name
    if not remote_filename:
        raise ValueError(f"Remote upload path must name a file: {remote_path!r}")

    staged_globus_path = _staging_globus_path(source_filesystem_path, staging)
    temporary_directory = None
    if staged_globus_path is None or source_filesystem_path.name != remote_filename:
        temporary_directory, staging_globus_directory = createGlobusStagingDirectory()
        staged_globus_path = str(pathlib.PurePosixPath(staging_globus_directory) / remote_filename)
        shutil.copy2(source_filesystem_path, temporary_directory / remote_filename)

    try:
        return globusCopy(
            remote_endpoint, f"{remote_globus_file.parent}/", "personal", staged_globus_path,
            allow_unsafe=True, block_until_complete=True,
            label="AmSC job manager file upload",
        )
    finally:
        if temporary_directory is not None:
            shutil.rmtree(temporary_directory, ignore_errors=True)


def downloadFileGlobus(machine: str, local_filesystem_path: str, remote_path: str, *,
                       remote_path_type: Literal["filesystem", "globus"] = "filesystem",
                       remote_endpoint: str | None = None) -> str:
    """Download through the personal endpoint, blocking until the file is local.

    local_filesystem_path is an OS-accessible destination filename or existing
    directory within a configured local sandbox or the personal-endpoint staging
    directory. remote_path names the source FILE, in the machine filesystem by
    default. Set remote_path_type='globus' for a Globus-visible path;
    remote_endpoint selects its collection UUID/tag if the machine has
    overlapping namespaces. Staging and renaming are automatic. Returns the
    local file path.
    """
    staging = getPersonalStagingConfiguration()
    remote_endpoint, remote_globus_path = _file_transfer_location(
        machine, remote_path, remote_path_type, remote_endpoint
    )
    output_filesystem_path = Path(local_filesystem_path).expanduser()
    if output_filesystem_path.is_dir():
        output_filesystem_path /= pathlib.PurePosixPath(remote_path).name
    output_filesystem_path = output_filesystem_path.resolve()
    staged_globus_path = _staging_globus_path(output_filesystem_path, staging)
    if staged_globus_path is None and not checkSafePath(
        "local", str(output_filesystem_path), allow_tmp=False
    ):
        raise ValueError(
            f"Local download destination is outside the configured local sandbox and staging directory: "
            f"{local_filesystem_path!r}"
        )
    output_filesystem_path.parent.mkdir(parents=True, exist_ok=True)

    remote_globus_file = pathlib.PurePosixPath(remote_globus_path)
    remote_filename = remote_globus_file.name
    if not remote_filename:
        raise ValueError(f"Remote download path must name a file: {remote_path!r}")

    temporary_directory = None
    if staged_globus_path is not None and output_filesystem_path.name == remote_filename:
        staging_destination_globus_path = str(pathlib.PurePosixPath(staged_globus_path).parent)
    else:
        temporary_directory, staging_destination_globus_path = createGlobusStagingDirectory()

    try:
        globusCopy(
            "personal", f"{staging_destination_globus_path}/", remote_endpoint, remote_globus_path,
            allow_unsafe=True, block_until_complete=True,
            label="AmSC job manager file download",
        )
        if temporary_directory is not None:
            downloaded_file = temporary_directory / remote_filename
            if _staging_globus_path(output_filesystem_path, staging) is not None:
                shutil.move(downloaded_file, output_filesystem_path)
            else:
                shutil.copy2(downloaded_file, output_filesystem_path)
        return str(output_filesystem_path)
    finally:
        if temporary_directory is not None:
            shutil.rmtree(temporary_directory, ignore_errors=True)


def _resolve_globus_endpoint(endpoint: str, globus_path: str) -> str:
    """Return the Globus collection ID to use for a path on an endpoint.

    Args:
        endpoint: A collection UUID, a configured endpoint tag (such as
            ``"dtn"``), or an IRI machine name. Matching is case-insensitive.
        globus_path: An absolute path in the Globus collection's namespace.
            It is used only when ``endpoint`` is a machine name, to find the
            configured collection whose Globus root contains the path. It is
            not a path on the machine's filesystem and is never translated.

    Returns:
        The collection UUID for a configured tag or for the machine's single
        matching collection. A supplied UUID (or any other unrecognized
        endpoint string) is returned lowercased, without validation.

    Raises:
        ValueError: When ``endpoint`` is a machine name and ``globus_path``
            is invalid or matches zero or multiple configured collections.
    """
    endpoint = endpoint.lower()
    if endpoint in special_globus_endpoints:
        return replaceSpecialGlobusEndpoint(endpoint)
    if endpoint in MACHINE_IMPLEMENTATIONS:
        try:
            return _file_transfer_location(endpoint, globus_path, "globus", None)[0]
        except ValueError as exc:
            raise ValueError(f"Cannot resolve machine tag {endpoint!r} for Globus path {globus_path!r}; use a collection UUID or endpoint tag instead") from exc
    return endpoint


def _resolve_transfer_path(endpoint: str, path: str, path_type: str) -> tuple[str, str]:
    """Resolve one side of a transfer to a collection and its Globus path.

    Args:
        endpoint: A machine name, configured endpoint tag, or collection UUID.
            A machine name selects a collection using the path; a tag or UUID
            selects that collection directly. Matching is case-insensitive.
        path: An absolute path without ``..``. Its namespace is specified by
            ``path_type``.
        path_type: ``"globus"`` if ``path`` is already visible on the Globus
            collection, or ``"filesystem"`` if it is a machine filesystem
            path. Personal endpoint paths must be within the configured staging
            root in the selected namespace.

    Returns:
        A ``(collection_id, globus_path)`` tuple of strings. The first value
        is normally a collection UUID; with ``path_type="globus"``, an unknown
        endpoint string is returned lowercased without validation. A Globus
        path stays in that namespace. A filesystem path is translated using
        the configured route with the longest matching filesystem root.

    Raises:
        ValueError: The path or path type is invalid, a personal endpoint path
            is outside staging, or a filesystem route is missing or ambiguous.
    """
    if path_type not in {"filesystem", "globus"}:
        raise ValueError("path type must be 'filesystem' or 'globus'")

    validated_path = _absolute_remote_path(path)
    endpoint = endpoint.lower()
    personal_uuid = special_globus_endpoints.get("personal")
    if endpoint == "personal" and personal_uuid is None:
        raise ValueError("Personal Globus endpoint is not configured")
    if personal_uuid is not None and replaceSpecialGlobusEndpoint(endpoint).lower() == personal_uuid.lower():
        staging = getPersonalStagingConfiguration()
        if path_type == "globus":
            staging_root = _absolute_remote_path(staging["staging_directory_globus"])
            if not validated_path.is_relative_to(staging_root):
                raise ValueError(f"Personal Globus path is outside the staging directory: {path!r}")
            return personal_uuid.lower(), str(validated_path)
        globus_path = _staging_globus_path(Path(path), staging)
        if globus_path is None:
            raise ValueError(f"Personal filesystem path is outside the staging directory: {path!r}")
        return personal_uuid.lower(), globus_path

    if path_type == "globus":
        globus_path = str(validated_path)
        return _resolve_globus_endpoint(endpoint, globus_path), globus_path

    filesystem_path = validated_path
    machine_endpoint = endpoint in MACHINE_IMPLEMENTATIONS
    collection = None if machine_endpoint else replaceSpecialGlobusEndpoint(endpoint)
    matches = []
    for machine in ([endpoint] if machine_endpoint else MACHINE_IMPLEMENTATIONS):
        for filesystem in getMountedFilesystems(machine).values():
            for route in filesystem.get("globus_filesystems", []):
                route_collection = replaceSpecialGlobusEndpoint(route["endpoint"])
                if collection is not None and route_collection != collection:
                    continue
                for root_name in route["filesystem_roots"]:
                    root = pathlib.PurePosixPath(root_name)
                    if filesystem_path.is_relative_to(root):
                        globus_path = str(pathlib.PurePosixPath(route["globus_root"]) / filesystem_path.relative_to(root))
                        matches.append((len(root.parts), route_collection, globus_path))
    if not matches:
        if machine_endpoint and (error := getUnmountedFilesystemError(endpoint, filesystem_path, "globus_filesystems")):
            raise error
        raise ValueError(f"No configured filesystem-to-Globus route for endpoint {endpoint!r} and path {path!r}")
    longest_root = max(length for length, _, _ in matches)
    locations = {(route_collection, globus_path) for length, route_collection, globus_path in matches if length == longest_root}
    if len(locations) != 1:
        raise ValueError(f"Ambiguous filesystem-to-Globus routes for endpoint {endpoint!r} and path {path!r}")
    return locations.pop()


def checkSafeGlobusPath(endpoint: str, destination_globus_path: str, *, machine=None) -> bool:
    """Decide whether a write to a Globus path is allowed by configured sandboxes.

    Note that, unlike checkSafePath, if an endpoint is not known (i.e. it cannot be mapped to a machine), this check will *allow* the Globus write. We
    only enforce sandboxing on known machines.

    Machine filesystem sandbox roots are translated to Globus paths before
    comparison. Personal endpoint staging roots are validated separately by
    _resolve_transfer_path for both sides of a transfer.

    Args:
        endpoint: Destination collection UUID, configured endpoint tag, or
            machine name. It is resolved to a collection identifier for the
            comparison.
        destination_globus_path: Absolute destination path in that collection's
            Globus namespace, rather than a machine filesystem path.
        machine: Optional machine name that limits the check to that machine's
            sandbox directories. When supplied, a path with no matching
            sandbox is rejected.

    Returns:
        ``True`` if the destination lies within an applicable sandbox, or if
        no configured sandbox applies and no machine was specified. ``False``
        if a machine was specified or a sandbox matches the collection, but
        the path lies outside the allowed roots. No permission prompt occurs.
    """
    endpoint_uuid = _resolve_globus_endpoint(endpoint, destination_globus_path)
    destination = _absolute_remote_path(destination_globus_path)
    known_endpoint = machine is not None
    for sandbox_machine, directories in (manager_globals.remote_workdir or {}).items():
        if sandbox_machine not in MACHINE_IMPLEMENTATIONS or (machine is not None and sandbox_machine != machine.lower()):
            continue
        for sandbox_filesystem_path in directories:
            try:
                sandbox_endpoint, sandbox_globus_path = getRemoteGlobusLocation(sandbox_machine, sandbox_filesystem_path)
            except ValueError:
                # An unmapped filesystem cannot authorize a Globus write.
                continue
            if replaceSpecialGlobusEndpoint(sandbox_endpoint) != endpoint_uuid:
                continue
            known_endpoint = True
            if destination.is_relative_to(_absolute_remote_path(sandbox_globus_path)):
                return True
    if known_endpoint:
        return False
    return True


def globusCopy(dest_endpoint: str, dest_path: str,
               source_endpoint: str, source_path: str,
               allow_unsafe=False,
               block_until_complete=False,
               label: str | None = None, *,
               source_path_type: Literal["filesystem", "globus"] = "globus",
               dest_path_type: Literal["filesystem", "globus"] = "globus") -> str:
    """
    Transfer between collections, translating filesystem paths when requested.
    Args:
       dest_endpoint: Destination collection UUID, endpoint tag, or machine name.
       dest_path: Destination directory in the selected path namespace.
       source_endpoint: Source collection UUID, endpoint tag, or machine name.
       source_path: Source file or directory in the selected path namespace.
       allow_unsafe : Allow movement to paths outside of the sandbox or user permissions
       block_until_complete : Poll the transfer status every 20s until the transfer is complete before returning
       source_path_type: Namespace of source_path; defaults to 'globus'.
       dest_path_type: Namespace of dest_path; defaults to 'globus'.
    Return:
       The transfer ID as a string

    Notes:
       Filesystem paths require a configured route for the named machine or
       collection; unknown or ambiguous routes raise ValueError. Personal
       endpoint paths must be within the configured staging root in their
       selected namespace. Local staging and file movement are handled by
       uploadFileGlobus and downloadFileGlobus, not by this function.
       Sandbox filesystem roots are translated before checking the destination.
       If the source is a file, its filename is preserved in the destination
       directory. For a directory source, only its contents are copied.
    """
       
    destination_uuid, dest_globus_path = _resolve_transfer_path(dest_endpoint, dest_path, dest_path_type)
    source_uuid, source_globus_path = _resolve_transfer_path(source_endpoint, source_path, source_path_type)
    dest_globus_path = dest_globus_path.rstrip("/") + "/"
    wfapiLog(f"Initiating globus copy from {source_uuid}:{source_globus_path} to {destination_uuid}:{dest_globus_path}")
    machine = dest_endpoint.lower() if dest_endpoint.lower() in MACHINE_IMPLEMENTATIONS else None
    if not allow_unsafe and not checkSafeGlobusPath(destination_uuid, dest_globus_path, machine=machine):
        raise Exception(f"Attempting to copy data to a disallowed Globus location on {dest_endpoint}")

    trans_args = {
        "source_uuid": source_uuid, "source_path": source_globus_path,
        "destination_uuid": destination_uuid, "destination_path": dest_globus_path,
        "label": label or "FemtoMeas transfer",
    }
    response, status = _transfer_post("movement/transfer/globus", data=trans_args)
    if status != 200:
        raise Exception("Globus transfer failed, response content: " + json.dumps(response))
    transfer_id = response["transfer_uuid"]
    if block_until_complete:
        while (status := globusTransferStatus(transfer_id)) == "ACTIVE":
            print(".", end="")
            time.sleep(20)
        print(status)
        if status != "SUCCEEDED":
            raise RuntimeError(f"Globus transfer {transfer_id} finished with status {status}")
    return transfer_id
