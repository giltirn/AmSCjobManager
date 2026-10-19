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
from ..utils import queryYesNo
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
    return str(pathlib.PurePosixPath(staging["staging_directory_globus"]) / relative_path)


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
        safe = _check_globus_destination(remote_endpoint, remote_globus_path, machine=machine)
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


def downloadFileGlobus(machine: str, remote_path: str, local_filesystem_path: str, *,
                       remote_path_type: Literal["filesystem", "globus"] = "filesystem",
                       remote_endpoint: str | None = None) -> str:
    """Download through the personal endpoint, blocking until the file is local.

    remote_path names the source FILE, in the machine filesystem by default.
    Set remote_path_type='globus' for a Globus-visible path; remote_endpoint
    selects its collection UUID/tag if the machine has overlapping namespaces.
    local_filesystem_path is an OS-accessible destination filename or existing
    directory. Staging and renaming are automatic. Returns the local file path.
    """
    staging = getPersonalStagingConfiguration()
    remote_endpoint, remote_globus_path = _file_transfer_location(
        machine, remote_path, remote_path_type, remote_endpoint
    )
    output_filesystem_path = Path(local_filesystem_path).expanduser()
    if output_filesystem_path.is_dir():
        output_filesystem_path /= pathlib.PurePosixPath(remote_path).name
    output_filesystem_path = output_filesystem_path.resolve()
    output_filesystem_path.parent.mkdir(parents=True, exist_ok=True)

    remote_globus_file = pathlib.PurePosixPath(remote_globus_path)
    remote_filename = remote_globus_file.name
    if not remote_filename:
        raise ValueError(f"Remote download path must name a file: {remote_path!r}")

    staged_globus_path = _staging_globus_path(output_filesystem_path, staging)
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


def _check_globus_destination(endpoint: str, destination_globus_path: str, *, machine=None) -> bool:
    """Compare a Globus destination with sandbox roots translated into Globus paths.

    Known collections deny writes outside their configured boundaries. Unknown
    collections retain the explicit permission prompt and need no IRI resource.
    A machine context limits permission to that machine's own sandboxes.
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
    if "personal" in special_globus_endpoints and endpoint_uuid == replaceSpecialGlobusEndpoint("personal"):
        known_endpoint = True
        staging = getPersonalStagingConfiguration()
        if destination.is_relative_to(_absolute_remote_path(staging["staging_directory_globus"])):
            return True
    if known_endpoint:
        return False
    return queryYesNo(f"Do you give permission to write to Globus path {destination} on endpoint {endpoint_uuid}?")


def globusCopy(dest_endpoint: str, dest_globus_path: str,
               source_endpoint: str, source_globus_path: str,
               allow_unsafe=False,
               block_until_complete=False,
               label: str | None = None)-> str:
    """
    Perform a transfer using Globus-visible paths, NEVER machine filesystem paths.
    Args:
       dest_endpoint: Destination collection UUID, endpoint tag, or unambiguous machine tag.
       dest_globus_path: Globus-visible destination DIRECTORY; include its trailing '/'.
       source_endpoint: Source collection UUID, endpoint tag, or unambiguous machine tag.
       source_globus_path: Globus-visible source file or directory.
       allow_unsafe : Allow movement to paths outside of the sandbox or user permissions
       block_until_complete : Poll the transfer status every 20s until the transfer is complete before returning
    Return:
       The transfer ID as a string

    Notes:
       Tags resolve collection identifiers only; paths remain in the Globus
       namespace. Machine tags with multiple matching collections require an
       explicit collection UUID instead. Sandbox filesystem roots are translated
       before comparison; unknown collections require explicit user permission.
       If the source is a file, its filename is preserved in the destination
       directory. For a directory source, only its contents are copied.
    """
       
    dest_globus_path = str(_absolute_remote_path(dest_globus_path)).rstrip("/") + "/"
    source_globus_path = str(_absolute_remote_path(source_globus_path))
    destination_uuid = _resolve_globus_endpoint(dest_endpoint, dest_globus_path)
    source_uuid = _resolve_globus_endpoint(source_endpoint, source_globus_path)
    wfapiLog(f"Initiating globus copy from {source_uuid}:{source_globus_path} to {destination_uuid}:{dest_globus_path}")
    machine = dest_endpoint.lower() if dest_endpoint.lower() in MACHINE_IMPLEMENTATIONS else None
    if not allow_unsafe and not _check_globus_destination(destination_uuid, dest_globus_path, machine=machine):
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
