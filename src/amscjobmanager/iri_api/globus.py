"""Globus transfer operations, endpoint routing, and local staging."""

import json
import pathlib
import shutil
import time
import uuid
from pathlib import Path

import globus_sdk

from .. import globals as manager_globals
from ..globals import checkSafePath
from ..logging import wfapiLog
from ..utils import queryYesNo
from .config import TRANSFER_API_BASE
from .filesystem import getMountedFilesystems, getUnmountedFilesystemError
from .state import (
    getPersonalStagingConfiguration,
    getTransferToken,
    iri_transfer_client,
    special_globus_endpoints,
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


def getRemoteGlobusLocation(machine: str, remote_path: str) -> tuple[str, str]:
    """Translate an absolute machine path to a Globus endpoint tag/UUID and path.

    Uses the longest matching root alias among mounted filesystems. Raises
    ValueError if the path is relative, unmounted, or has no configured route.
    """
    path = pathlib.PurePosixPath(remote_path)
    if not path.is_absolute():
        raise ValueError(f"Remote Globus path must be absolute: {remote_path!r}")

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


def _staging_globus_path(local_path: Path, staging: dict[str, str]) -> str | None:
    staging_local = Path(staging["staging_directory_local"]).expanduser().resolve()
    try:
        relative_path = local_path.expanduser().resolve().relative_to(staging_local)
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


def uploadFileGlobus(machine: str, remote_path: str, local_path: str) -> str:
    """Blocking upload of one local file through the configured personal endpoint."""
    source_path = Path(local_path).expanduser().resolve()
    if not source_path.is_file():
        raise ValueError(f"Local upload source is not a file: {local_path!r}")
    if not checkSafePath(machine, remote_path):
        raise Exception(f"Attempting to copy data to a disallowed location on {machine}")

    staging = getPersonalStagingConfiguration()
    remote_endpoint, remote_globus_path = getRemoteGlobusLocation(machine, remote_path)
    remote_globus_file = pathlib.PurePosixPath(remote_globus_path)
    remote_filename = remote_globus_file.name
    if not remote_filename:
        raise ValueError(f"Remote upload path must name a file: {remote_path!r}")

    staged_path = _staging_globus_path(source_path, staging)
    temporary_directory = None
    if staged_path is None or source_path.name != remote_filename:
        temporary_directory, staging_directory = createGlobusStagingDirectory()
        staged_path = str(pathlib.PurePosixPath(staging_directory) / remote_filename)
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
    staging = getPersonalStagingConfiguration()
    output_path = Path(local_path).expanduser()
    if output_path.is_dir():
        output_path /= pathlib.PurePosixPath(remote_path).name
    output_path = output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    remote_endpoint, remote_globus_path = getRemoteGlobusLocation(machine, remote_path)
    remote_globus_file = pathlib.PurePosixPath(remote_globus_path)
    remote_filename = remote_globus_file.name
    if not remote_filename:
        raise ValueError(f"Remote download path must name a file: {remote_path!r}")

    staged_path = _staging_globus_path(output_path, staging)
    temporary_directory = None
    if staged_path is not None and output_path.name == remote_filename:
        staging_destination = str(pathlib.PurePosixPath(staged_path).parent)
    else:
        temporary_directory, staging_destination = createGlobusStagingDirectory()

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

    trans_args = {
        "source_uuid": source_uuid, "source_path": source_path,
        "destination_uuid": dest_uuid, "destination_path": dest_path,
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
