"""IRI filesystem operations and their configured Globus fallbacks."""

import base64
import io
import json
import pathlib
import re
import shutil
from typing import List

from .. import local_api
from ..globals import checkSafePath
from ..logging import wfapiLog
from .config import IRI_IMPLEMENTATIONS, getImplementationAPIVersion
from .state import getMachineImplementation, requireGlobusFallback
from .rest import get, post, put, submitTask


def getMountedFilesystems(machine: str) -> dict[str, dict]:
    """Return named facility filesystem definitions mounted on a machine.

    Rejects unknown filesystem references in the machine configuration. Values
    reference the shared definitions and should be treated as read-only.
    """
    machine = machine.lower()
    implementation = getMachineImplementation(machine)
    implementation_specification = IRI_IMPLEMENTATIONS[implementation]
    filesystems = implementation_specification.get("filesystems", {})
    mounted_names = implementation_specification["machines"][machine].get(
        "mounted_filesystems", []
    )
    unknown_filesystems = set(mounted_names) - filesystems.keys()
    if unknown_filesystems:
        raise RuntimeError(
            f"Machine {machine!r} references unknown filesystem definitions: "
            f"{sorted(unknown_filesystems)}"
        )
    return {name: filesystems[name] for name in mounted_names}


def getStorageResourcePaths(machine: str) -> list[dict]:
    """Expand mounted IRI storage routes into one entry per root alias.

    Entries contain filesystem_root and resource_name for resource selection,
    with an optional authoritative resource_id for discovered routes.
    Unmounted facility filesystems are excluded.
    """
    return [
        {
            **{
                key: value
                for key, value in storage_mapping.items()
                if key != "filesystem_roots"
            },
            "filesystem_root": filesystem_root,
        }
        for filesystem in getMountedFilesystems(machine).values()
        for storage_mapping in filesystem.get("storage_resource_paths", [])
        for filesystem_root in storage_mapping["filesystem_roots"]
    ]


def getUnmountedFilesystemError(
    machine: str, path: pathlib.PurePosixPath, mapping: str
) -> ValueError | None:
    """Return a descriptive ValueError for a known but unmounted path, or None.

    mapping selects storage_resource_paths or globus_filesystems. Checks the
    facility's filesystem definitions against the machine's mounted list.
    """
    if mapping not in {"storage_resource_paths", "globus_filesystems"}:
        raise ValueError(f"Unknown filesystem mapping type: {mapping!r}")
    implementation = getMachineImplementation(machine)
    matching_filesystems = {
        name: filesystem
        for name, filesystem in IRI_IMPLEMENTATIONS[implementation].get("filesystems", {}).items()
        if any(
            path.is_relative_to(pathlib.PurePosixPath(root))
            for route in filesystem.get(mapping, [])
            for root in route["filesystem_roots"]
        )
    }
    mounted_filesystems = getMountedFilesystems(machine)
    unmounted_names = set(matching_filesystems) - set(mounted_filesystems)
    if not unmounted_names:
        return None
    descriptions = "; ".join(
        f"{name!r} ({matching_filesystems[name]['description']})"
        for name in sorted(unmounted_names)
    )
    return ValueError(
        f"Path {path!s} belongs to filesystem(s) not mounted on {machine!r}: "
        f"{descriptions}"
    )

def getFilesystemOperationHandling(machine: str, operation: str) -> str | None:
    """Return a configured filesystem exception: 'globus', 'skip', or None.

    None means the implementation uses the native IRI operation.
    """
    implementation = getMachineImplementation(machine)
    return IRI_IMPLEMENTATIONS[implementation].get(
        "unsupported_filesystem_operations", {}
    ).get(operation)


def _require_filesystem_operation(machine: str, operation: str) -> None:
    if getFilesystemOperationHandling(machine, operation) == "skip":
        implementation = getMachineImplementation(machine)
        raise NotImplementedError(
            f"{operation} is not implemented by the {implementation!r} IRI API"
        )



def remoteLs(machine: str, path: str)-> List[str]:
    """
    Query the contents of a path on a given machine
    Args:
       machine - The name of the machine.
       path - The absolute path
    Return:
       A list of provider filenames, or sorted absolute child paths locally

    Uses GET with query parameters for API v1, or POST with a JSON body for
    API v2. Both versions submit an asynchronous directory-listing task.

    TODO: Explore behavior of trailing slashes, and the fact that the directory name itself seems to be listed among the directory content; should we unify the behavior with SFAPI?
    """
    machine = machine.lower()
    wfapiLog(f"Listing contents of directory {machine}:{path}")
    if machine == "local":
        return local_api.localLs(path)
    
    rid = getResourceID(machine, rtype="storage", path=path)
    version = getImplementationAPIVersion(getMachineImplementation(machine))

    def submit_ls_task() -> str:
        if version == 2:
            response, status = post(machine, f"filesystem/ls/{rid}", data={"path": path})
            if not 200 <= status < 300:
                raise RuntimeError(f"Directory listing submission failed with HTTP {status}: {json.dumps(response)}")
        else:
            response = get(machine, f"filesystem/ls/{rid}", params={"path": path})
        return response["task_id"]

    j = submitTask(machine, submit_ls_task)

    files = [ f['name'] for f in j['result']['output'] ]
    return files





def remoteChmod(machine: str, path : str, mode : str, allow_unsafe = False):
    """Change path permissions on machine, returning None on success.

    mode is a permission string such as '700'. For machine='local', numeric
    and symbolic chmod modes are supported and symlinks are followed. Unless
    allow_unsafe is True, the target must pass the machine's sandbox checks.
    """
    machine = machine.lower()
    wfapiLog(f"Changing permissions of file {machine}:{path} to {mode}")
    if machine == "local":
        return local_api.localChmod(path, mode, allow_unsafe=allow_unsafe)
    
    if not allow_unsafe and not checkSafePath(machine, path):
        raise Exception("Path is not a subdirectory of the sandbox path")

    if not pathlib.Path(path).is_absolute():
        raise Exception("Path must be absolute")

    rid = getResourceID(machine, rtype="storage", path=path)

    def submit_chmod_task() -> str:
        response, _ = put(
            machine, f"filesystem/chmod/{rid}", data={"path": path, "mode": mode}
        )
        return response["task_id"]

    j = submitTask(machine, submit_chmod_task)
    
    if j["status"] != "completed":        
        raise Exception(f"Permission change failed: {json.dumps(j,indent=2)}")        
    


    
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

    def submit_mkdir_task() -> str:
        response, _ = post(
            machine,
            f"filesystem/mkdir/{rid}",
            data={"path": path, "parent": create_parents},
        )
        return response["task_id"]

    j = submitTask(machine, submit_mkdir_task)

    if j["status"] != "completed":
        raise Exception(f"Directory creation failed, response: {json.dumps(j, indent=2)}")


def pathStat(machine: str, remote_path: str, dereference=True)->str:
    """
    Run stat on the provided path
    Args:
       machine - The name of the machine.
       remote_path - The absolute path on the remote machine
       dereference - Inspect a symlink's target when True, or the link itself
    Return:
       Provider stat output, or the local stat command's output, as a string
    """
    machine = machine.lower()    

    if machine == "local":
        return local_api.localPathStat(remote_path, dereference=dereference)

    _require_filesystem_operation(machine, "pathStat")
       
    if not pathlib.Path(remote_path).is_absolute():
        raise Exception("Path must be absolute")
    
    rid = getResourceID(machine, rtype="storage", path=remote_path)

    def submit_stat_task() -> str:
        response = get(
            machine,
            f"filesystem/stat/{rid}",
            params={"path": remote_path, "dereference": dereference},
        )
        return response["task_id"]

    j = submitTask(machine, submit_stat_task)

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

    def submit_file_task() -> str:
        response = get(machine, f"filesystem/file/{rid}", params={"path": remote_path})
        return response["task_id"]

    j = submitTask(machine, submit_file_task)

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

    if getFilesystemOperationHandling(machine, "uploadBytes") == "globus":
        requireGlobusFallback(machine, "uploadBytes")
        remote_filename = pathlib.PurePosixPath(remote_path).name
        if not remote_filename:
            raise ValueError(f"Remote upload path must name a file: {remote_path!r}")
        temporary_directory, _ = createGlobusStagingDirectory()
        staging_file = temporary_directory / remote_filename
        try:
            with staging_file.open("wb") as file_handle:
                file_handle.write(content.getbuffer())
            return uploadFileGlobus(machine, remote_path, str(staging_file))
        finally:
            shutil.rmtree(temporary_directory, ignore_errors=True)
   
    rid = getResourceID(machine, rtype="storage", path=remote_path)

    def submit_upload_task() -> str:
        content.seek(0)
        response, _ = post(
            machine,
            f"filesystem/upload/{rid}",
            params={"path": remote_path},
            files={"file": content},
        )
        return response["task_id"]

    j = submitTask(machine, submit_upload_task)

    if j["status"] != "completed":
        raise Exception(f"Upload failed, status: {j["status"]},  response: {json.dumps(j,indent=2)}")        


def downloadFileContents(machine: str, remote_path: str)->str:
    """
    Download a (small) remote file. Returns the file contents as a string (base64 or plaintext encoded, depending on the file)
    Args:
       machine - The name of the machine.
       remote_path - The absolute path on the remote machine

    machine='local' reads directly from the manager host. Local and Globus
    fallback reads return UTF-8 plaintext for files identified as text or empty,
    and an ASCII base64 string otherwise. Reads are not sandbox-restricted.
    """
    if "~" in remote_path:
        raise ValueError("Paths containing '~' are not supported; use an explicit path")
    machine = machine.lower()

    if machine == "local":
        local_path = local_api.getAbsoluteLocalPath(remote_path)
        contents = local_api.readFileBinary(local_path)
        file_description = local_api.localPathType(local_path).lower()
        if "text" in file_description or "empty" in file_description:
            try:
                return contents.decode("utf-8")
            except UnicodeDecodeError:
                pass
        return base64.b64encode(contents).decode("ascii")

    _require_filesystem_operation(machine, "downloadFileContents")

    if getFilesystemOperationHandling(machine, "downloadFileContents") == "globus":
        requireGlobusFallback(machine, "downloadFileContents")
        remote_filename = pathlib.PurePosixPath(remote_path).name
        if not remote_filename:
            raise ValueError(f"Remote download path must name a file: {remote_path!r}")
        temporary_directory, _ = createGlobusStagingDirectory()
        staging_file = temporary_directory / remote_filename
        try:
            downloadFileGlobus(machine, str(staging_file), remote_path)
            return downloadFileContents("local", str(staging_file))
        finally:
            shutil.rmtree(temporary_directory, ignore_errors=True)

    wfapiLog(f"Downloading file contents {machine}:{remote_path}")    

    if not pathlib.Path(remote_path).is_absolute():
        raise Exception("Path must be absolute")
    
    rid = getResourceID(machine, rtype="storage", path=remote_path)

    def submit_download_task() -> str:
        response = get(
            machine, f"filesystem/download/{rid}", params={"path": remote_path}
        )
        return response["task_id"]

    j = submitTask(machine, submit_download_task)
    
    if j["status"] == "completed":
        return j["result"]["output"]
    else:
        raise Exception(f"Download failed, response: { json.dumps(j,indent=2)}")        

def downloadFile(machine: str, local_path_out: str, remote_path_in: str, *,
                 allow_unsafe=False)->str:
    """Download a source file to a local filename or existing directory.

    Check the resolved output against local sandboxes and /tmp before reading
    or copying data, unless allow_unsafe=True. A Globus fallback also permits
    its configured staging directory. Return the final local filename.
    """
    from ..api_general import isDirectory

    if machine.lower() != "local" and getFilesystemOperationHandling(
        machine, "downloadFile"
    ) == "globus":
        requireGlobusFallback(machine, "downloadFile")
        return downloadFileGlobus(
            machine, local_path_out, remote_path_in, allow_unsafe=allow_unsafe
        )

    if "~" in remote_path_in:
        raise ValueError("Paths containing '~' are not supported; use an explicit path")
    output_path = pathlib.Path(local_api.getAbsoluteLocalPath(local_path_out))
    if isDirectory("local", str(output_path), check_exists=True):
        output_path /= pathlib.PurePosixPath(remote_path_in).name
    local_path_out = str(output_path.resolve())
    if not allow_unsafe and not checkSafePath("local", local_path_out):
        raise ValueError("Local download destination is outside the sandbox directories and /tmp")

    if machine.lower() == "local":
        wfapiLog(f"Copying local file {remote_path_in} to {local_path_out}")    
        local_api.localCopyFile(local_path_out, remote_path_in, allow_unsafe=allow_unsafe)
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


# Resolve cross-module operations after the routing helpers above exist.
from .resources import getResourceID
from .globus import (
    createGlobusStagingDirectory,
    downloadFileGlobus,
    uploadFileGlobus,
)
