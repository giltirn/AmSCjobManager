import pathlib
import io
import time
from . import globals
from .logging import wfapiLog

if globals.api_impl == "IRI":
    #Pure IRI
    print("Using IRI API")
    from .iri_api import setupWorkflowAgentFromConfig
    from .iri_api import getUsername
    from .iri_api import pathStat, downloadFileContents
    from .iri_api import setupWorkflowAgent, remoteLs, remoteMkdir, uploadBytes, uploadFileGlobus, executeJob, executeBatchJobCompat, remoteChmod, getJobState, findJobByName, cancelJob, cancelGlobusTransfer, queryMachineStatus, globusTransferStatus, findGlobusTransferByLabel, findGlobusTransfersByLabel, getUserAccountProjects, getProjectAllocation, getKnownMachines, getMachineQueues, downloadFile, downloadFileGlobus, listSpecialGlobusEndpoints, globusCopy, pathType
elif globals.api_impl == "SPOOF":
    print("Using Spoof API")
    from .spoof_api import setupWorkflowAgentFromConfig
    from .spoof_api import getUsername
    from .spoof_api import remoteLs, remoteChmod, pathStat, pathType, downloadFileContents
    from .spoof_api import setupWorkflowAgent, remoteMkdir, uploadBytes, uploadFileGlobus, executeJob, executeBatchJobCompat, getJobState, findJobByName, cancelJob, cancelGlobusTransfer, globusTransferStatus, findGlobusTransferByLabel, findGlobusTransfersByLabel, queryMachineStatus, getUserAccountProjects, getProjectAllocation, getKnownMachines, getMachineQueues, remoteRun, downloadFile, downloadFileGlobus, listSpecialGlobusEndpoints, globusCopy
else:
    raise Exception("Unknown API implementation")


def testExecutablePrivileges(machine: str)-> bool:
    try:
        ret = remoteRun(machine, ["echo","'TEST'"])
        if ret.strip() == "TEST":
            return True
        else:
            return False
    except Exception as e:
        return False    

def isDirectory(machine: str, remote_path: str, *, check_exists: bool = False)->bool:
    """Return whether a path names a directory on the selected machine.

    machine selects a configured remote machine or 'local'. remote_path must
    be absolute remotely; relative local paths are resolved on the manager host.
    With check_exists=False, a trailing '/' is accepted without querying its
    existence. Set check_exists=True to query even those paths. Local queries
    inspect filesystem metadata and follow symlinks; remote queries use pathType
    and may incur an API call. Query errors propagate to the caller.
    """
    machine = machine.lower()
    if "~" in remote_path:
        raise ValueError("Paths containing '~' are not supported; use an explicit path")
    if not pathlib.Path(remote_path).is_absolute():
        if machine == "local":
            remote_path = str(pathlib.Path(remote_path).resolve())
        else:
            raise Exception("Path must be absolute")

    if remote_path.endswith('/') and not check_exists:
        return True

    if machine == "local":
        return pathlib.Path(remote_path).is_dir()

    pt = pathType(machine, remote_path)
    return "directory" in pt and "cannot open" not in pt

def pathExists(machine: str, remote_path: str)->bool:
    if "~" in remote_path:
        raise ValueError("Paths containing '~' are not supported; use an explicit path")
    if not pathlib.Path(remote_path).is_absolute():
        raise Exception("Path must be absolute")

    pt = pathType(machine, remote_path)
    return "cannot open" not in pt
    
def uploadSmallFile(machine: str, remote_path: str, local_path: str, allow_unsafe = False, definitely_is_file = False):
    """
    Upload a small file to a remote path
    Args:
       machine - The name of the machine. Valid values are 'Perlmutter'
       remote_path - The absolute path on the remote machine
       local_path - The path on the local machine
       allow_unsafe - Allow uploading to directories other than within the sandbox
       definitely_is_file - Assert that the remote path is an absolute file path, not a directory. Use to skip checking the type of the remote path.
    """

    if "~" in remote_path or "~" in str(local_path):
        raise ValueError("Paths containing '~' are not supported; use an explicit path")
    if not pathlib.Path(remote_path).is_absolute():
        raise Exception("Path must be absolute")

    #uploadBytes needs a full filename, not just a directory
    if not definitely_is_file:
        if isDirectory(machine, remote_path):        
            fname = pathlib.Path(local_path).name
            remote_path += "/" + fname

    wfapiLog(f"Uploading small file {local_path} to {machine}:{remote_path}")
    with open(local_path, "rb") as fh:
        return uploadBytes(machine, remote_path, io.BytesIO( fh.read() ), allow_unsafe=allow_unsafe )


def watchJobStatus(machine, jobid, howlong=300, poll_freq=10):
    t0=int(time.time())
    while( int(time.time()) - t0 < howlong  ):
        state = getJobState(machine, jobid)
        print(state)
        if state not in ("new", "queued", "active"):
            print("Detected job completion")
            break    
        time.sleep(poll_freq)
        
