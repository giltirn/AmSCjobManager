import pathlib
import io
import time
from . import globals
from .logging import wfapiLog

if globals.api_impl == "IRI":
    #Pure IRI
    print("Using IRI API")
    from .iri_api import setupWorkflowAgent, remoteLs, remoteMkdir, uploadBytes, executeBatchJobCompat, remoteChmod, getJobState, cancelJob, queryMachineStatus, globusTransferStatus, getUserAccountProjects, getKnownMachines, getMachineQueues, downloadFile, listSpecialGlobusEndpoints, globusCopy, pathType
elif globals.api_impl == "SPOOF":
    print("Using Spoof API")
    from .spoof_api import setupWorkflowAgent, remoteMkdir, uploadBytes, executeBatchJobCompat, getJobState, globusTransferStatus, queryMachineStatus, getUserAccountProjects, getKnownMachines, getMachineQueues, remoteRun, downloadFile, listSpecialGlobusEndpoints, globusCopy
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

    
def uploadSmallFile(machine: str, remote_path: str, local_path: str, allow_unsafe = False, definitely_is_file = False) -> bool:
    """
    Upload a small file to a remote path
    Args:
       machine - The name of the machine. Valid values are 'Perlmutter'
       remote_path - The absolute path on the remote machine
       local_path - The path on the local machine
       allow_unsafe - Allow uploading to directories other than within the sandbox
       definitely_is_file - Assert that the path is an absolute file path, not a directory. Use to skip checking the type of the remote path.
    Return:
       True if successful, False otherwise
    """

    if not pathlib.Path(remote_path).is_absolute():
        raise Exception("Path must be absolute")

    #uploadBytes needs a full filename, not just a directory
    if not definitely_is_file:
        pt = pathType(machine, remote_path)
        if "directory" in pt and "cannot open" not in pt:
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
        
