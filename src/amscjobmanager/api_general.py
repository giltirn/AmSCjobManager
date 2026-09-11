import pathlib
import io
import time
from . import globals
from .logging import wfapiLog

if globals.api_impl == "IRI":
    #Pure IRI
    print("Using IRI API")
    from .iri_api import setupWorkflowAgent, remoteLs, remoteMkdir, uploadBytes, executeBatchJobCompat, remoteChmod, getJobState, cancelJob, queryMachineStatus, globusTransferStatus, getUserAccountProjects, getKnownMachines, getMachineQueues, downloadFile, listSpecialGlobusEndpoints, globusCopy
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

    
def uploadSmallFile(machine: str, remote_path: str, local_path: str, allow_unsafe = False) -> bool:
    """
    Upload a small file to a remote path
    Args:
       machine - The name of the machine. Valid values are 'Perlmutter'
       remote_path - The absolute path on the remote machine
       local_path - The path on the local machine
       allow_unsafe - Allow uploading to directories other than within the sandbox
    Return:
       True if successful, False otherwise
    """
    wfapiLog(f"Uploading small file {local_path} to {machine}:{remote_path}")
    with open(local_path, "rb") as fh:
        return uploadBytes(machine, remote_path, io.BytesIO( fh.read() ) )

def watchJobStatus(machine, jobid, howlong=300, poll_freq=10):
    t0=int(time.time())
    while( int(time.time()) - t0 < howlong  ):
        state = getJobState(machine, jobid)
        print(state)
        if state not in ("new", "queued", "active"):
            print("Detected job completion")
            break    
        time.sleep(poll_freq)
        
