import random
import time as timemodule
import io
from . import globals
from .logging import wfapiLog
from typing import Literal, Union, List, Optional, Tuple

def listSpecialGlobusEndpoints():
    return ["fake_endpoint1","fake_endpoint2"]

def setupWorkflowAgent(iriapi_key_path : str, iriapi_transfer_key_path : str, work_dir : dict):
    globals.remote_workdir={ machine.lower() : directory for machine, directory in work_dir.items() }
    wfapiLog("Using SPOOF api with workdir", globals.remote_workdir)

tid = 0
transfers = { }

def _fakeGlobusCopy():
    #Assign the transfer a fake active time
    active_time = random.randint(3,8)

    #Generate a unique_key
    global tid
    key = f"transfer_{tid}"
    tid +=1

    transfers[key] = timemodule.time() + active_time
    wfapiLog("Fake transfer",key,"time",active_time)
    
    return key

def globusCopy(dest_uuid: str, dest_path: str,
               source_uuid: str, source_path: str,
               allow_unsafe=False,
               block_until_complete=False)-> str: 
    wfapiLog(f"Initiating globus copy from {source_uuid}:{source_path} to {dest_uuid}:{dest_path} to ")
    return _fakeGlobusCopy()
   
def globusTransferStatus(transfer_id):
    if timemodule.time() >= transfers[transfer_id]:
        return "SUCCEEDED"
    else:
        return "ACTIVE"
    
def remoteMkdir(machine: str, path: str, create_parents = True, allow_unsafe = False)-> int:
    wfapiLog(f"Creating directory {machine}:{path}")
    return 1

def uploadBytes(machine: str, remote_path: str, content: io.BytesIO, allow_unsafe = False) -> bool:
    wfapiLog(f"Uploading binary data to {machine}:{remote_path}")
    return True

def queryMachineStatus(machine: str, rtype="compute")-> bool:
    return True

def getKnownMachines():
    return list(globals.remote_workdir.keys()) + ["local"]

def getUserAccountProjects(machine):
    if machine not in getKnownMachines():
        raise Exception(f"Invalid machine: {machine}")
    
    return ["my_proj1", "my_proj2"]

def getMachineQueues(machine)->List[ Tuple[str,str] ]:
    """
    Provide a list of queues and associated information for a given machine
    
    Return: a list of string tuples, with the first tuple entry being the queue name and the second relevant information about the queue
    """
    return [ ("debug", "max runtime 0.5 hours, max nodes 8"), ("regular", "use for regular production jobs or those that are unsuitable for debug") ]


jid=0
compute_jobs = { }

def executeBatchJobCompat(machine: str, script_body: str,
                    nodes : int, ranks_per_node : int, gpus_per_rank : int,
                    time : str, queue : str, account : str,
                    job_run_dir : str, exclusive=True, allow_unsafe=False) -> str:
    wfapiLog(f"Executing batch job on machine {machine} with nodes:{nodes}, ranks/node:{ranks_per_node}, gpus/rank:{gpus_per_rank}, time:{time}, queue:{queue}, account:{account}")
   
    #Assign the job a fake active time
    active_time = random.randint(3,8)

    #Generate a unique_key
    global jid
    key = f"compute_{jid}"
    jid +=1

    compute_jobs[key] = timemodule.time() + active_time
    wfapiLog("Fake compute",key,"time",active_time)
    
    return key

def getJobState(machine: str, jobid: str) -> str:    
    if timemodule.time() >= compute_jobs[jobid]:
        status = "completed"
    else:
        status = "active"
    wfapiLog(f"Queried job state {machine}:{jobid}, got {status}")
    return status

def downloadFile(machine: str, remote_path: str)->str:
    wfapiLog(f"Downloading file {machine}:{remote_path}")
    return "FAKE CONTENTS"

def remoteRun(machine: str, args : str | List[str] ):
    cmd = "bash -c \""
    if isinstance(args, list):
        for c in args:
            cmd = cmd + c + ";"
    else:
        cmd = cmd + args
    cmd = cmd + "\""

    wfapiLog(f"Fake executing command {cmd} on machine {machine}")
