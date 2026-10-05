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
    return globals.remote_workdir

tid = 0
transfers = { }
transfer_labels = { }
transfer_details = { }
compute_jobs = { }
compute_job_names = { }
compute_job_details = { }
spoof_action_history = []
_spoof_completion_delay = None

def resetSpoofActionState(completion_delay=None):
    """Reset SPOOF action state and make subsequent actions deterministic.

    This is intended for workflow tests which need to inspect submission and
    completion ordering without waiting for randomized fake action durations.
    """
    global tid, jid, _spoof_completion_delay
    tid = 0
    jid = 0
    _spoof_completion_delay = completion_delay
    transfers.clear()
    transfer_labels.clear()
    transfer_details.clear()
    compute_jobs.clear()
    compute_job_names.clear()
    compute_job_details.clear()
    spoof_action_history.clear()

def getSpoofActionHistory():
    """Return the ordered remote action events recorded by the SPOOF API."""
    return list(spoof_action_history)

def _recordSpoofActionEvent(event, action_type, api_key, name=None, details=None):
    spoof_action_history.append({
        "sequence": len(spoof_action_history),
        "event": event,
        "action_type": action_type,
        "api_key": api_key,
        "name": name,
        "details": details,
    })

def _fakeGlobusCopy(label=None, details=None):
    #Assign the transfer a fake active time
    active_time = (
        random.randint(3, 8)
        if _spoof_completion_delay is None
        else _spoof_completion_delay
    )

    #Generate a unique_key
    global tid
    key = f"transfer_{tid}"
    tid +=1

    transfers[key] = timemodule.time() + active_time
    transfer_labels[key] = label
    transfer_details[key] = details
    _recordSpoofActionEvent("submitted", "transfer", key, label, details)
    wfapiLog("Fake transfer",key,"time",active_time)
    
    return key

def globusCopy(dest_uuid: str, dest_path: str,
               source_uuid: str, source_path: str,
               allow_unsafe=False,
               block_until_complete=False,
               label: str | None = None)-> str:
    wfapiLog(f"Initiating globus copy from {source_uuid}:{source_path} to {dest_uuid}:{dest_path} to ")
    return _fakeGlobusCopy(label, {
        "source_endpoint": source_uuid,
        "source_path": source_path,
        "dest_endpoint": dest_uuid,
        "dest_path": dest_path,
    })
   
def globusTransferStatus(transfer_id):
    if timemodule.time() >= transfers[transfer_id]:
        status = "SUCCEEDED"
    else:
        status = "ACTIVE"
    if status == "SUCCEEDED" and not any(
        event["event"] == "completed" and event["api_key"] == transfer_id
        for event in spoof_action_history
    ):
        _recordSpoofActionEvent(
            "completed", "transfer", transfer_id, transfer_labels[transfer_id],
            transfer_details[transfer_id],
        )
    return status

def findGlobusTransfersByLabel(label: str) -> str | None:
    matches = [transfer_id for transfer_id, task_label in transfer_labels.items() if task_label == label]
    if len(matches) > 1:
        raise RuntimeError(f"Multiple fake Globus transfers use submission label {label!r}")
    return None if not matches else matches[0]

def findGlobusTransferByLabel(label: str) -> str | None:
    """Compatibility wrapper for the singular form of the lookup helper."""
    return findGlobusTransfersByLabel(label)
    
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

def executeBatchJobCompat(machine: str, script_body: str,
                    nodes : int, ranks_per_node : int, gpus_per_rank : int,
                    time : str, queue : str, account : str,
                    job_run_dir : str, exclusive=True, allow_unsafe=False,
                    name: str | None = None) -> str:
    wfapiLog(f"Executing batch job on machine {machine} with nodes:{nodes}, ranks/node:{ranks_per_node}, gpus/rank:{gpus_per_rank}, time:{time}, queue:{queue}, account:{account}")
   
    #Assign the job a fake active time
    active_time = (
        random.randint(3, 8)
        if _spoof_completion_delay is None
        else _spoof_completion_delay
    )

    #Generate a unique_key
    global jid
    key = f"compute_{jid}"
    jid +=1

    compute_jobs[key] = timemodule.time() + active_time
    compute_job_names[key] = name
    compute_job_details[key] = {
        "machine": machine,
        "script_body": script_body,
        "job_run_dir": job_run_dir,
    }
    _recordSpoofActionEvent(
        "submitted", "compute", key, name, compute_job_details[key]
    )
    wfapiLog("Fake compute",key,"time",active_time)
    
    return key

def getJobState(machine: str, jobid: str) -> str:    
    if timemodule.time() >= compute_jobs[jobid]:
        status = "completed"
    else:
        status = "active"
    if status == "completed" and not any(
        event["event"] == "completed" and event["api_key"] == jobid
        for event in spoof_action_history
    ):
        _recordSpoofActionEvent(
            "completed", "compute", jobid, compute_job_names[jobid],
            compute_job_details[jobid],
        )
    wfapiLog(f"Queried job state {machine}:{jobid}, got {status}")
    return status

def findJobByName(machine: str, name: str) -> str | None:
    for job_id, job_name in compute_job_names.items():
        if job_name == name:
            return job_id
    return None

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
