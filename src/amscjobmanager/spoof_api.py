import random
import time as timemodule
import io
import shlex
from pathlib import Path, PurePosixPath
from . import globals
from .logging import wfapiLog
from typing import Literal, Union, List, Optional, Tuple

def listSpecialGlobusEndpoints():
    return ["fake_endpoint1","fake_endpoint2"]

def getUsername(machine: str | None = None, *, implementation: str | None = None) -> str:
    """Return a simulated remote username, or run whoami for the local machine."""
    if machine is not None and machine.lower() == "local":
        if implementation is not None:
            raise ValueError("Cannot specify an IRI implementation for machine='local'")
        from .local_api import localUsername
        return localUsername()
    if machine is None and implementation is None:
        raise ValueError("Specify machine or implementation")
    return "spoof_user"

def setupWorkflowAgentFromConfig(config) -> dict:
    """Initialize the spoof agent from a manager configuration and return sandboxes."""
    return setupWorkflowAgent(
        config.iri_implementations,
        config.transferapi_key_path,
        config.sandbox_directories,
        config.globus_personal_endpoint,
        iri_config_cache_directory=config.iri_config_cache_directory,
    )

def setupWorkflowAgent(iri_implementations: dict, iriapi_transfer_key_path: str,
                       work_dir: dict, globus_personal_endpoint_config=None,
                       iri_config_cache_directory: str | None = None):
    from .iri_api.state import _configure_personal_globus_endpoint
    _configure_personal_globus_endpoint(globus_personal_endpoint_config)
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
        random.randint(1, 3)
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

def globusCopy(dest_endpoint: str, dest_path: str,
               source_endpoint: str, source_path: str,
               allow_unsafe=False,
               block_until_complete=False,
               label: str | None = None, *,
               source_path_type: Literal["filesystem", "globus"] = "globus",
               dest_path_type: Literal["filesystem", "globus"] = "globus") -> str:
    """Simulate a transfer, translating configured filesystem paths if selected.

    Endpoint arguments may be UUIDs or tags. SPOOF records the resolved request
    but does not enforce sandbox bounds.
    dest_path names a directory; source_path names a file or directory.
    """
    from .iri_api.state import special_globus_endpoints
    personal_uuid = special_globus_endpoints.get("personal", "").lower()
    personal_endpoints = {"personal", personal_uuid} if personal_uuid else {"personal"}
    if (source_path_type != "globus" or dest_path_type != "globus"
            or source_endpoint.lower() in personal_endpoints or dest_endpoint.lower() in personal_endpoints):
        from .iri_api.globus import _resolve_transfer_path
        dest_endpoint, dest_path = _resolve_transfer_path(dest_endpoint, dest_path, dest_path_type)
        source_endpoint, source_path = _resolve_transfer_path(source_endpoint, source_path, source_path_type)
    wfapiLog(f"Initiating globus copy from {source_endpoint}:{source_path} to {dest_endpoint}:{dest_path}")
    return _fakeGlobusCopy(label, {
        "source_endpoint": source_endpoint,
        "source_path": source_path,
        "dest_endpoint": dest_endpoint,
        "dest_path": dest_path,
    })

def uploadFileGlobus(machine: str, remote_path: str, local_filesystem_path: str, *,
                     remote_path_type: Literal["filesystem", "globus"] = "filesystem",
                     remote_endpoint: str | None = None) -> str:
    """Match IRI's selectable remote namespace; file staging is not spoofed."""
    raise NotImplementedError("Globus file staging is unavailable with the SPOOF API")

def downloadFileGlobus(machine: str, remote_path: str, local_filesystem_path: str, *,
                       remote_path_type: Literal["filesystem", "globus"] = "filesystem",
                       remote_endpoint: str | None = None) -> str:
    """Match IRI's selectable remote namespace; file staging is not spoofed."""
    raise NotImplementedError("Globus file staging is unavailable with the SPOOF API")
   
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

def remoteLs(machine: str, path: str) -> List[str]:
    """Return fixed illustrative filenames, regardless of earlier filesystem calls."""
    wfapiLog(f"Listing fake directory {machine}:{path}")
    return [str(PurePosixPath(path) / name) for name in
            ("calculate.py", "settings.json", "result.txt", "run.log", "err.log")]

def remoteChmod(machine: str, path: str, mode: str, allow_unsafe=False):
    """Log a fake permission change without modifying any files."""
    wfapiLog(f"Changing fake permissions of {machine}:{path} to {mode}")

def pathStat(machine: str, remote_path: str, dereference=True) -> str:
    """Return illustrative stat output without inspecting a remote filesystem."""
    wfapiLog(f"Inspecting fake file metadata {machine}:{remote_path}")
    return f"File: {remote_path}\nSize: 128\nAccess: (0644/-rw-r--r--)\nUid: 1000\nGid: 1000"

def pathType(machine: str, remote_path: str) -> str:
    """Guess a fake remote type by suffix; use the real file command locally.

    Remote paths without a filename suffix are treated as directories. This
    deliberately does not track existence or represent actual file contents.
    """
    if machine.lower() == "local":
        from .local_api import localPathType
        return localPathType(remote_path)
    wfapiLog(f"Identifying fake path {machine}:{remote_path}")
    kind = "ASCII text" if PurePosixPath(remote_path).suffix else "directory"
    return f"{remote_path}: {kind}"

def downloadFileContents(machine: str, remote_path: str) -> str:
    """Return placeholder contents, not data produced by the simulated compute job."""
    wfapiLog(f"Downloading fake file contents {machine}:{remote_path}")
    return "FAKE CONTENTS\n"

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


def getProjectAllocation(machine: str, project_name: str) -> List[dict]:
    if project_name not in getUserAccountProjects(machine):
        raise Exception(f"Invalid project {project_name!r} for machine {machine!r}")
    return [{"allocation": 100000, "usage": 0, "unit": "node_hours"}]

def getMachineQueues(machine)->List[ Tuple[str,str] ]:
    """
    Provide a list of queues and associated information for a given machine
    
    Return: a list of string tuples, with the first tuple entry being the queue name and the second relevant information about the queue
    """
    return [ ("debug", "max runtime 0.5 hours, max nodes 8"), ("regular", "use for regular production jobs or those that are unsuitable for debug") ]


jid=0

def executeJob(machine: str, executable: str, arguments: List[str], nodes: int, ranks_per_node: int,
               gpus_per_rank: int, time: str, queue: str, account: str,
               job_run_dir: str, exclusive=True, allow_unsafe=False,
               name: str | None = None, *, pre_launch: str | None = None,
               post_launch: str | None = None) -> str:
    """Spoof the native IRI pre-launch/executable/post-launch workflow."""
    script_body = "\n".join(
        command for command in (
            pre_launch, shlex.join([executable, *arguments]), post_launch
        ) if command
    )
    return executeBatchJobCompat(
        machine, script_body, nodes, ranks_per_node, gpus_per_rank, time,
        queue, account, job_run_dir, exclusive=exclusive,
        allow_unsafe=allow_unsafe, name=name,
    )

def executeBatchJobCompat(machine: str, script_body: str,
                    nodes : int, ranks_per_node : int, gpus_per_rank : int,
                    time : str, queue : str, account : str,
                    job_run_dir : str, exclusive=True, allow_unsafe=False,
                    name: str | None = None) -> str:
    wfapiLog(f"Executing batch job on machine {machine} with nodes:{nodes}, ranks/node:{ranks_per_node}, gpus/rank:{gpus_per_rank}, time:{time}, queue:{queue}, account:{account}")
   
    #Assign the job a fake active time
    active_time = (
        random.randint(1, 3)
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

def downloadFile(machine: str, local_path_out: str, remote_path_in: str) -> str:
    """Write placeholder contents locally using the same signature as the IRI API."""
    output_path = Path(local_path_out).expanduser()
    if output_path.is_dir():
        output_path /= PurePosixPath(remote_path_in).name
    output_path.write_text(downloadFileContents(machine, remote_path_in), encoding="utf-8")
    return str(output_path)

def remoteRun(machine: str, args : str | List[str] ):
    cmd = "bash -c \""
    if isinstance(args, list):
        for c in args:
            cmd = cmd + c + ";"
    else:
        cmd = cmd + args
    cmd = cmd + "\""

    wfapiLog(f"Fake executing command {cmd} on machine {machine}")
