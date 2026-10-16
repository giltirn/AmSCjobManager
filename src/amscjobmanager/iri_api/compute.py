"""IRI compute submission and job management."""

import base64
import json
import shlex
import uuid
from typing import List

from .. import local_api
from ..globals import checkSafePath
from ..logging import wfapiLog
from .config import IRI_IMPLEMENTATIONS
from .resources import getResourceID
from .rest import delete, get, post
from .state import getMachineImplementation

def _alcf_post_launch_wrapper(
    job_run_dir: str,
    barrier_dir: str,
    executable: str,
    arguments: List[str],
    post_launch: str,
    ranks: int,
) -> str:
    """Build the per-rank wrapper used for ALCF post-launch processing.

    The ALCF IRI service does not return control to the head-node process after
    ``mpiexec``.  The wrapper therefore runs the requested executable on every
    rank, synchronizes through the shared filesystem, then has rank zero run
    the post-launch commands.
    """
    launch_command = shlex.join([executable, *arguments])
    quoted_job_dir = shlex.quote(job_run_dir)
    quoted_barrier_dir = shlex.quote(barrier_dir)
    return f"""#!/bin/bash
cd {quoted_job_dir} || exit 125

barrier_dir={quoted_barrier_dir}
expected_ranks={ranks}

barrier() {{
    local count
    (
        flock -x 9 || exit 1
        count=$(cat "$barrier_dir/count") || exit 1
        [[ "$count" =~ ^[0-9]+$ ]] || exit 1
        printf '%s\\n' "$((count + 1))" > "$barrier_dir/count"
    ) 9>"$barrier_dir/lock" || return 1

    while true; do
        count=$( (
            flock -s 9 || exit 1
            cat "$barrier_dir/count"
        ) 9>"$barrier_dir/lock" ) || return 1
        [[ "$count" =~ ^[0-9]+$ ]] || return 1
        (( count >= expected_ranks )) && return 0
        sleep 0.1
    done
}}

{launch_command}
application_status=$?
barrier || exit 125

# Aurora exposes PALS_RANKID.  The fallbacks allow the same wrapper to work
# with common PMI/PMIx launch environments on other ALCF systems.
rank="${{PALS_RANKID:-${{PMI_RANK:-${{PMIX_RANK:-}}}}}}"
[[ -n "$rank" ]] || {{
    echo "Unable to identify the MPI rank for ALCF post-launch" >&2
    exit 125
}}
if [[ "$rank" == "0" ]]; then
    (
{post_launch}
    )
    post_launch_status=$?
    if (( application_status != 0 )); then
        exit "$application_status"
    fi
    exit "$post_launch_status"
fi

exit "$application_status"
"""
    
def executeJob(machine: str, executable: str, arguments: List[str], nodes: int, ranks_per_node: int,
               gpus_per_rank: int, time: str, queue: str, account: str,
               job_run_dir: str, exclusive=True, allow_unsafe=False,
               name: str | None = None, *, pre_launch: str | None = None,
               post_launch: str | None = None) -> str:
    """Submit a compute job using the IRI pre-launch/executable/post-launch API.

    On implementations that support native hooks, ``pre_launch`` and
    ``post_launch`` run on the login/head node before and after the launcher
    runs ``executable``.  ALCF runs ``pre_launch`` on the head node, then uses
    a shared-filesystem barrier to run ``post_launch`` on MPI rank zero after
    every rank has completed the executable.
    """
    machine = machine.lower()
    if machine == "local":
        script_body = "\n".join(
            command for command in (
                pre_launch, shlex.join([executable, *arguments]), post_launch
            )
            if command
        )
        return local_api.executeJobScript(
            script_body, job_run_dir, allow_unsafe, name=name
        )

    wfapiLog(
        f"Executing job on machine {machine} with nodes:{nodes}, "
        f"ranks/node:{ranks_per_node}, gpus/rank:{gpus_per_rank}, "
        f"time:{time}, queue:{queue}, account:{account}"
    )
    if not allow_unsafe and not checkSafePath(machine, job_run_dir):
        raise Exception("Path is not below the privileged directory")

    implementation = getMachineImplementation(machine)
    machine_specification = IRI_IMPLEMENTATIONS[implementation]["machines"][machine]
    if implementation == "alcf":
        ranks = nodes * ranks_per_node
        if post_launch is None:
            launch_command = shlex.join([executable, *arguments])
            script_body = "\n".join(
                command for command in (
                    pre_launch,
                    f"mpiexec -np {ranks} {launch_command}",
                )
                if command
            )
        else:
            barrier_dir = (
                f"{job_run_dir}/.amsc-post-launch-barrier-{uuid.uuid4().hex}"
            )
            wrapper_path = f"{barrier_dir}/wrapper.sh"
            wrapper = _alcf_post_launch_wrapper(
                job_run_dir, barrier_dir, executable, arguments, post_launch, ranks
            )
            encoded_wrapper = base64.b64encode(wrapper.encode()).decode()
            script_body = "\n".join(
                command for command in (
                    pre_launch,
                    f"mkdir -p {shlex.quote(barrier_dir)}",
                    f"printf '0\\n' > {shlex.quote(barrier_dir + '/count')}",
                    f": > {shlex.quote(barrier_dir + '/lock')}",
                    f"echo {encoded_wrapper} | base64 -d > {shlex.quote(wrapper_path)}",
                    f"chmod u+x {shlex.quote(wrapper_path)}",
                    f"mpiexec -np {ranks} /bin/bash {shlex.quote(wrapper_path)}",
                )
                if command
            )
        return executeBatchJobCompat(
            machine, script_body, nodes, ranks_per_node, gpus_per_rank,
            time, queue, account, job_run_dir, exclusive=exclusive,
            allow_unsafe=allow_unsafe, name=name,
        )

    spec = {
        "executable": executable,
        "arguments" : arguments,
        "directory": job_run_dir,
        "name": name,
        "inherit_environment": True,
        "stdin_path": None,
        "stdout_path": f"{job_run_dir}/run.log",
        "stderr_path": f"{job_run_dir}/err.log",
        "resources": {
            "node_count": nodes,
            "processes_per_node": ranks_per_node,
            "exclusive_node_use": exclusive,
            "gpu_cores_per_process": gpus_per_rank,
        },
        "attributes": {
            "duration": time,
            "account": account,
            "queue_name": queue,
        },
    }
    if launcher := machine_specification.get("launcher"):
        spec["launcher"] = launcher
    if pre_launch is not None:
        spec["pre_launch"] = pre_launch
    if post_launch is not None:
        spec["post_launch"] = post_launch
    if custom_attributes := machine_specification.get("custom_attributes"):
        spec["attributes"]["custom_attributes"] = custom_attributes
    rid = getResourceID(machine, rtype="compute")
    response, status = post(machine, f"compute/job/{rid}", data=spec)
    if status == 200:
        return response["id"]
    raise Exception(
        f"Job submission failed, status: {status}, reason: "
        f"{json.dumps(response, indent=2)}"
    )


def executeBatchJobCompat(machine: str, script_body: str,
                    nodes : int, ranks_per_node : int, gpus_per_rank : int,
                    time : str, queue : str, account : str,
                    job_run_dir : str, exclusive=True, allow_unsafe=False,
                    name: str | None = None) -> str:
    """
    Execute batch script on the machine
    script_body: The content of the batch script. If you are executing an existing remote script, use "source /path/to/script"    
    Note that any SLURM/PBS headers will be ignored; ensure that SLURM headers that are usually passed to srun are manually passed instead

    time: the job duration. Currently it seems to only accept integers, which my testing indicates is in *seconds*

    NOTE: This relies on hacking the API to bypass the automatic srun command in favor of the (more flexible) script contents. Hopefully they will allow arbitrary batch script submission as part of the API eventually!
    """
    machine = machine.lower()
    if machine == "local":
        return local_api.executeJobScript(
            script_body, job_run_dir, allow_unsafe, name=name
        )

    wfapiLog(f"Executing batch job on machine {machine} with nodes:{nodes}, ranks/node:{ranks_per_node}, gpus/rank:{gpus_per_rank}, time:{time}, queue:{queue}, account:{account}")
    
    if not allow_unsafe and not checkSafePath(machine, job_run_dir):
        raise Exception("Path is not below the privileged directory")

    implementation = getMachineImplementation(machine)
    machine_specification = IRI_IMPLEMENTATIONS[implementation]["machines"][machine]
    attributes = {
        "duration" : time,
        "account": account,
        "queue_name": queue
        }
    if custom_attributes := machine_specification.get("custom_attributes"):
        attributes["custom_attributes"] = custom_attributes

    if implementation == "alcf":
        encoded_script = base64.b64encode(script_body.encode()).decode()
        spec = {
            "executable": "/bin/bash",
            "arguments": [
                "-lc",
                f"cd {shlex.quote(job_run_dir)}; "
                f"echo {encoded_script} | base64 -d | /bin/bash",
            ],
            "directory": job_run_dir,
            "name": name,
            "inherit_environment": True,
            "stdin_path": None,
            "stdout_path": f"{job_run_dir}/run.log",
            "stderr_path": f"{job_run_dir}/err.log",
            "resources": {
                "node_count": nodes,
                "exclusive_node_use": exclusive,
            },
            "attributes": attributes,
        }
    else:
        spec = {
            "executable": "date",
            "directory": job_run_dir,
            "name": name,
            "inherit_environment": True,
            "stdin_path": None,
            "stdout_path": f"{job_run_dir}/run.log",
            "stderr_path": f"{job_run_dir}/err.log",
            "resources": {
                "node_count": nodes,
                "processes_per_node": ranks_per_node,
                "exclusive_node_use": exclusive,
                "gpu_cores_per_process": gpus_per_rank,
            },
            "attributes": attributes,
            "pre_launch": f"cd {job_run_dir}\n" + script_body,
            "post_launch": None,
        }
        if launcher := machine_specification.get("launcher"):
            spec["launcher"] = launcher
    
    rid = getResourceID(machine, rtype="compute")

    j, status = post(machine, f"compute/job/{rid}", data=spec)
    if status == 200:
        return j['id']
    else:        
        raise Exception(f"Job submission failed, status: {status}, reason: { json.dumps(j,indent=2)}")
    
def getJobState(machine: str, jobid: str) -> str:
    machine = machine.lower()
    if machine == "local":
        return local_api.getJobState(jobid)

    rid = getResourceID(machine, rtype="compute")
    j = get(machine, f"compute/status/{rid}/{jobid}", params = { "historical" : True })
    wfapiLog(f"Queried job state {machine}:{jobid}, got {j['status']['state']}")
    return j['status']['state']

def findJobByName(machine: str, name: str) -> str | None:
    """Return the remote job ID with an exact JobSpec name, if present."""
    machine = machine.lower()
    if machine == "local":
        return local_api.findJobByName(name)

    rid = getResourceID(machine, rtype="compute")
    offset = 0
    limit = 1000
    while True:
        jobs, status = post(
            machine,
            f"compute/status/{rid}",
            params={"historical": True, "include_spec": True, "offset": offset, "limit": limit},
        )
        if status != 200:
            raise Exception(f"Job lookup failed, status: {status}, reason: {json.dumps(jobs, indent=2)}")
        for job in jobs:
            if job.get("job_spec", {}).get("name") == name:
                return job["id"]
        if len(jobs) < limit:
            return None
        offset += limit
    
    
def cancelJob(machine: str, jobid: str):
    machine = machine.lower()

    wfapiLog(f"Canceling job {machine}:{jobid}")
    rid = getResourceID(machine, rtype="compute")
    j, status = delete(machine, f"compute/cancel/{rid}/{jobid}")
    if status != 204:
        raise Exception("Job cancellation failed:",json.dumps(j))
