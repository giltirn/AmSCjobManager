# Building workflows

[Documentation home](../README.md#documentation)

## Start with the reference example

Run [spoof_workflow_apiv1.py](../examples/spoof_workflow_apiv1.py) from the
repository root:

```bash
python examples/spoof_workflow_apiv1.py
```

The example performs three kinds of work:

1. **Setup:** select SPOOF before imports, configure the library, create local
   input files, and make illustrative filesystem calls for two remote machines.
2. **Managed workflow:** copy input from Polaris to Perlmutter, submit an
   executable compute action, then copy its result to the personal collection.
3. **Monitoring and inspection:** enqueue under the manager lock, start the
   background manager, inspect state, stop it, and inspect SPOOF's event history.

Its remote files and calculation results are simulated. The real local inputs,
placeholder download, and SQLite database live in a temporary directory. To
adapt it for real IRI execution, select IRI in a fresh process, initialize real
configuration, supply valid accounts/queues and endpoints, use persistent local
state paths, and stage the actual inputs. SPOOF output does not confirm that
real launch commands or remote paths will work.

## Construct actions

The following extracts show the workflow model; use the complete reference
example for runnable setup and monitoring.

```python
from amscjobmanager.compute_actions import ExecuteJobComputeAction
from amscjobmanager.globus_transfer_action import GlobusTransfer

run_dir = "/global/cfs/cdirs/MY_PROJECT/example/workflows/run-001"
workflow = [
    GlobusTransfer(
        dest_endpoint="perlmutter", dest_path=f"{run_dir}/input/",
        source_endpoint="polaris",
        source_path="/lus/eagle/projects/MY_PROJECT/example/input.dat",
        source_path_type="filesystem", dest_path_type="filesystem",
    ),
    ExecuteJobComputeAction(
        machine="perlmutter", account="MY_PROJECT", queue="debug", time="300",
        executable="/usr/bin/python3", arguments=["calculate.py"],
        rundir=run_dir, nodes=1, ranks_per_node=4, gpus_per_rank=1,
        pre_launch="mkdir -p output",
        post_launch="test -s output/result.txt",
    ),
    GlobusTransfer(
        dest_endpoint="personal", dest_path="/staging/results/run-001/",
        source_endpoint="perlmutter", source_path=f"{run_dir}/output/result.txt",
        source_path_type="filesystem",
    ),
]
```

The script and settings for this example must already be staged into `run_dir`.
The personal destination uses the Globus namespace, its default path type.
Constructing this list does not make API calls. Enqueue it as one workflow:

```python
from amscjobmanager import JobManager

manager = JobManager(filename="workflow.sqlite", poll_freq=30,
                     max_workflows_active=4)
with manager as jobs:
    job_id = jobs.enqueueJob(workflow, job_group="example-calculations")
manager.start()
```

The `with manager` block acquires the database lock; it does not start or stop
the manager. Keep the application alive to monitor work and call `stop()` on
shutdown. See [operations](operations.md) for a complete monitoring pattern.

## Compute parameters

Both compute action types inherit these required fields:

| Field | Meaning |
| --- | --- |
| `machine` | Configured machine name, or `local` for local execution under IRI. |
| `account` | Facility allocation/account string accepted by the machine. |
| `queue` | Facility queue name. Query available configured queues before choosing one. |
| `time` | Requested duration; current examples use an integer number of seconds represented as a string, e.g. `"300"`. Confirm provider requirements. |

They also require `rundir`, `nodes`, `ranks_per_node`, and `gpus_per_rank`.
`exclusive` defaults to `True`. The run directory must pass the sandbox check
and should exist before submission. Use a distinct run directory per concurrent
job so logs and wrappers do not overwrite one another.

`ExecuteJobComputeAction` describes an executable plus a list of arguments. Its
optional `pre_launch` and `post_launch` are shell commands. Providers supporting
native hooks run these around the executable's launcher. On ALCF, the library
uses `mpiexec`; for post-launch it generates a shared-filesystem barrier and
runs the post-launch commands on MPI rank zero after all ranks finish. This
requires the relevant Bash/MPI tools, `flock`, and shared writable storage. A
rank failing before the barrier can prevent it from completing.

For a script body instead:

```python
from amscjobmanager.compute_actions import ExecuteBatchScriptComputeAction

action = ExecuteBatchScriptComputeAction(
    machine="perlmutter", account="MY_PROJECT", queue="debug", time="300",
    script_body="srun -n 4 python3 calculate.py",
    rundir=run_dir, nodes=1, ranks_per_node=4, gpus_per_rank=1,
)
```

The compatibility submission runs the script body through provider-specific
handling. Scheduler headers embedded in the body do not configure the job;
supply resource parameters explicitly. Include the needed launch commands in
the script. Native and compatibility IRI submissions request `run.log` and
`err.log` under the run directory. Commands execute with the job user's
permissions; the run-directory check does not restrict their behavior.

## Real local hello-world

This standalone Python example uses the IRI selection's local runner, executes
real commands, and needs no remote credentials:

```python
import os
os.environ["AMSC_JOB_MANAGER_API_IMPL"] = "IRI"

from pathlib import Path
import tempfile
import time

from amscjobmanager import ActionStatus, JobManager
from amscjobmanager.compute_actions import ExecuteBatchScriptComputeAction
from amscjobmanager.local_api import setStatusManagerDatabasePath
from amscjobmanager.manager_config import setupManager
from amscjobmanager.manager_config_models import ManagerConfig

with tempfile.TemporaryDirectory() as directory:
    root = Path(directory)
    setupManager(ManagerConfig(
        iri_implementations={}, sandbox_directories={"local": [str(root)]},
    ))
    setStatusManagerDatabasePath(str(root / "localjobs.sqlite"))
    action = ExecuteBatchScriptComputeAction(
        machine="local", account="", queue="", time="60",
        script_body="#!/bin/bash\nprintf 'hello\\n' > result.txt\n",
        rundir=str(root), nodes=1, ranks_per_node=1, gpus_per_rank=0,
    )
    manager = JobManager(filename=str(root / "workflows.sqlite"), poll_freq=0)
    try:
        with manager as jobs:
            job_id = jobs.enqueueJob(action)
        manager.start()
        while True:
            if not manager.isAlive():
                raise RuntimeError("Manager stopped before completion")
            with manager as jobs:
                status = jobs.jobStatus(job_id)
            if status == ActionStatus.COMPLETED:
                break
            if status in (ActionStatus.FAILED, ActionStatus.CANCELED):
                raise RuntimeError(f"Workflow ended: {status.name}")
            time.sleep(0.2)
        print((root / "result.txt").read_text())
    finally:
        manager.stop(wait_until_done=False, close_database=True)
```

Run this in a fresh process: backend selection and the local status database
are initialized once. Local resource parameters do not impose limits on the
process. Persistent use requires directories and database files that outlive
the temporary directory in this demonstration.
