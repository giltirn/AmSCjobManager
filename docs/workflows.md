# Building workflows

[Documentation home](../README.md#documentation)

## Start with the reference example

For real execution, see [the MPI workflow example](../examples/README.md).
It uploads C++ source, compiles in pre-launch, runs an MPI reduction under
`WorkflowManager`, then downloads and verifies the result. It supports local MPI
and configured IRI machines, with configurable build profiles.

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

## Connect workflows with dependencies

Each workflow remains sequential. To split into parallel branches, submit
separate workflows whose actions depend on a key published by an earlier action.
Join the branches with an AND/OR expression:

```python
from amscjobmanager.api_actions import APIAction, WriteFile

with manager as workflows:
    setup = workflows.enqueueWorkflow(APIAction(
        [WriteFile("local", "/tmp/campaign-input", "prepared")],
        producer_key="campaign/prepared",
    ))
    first = workflows.enqueueWorkflow(APIAction(
        [WriteFile("local", "/tmp/campaign-first", "first")],
        depends_on="campaign/prepared", producer_key="campaign/first",
    ))
    second = workflows.enqueueWorkflow(APIAction(
        [WriteFile("local", "/tmp/campaign-second", "second")],
        depends_on="campaign/prepared", producer_key="campaign/second",
    ))
    joined = workflows.enqueueWorkflow(APIAction(
        [WriteFile("local", "/tmp/campaign-joined", "both branches completed")],
        depends_on=["campaign/first", "AND", "campaign/second"],
    ))
```

The manager runs ready workflows up to its configured capacity. Dependencies
can also be attached to later stages within a chain. Use compute actions on
`local` for long-running local work; these API writes demonstrate staging only.
An empty `APIAction` with `depends_on` and `producer_key` can serve as a barrier
that publishes a new key when its prerequisite branches have succeeded.

`depends_on` accepts a key string, a nested list, or `None`. For example,
`["prepared", "AND", ["solver_a", "OR", "solver_b"]]` means prepared and either
solver. AND takes precedence over OR; nesting acts as parentheses. There is no
NOT operator. The same representation is accepted through MCP/CLI stage JSON.

Submit producers before consumers: every key must already be registered by a
previously submitted workflow or an earlier stage in the current submission.
Repeated keys are allowed, and any successful producer satisfies the key. Keys
and completed history persist across restarts in the same database, so use a
campaign-specific prefix when runs must remain independent. Workflow submission
keys remain separate idempotency identifiers.

Unsatisfied workflows remain PENDING and consume no execution slots. Failed
producers leave consumers pending. Canceling a workflow also cancels consumers
that require its unfinished producers, transitively, including chains already
executing earlier stages. Successful producer history remains valid; OR branches
and repeated keys survive while another producer is not ruled out by cancellation. New
submissions requiring canceled producers are rejected. Later replacement
producers do not restart workflows already canceled by a cascade.

### Inspect dependency groups

Inspect the connected workflows without starting work or polling providers:

```python
with manager as workflows:
    groups = workflows.listWorkflowDependencyGroups()
    selected = workflows.listWorkflowDependencyGroups(workflow_id=joined)
```

The return value is a list of groups, each a list of workflow records. Selecting
an ID still returns an outer list containing its one complete group. Members
are ordered by workflow ID, and groups by their smallest ID. Independent
workflows form singleton groups; empty storage returns `[]`. An unknown explicit
ID raises `ValueError`.

Each record contains `workflow_id`, the recorded `workflow_status` (`ActionStatus`
in Python), and two lists ordered by action stage:

- `producer_keys`: dictionaries containing `action_id` and `producer_key`.
- `dependency_conditions`: dictionaries containing `action_id` and `depends_on`,
  preserving the original key or nested AND/OR expression.

Only actions with the corresponding metadata appear in each list. Repeated
keys retain separate action entries. Action IDs are global database IDs, not
workflow IDs or stage positions. Future stages and completed/failed/canceled
history are included; action definitions are not unpickled.

Grouping follows declared relationships in both directions, so selecting a join
also returns prerequisites, sibling branches, and downstream consumers. All
producers of every referenced key are included, even for OR alternatives that
are no longer needed to run. Shared producer labels alone do not connect
workflows until some action references that label. `workflow_group` metadata
does not affect these groups. Results are rebuilt from current records on each
call, so later submissions can join previously separate groups.

The same query is available through MCP and the generated CLI; see
[dependency-group inspection](server.md#inspecting-workflow-dependency-groups).

## Construct actions

Short filesystem preparation and local processing can be distinct `APIAction`
stages before or after compute and transfer stages. See [API action stages](api_actions.md)
for the supported operations and checkpoint recovery. Long-running local tasks
should be compute actions targeting `local`.

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
from amscjobmanager import WorkflowManager

manager = WorkflowManager(filename="workflow.sqlite", poll_freq=30,
                     max_workflows_active=4)
with manager as workflows:
    workflow_id = workflows.enqueueWorkflow(workflow, workflow_group="example-calculations")
manager.start()
```

The `with manager` block acquires the database lock; it does not start or stop
the manager. Keep the application alive to monitor work and call `stop()` on
shutdown. See [operations](operations.md) for a complete monitoring pattern.

For retry-safe submission, supply an optional `submission_key` through either
WorkflowManager's locked context or its callable interface:

```python
workflow_id = manager(lambda workflows: workflows.enqueueWorkflow(
    workflow, workflow_group="example-calculations", submission_key="campaign-42/run-7",
))
```

Keys are exact strings, unique within the workflow database and retained across
restarts, completion, and cancellation. Repeating a key returns the original
workflow ID without comparing or replacing its actions, group, or recorded
progress. Reuse the key when retrying after a lost response; choose a new key
for changed work. Leaving it as `None` always creates a new workflow.
Use one live WorkflowData instance per database, including the instance owned by a
WorkflowManager. Its connection lock serializes concurrent submissions through that
instance; separate instances or processes are not coordinated.

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

For `machine="local"`, `nodes` must equal one and `ranks_per_node` sets the
number of local MPI processes when `local_mpi_launcher` is configured. With the
default `null` setting, the total must equal one. Pre-launch and post-launch run
once around the launcher; local execution does not allocate scheduler nodes.

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

from amscjobmanager import ActionStatus, WorkflowManager
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
    manager = WorkflowManager(filename=str(root / "workflows.sqlite"), poll_freq=0)
    try:
        with manager as workflows:
            workflow_id = workflows.enqueueWorkflow(action)
        manager.start()
        while True:
            if not manager.isAlive():
                raise RuntimeError("Manager stopped before completion")
            with manager as workflows:
                status = workflows.workflowStatus(workflow_id)
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

## JSON workflow representations

Built-in actions and API operations are Pydantic dataclasses. Their positional
Python constructors remain available, and `amscjobmanager.workflow.workflow_adapter`
exports/imports complete nonempty workflow lists with `dump_json`/`validate_json`.
Each stage and API operation has a `type` discriminator. Binary `WriteFile`
contents round-trip as a tagged Base64 object; Python constructors still accept
raw bytes. See [server workflow submission](server.md#submit-and-cancel-workflows)
for the shared JSON schema and client examples. Custom derived Python actions
continue to use the Python API; they are not part of the MCP stage allowlist.
