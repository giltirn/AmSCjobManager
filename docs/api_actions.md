# API action stages

[Documentation home](../README.md#documentation)

`APIAction` adds short preparation and processing sequences directly to a
workflow. Each sequence is a distinct stage, rather than a callback attached to
compute submission or completion. A failed stage prevents subsequent stages
from starting.

## Build a sequence

```python
import sys

from amscjobmanager.api_actions import (
    APIAction, MakeDirectory, WriteFile, UploadFile, DownloadFile, CopyFile, RunLocal,
)
from amscjobmanager.compute_actions import ExecuteJobComputeAction

run_dir = "/tmp/amsc-api-example"
workflow = [
    APIAction([
        MakeDirectory("local", run_dir),
        WriteFile("local", f"{run_dir}/input.txt", "42\n"),
    ]),
    ExecuteJobComputeAction(
        machine="local", account="local", queue="local", time="60",
        executable=sys.executable,
        arguments=["-c", "from pathlib import Path; "
                   "Path('output.txt').write_text(Path('input.txt').read_text())"],
        rundir=run_dir, nodes=1, ranks_per_node=1, gpus_per_rank=0,
    ),
    APIAction([
        CopyFile(f"{run_dir}/result.txt", f"{run_dir}/output.txt"),
    ]),
]
```

Enqueue this list with `JobData.enqueueJob` under the `JobManager` lock, as in
[the workflow guide](workflows.md). For remote execution, configure the target
and sandbox paths first. You can insert a `GlobusTransfer` stage between remote
compute and local processing; processing will wait until the transfer succeeds.
A transfer to the personal endpoint must target its configured staging path.
An API stage can then copy the staged result into a permitted local directory.

## Supported operations

| Operation | Inputs and effect |
| --- | --- |
| `MakeDirectory(machine, path, create_parents=True)` | Ensure a directory exists on a configured target or `local`; an existing file is an error. |
| `WriteFile(machine, path, content)` | Write complete UTF-8 text or bytes to a full filename, overwriting existing contents. Content is stored verbatim. |
| `UploadFile(machine, dest_path, source_path)` | Upload a small file from the manager host to a full destination filename. Uses the existing small-file API and its size limits. |
| `DownloadFile(machine, dest_path, source_path)` | Download a file synchronously to a local filename or existing directory. |
| `CopyFile(dest_path, source_path)` | Copy a file on the manager host into a local filename or existing directory. |
| `RunLocal(executable, arguments, cwd, timeout=60)` | Run a short local executable with literal arguments, an existing working directory and a positive finite timeout in seconds. No shell is invoked. |

Filesystem mutations use the existing sandbox checks, with no `allow_unsafe`
option. Local `/tmp` writes follow the library's existing allowance. Paths
containing `~` are rejected. Paths support `<JOBID>`, replaced by the owning
workflow's positive integer ID. `RunLocal` also expands this placeholder in its
executable and argument strings. File contents are not expanded.

All local files and processes belong to the **manager host**, not the machine
running an MCP or CLI client. The operations currently have a Python interface;
this change does not add MCP workflow submission tools.

## Execution and recovery

`APIActions` implements the abstract `ActionManager` interface independently of
`AsyncActionManager`. Its `api_actions` table stores the action and workflow
IDs, serialized definition, status, `next_operation`, update timestamp and any
failure reason. There are no remote task IDs or submission-attempt fields.

The manager commits the action's `SCHEDULING` intent before executing anything.
After each successful operation it commits `next_operation`, the zero-based
index of the next unfinished operation. A value equal to `operation_count`
means all operations have finished. SQLite enforces integer, nonnegative cursor
storage; the manager also validates it against the persisted sequence length.
The final cursor and `COMPLETED` status commit together. Empty sequences complete
immediately.

After a crash, recovery skips checkpointed operations and resumes at the cursor.
The interrupted operation must be safe to repeat: a crash may occur after its
external effects succeed but before its checkpoint commits. File operations
therefore overwrite partial destinations rather than treating existence as
completion. Local programs must tolerate partial or complete re-execution and
must not create untracked compute or transfer jobs. Their input files must remain
stable and available until the corresponding operation has completed.

Ordinary operation exceptions stop the sequence and mark the action `FAILED`.
The cursor remains at the failed operation, the reason is available through
`getActionInfo`, and `wfmanLog` records the action/workflow IDs and operation
index. There is no automatic retry of failed stages. Status queries never execute
operations, even when a forced poll is requested.

`queryStatus` returns `(ActionStatus, None)`: these stages have no provider
status. `getActionInfo` includes `next_operation`, `operation_count`, operation
type names, and `error`, alongside the common `job_id`, `api_key=None` and
`api_status=None` fields.

Cancellation of interrupted work records `CANCELED` directly without replaying
or undoing operations: there is no external cancellation to reconcile. JobData
retains its separate workflow-level cancellation recovery. Existing files remain. Cancellation and other
requests using the manager lock wait for an executing sequence to finish.

## Keep API stages short

API sequences run synchronously and occupy the manager while executing.
**Long-running local programs should be compute actions with `machine="local"`**,
using asynchronous execution, monitoring and cancellation. Large transfers should
be separate `GlobusTransfer` stages rather than blocking uploads or downloads.

`RunLocal` is trusted program execution. Checking its working directory does not
restrict where the program can write, and the timeout does not provide a process
sandbox. It is not exposed as an MCP operation; future client access needs the
privileged-operation controls discussed for arbitrary local execution.

## Tests

Run the local operation, checkpoint recovery and mixed-workflow tests with:

```bash
python -m unittest discover -s test -p test_api_actions.py -v
```

They use temporary files and real local processes, and mock remote transfers.
No remote resources, authentication or MPI installation are required.
