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

Enqueue this list with `WorkflowData.enqueueWorkflow` under the `WorkflowManager` lock, as in
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
containing `~` are rejected. Paths support `<WORKFLOWID>`, replaced by the owning
workflow's positive integer ID. `RunLocal` also expands this placeholder in its
executable and argument strings. File contents are not expanded.

All local files and processes belong to the **manager host**, not the machine
running an MCP or CLI client. Built-in operations also support workflow JSON and
MCP submission through their `type` tags.

## Application-defined Python operations

Subclass `APIActionStageBase` and implement `execute(workflow_id)`. No registration
or manager modification is required. For example, define this class in an
importable application module:

```python
from dataclasses import dataclass
from amscjobmanager.api_actions import APIAction, APIActionStageBase

@dataclass
class CollectResult(APIActionStageBase):
    source_path: str
    dest_path: str

    def execute(self, workflow_id: int) -> None:
        """Replay-safe copy for workflow database ID workflow_id."""
        from amscjobmanager.api_general import downloadFile
        downloadFile("local", self.dest_path, self.source_path)

action = APIAction([CollectResult("/tmp/source.txt", "/tmp/result.txt")])
```

Python construction accepts subclass instances, including mixtures with built-in
operations. A Python dictionary or JSON payload still selects from the built-in
discriminated union; arbitrary application types cannot be reconstructed through
JSON/MCP. The generated JSON schemas continue to describe the built-ins only.
JSON export preserves fields of supported dataclasses/models, but exporting a
custom operation does not make it JSON-importable. Ordinary Python classes may
need their own serialization support for JSON export; database storage uses
pickle independently of JSON.

`execute` runs on a worker thread in the existing interpreter, with access to
module globals. The manager ignores its return value and records exceptions as
action failures. Shared mutable state needs thread coordination. Custom Python
operations run with the application's permissions; use the existing API helpers
when sandbox-checked file operations are required.

Capture every recovery input in pickleable instance fields before enqueueing.
Do not store open files, clients, manager objects, closures, or thread state.
Keep the class importable at the same module path after restart. Module globals
are shared during execution but are not persisted; restore authentication and
manager configuration at startup. Referenced input files must remain available.

The manager snapshots the operation with its workflow and checkpoints successful
operations. An unfinished operation may run again even if its effects completed
before a crash. Make it replay-safe and save results to a stable location,
preferably by atomically replacing the output after writing a temporary file.

## Execution and recovery

`APIActions` implements the abstract `ActionManager` interface independently of
`AsyncActionManager`. Its `api_actions` table stores the action and workflow
IDs, serialized definition, status, `next_operation`, update timestamp and any
failure reason. There are no remote task IDs or submission-attempt fields.

The manager commits the action's `SCHEDULING` intent before launching its
worker. That thread runs all remaining operations sequentially, retaining
its stack across API calls and user queries. `WorkflowData` need not call it again
for execution to continue. Unfinished operations leave the cursor unchanged.
After each successful operation the worker commits `next_operation`, the zero-based
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

`queryRecordedStatus` returns `(ActionStatus, None)`: these stages have no provider
status. `getActionInfo` includes `next_operation`, `operation_count`, operation
type names, and `error`, alongside the common `workflow_id`, `api_key=None` and
`api_status=None` fields.

Cancellation with no live worker records `CANCELED` directly without replaying
or undoing operations. If an operation is still running, cancellation retains
`CANCELING` until the execution worker finishes or acknowledges interruption,
after which a separate cancellation worker records `CANCELED`.
Later operations are never started. Existing file effects remain; WorkflowData retains
its separate workflow-level cancellation recovery.

## Keep API stages short

API operations run on background workers and do not occupy the manager while
waiting. They are intended for short preparation and postprocessing.
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
