# Public API reference

[Documentation home](../README.md#documentation)

Use `from amscjobmanager import api_general as api` for the selected IRI/SPOOF
implementation. The top-level package exports `WorkflowManager`, `WorkflowData`, and
`ActionStatus`. The interfaces below describe the current Python API; provider
support and sandbox enforcement are detailed in the linked guides.

Filesystem path arguments and configured paths reject `~` on all machines,
including with `allow_unsafe=True`. Supply explicit account-specific paths;
the library does not expand paths using the manager host's username.

## Setup and configuration

| Entry point | Arguments and result |
| --- | --- |
| `ManagerConfig(...)` in `manager_config_models` | Validates configuration fields; use `model_validate_json(text)` for JSON text. |
| `ManagerConfig.write_template_json(output_path="manager-config-template.json")` | Writes a starter JSON file if absent; returns its `Path`. Does not overwrite an existing file. |
| `readManagerConfigFile(filename)` in `manager_config` | Loads and validates a JSON file; returns `ManagerConfig`. |
| `setupManager(config)` in `manager_config` | Initializes the selected implementation; returns `None`. |
| `api.setupWorkflowAgentFromConfig(config)` | Initializes from `ManagerConfig`; returns a sandbox mapping. |
| `api.setupWorkflowAgent(iri_implementations, iriapi_transfer_key_path, work_dir, globus_personal_endpoint_config=None, iri_config_cache_directory=None)` | Field-based setup; `work_dir` is the machine-to-sandbox mapping. |
| `globals.getSandboxDirs(machine=None)` | Returns a copied machine-to-path-list mapping, optionally filtered by case-insensitive machine name. Includes unvalidated roots and empty lists; an unmatched name returns `{}`. Performs no directory probes. |
| `globals.addSandboxDirs(dirs, warn_unavailable=True, *, replace=False)` | Adds machine-to-root string/list registrations without directory probes. Local and remote roots are validated on a subsequent sandbox check. Returns `None`. |
| `globals.removeSandboxDirs(dirs)` | Removes exact registered root strings and matching pending checks. Missing roots are ignored; files, overlapping roots, and `/tmp` access remain. Returns `None`. |

Import these sandbox operations from `amscjobmanager.globals`. Changes are
session-local. Running servers expose listing through MCP's
`list_sandbox_directories(machine=None)`; changes require the password-protected
[local admin utility](server.md#local-administration).

## Workflow management

| Entry point | Arguments and result |
| --- | --- |
| `WorkflowManager(filename=None, poll_freq=30, max_workflows_active=10)` | Creates a single-use manager and WorkflowData, immediately recovering workers for unfinished persisted actions. The workflow-progression thread starts later. |
| `manager.start()` | Starts progression in a background thread. Repeated calls while running are harmless; calls after shutdown or failure raise an error. |
| `manager.isAlive()` | Returns whether its background thread is running. |
| `with manager as workflows:` | Locks access and yields WorkflowData. Does not manage thread lifetime. |
| `manager(operation)` | Calls `operation(workflow_data)` under lock and returns its result. |
| `manager.stop(wait_until_done=True, *, close_database=False)` | Waits according to its arguments, permanently shuts down the instance, and joins the thread. Repeated shutdown is safe; recovery requires a new manager. |
| `WorkflowData.enqueueWorkflow(workflow, workflow_group=None, *, submission_key=None)` | Accepts one action or a nonempty list; returns the integer workflow ID. Validates dependency keys against earlier producers and rejects conditions requiring canceled producers atomically. Reusing an optional submission key returns the original ID without changing or revalidating its replacement payload. |
| `WorkflowData.workflowStatus(workflow_id)` | Returns a coarse `ActionStatus`; scheduling/intermediate completion appear as `ACTIVE`. |
| `WorkflowData.headActionStatus(workflow_id, force_poll=False, return_api_status=False)` | Returns the head `ActionStatus`, optionally paired with its provider status string; forced polling refreshes all active heads. Options are keyword-only. |
| `WorkflowData.workflowState(workflow_id)` | Returns workflow/head metadata with enum-valued workflow status, head status, and head class. Action definitions and serialized blobs are excluded. |
| `WorkflowData.listWorkflows(workflow_id=None, statuses=None)` | Lists state dictionaries in ID order, optionally filtering by ID and overall `ActionStatus` value(s). Includes terminal workflows; an explicit unknown ID raises `ValueError`. Does not poll. |
| `WorkflowData.cancelWorkflow(workflow_id)` | Marks cancellation while retaining records and cancels executing work. Returns `None`; may raise while leaving `CANCELING`. |
| `WorkflowData.getManager(action_class)` | Returns the existing manager for an `ActionClass` (COMPUTE, TRANSFER, or API). Call its public methods under the WorkflowManager lock; each manager owns its action state and implementation-specific operations. |
| `workflows.getManager(action_class).getLiveActions()` | Returns info dictionaries for ACTIVE or PROVIDER_HELD actions of the selected `ActionClass`. Entries include workflow ID, remote `api_key`, and raw `api_status`. `ActionClass.API` returns an empty list because unfinished API sequences retain `SCHEDULING`. |
| `WorkflowData.countWorkflowsWithStatus(statuses)` | Accepts one `ActionStatus` or a list; counts matching persisted overall workflow statuses, including terminal workflows. Submission and intermediate action completion belong to ACTIVE workflows. |
| `WorkflowData.startWorkflows(workflow_ids=None)` | Reconciles submissions, then starts selected eligible workflows or fills automatic slots. |
| `WorkflowData.progressActiveState(force_poll=False)` | Restores missing supervision, observes recorded action states, and progresses successful workflows. Forced polling requests an early worker poll without waiting or changing its interval. Failed polls are logged and deferred per action while other workflows progress. Used by WorkflowManager or an application driving WorkflowData manually. |

When a manager thread runs, call these WorkflowData operations through its lock.
See [operations](operations.md) for status semantics and recovery limits.

## Action constructors

Import compute actions from `amscjobmanager.compute_actions`:

```python
ExecuteJobComputeAction(
    machine, account, queue, time,
    executable, arguments, rundir, nodes, ranks_per_node, gpus_per_rank,
    pre_launch=None, post_launch=None, exclusive=True,
)

ExecuteBatchScriptComputeAction(
    machine, account, queue, time,
    script_body, rundir, nodes, ranks_per_node, gpus_per_rank, exclusive=True,
)
```

Import transfers from `amscjobmanager.globus_transfer_action`:

```python
GlobusTransfer(
    dest_endpoint, dest_path, source_endpoint, source_path,
    source_path_type="globus", dest_path_type="globus",
)
```

Prefer keyword arguments. These dataclasses are descriptions, not submitted
jobs. `initiateAction(workflow_id, name=None)` returns the remote ID when called by
AsyncActionManager; exceptions are handled by its submission policy.
`queryResourceStatus()` returns `True`, `False`, or `None` for no available
signal. See [workflows](workflows.md) for parameter meanings.

## Resource queries

| Function | Result and notes |
| --- | --- |
| `api.getKnownMachines()` | List of machine names available through configured providers, plus `local`. |
| `api.queryMachineStatus(machine, rtype="compute")` | Boolean for `current_status == "up"`; local is `True`. Storage selection can require a path unavailable in this convenience signature; use resource discovery/`getResource` when necessary. |
| `api.getMachineQueues(machine)` | Configured `(name, description)` pairs; discovery does not fill queue definitions. |
| `api.getUserAccountProjects(machine)` | Configured account/project names plus unique discovered names, merged in memory. IRI also supports querying by `implementation=`. Without discovery or configured names, raises configuration guidance. |
| `api.getProjectAllocation(machine, project_name)` | List of allocation dictionaries returned for a discovered project ID. Fields depend on the provider; configured account names alone cannot supply this query. |
| `api.getUsername(machine=None, *, implementation=None)` | Username; exactly one target is required. `machine="local"` runs `whoami`; remote IRI needs API v2. |
| `api.listSpecialGlobusEndpoints()` | Configured endpoint tags. |

The IRI-specific API additionally exports `getResources(...)`,
`getResource(resource_id, *, machine=None, implementation=None)`,
`getResourceID(machine, rtype="compute", path=None)`, `getMountedFilesystems(machine)`,
and `getStorageResourcePaths(machine)`. See their source in
[`resources.py`](../src/amscjobmanager/iri_api/resources.py) and
[`filesystem.py`](../src/amscjobmanager/iri_api/filesystem.py) for filter and
provider/version constraints. They are not a uniform SPOOF interface.

## Filesystem operations

All remote paths should be absolute. Local file arguments refer to the manager
host. A successful mutation's incidental return value is not a portable success
code; use exceptions and follow-up queries as appropriate.

| Function | Result and notes |
| --- | --- |
| `api.remoteLs(machine, path)` | List of filenames returned by the provider; locally, sorted absolute child paths including hidden entries. |
| `api.remoteMkdir(machine, path, create_parents=True, allow_unsafe=False)` | Creates a directory; checks supported destination paths. |
| `api.remoteChmod(machine, path, mode, allow_unsafe=False)` | Changes permissions; mode is a string such as `"700"`. |
| `api.pathStat(machine, remote_path, dereference=True, *, raw=False)` | Shared metadata dictionary; `raw=True` returns native stat text and raises `NotImplementedError` for a Globus fallback. Globus cannot supply link metadata with `dereference=False`. SPOOF returns illustrative metadata or text. |
| `api.pathType(machine, remote_path)` | File-description string, using `file` locally. Globus fallback identifies basic types without inspecting contents. |
| `api.pathExists(machine, remote_path)` | Boolean derived from path-type output; subject to provider/SPOOF limitations. |
| `api.isDirectory(machine, remote_path, *, check_exists=False)` | A trailing slash indicates a directory without a query by default. Set `check_exists=True` to verify it exists as a directory. Local checks inspect the filesystem and follow symlinks; remote checks use path-type API output. Local copy/download helpers enable this option for destination checks. |
| `api.uploadBytes(machine, remote_path, content, allow_unsafe=False)` | Uploads `io.BytesIO` contents to a destination file. |
| `api.uploadSmallFile(machine, remote_path, local_path, allow_unsafe=False, definitely_is_file=False)` | Reads a local file and uploads it; remote directory paths append the local filename. |
| `api.downloadFileContents(machine, remote_path)` | Text/base64 string, according to provider behavior. Locally, UTF-8 plaintext for text/empty files and base64 for other contents. |
| `api.downloadFile(machine, local_path_out, remote_path_in, *, allow_unsafe=False)` | Writes and returns the resolved local filename; an existing destination directory receives the source filename. Checks local sandbox or `/tmp` destinations unless explicitly overridden. A Globus fallback also permits its configured staging root. |

`pathStat` returns `type` (`file`, `dir`, `link`, `chr`, `blk`, `pipe`, or
`other`), `size` in bytes, `permissions` as an octal string, `user`, `group`,
and `last_modified` as an ISO 8601 UTC timestamp. Optional values unavailable
from Globus are `None`. Native normalization reads standard English GNU stat
output; an unrecognized format raises `ValueError` with `raw=True` available for
inspection. Globus stat follows valid symlinks; requesting the link's own
metadata raises `NotImplementedError`. Native and local calls support both
dereference modes.

The IRI module also exports `remoteLsGlobus(machine, remote_path)`,
`pathStatGlobus(machine, remote_path, dereference=True)`,
`pathTypeGlobus(machine, remote_path)`, and
`remoteMkdirGlobus(machine, remote_path, create_parents=True, allow_unsafe=False)`.
These take machine filesystem paths and translate them through configured
collection mappings. Metadata and directory calls require transfer credentials,
but no personal endpoint or staging directory. Directory creation validates
every missing ancestor against the sandbox before creating any directories,
unless explicitly overridden. Listing includes hidden entries; stat/list/mkdir
are foreground Globus operations. Listing directories over Globus's 100,000-entry
limit raises a service error rather than returning a complete list.

See [sandboxing](sandboxing.md) and [implementations](implementations.md) before
assuming equivalent checks or local dispatch for these functions.

## Transfers and direct compute control

```python
api.globusCopy(
    dest_endpoint, dest_path, source_endpoint, source_path,
    allow_unsafe=False, block_until_complete=False, label=None,
    source_path_type="globus", dest_path_type="globus",
)

api.uploadFileGlobus(
    machine, remote_path, local_filesystem_path,
    remote_path_type="filesystem", remote_endpoint=None,
)

api.downloadFileGlobus(
    machine, local_filesystem_path, remote_path,
    remote_path_type="filesystem", remote_endpoint=None, allow_unsafe=False,
)
```

The last two functions require their optional arguments as keywords.
`globusCopy()` returns a task ID; `uploadFileGlobus()` blocks and returns a task
ID; `downloadFileGlobus()` blocks and returns a local filename.
Its local destination must be in a sandbox, `/tmp`, or configured staging unless
`allow_unsafe=True` authorizes another final output path. Temporary staging
remains confined to the configured staging root.

| Function | Result and notes |
| --- | --- |
| `api.globusTransferStatus(transfer_id)` | Raw transfer status such as `ACTIVE`, `INACTIVE`, `SUCCEEDED`, or `FAILED`. |
| `api.findGlobusTransferByLabel(label)` | Unique matching ID or `None`; raises on multiple matches. The plural-named alias has the same single-result behavior. |
| `api.cancelGlobusTransfer(transfer_id)` | Sends a cancellation request; errors propagate. |
| `api.executeJob(...)` | Direct counterpart of the executable action, with `job_run_dir` instead of `rundir` and an optional `name`. Also accepts `allow_unsafe=False`. Returns a remote job ID. |
| `api.executeBatchJobCompat(...)` | Direct counterpart of the script action with `job_run_dir`, optional `name`, and `allow_unsafe=False`. Returns a remote job ID. |
| `api.getJobState(machine, jobid)` | Raw compute state, such as `queued`, `active`, `completed`, `failed`, or `canceled`. |
| `api.findJobByName(machine, name)` | Matching remote ID or `None`. |
| `api.cancelJob(machine, jobid)` | Sends a remote compute cancellation request. Locally, prevents launch or terminates the job's process group and waits before recording `canceled`; repeated cancellation preserves terminal statuses. Returns `None`. |

Direct submissions are not added to WorkflowData. Use actions when the manager
should own ordering, monitoring, and cancellation. See [transfers](transfers.md)
for namespaces and staging.

## Local write primitives

`amscjobmanager.local_api` provides `localMkdir(path, create_parents=True,
allow_unsafe=False)`, `writeBytes(path, content, allow_unsafe=False)`, and
`localCopyFile(path_to, path_from, allow_unsafe=False)`. Each checks its resolved
output against local sandbox roots or `/tmp`. Copies expand directory
destinations to the final source filename before checking, including symlinks.
`allow_unsafe=True` explicitly bypasses the destination restriction.

`executeJobScript(script_body, job_run_dir, allow_unsafe=False, name=None)` checks
the run directory, wrapper, and log before recording the job or starting its
worker. Its commands still run with the user's OS permissions.

`local_api.executeJob(executable, arguments, job_run_dir, allow_unsafe=False,
name=None, *, ranks=1, pre_launch=None, post_launch=None)` uses the same output checks
and asynchronous runner. It returns a local job ID, preserves literal executable
arguments, and supports Bash pre/post hooks. Pre-launch failure stops the job;
post-launch runs only after the executable succeeds. Failure in any stage marks
the job failed. With `local_mpi_launcher` configured, it invokes that executable
with `-n ranks` before the application command. Without a launcher, ranks must
equal one. Ranks must always be positive integers. `api.executeJob("local", ...)`
requires `nodes=1` and forwards `ranks_per_node` as this total. Both entry points
support local cancellation.
