# Public API reference

[Documentation home](../README.md#documentation)

Use `from amscjobmanager import api_general as api` for the selected IRI/SPOOF
implementation. The top-level package exports `JobManager`, `JobData`, and
`ActionStatus`. The interfaces below describe the current Python API; provider
support and sandbox enforcement are detailed in the linked guides.

## Setup and configuration

| Entry point | Arguments and result |
| --- | --- |
| `ManagerConfig(...)` in `manager_config_models` | Validates configuration fields; use `model_validate_json(text)` for JSON text. |
| `ManagerConfig.write_template_json(output_path="manager-config-template.json")` | Writes a starter JSON file if absent; returns its `Path`. Does not overwrite an existing file. |
| `readManagerConfigFile(filename)` in `manager_config` | Loads and validates a JSON file; returns `ManagerConfig`. |
| `setupManager(config)` in `manager_config` | Initializes the selected implementation; returns `None`. |
| `api.setupWorkflowAgentFromConfig(config)` | Initializes from `ManagerConfig`; returns a sandbox mapping. |
| `api.setupWorkflowAgent(iri_implementations, iriapi_transfer_key_path, work_dir, globus_personal_endpoint_config=None, iri_config_cache_directory=None)` | Field-based setup; `work_dir` is the machine-to-sandbox mapping. |

## Workflow management

| Entry point | Arguments and result |
| --- | --- |
| `JobManager(filename=None, poll_freq=30, max_workflows_active=10)` | Creates a manager and JobData; no background thread starts yet. |
| `manager.start()` | Starts progression in a background thread. |
| `manager.isAlive()` | Returns whether its background thread is running. |
| `with manager as jobs:` | Locks access and yields JobData. Does not manage thread lifetime. |
| `manager(operation)` | Calls `operation(job_data)` under lock and returns its result. |
| `manager.stop(wait_until_done=True, *, close_database=False)` | Waits according to its arguments, requests shutdown, and joins the thread. |
| `JobData.enqueueJob(workflow, job_group=None)` | Accepts one action or a nonempty list; returns the integer workflow ID. |
| `JobData.jobStatus(job_id)` | Returns a coarse `ActionStatus`; scheduling/intermediate completion appear as `ACTIVE`. |
| `JobData.jobState(job_id)` | Returns a state dictionary with enum-valued head status/class and serialized workflow. |
| `JobData.removeJob(job_id, cancel_running=True)` | Marks cancellation while retaining records; optionally cancels current remote work. Returns `None`; may raise while leaving `CANCELING`. |
| `JobData.getActiveActions(action_class)` | Returns a list of info dictionaries for active actions of `ActionClass.COMPUTE` or `TRANSFER`. Entries include workflow ID, remote `api_key`, and raw `api_status`. |
| `JobData.countWorkflowsWithStatus(statuses)` | Accepts one `ActionStatus` or a list; counts matching head statuses with a non-`NONE` head class. Fully completed workflows are excluded. |
| `JobData.startWorkflows(job_ids=None)` | Reconciles submissions, then starts selected eligible workflows or fills automatic slots. |
| `JobData.progressActiveState(poll_freq=30, force_poll=False)` | Reconciles cancellation/submission, polls active actions, and progresses successful workflows. Used by JobManager or an application driving JobData manually. |

When a manager thread runs, call these JobData operations through its lock.
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
jobs. `initiateAction(job_id, name=None)` returns the remote ID when called by
ActionManager; exceptions are handled by its submission policy.
`queryResourceStatus()` returns `True`, `False`, or `None` for no available
signal. See [workflows](workflows.md) for parameter meanings.

## Resource queries

| Function | Result and notes |
| --- | --- |
| `api.getKnownMachines()` | List of machine names available through configured providers, plus `local`. |
| `api.queryMachineStatus(machine, rtype="compute")` | Boolean for `current_status == "up"`; local is `True`. Storage selection can require a path unavailable in this convenience signature; use resource discovery/`getResource` when necessary. |
| `api.getMachineQueues(machine)` | Configured `(name, description)` pairs; discovery does not fill queue definitions. |
| `api.getUserAccountProjects(machine)` | Provider-specific account/project names. IRI also supports querying by `implementation=`. |
| `api.getProjectAllocation(machine, project_name)` | List of allocation dictionaries returned for the project. Fields depend on the provider. |
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
| `api.remoteLs(machine, path)` | List of filenames returned by the provider. |
| `api.remoteMkdir(machine, path, create_parents=True, allow_unsafe=False)` | Creates a directory; checks supported destination paths. |
| `api.remoteChmod(machine, path, mode, allow_unsafe=False)` | Changes permissions; mode is a string such as `"700"`. |
| `api.pathStat(machine, remote_path, dereference=True)` | Provider stat output; SPOOF returns illustrative text. |
| `api.pathType(machine, remote_path)` | File-description string, using `file` for local paths. |
| `api.pathExists(machine, remote_path)` | Boolean derived from path-type output; subject to provider/SPOOF limitations. |
| `api.isDirectory(machine, remote_path)` | Boolean derived from path-type output. A trailing slash is treated as indicating a directory without an existence check. |
| `api.uploadBytes(machine, remote_path, content, allow_unsafe=False)` | Uploads `io.BytesIO` contents to a destination file. |
| `api.uploadSmallFile(machine, remote_path, local_path, allow_unsafe=False, definitely_is_file=False)` | Reads a local file and uploads it; remote directory paths append the local filename. |
| `api.downloadFileContents(machine, remote_path)` | Text/base64 string, according to provider behavior. |
| `api.downloadFile(machine, local_path_out, remote_path_in)` | Writes and returns the local filename; an existing destination directory receives the source filename. Native downloads currently lack a destination sandbox check. |

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
    remote_path_type="filesystem", remote_endpoint=None,
)
```

The last two functions require their optional arguments as keywords.
`globusCopy()` returns a task ID; `uploadFileGlobus()` blocks and returns a task
ID; `downloadFileGlobus()` blocks and returns a local filename.

| Function | Result and notes |
| --- | --- |
| `api.globusTransferStatus(transfer_id)` | Raw transfer status such as `ACTIVE`, `INACTIVE`, `SUCCEEDED`, or `FAILED`. |
| `api.findGlobusTransferByLabel(label)` | Unique matching ID or `None`; raises on multiple matches. The plural-named alias has the same single-result behavior. |
| `api.cancelGlobusTransfer(transfer_id)` | Sends a cancellation request; errors propagate. |
| `api.executeJob(...)` | Direct counterpart of the executable action, with `job_run_dir` instead of `rundir` and an optional `name`. Also accepts `allow_unsafe=False`. Returns a remote job ID. |
| `api.executeBatchJobCompat(...)` | Direct counterpart of the script action with `job_run_dir`, optional `name`, and `allow_unsafe=False`. Returns a remote job ID. |
| `api.getJobState(machine, jobid)` | Raw compute state, such as `queued`, `active`, `completed`, `failed`, or `canceled`. |
| `api.findJobByName(machine, name)` | Matching remote ID or `None`. |
| `api.cancelJob(machine, jobid)` | Sends a remote compute cancellation request. Local cancellation is unavailable. |

Direct submissions are not added to JobData. Use actions when the manager
should own ordering, monitoring, and cancellation. See [transfers](transfers.md)
for namespaces and staging.
