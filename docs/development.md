# Development and tests

[Documentation home](../README.md#documentation)

## Source layout

| Location | Responsibility |
| --- | --- |
| `src/amscjobmanager/manager.py` | Background progression and application lock |
| `workflow_data.py` | Workflow records and progression |
| `action_manager.py` | Action records, durable intent, reconciliation, retry, and status mapping |
| `actions_base.py`, `compute_actions.py`, `globus_transfer_action.py` | Action descriptions and API wrappers |
| `api_general.py` | Implementation selection and shared convenience API |
| `globals.py` | Registered sandboxes and path policy |
| `iri_api/` | Provider definitions, discovery, authentication, resource/filesystem/compute/transfer requests |
| `local_api.py`, `spoof_api.py` | Local execution and simulated service behavior |
| `manager_config_models.py`, `manager_config.py` | Configuration schema and setup |
| `examples/` | Runnable usage examples |
| `test/` | Unit tests, local integration suites, explicit live scripts, and shared fixtures |

Install in editable mode as described in [installation](installation.md).

## Offline tests

The test tree separates isolated unit checks, local component integration, and
explicit live checks. See the [test guide](../test/README.md) for the directory
layout, required capabilities, and live-script commands.

From the repository root:

```bash
python test/run_tests.py unit
python test/run_tests.py integration
python test/run_tests.py all

# Select a filename, or inspect the selection without importing tests.
python test/run_tests.py integration --pattern test_scheduling_reconciliation.py -v
python test/run_tests.py all --list
```

The runner defaults to unit tests and launches every selected module in a fresh
process using the active Python interpreter. This isolates backend selection and
mutable library state: SPOOF modules can select their backend without affecting
IRI tests. It runs from the repository root, adds `test/` to child `PYTHONPATH`,
and starts children with the IRI backend unless a SPOOF module overrides it.
It returns nonzero if any selected module fails. Live scripts are never selected.

Ordinary unittest discovery remains available for unit tests:

```bash
python -m unittest discover -s test/unit -t test -v
```

For a specific integration case, use its package-qualified name:

```bash
PYTHONPATH=test python -m unittest integration.workflows.test_submission_keys -v
```

`test/live/` has no package initializer, so ordinary discovery from `test/`
does not descend into it. Use the runner for combined offline runs rather than
combining backend-dependent modules in one interpreter.

The scheduling tests inject crashes around persisted workflow/action intents,
remote submission, cancellation, and resource pre-checks. Resource tests verify
that downtime/query failures consume no attempt, restart recovery repeats the
check, and name recovery precedes availability checks. SPOOF tests verify mixed
action ordering without remote credentials. Path tests exercise namespace,
staging, and sandbox rules using controlled configuration.

Local-write tests exercise checked output paths, directory destinations,
symlink escapes, `/tmp`, explicit overrides, and the native/Globus download
fallback contract. SPOOF filesystem tests also cover its real local writes.
Local cancellation tests run real subprocesses and exercise queued cancellation,
child processes, signal escalation, races, handle cleanup, and workflow retries.
They also verify worker ownership of signals/status updates, immediate event
wakeups, and retained cancellation intent when a process exits before a retry.
Path-validation tests ensure that `~` is rejected before filesystem writes,
authentication, or submission, including unsafe overrides and configured paths.

Polling tests verify that repeated status failures preserve action/workflow
state, allow other workflows to finish, and recover without duplicate
submission. They also run the actual background loop to check thread and lock
availability, and confirm that simulated crashes are not swallowed as API errors.

## Local server integration

The optional MCP server has a local subprocess integration test:
`python test/run_tests.py integration --pattern test_mcp_server.py -v`.
Install `.[server]` with `python -m pip install -e '.[server]'` first. No separate
Uvicorn command is required: the test starts its own HTTP listener. It needs
loopback socket access and runs a real local sleep job, canceled in cleanup.
See [the server guide](server.md#test-locally) for environment setup and commands.
`test_manager_failures.py` also injects failures into the real background worker
to check lock release, logging, and shutdown with unfinished work. MCP tests
verify unavailable-manager errors, continued tool discovery, and rejection of
restart after failure. Constructor-recovery tests create a new manager against
the same database and verify that existing work resumes without resubmission.

## Manual live checks

Read each script before running it. These commands can authenticate, create
remote files, or submit real jobs/transfers, and some intentionally leave
artifacts for inspection. Supply a configuration with the necessary sandbox
roots, credentials, accounts, collections, and personal staging.

| Script | Invocation and effect |
| --- | --- |
| `test_iri_discovery.py` | `python test/live/iri/test_iri_discovery.py manager-config.json IMPLEMENTATION --refresh`; authenticates the selected provider, prompts for mappings, and updates its configuration cache. |
| `test_aurora_job_names_live.py` | `python test/live/iri/test_aurora_job_names_live.py manager-config.json --account PROJECT`; compares rejected/accepted names using real Aurora submissions and requests cancellation. |
| `test_iri_authorization.py` | `python test/live/iri/test_iri_authorization.py manager-config.json`; sets up credentials. PAT validation is local and does not prove server acceptance. Unknown provider definitions must already be cached. |
| `test_iri_api_smoke.py` | `python test/live/iri/test_iri_api_smoke.py manager-config.json MACHINE`; real resource/filesystem queries and a short compute submission followed by cancellation. |
| `test_iri_file_roundtrip.py` | `python test/live/iri/test_iri_file_roundtrip.py manager-config.json`; real upload/list/download across configured mounted filesystems. |
| `test_globus_file_roundtrip.py` | `python test/live/globus/test_globus_file_roundtrip.py manager-config.json`; real blocking transfers through the personal collection. |
| `test_globus_transfer_lookup.py` | `python test/live/globus/test_globus_transfer_lookup.py manager-config.json`; submits a labeled IRI transfer and compares its ID with direct Globus label discovery. Requires the configured Perlmutter/DTN route. |
| `test_globus_operations_live.py` | `python test/live/globus/test_globus_operations_live.py manager-config.json frontier`; verifies the first sandbox's Globus mapping, then tests all implemented Globus filesystem/transfer operations in a unique child directory. Requires transfer credentials and a reachable personal endpoint. |
| `test_iri_compute_execution.py` | `python test/live/iri/test_iri_compute_execution.py manager-config.json perlmutter ACCOUNT`; submits native and compatibility jobs. Also supports Aurora. |
| `test_local.py` | `python test/live/iri/test_local.py manager-config.json`; launches real local commands. |
| `examples/mpi_workflow.py` | `python examples/mpi_workflow.py manager-config.json local --ranks-per-node 4`; compiles and executes the bundled MPI reduction and verifies the downloaded result. See [example configuration and remote usage](../examples/README.md). |

The comprehensive Globus check exercises mkdir (with and without parents),
stat/type/missing-path queries, hidden-file listings, binary/text/empty-file
uploads and downloads in filesystem and Globus namespaces, direct personal
staging, file and recursive copies, task status, both label-lookup wrappers,
and cancellation. Downloads verify content, not just task acceptance. It keeps
remote files plus its `/tmp` and personal-staging directories for inspection;
known outstanding nonblocking copies are canceled on failure. It never chooses
a later sandbox if the first has no Globus route. It does not run compute jobs
or test chmod/rename/delete, which are not implemented as Globus wrappers.
`--timeout` (default 300 seconds) and `--poll-interval` (default 2 seconds)
control explicit polling/label lookup; existing blocking upload/download waits
are unchanged. A cancellation race is reported when a small task completes
before cancellation takes effect.

## Adding a compute or transfer action

For short filesystem preparation or processing, use the built-in
[`APIAction` operations](api_actions.md) rather than a custom compute action.
They run under `APIActions`, which implements `ActionManager` directly and owns
its operation cursor. `Workers` runs API calls and authentication while the
manager owns all database checkpoints. Long-running local tasks remain compute actions targeting
`local`.

Define a module-level, pickle-compatible dataclass inheriting
`ComputeActionBase` or `TransferActionBase`. Keep its fields as the durable
description of requested work, rather than open clients, threads, or connections.

Implement `initiateAction(workflow_id, name=None)` to submit via the API and return
the remote identifier. Pass `name` through to the provider: recovery depends on
the deterministic name/label supplied by AsyncActionManager. Submission should return
after acceptance so the manager can monitor asynchronously. API errors currently
raise exceptions; a richer result protocol has not been introduced.

`queryResourceStatus()` has no submission side effects. It returns `True` when
ready, `False` when submission should wait, or `None` when the action has no
reliable availability query. A query exception also defers submission. The
compute base implements the IRI/local query for both compute action types;
the transfer base currently returns `None`. Override it when a new action has a
better resource signal.

`getInfo()` returns a dictionary of human-readable details; compute actions
inherit machine/queue/time fields, while transfer actions must supply their own
origin/destination description. The existing manager for that action class must
support its remote ID, status query, name lookup, and cancellation semantics.
If introducing a new action category, also extend `ActionClass`, `actionClass()`,
and WorkflowData's manager registry.

`ActionManager` is the abstract workflow-facing interface in `action_manager.py`.
Implementations own their database schemas and transitions. Its public methods
are `startOrResumeAction`, `cancelOrResumeCancelingAction`, `hasAction`, `getMaxActionId`, `queryRecordedStatus`,
`updateStatuses`, `waitForAction`, `getLiveActions`, `getActionInfo`, and
`stopWorkers`.
`getLiveActions` selects the LIVE group (ACTIVE and PROVIDER_HELD). EXECUTING
adds SCHEDULING and is used for execution supervision and recovery. These are
`ActionStatusGroup` members whose values are tuples of `ActionStatus` members.
`initializeActionStatusViews` creates the shared read-only SQL group and workflow
mapping views from the same definitions, including for standalone action managers.
`getMaxActionId` reports execution history, not IDs of stages that are still queued.
Internal reconciliation hooks are not part of the interface.
WorkflowData allocates all action IDs as primary keys when the workflow is enqueued and supplies the
same ID during recovery. "Resume" in `startOrResumeAction` specifically means
recovering supervision from persisted state after a worker stopped, for example
when constructing a new manager after shutdown or process failure. Normal execution needs no
repeated calls: calls for a live worker reuse it. This is not a user-facing
workflow pause/resume operation, and terminal actions are not restarted.
`startOrResumeAction` and `cancelOrResumeCancelingAction` return `None`: they
persist intent and start or signal supervision without waiting for provider work.
`queryRecordedStatus` observes recorded state without accepting or changing a
polling interval. Configure the interval at manager construction. Forced refresh
wakes the worker and may return before the requested API poll finishes.

The base supplies shared worker startup, interruption, error handling and cleanup.
Derived managers explicitly pass execution and cancellation callables to the
worker starters; neither callable is an abstract ActionManager method.
`_startExecutionWorker(action_id, execute_action=..., complete_cancellation=...)`
starts normal execution and retains the supplied cancellation callable for its
handoff. `_startCancellationWorker(action_id, complete_cancellation=...)` starts
cancellation recovery directly. Both callables take the global action ID and
return None; they own persistence/recovery using the derived manager's schema.
`_recordWorkerFailure` remains an internal abstract hook for unexpected errors. Shared `_startWorker`
handles registration, failures and cleanup for either operation. When execution has
finished running with a recorded CANCELING intent, its Future completion handoff starts
cancellation on a separate worker. The handoff is serialized with new requests
and shutdown, so execution and cancellation never perform work concurrently for
one action. Recovery of CANCELING goes directly to the cancellation starter;
shutdown suppresses the handoff and preserves intent.
`AsyncActionManager._executeAction` supervises remote actions with polling/retry
delays; `APIActions._executeAction` runs its remaining sequence
directly without a periodic loop or polling interval. Remote cancellation has
its own reconciliation/retry loop in `AsyncActionManager._completeCancellation`;
API cancellation simply records CANCELED after the current operation has finished running.
Keep ordinary API calls sequential inside these hooks, and use short database
transactions through WorkflowData's shared `LockedConnection` context manager.
Use `with self.conn as conn:` for every SQL operation and fetch results before
leaving the block. It yields a normal SQLite connection; neither connections
nor cursors require SQL-method wrappers. Never hold its lock across API I/O or
user input. WorkflowData construction calls `resumeActions` after its tables and head
view are initialized, restoring workers for current unfinished heads,
including ACTIVE actions. Workflow starting/progression does not trigger this
recovery pass. WorkflowManager is single-use; neither shutdown nor failure can be
cleared by calling start() on the same instance.

`AsyncActionManager` in `async_action_manager.py` supplies the existing compute
and transfer persistence, retries, polling, and crash recovery. `ComputeActions`
and `GlobusDataTransfers` inherit it and provide provider-specific operations.
New managers with different execution semantics can implement `ActionManager`
directly without adopting the asynchronous manager's database fields.

Actions should wrap API operations and
leave transaction, retry, and workflow decisions to the manager. The simulator
and test-only factory hooks let tests replace remote behavior without changing
the production persistence path.

## Server user interactions

`user_queries.UserQueryQueue` owns session-local pending questions, response
validation and condition waits; it uses no database or MCP dependency.
`server.runtime.ServerRuntime` owns the queue and asynchronous configuration/
manager startup. `server.runner.ManagerHTTPServer` constructs this runtime from
a manager factory. `server.tools.ManagerMCPServer` adapts backend-provided
function definitions; query models live in `user_queries`, and workflow models
live in `manager_models`. Its
`mcp_server` supports in-process SDK tests. Query tools use only the queue lock,
never the manager or SQLite lock. Runtime failure does not take down the listener.

`wfapiUserQuery` always calls the documented `api_query_user_func` hook. The
server utility installs `runtime.queries.ask` before startup and restores the
previous hook after shutdown; the default hook prompts in the terminal.
Worker threads set their own `_thread_current_worker` handle; no thread context
or ancestor shutdown signal is inherited. Action managers retain their own
registries, and authentication remains shared by credential context. Stopping an
individual caller leaves authentication pending, even if nobody still needs it.
Server shutdown explicitly calls `auth.stopAuthenticationWorkers`, alongside
closing the query queue and stopping its startup/manager workers. Late OAuth
responses are checked for shutdown before credentials are saved. Stopped workers
are replaced on later calls, so a new server can ask fresh questions.

Run `test_server_queries.py` for queue races, interruption, shared OAuth routing,
startup and shutdown tests. Run `test_mcp_queries.py` with the server extra for SDK
and real HTTP/CLI tests, including killing a queried server and recovering its
SQLite workflow with a fresh session. These use fake credentials/provider calls;
no live site is required. Query replies are omitted from diagnostic output.

## Crash hooks

`WorkflowData(..., _test_crash_at=POINT)` passes a test-only crash point to its action
managers. A matching hook raises `SimulatedCrash`; tests reopen the same database
with the hook disabled and reconcile against the simulated remote service.
Hooks inside a supervisor are captured by its Future; `waitForAction` surfaces
them to tests. `test_action_workers.py` also kills real child processes and
checks autonomous polling and restart recovery. Existing reconciliation unit
tests use the direct adapters in `test/support/action_manager_helpers.py` to exercise the
same sequential production methods with deterministic crash boundaries.

| Area | Hook names |
| --- | --- |
| Workflow submission | `workflow_head_scheduling_persisted`, `action_state_persisted_before_workflow_head_update` |
| Action submission | `action_intent_persisted`, `before_remote_submission`, `remote_submission_completed`, `remote_id_persisted`, `remote_status_obtained`, `action_state_persisted` |
| API sequences | `action_intent_persisted`, `api_operation_N_started`, `api_operation_N_completed`, `api_operation_N_checkpoint_persisted`, `api_action_completed` (N is the zero-based operation index) |
| Resource pre-check | `before_resource_status_check`, `resource_status_checked` |
| Cancellation | `workflow_head_canceling_persisted`, `action_canceling_persisted`, `remote_cancellation_completed`, `action_canceled_before_workflow_head_update` |

Do not enable these hooks in real applications. They simulate abrupt termination
at explicit boundaries and should propagate through operational error handlers.
The critical invariant is that intent is committed before the remote mutation;
resource checks occur before committing a new submission attempt.

## Documentation updates

Keep examples and guide statements aligned with actual behavior. In particular,
check backend selection before imports, path namespaces, destination argument
order, keyword-only arguments, parameter units, state transitions, and sandbox
exceptions. Run the SPOOF reference example after changing its walkthrough.
Describe capabilities and error handling that the code currently implements.

## Native workflow schemas and MCP lifecycle tools

Built-in stage/operation definitions use Pydantic dataclasses with strict field
shapes and forbidden extra keys. Keyword-only `type` discriminator fields preserve
positional Python constructors. `workflow.Workflow` is a nonempty tagged stage
list, and `workflow_adapter` validates/dumps this same representation for Python
JSON use and the MCP submission tool. APIAction has its own tagged operation
union. WriteFile serializes bytes as a Base64 object while keeping raw bytes in
Python and pickle storage. Custom Python actions remain supported by WorkflowData;
only the built-in union is accepted from JSON/MCP.

`submit_workflow` and `cancel_workflow` in `server/tools.py` use the public
WorkflowData methods under the WorkflowManager lock, with health checks before/after lock
acquisition. They return a small workflow ID/status receipt without awaiting
provider work. Cancellation always requests remote cancellation. Submission keys
are delegated to WorkflowData; tools do not maintain another deduplication store.
The generic CLI discovers these tools and prints nested union schemas in help.
Run `test_workflow_schema.py` and `test_mcp_workflows.py` for native JSON/pickle,
malformed inputs, local execution, simulated Globus work, HTTP/CLI lifecycle and
submission-key recovery tests.


## Exposing backend functions

`WorkflowData.getManagerClasses()` enumerates registered `ActionClass` values;
`getManager(action_class)` retrieves their existing instances. WorkflowData and
action managers advertise
transport-independent `FunctionDefinition` tuples through `getMCPFunctions()`
and `getAdminFunctions()`. The ActionManager base implementation returns empty tuples.

Definitions in `amscjobmanager.manager_functions` contain an external name,
description, annotated `invoke` callable, effect/repeatability hints, and admin
acknowledgment. There are no stored input/output model classes, method names,
or fixed-argument dictionaries. `inspect.signature` and `get_type_hints` infer
models from the callable's second parameter and return annotations. Keep models
at module scope when using postponed annotations. The callable accepts its owner
and a validated input instance positionally and returns its annotated output
model. Input models should derive from `FunctionArguments` to reject extra fields
and hide values in validation errors. Output models can preserve their own field
layout; no mandatory `result` wrapper is imposed. Declaration order of input
fields determines admin CLI positional ordering.

Getters define local named `_functions` for their operations. `ComputeActions`
provides wallet adapters, `WorkflowData` provides workflow and sandbox adapters,
and `UserQueryQueue` provides authentication question/response adapters. Each
callable receives its owner explicitly and must not capture the discovery-time
instance. Enable/disable adapters pass their Boolean directly to the public
method. Existing tool names, response shapes and privileges are retained.

`ServerRuntime` builds `FunctionRegistry` from WorkflowData, its action managers,
and the query provider
before publishing readiness. Duplicate names and invalid/missing model
annotations fail initialization. MCP and admin maps remain separate. At dispatch
the registry validates input, resolves the current provider, invokes once, and
validates the returned model. WorkflowData and action-manager callbacks share
one dispatch path that acquires the WorkflowManager lock, checks its health,
and resolves the current owner. Direct callers can instead supply an already-locked
WorkflowData without constructing a background manager. Query operations remain
available without WorkflowManager. Registry code contains no operation definitions,
and MCP code handles only transport/schema adaptation. Do not perform provider
calls or user waits while holding the WorkflowManager lock; existing workers handle
such work.

The admin wire format uses `AdminDiscoveryRequest`/`AdminCallRequest` and
`execute_admin_request`. Discovery has no password and returns only public
`FunctionDescription` models. The shared CLI code consumes those models rather
than transport objects. MCP clients normalize SDK tool schemas to the same
format. Required admin arguments are positional, while MCP arguments remain
flags. New manager functions need no changes to frontend parsers or dispatchers.
