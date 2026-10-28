# Development and tests

[Documentation home](../README.md#documentation)

## Source layout

| Location | Responsibility |
| --- | --- |
| `src/amscjobmanager/manager.py` | Background progression and application lock |
| `job_data.py` | Workflow records and progression |
| `action_manager.py` | Action records, durable intent, reconciliation, retry, and status mapping |
| `actions_base.py`, `compute_actions.py`, `globus_transfer_action.py` | Action descriptions and API wrappers |
| `api_general.py` | Implementation selection and shared convenience API |
| `globals.py` | Registered sandboxes and path policy |
| `iri_api/` | Provider definitions, discovery, authentication, resource/filesystem/compute/transfer requests |
| `local_api.py`, `spoof_api.py` | Local execution and simulated service behavior |
| `manager_config_models.py`, `manager_config.py` | Configuration schema and setup |
| `examples/` | Runnable usage examples |
| `test/` | Offline suites and manual live scripts |

Install in editable mode as described in [installation](installation.md).

## Offline tests

Run individual suites using discovery from `test/`. This avoids a possible
collision with Python's own `test` package:

```bash
python -m unittest discover -s test -p test_compute_resource_scheduling.py
python -m unittest discover -s test -p test_scheduling_reconciliation.py
python -m unittest discover -s test -p test_spoof_workflows.py
```

Each command starts a separate process. SPOOF suites select their backend before
package imports; combining backend-dependent suites in one process can make
their import order affect behavior.

Additional offline suites can be run explicitly:

```bash
for suite in \
    test_globus_path_namespaces.py \
    test_iri_discovery.py \
    test_iri_lazy_setup.py \
    test_iri_public_api.py \
    test_iri_queries.py \
    test_iri_storage_paths.py \
    test_local_username.py \
    test_spoof_filesystem.py
do
    python -m unittest discover -s test -p "$suite" || exit 1
done
```

Do not use a broad `test_*.py` discovery command: this directory also contains
scripts that execute real operations or expect command-line arguments at
import time.

The scheduling tests inject crashes around persisted workflow/action intents,
remote submission, cancellation, and resource pre-checks. Resource tests verify
that downtime/query failures consume no attempt, restart recovery repeats the
check, and name recovery precedes availability checks. SPOOF tests verify mixed
action ordering without remote credentials. Path tests exercise namespace,
staging, and sandbox rules using controlled configuration.

Polling tests verify that repeated status failures preserve action/workflow
state, allow other workflows to finish, and recover without duplicate
submission. They also run the actual background loop to check thread and lock
availability, and confirm that simulated crashes are not swallowed as API errors.

## Manual live checks

Read each script before running it. These commands can authenticate, create
remote files, or submit real jobs/transfers, and some intentionally leave
artifacts for inspection. Supply a configuration with the necessary sandbox
roots, credentials, accounts, collections, and personal staging.

| Script | Invocation and effect |
| --- | --- |
| `test_iri_authorization.py` | `python test/test_iri_authorization.py manager-config.json`; sets up credentials. PAT validation is local and does not prove server acceptance. Unknown provider definitions must already be cached. |
| `test_iri_api_smoke.py` | `python test/test_iri_api_smoke.py manager-config.json MACHINE`; real resource/filesystem queries and a short compute submission followed by cancellation. |
| `test_iri_file_roundtrip.py` | `python test/test_iri_file_roundtrip.py manager-config.json`; real upload/list/download across configured mounted filesystems. |
| `test_globus_file_roundtrip.py` | `python test/test_globus_file_roundtrip.py manager-config.json`; real blocking transfers through the personal collection. |
| `test_globus_transfer_lookup.py` | `python test/test_globus_transfer_lookup.py manager-config.json`; submits a labeled IRI transfer and compares its ID with direct Globus label discovery. Requires the configured Perlmutter/DTN route. |
| `test_iri_compute_execution.py` | `python test/test_iri_compute_execution.py manager-config.json perlmutter ACCOUNT`; submits native and compatibility jobs. Also supports Aurora. |
| `test_local.py` | `python test/test_local.py manager-config.json`; launches real local commands. |

## Adding a compute or transfer action

Define a module-level, pickle-compatible dataclass inheriting
`ComputeActionBase` or `TransferActionBase`. Keep its fields as the durable
description of requested work, rather than open clients, threads, or connections.

Implement `initiateAction(job_id, name=None)` to submit via the API and return
the remote identifier. Pass `name` through to the provider: recovery depends on
the deterministic name/label supplied by ActionManager. Submission should return
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
and JobData's manager registry/table handling.

ActionManager owns database transitions. Actions should wrap API operations and
leave transaction, retry, and workflow decisions to the manager. The simulator
and test-only factory hooks let tests replace remote behavior without changing
the production persistence path.

## Crash hooks

`JobData(..., _test_crash_at=POINT)` passes a test-only crash point to its action
managers. A matching hook raises `SimulatedCrash`; tests reopen the same database
with the hook disabled and reconcile against the simulated remote service.

| Area | Hook names |
| --- | --- |
| Workflow submission | `job_head_scheduling_persisted`, `action_state_persisted_before_job_head_update` |
| Action submission | `action_intent_persisted`, `before_remote_submission`, `remote_submission_completed`, `remote_status_obtained`, `action_state_persisted` |
| Resource pre-check | `before_resource_status_check`, `resource_status_checked` |
| Cancellation | `job_head_canceling_persisted`, `action_canceling_persisted`, `remote_cancellation_completed`, `action_canceled_before_job_head_update` |

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
