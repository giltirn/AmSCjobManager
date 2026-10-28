# Troubleshooting and current limitations

[Documentation home](../README.md#documentation)

## Import and configuration problems

| Symptom | What to check |
| --- | --- |
| `ModuleNotFoundError: amscjobmanager` | Install into the interpreter running the application with `python -m pip install -e .`. |
| Syntax error while importing | Use Python 3.12 or newer. The package currently has no declared Python-version installer constraint. |
| Real IRI behavior when SPOOF was expected | Set `AMSC_JOB_MANAGER_API_IMPL` before any package import, then restart the process/kernel. |
| “setupWorkflowAgent has not been called” | Call `setupManager(config)` or `api.setupWorkflowAgentFromConfig(config)` before path/API operations. |
| Unknown machine or implementation | Check configured providers and names returned by `getKnownMachines()`. A provider tag, machine name, and collection UUID are different targets. |
| Discovery demands a cache directory | Set `iri_config_cache_directory`. Unknown providers also need an API URL and version for unattended use. |
| Missing sandbox creation prompt | Pre-create intended directories, or run initialization interactively and approve their creation. Pending remote validation can occur on first provider use. |

The template generator preserves an existing file. If its placeholders appear
unchanged after rerunning it, edit that file or choose another output path
through `ManagerConfig.write_template_json()`.

## Authentication and provider capabilities

Token setup and protected API access are different checks. A correctly formatted
PAT file can still contain an expired or rejected credential. For Globus,
complete login/consent for the intended identity and collection. Compute token
files do not replace the transfer token file.

The manual authorization script:

```bash
python test/test_iri_authorization.py manager-config.json
```

sets up credentials for configured providers. It does not prove that a PAT is
accepted by a server, run sandbox validation, or log in to the transfer service.
Unknown provider definitions must already be cached for this script's
cache-only setup. Other manual scripts make real requests and can submit jobs
or write remote files; see [development](development.md).

`getUsername()` on a remote v1 provider raises `NotImplementedError`.
ALCF-specific filesystem gaps can require Globus staging or raise an
unsupported-operation error. Check [implementations](implementations.md), the
provider definition, mounted filesystems, and collection routes rather than
assuming every API method is supported everywhere.

## Path and transfer failures

“Outside sandbox” means the destination/run directory did not pass the relevant
path policy. Confirm the machine, absolute path, configured root, and selected
namespace. A filesystem path may need a prefix removed or changed for Globus.
See [transfers](transfers.md).

A missing/ambiguous Globus route requires configuration or an explicit
collection UUID/tag. `path_type="filesystem"` requires a known configuration;
an unknown UUID cannot supply filesystem-to-Globus translation. For overlapping
Globus namespaces, select the desired collection explicitly.

For personal collection issues, check that the two staging paths identify the
same physical directory, the personal client is running with access to it, and
direct personal paths remain below staging. Globus downloads outside staging
require a configured local sandbox, even when the destination is under `/tmp`.

Inspect the transfer's raw status and service event log using its task ID.
Manager `FAILED` can represent a transfer's `INACTIVE` state, which may still
need intervention or explicit cancellation. A remotely canceled transfer also
appears as Globus `FAILED`.

The native `downloadFile()` implementation distinguishes plaintext from base64
using a heuristic; some text can look like base64. Use a binary-preserving
Globus path when exact file contents matter. Native downloads also lack the
Globus helper's local destination sandbox check.

## Workflows that do not advance

Inspect `jobState(job_id)["head_action_status"]`, not just `jobStatus(job_id)`:
the latter reports a scheduling workflow as `ACTIVE`.

- `PENDING`: confirm the manager was started and automatic slots are available.
- `SCHEDULING`: check resource availability, failed status requests, submission
  backoff, remote name discovery, and the action's `last_submission_error`.
- `ACTIVE`: inspect the remote task and job output; a queued scheduler job is
  considered active by this library.
- `FAILED` or `CANCELED`: the manager does not automatically advance or restart
  these workflows.
- `CANCELING`: check cancellation/status API errors and allow reconciliation to
  retry. Local process cancellation is not supported.

Deferred scheduling and cancellation consume automatic workflow slots. If all
slots are occupied by a down machine's workflows, other pending workflows can
wait even when their machines are available. Explicit manual starts bypass the
automatic slot selection and should be used deliberately.

There is no general public retry-from-stage function. Re-enqueuing creates a new
workflow and can repeat prior operations. Inspect and cancel old remote work
before choosing a recovery strategy.

## A stopped or unresponsive manager

Status polling errors are logged per action. The manager keeps the last known
status, polls other workflows, and retries the failed query on a later pass.
An outage can therefore leave a completed remote task appearing active until
its status becomes readable; its workflow waits rather than advancing early.
Polling errors after submission also leave the scheduling head available for
reconciliation without submitting a duplicate job.

If the manager thread has stopped, inspect its traceback for an error outside
these handled status polls. Such uncaught errors can still leave its lock held.
`isAlive()` detects a dead thread without acquiring the lock; lock-based status
queries may block if an unexpected error left it held.

After recording the error, restart the application with the same persistent
workflow database and API configuration to recover remote work. Do not assume
calling `start()` again on the same object repairs its held lock.

`stop(wait_until_done=True)` can also wait indefinitely during resource downtime,
unresolved cancellation, or thread failure. A non-waiting stop still joins an
in-flight request; API requests may have long timeouts. Stopping the manager
does not cancel remote tasks.

## Local execution limitations

The local runner uses a worker thread and Bash rather than a batch scheduler.
Resource requests and `time` do not constrain the process. Missing run
directories, permission errors, or other worker exceptions can leave status
active because not all worker failures update the local status database.
Concurrent local jobs sharing a directory overwrite `exec_wrap.sh`.

Local cancellation is unavailable, and durable local metadata does not track
processes after application failure. Several filesystem methods have no local
dispatch. The low-level `localCopyFile(..., allow_unsafe=False)` also currently
references an undefined variable during its check; avoid treating it as a
validated copy primitive. Native `downloadFile("local", ...)` bypasses that
check and does not enforce a destination sandbox.

## Persistence and version changes

A workflow database stores pickle objects with Python class/module references.
Changing action class definitions or moving their modules can break recovery.
There is no general database migration/version compatibility guarantee. Back up
state before changing the code used by a running application, keep compatible
class definitions, and open only trusted databases.

SPOOF records its fake remote service state only in process memory. An on-disk
workflow database does not make a SPOOF run recoverable across processes.
