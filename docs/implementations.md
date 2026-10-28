# IRI, local execution, and SPOOF

[Documentation home](../README.md#documentation)

The backend selector supports `IRI` and `SPOOF`. Within IRI, supported operations
on `machine="local"` dispatch to the local OS. Configuration, authentication,
and backend selection are process-wide; use a fresh process to switch backends.

## Capability summary

| Behavior | IRI remote machines | IRI with `machine="local"` | SPOOF |
| --- | --- | --- | --- |
| Compute submission | Real provider jobs | Real shell processes on the manager host | Simulated completion |
| Globus collection transfers | Real transfers through the IRI movement service | Use a configured personal collection for OS-visible files | Simulated tasks |
| Filesystem operations | Provider-dependent, with some Globus fallbacks | Local listings, permissions, metadata, uploads, and downloads | Mostly logs and fixed illustrative responses |
| Authentication | Provider tokens; separate transfer credentials | No remote token needed for local operations | No tokens read |
| Resource availability | Queries IRI resource status | Reports up | Reports up |
| Remote cancellation | Compute and Globus APIs | Local process cancellation is not implemented | Simulated cancellation |
| State after process restart | Persistent workflow DB plus remote discovery | Limited local status tracking; see below | Simulated remote state is lost |
| Sandbox enforcement | Supported writes are checked; see exceptions | Local data outputs and run directories are checked | Real local downloads are checked; remote mutations are simulated |

## IRI

IRI supplies compute, filesystem, and resource APIs through facility-specific
providers. A provider tag such as `nersc` identifies an API implementation; a
machine name such as `perlmutter` identifies a resource within that provider.
Globus collection UUIDs identify data access locations and are a third kind of
identifier.

Built-in definitions include NERSC and ALCF machines. Treat the definitions in
[`iri_api/config.py`](../src/amscjobmanager/iri_api/config.py) as the source for
supported names and routes. Calls requiring a provider fail if its credentials
were not configured. API v1 and v2 differ in available queries and request
formats; for example, remote `getUsername()` requires v2.

ALCF's configuration routes file upload/download operations through Globus where
the filesystem API does not supply them. Those fallbacks require transfer
authentication and a personal collection. Its configured `pathStat` operation
currently raises `NotImplementedError`. Fresh discovery of another provider does
not automatically reproduce these special cases or discover Globus routes.

## Local execution

Select IRI, configure a `local` sandbox, and construct a compute action with
`machine="local"`. Both compute action types ultimately run through
`local_api.executeJobScript()` in a worker thread, which starts a Bash process.
There is no local batch scheduler: nodes, ranks, GPUs, account, queue, and time
do not allocate or limit local resources.

Before starting the worker, the runner checks the run directory and resolved
wrapper/log destinations against local sandboxes and `/tmp`, unless explicitly
overridden with `allow_unsafe=True`. It writes `exec_wrap.sh` into the run directory and captures output in
`run.<remote-job-id>.log`. Give simultaneous local jobs distinct run directories
because the wrapper filename is shared within a directory. A local executable
action runs its pre-launch commands, executable, and post-launch commands in
sequence; it does not add MPI launch behavior.

`remoteLs`, `remoteChmod`, `pathStat`, `downloadFileContents`, `remoteMkdir`,
`pathType`, uploads, downloads, and compute submission dispatch locally.
Listings return sorted absolute child paths, including hidden entries.
Permission changes check the resolved target against local sandboxes and `/tmp`
unless `allow_unsafe=True`. File reads and metadata queries are unrestricted;
`pathStat(..., dereference=False)` inspects the symlink itself.
General IRI resource discovery still requires an IRI machine.
`cancelJob("local", ...)` has no local
cancellation implementation. Do not use local execution to test remote
scheduler, launcher, or cancellation behavior.

Local job status uses a separate SQLite database, `localjobs.db` by default in
the working directory. Set its path before the first local job/status operation:

```python
from amscjobmanager.local_api import setStatusManagerDatabasePath

setStatusManagerDatabasePath("/absolute/path/localjobs.sqlite")
```

`None` selects an in-memory status database. Stored local statuses do not provide
the remote scheduler's recovery guarantees: process IDs are not used to
reconcile surviving jobs after an application crash, and a worker exception may
leave a stale active status. See [troubleshooting](troubleshooting.md).

## SPOOF

SPOOF exercises real workflow orchestration with fake remote services. Compute
and transfer submissions return synthetic IDs and normally complete after a
random one-to-three-second interval. It records submitted, completed, and
canceled events for inspection.

It does not execute commands or retain a remote filesystem. Listings, metadata,
file types, project information, and downloaded contents are illustrative.
`downloadFile()` writes placeholder contents to a checked real local destination, and
`pathType("local", ...)` uses the real local `file` command. The reference example
also creates real temporary input files and a workflow database.

SPOOF does not enforce the sandbox policy for simulated remote filesystem or
known-endpoint mutations. Its real local `downloadFile()` writes are checked
against local sandboxes and `/tmp`, with the usual `allow_unsafe` override.
It does apply personal staging/path translation rules for relevant transfers.
The blocking `uploadFileGlobus()` and `downloadFileGlobus()` helpers raise
`NotImplementedError` under SPOOF. Its simulated `globusCopy()` returns promptly
regardless of the `block_until_complete` argument.

For deterministic tests:

```python
from amscjobmanager import spoof_api

spoof_api.resetSpoofActionState(completion_delay=0)
history = spoof_api.getSpoofActionHistory()
```

Call reset before submitting the work you want to inspect. It clears fake job
and transfer records. SPOOF service state is in memory, so a persisted workflow
database alone cannot recover a SPOOF run in a different process.
