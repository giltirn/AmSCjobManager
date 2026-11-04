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
| Remote cancellation | Compute and Globus APIs | Terminates the local job's process group | Simulated cancellation |
| State after process restart | Persistent workflow DB plus remote discovery | Limited local status tracking; see below | Simulated remote state is lost |
| Sandbox enforcement | Supported writes are checked; see exceptions | Local data outputs and run directories are checked | Real local downloads are checked; remote mutations are simulated |

## IRI

IRI supplies compute, filesystem, and resource APIs through facility-specific
providers. A provider tag such as `nersc` identifies an API implementation; a
machine name such as `perlmutter` identifies a resource within that provider.
Globus collection UUIDs identify data access locations and are a third kind of
identifier.

Built-in definitions include NERSC, ALCF, and OLCF machines. Treat the definitions in
[`iri_api/implementations/`](../src/amscjobmanager/iri_api/implementations/) as the
source for default names and routes. Same-name definitions in the user cache
replace those defaults in full. Calls requiring a provider fail if its credentials
were not configured. API v1 and v2 differ in available queries and request
formats; for example, remote `getUsername()` requires v2.

ALCF's configuration routes file upload/download operations through Globus where
the filesystem API does not supply them. Those fallbacks require transfer
authentication and a personal collection. Its `pathStat` operation uses Globus
metadata, without requiring a personal collection. Fresh discovery of another provider does
not automatically reproduce these special cases or discover Globus routes.

The OLCF definition supplies `frontier` through the v2 API at
`https://amsc-moderate.s3m.olcf.ornl.gov/api/v2`. Configure its credentials with
`token_type="amsc_pat"` and the PAT JSON path. The manual Globus route uses
**OLCF DTN (Globus 5)** (`36d521b3-c182-4071-b7d5-91db5d380d42`), with `/`
mapped to `/`, preserving absolute filesystem paths. Use `uploadFileGlobus`,
`downloadFileGlobus`, or `globusCopy` for this route. OLCF's packaged definition
also routes listings, path-type checks, stat, mkdir, and file upload/download
through Globus; chmod raises `NotImplementedError`. Metadata and mkdir need
only transfer credentials; file uploads/downloads also require a configured
personal endpoint. The native filesystem mapping to `frontier` at `/` is
retained but bypassed for the configured fallbacks.
The root mapping describes path translation;
configured sandbox checks still restrict writes.

OLCF account discovery is marked unavailable. Set
`iri_implementations.olcf.accounts` in your user configuration to your project
names for account listing and job submission. These names do not provide project
IDs for allocation queries; the smoke test skips that query for this provider.

Frontier's queue list describes the `batch`, `extended`, and `service` partitions
using the [Frontier Slurm policies](https://docs.olcf.ornl.gov/systems/frontier_user_guide.html#slurm).
`debug` and `develop` are QoS classes, not partitions; `service` requires
`develop` QoS. The library forwards `queue` as `queue_name` without a separate
QoS argument, so listing a partition does not configure its required QoS.

An existing `olcf.json` in the user cache replaces this packaged definition in
full. To use the packaged route, move that cache file outside the cache directory
or add the filesystem mapping to your cached definition. Rediscovery currently
does not preserve manually supplied filesystem routes.

## Local execution

Select IRI, configure a `local` sandbox, and construct a compute action with
`machine="local"`. Executable actions use `local_api.executeJob()` and batch
script actions use `local_api.executeJobScript()`. Both share a background
runner that starts a Bash process and manages status, logs, and cancellation.
There is no local batch scheduler. Local executable jobs use
`nodes * ranks_per_node` as their total rank count. With `local_mpi_launcher`
configured, execution uses `launcher -n RANKS executable arguments...`;
otherwise only one rank is accepted and execution is direct. All processes
run locally; node placement, GPU allocation, account, queue, and time are not
enforced as scheduler resource limits. Arbitrary batch scripts control their
own launcher commands.

Before starting the worker, the runner checks the run directory and resolved
wrapper/log destinations against local sandboxes and `/tmp`, unless explicitly
overridden with `allow_unsafe=True`. It writes `exec_wrap.sh` into the run directory and captures output in
`run.<remote-job-id>.log`. Give simultaneous local jobs distinct run directories
because the wrapper filename is shared within a directory. A local executable
action runs its pre-launch commands, executable, and post-launch commands in
sequence, adding the configured MPI launcher around the executable only.
Pre-launch exports and directory
changes carry into execution. A failing pre-launch skips execution and
post-launch. Post-launch runs in a subshell only after successful execution;
failure in any stage stops the job and marks it failed.

`remoteLs`, `remoteChmod`, `pathStat`, `downloadFileContents`, `remoteMkdir`,
`pathType`, uploads, downloads, and compute submission dispatch locally.
Listings return sorted absolute child paths, including hidden entries.
Permission changes check the resolved target against local sandboxes and `/tmp`
unless `allow_unsafe=True`. File reads and metadata queries are unrestricted;
`pathStat(..., dereference=False)` inspects the symlink itself.
General IRI resource discovery still requires an IRI machine.
`cancelJob("local", jobid)` prevents a queued job from starting or terminates a
running job's process group. It sends `SIGTERM`, escalates to `SIGKILL` after a
one-second grace period, and waits for termination before recording `canceled`.
The caller signals an event and waits for acknowledgment; the worker performs
termination, records the final status, and releases handles. Cancellation wakes
the worker immediately; normal process completion is checked every 0.1 seconds.
Repeated cancellation leaves terminal jobs unchanged. Normal child processes
are included; processes that deliberately detach into a new session are not.
Do not use local execution to test facility scheduler or launcher behavior.

Local job status uses a separate SQLite database, `localjobs.db` by default in
the working directory. Set its path before the first local job/status operation:

```python
from amscjobmanager.local_api import setStatusManagerDatabasePath

setStatusManagerDatabasePath("/absolute/path/localjobs.sqlite")
```

`None` selects an in-memory status database. Stored local statuses do not provide
the remote scheduler's recovery guarantees: process IDs are not used to
reconcile surviving jobs after an application crash. Cancellation handles exist
only in memory and cannot be recovered after a restart. Worker exceptions mark
the job failed; signaling errors are raised to the caller while the worker stays
alive with its handles for cancellation retries.
See [troubleshooting](troubleshooting.md).

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
