# Paths and data transfers

[Documentation home](../README.md#documentation)

## Choose the path namespace

A filesystem path is interpreted by a machine or the manager's OS. A Globus
path is interpreted by a collection. The prefixes can differ for the same file.
For example, the configured ALCF Eagle mapping removes `/lus/eagle/projects`
from the path presented to its collection.

Use `source_path_type` and `dest_path_type` independently on `globusCopy()` and
`GlobusTransfer`. Both default to `"globus"`.

```python
from amscjobmanager import api_general as api

transfer_id = api.globusCopy(
    dest_endpoint="perlmutter",
    dest_path="/global/cfs/cdirs/MY_PROJECT/example/workflows/input/",
    source_endpoint="polaris",
    source_path="/lus/eagle/projects/MY_PROJECT/example/input.dat",
    dest_path_type="filesystem",
    source_path_type="filesystem",
    label="example-input-copy",
)
```

This illustrative call requires corresponding configured filesystem/Globus
routes, sandbox destinations, credentials, and collection permissions. Use names
returned by `api.getKnownMachines()` for your setup.

For filesystem paths, the library selects the configured route with the longest
matching filesystem root and converts the path. A UUID can be used in this mode
only if it maps to a known configuration. Missing or ambiguous routes raise
`ValueError`; the library does not guess a machine mapping.

Globus path mode accepts collection UUIDs, configured tags, and machine names.
A machine name must resolve to a unique collection for the supplied path. With
overlapping collection namespaces, use a UUID or endpoint tag to select one
explicitly. Unknown endpoint strings are passed through for the service to
validate. Paths must be absolute and may not contain `..` or `~`. Local upload
sources and download destinations also reject `~`; supply the intended user's
explicit path rather than relying on the manager host's home directory.

## Destination and source semantics

The direct function takes the destination before the source:

```text
globusCopy(dest_endpoint, dest_path, source_endpoint, source_path, ...)
```

`dest_path` is a **directory**. A source file keeps its filename. A directory
source copies its contents into that destination directory. This function does
not implement arbitrary file renaming; use the blocking file helpers below
when moving individual files between the manager host and a remote machine.

The `GlobusTransfer` action adds a trailing slash to its destination and replaces
`<JOBID>` in its source/destination path strings with the workflow's integer ID.
The compute actions do not substitute that placeholder in their run directories.

The built-in `dtn` tag selects the NERSC DTN collection. `personal` is registered
only when a personal endpoint is configured. `listSpecialGlobusEndpoints()` lists
the available tags. A machine name and a special tag are different routing
mechanisms; do not assume they are interchangeable.

## Asynchronous actions and direct calls

Managed `GlobusTransfer` actions submit without waiting for completion. The
manager persists the returned task ID and polls it. A later workflow action
starts only after `SUCCEEDED` is mapped to `ActionStatus.COMPLETED`.

Direct `api.globusCopy()` is also nonblocking by default and returns a transfer
ID. With `block_until_complete=True`, it polls at 20-second intervals until the
task stops reporting `ACTIVE`; a result other than `SUCCEEDED` raises an error.
Direct calls are not added to a workflow database.

```python
status = api.globusTransferStatus(transfer_id)
api.cancelGlobusTransfer(transfer_id)
```

Submission/status requests use the IRI movement API. Label lookup and
cancellation use the Globus Transfer API directly. They require the separately
configured transfer credentials. Do not reuse labels for unrelated transfers:
`findGlobusTransferByLabel(label)` expects a unique exact match and raises if
multiple tasks match.

## Blocking file helpers

The file helpers keep `machine` first and destination before source:

```python
api.uploadFileGlobus(
    "perlmutter",
    "/global/cfs/cdirs/MY_PROJECT/example/workflows/input/settings.json",
    "/home/example/settings.json",
)

local_path = api.downloadFileGlobus(
    "perlmutter",
    "/home/example/workflows/result.json",
    "/global/cfs/cdirs/MY_PROJECT/example/workflows/output/result.json",
)
```

The remote path names a **file** and defaults to the machine's filesystem
namespace. `remote_path_type="globus"` selects the collection namespace instead;
`remote_endpoint` can disambiguate the collection in that mode.

`uploadFileGlobus()` returns a transfer ID after completion.
`downloadFileGlobus()` returns the final local filename after completion. An
existing local destination directory receives the remote source filename.
These helpers can rename files while staging. They require a personal endpoint
and are unavailable in SPOOF.

`downloadFileGlobus(..., allow_unsafe=False)` checks its final local destination.
Configured local sandboxes, `/tmp`, and the configured staging root are allowed.
Set `allow_unsafe=True` to authorize another local destination explicitly. The
same option on `downloadFile()` is forwarded when a provider uses this fallback.

## Personal staging rules

For direct transfers, personal collection paths must stay within configured
staging. A direct transfer does not move the file elsewhere afterward. These two
paths could address the same staging file:

```text
Globus:     /staging/input.dat
Filesystem: /home/example/globus-staging/input.dat
```

The blocking helpers bridge local paths outside staging by creating temporary
`.amsc-globus-*` staging directories, transferring, and cleaning up in `finally`
blocks. Downloads outside staging require a configured local sandbox or `/tmp`,
unless `allow_unsafe=True` explicitly permits the output path.
See [sandboxing](sandboxing.md). A process crash can leave staging directories
behind; check active transfers before removing them.

## Small-file filesystem operations

`uploadSmallFile()` reads a manager-host file and calls `uploadBytes()`.
`downloadFileContents()` returns provider output as text or a base64 string;
it is not a general bytes-returning API. `downloadFile()` attempts to distinguish
text from base64 when writing the result. For binary data whose exact contents
matter, use the Globus helpers when supported. Provider-specific fallbacks may
perform those filesystem calls through staging automatically.
