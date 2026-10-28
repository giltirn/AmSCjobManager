# Sandboxing and its limits

[Documentation home](../README.md#documentation)

The sandbox configuration restricts destination paths and compute run
directories in supported library calls. It is an application-level path policy.
It does not create containers, change OS privileges, restrict a batch script's
commands, or impose filesystem permissions on a remote job.

## Filesystem paths

`sandbox_directories` maps machine names to one or more filesystem roots. A
supported write is allowed if its destination is the configured root or a
descendant. Workflows submit compute actions with `allow_unsafe=False` and check
their run directories through the same path policy.

```json
{
  "sandbox_directories": {
    "perlmutter": ["/global/cfs/cdirs/MY_PROJECT/example/workflows"],
    "local": ["/home/example/workflows", "/scratch/example/results"]
  }
}
```

`globals.checkSafePath(machine, path)` also permits paths under `/tmp` by default.
It accepts a string or `Path`, rejects `~`, and resolves symlinks internally;
callers do not need to normalize paths just for validation. The boolean result
does not change the caller's path, so filesystem operations still resolve it
where needed for their own use.

Path arguments and configured path fields must not contain `~`, including in
local paths or literal filenames such as `result~`. The manager host's username
and home directory may differ from the user or remote resource's account. Use
explicit paths instead. This validation also applies with `allow_unsafe=True`;
that option bypasses sandbox membership checks, not path syntax validation.
Local `/tmp` paths are available even when no local sandbox has been configured.
Callers can set `allow_tmp=False` to require a configured sandbox root. Other
checks require configuration and a known machine. Use absolute paths;
filesystem checks resolve paths on the manager host and cannot inspect remote
symlink targets or prevent remote changes between checking and writing.

Operations such as `remoteMkdir`, `remoteChmod`, `uploadBytes`, and direct compute
submission accept `allow_unsafe=True`. That explicitly bypasses their supported
path check. Managed compute/transfer actions use the checked form. A caller with
access to the unrestricted Python API can choose the bypass themselves.

## Globus paths

`globusCopy()` checks its destination through `checkSafeGlobusPath()`. The check
translates configured machine sandbox roots into the destination collection's
Globus namespace before comparison. When a machine name selects the destination,
it restricts the check to that machine's sandboxes.

When a supplied UUID/tag matches a collection used by configured sandbox routes,
the destination must lie within an applicable translated sandbox. An external
collection with no applicable configured sandbox is permitted without asking
for transfer permission. This implements the policy of enforcing sandboxes on
known configured destinations; it does not assert that an external collection
is safe or writable. Filesystem path translation still requires a known route,
even when Globus-path writes to an external UUID are permitted.

The general Globus destination check does not restrict the source to a sandbox.
The personal collection has additional rules for both sides, described below.

## Personal collection and staging

The configured personal staging directory is an explicit write contract. It is
allowed to be outside `sandbox_directories["local"]`.

- Direct `globusCopy()` and `GlobusTransfer` paths on the personal collection
  must lie within its staging root: the Globus root for `path_type="globus"`,
  or its local OS root for `path_type="filesystem"`. This applies to both source
  and destination and remains required when `allow_unsafe=True`.
- `uploadFileGlobus()` can read a local source outside staging, copy it into a
  temporary staging directory, and transfer it to a checked remote filename.
- `downloadFileGlobus()` can write to staging, a configured `local` sandbox, or
  `/tmp`. Files copied out of staging must pass the local destination check,
  unless the caller explicitly sets `allow_unsafe=True`. The helper checks the
  final filename before starting the transfer or creating destination parents.

The two configured staging paths must identify the same physical directory.
Path checks cannot verify that the collection exposes the intended host path.

## Scope of protection

| Operation | Current protection/limitation |
| --- | --- |
| Supported filesystem mutations | Check the destination unless an available `allow_unsafe` bypass is enabled. |
| Compute jobs | Check the run directory; the executable, script, and pre/post commands retain the user's OS/scheduler permissions. |
| Globus transfers | Check applicable known destinations; unknown external collections are allowed. Personal paths are confined to staging. |
| Reads and listings | Generally use provider permissions rather than restricting reads to sandbox roots. |
| `downloadFileGlobus` | Allows configured staging, configured local sandboxes, or `/tmp`; `allow_unsafe=True` bypasses the final local destination restriction. |
| Native `downloadFile`, local copies, and byte writes | Check the resolved final destination against local sandboxes and `/tmp` before fetching/copying/writing data, unless `allow_unsafe=True`. Directory destinations are expanded before checking their child filename. |
| Local compute output | Checks the run directory, wrapper, and log before recording a job or starting its worker. Commands inside the job retain their OS permissions. |
| SPOOF | Simulated remote mutations do not validate the general sandbox policy. Real local placeholder downloads do check their destination and accept `allow_unsafe`. |

Credential files, discovery caches, configuration templates, and manager/status
databases are written to paths explicitly chosen during configuration or object
creation. Those setup/state paths are separate write contracts and do not need
to lie inside data sandbox roots. The configured staging root is likewise
authorized for temporary transfer files without requiring a data sandbox entry.

Use the OS, scheduler, accounts, and facility permissions for stronger
confinement. Configure sandbox roots and staging directories deliberately, and
do not present the raw library as a security boundary for untrusted code.
