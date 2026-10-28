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
Callers can set `allow_tmp=False` to require a configured sandbox root. Unknown
machines and use before configuration raise errors. Use absolute paths;
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
- `downloadFileGlobus()` can write to staging or to a configured `local` sandbox.
  `/tmp` alone does not authorize a destination outside staging: this helper
  uses `allow_tmp=False`. It stages and moves/copies the result when necessary.

The two configured staging paths must identify the same physical directory.
Path checks cannot verify that the collection exposes the intended host path.

## Scope of protection

| Operation | Current protection/limitation |
| --- | --- |
| Supported filesystem mutations | Check the destination unless an available `allow_unsafe` bypass is enabled. |
| Compute jobs | Check the run directory; the executable, script, and pre/post commands retain the user's OS/scheduler permissions. |
| Globus transfers | Check applicable known destinations; unknown external collections are allowed. Personal paths are confined to staging. |
| Reads and listings | Generally use provider permissions rather than restricting reads to sandbox roots. |
| `downloadFileGlobus` | Enforces staging or configured local sandbox destinations. |
| Native `downloadFile` | Currently writes the local destination without a sandbox check. A provider's Globus fallback follows the stricter Globus helper instead. |
| SPOOF | Does not validate the general sandbox policy and can write placeholder downloads locally. |

Use the OS, scheduler, accounts, and facility permissions for stronger
confinement. Configure sandbox roots and staging directories deliberately, and
do not present the raw library as a security boundary for untrusted code.
