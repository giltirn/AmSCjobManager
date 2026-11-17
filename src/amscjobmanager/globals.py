"""Process-local backend selection, sandbox registrations, and directory validation state."""

import os
import warnings
from typing import Dict, List, NamedTuple
import pathlib
from threading import Lock, RLock

class SandboxDirectory(NamedTuple):
    """Immutable sandbox registration with its cached directory-validation result.

    path (str) is the exact filesystem root, already checked for forbidden '~'.
    validated (bool, default False) means a previous directory probe or approved
    creation succeeded; it does not guarantee future existence or path safety.
    New registrations have distinct identities even when their paths match, so
    a late validation result cannot validate a removed-and-readded registration.
    """

    path: str
    validated: bool = False


remote_workdir: dict[str, list[SandboxDirectory]] | None = None
"""Registered filesystem sandbox roots and validation results, by lowercase machine.

Values are lists of immutable SandboxDirectory records. None means setup has
not initialized the mapping; an empty list grants no explicit roots. Built-in
/tmp access is handled separately. Registration changes and validation results
are process-local, not persisted. New local/remote roots await a sandbox check
or explicit validateSandboxDirs call; registration itself performs no probes.
"""

_sandbox_validation_locks: dict[str, Lock] = {}
"""Per-machine locks serializing blocking directory-validation passes.

Keys are lowercase machine names. A second caller waits for the first pass,
then checks any roots still awaiting validation instead of skipping validation.
Locks remain registered across configuration changes so existing waiters keep
sharing the same lock. Different machines can validate independently.
"""

_sandbox_lock = RLock()
"""Reentrant thread lock protecting sandbox-state updates and root-list snapshots.

Registration and validation bookkeeping use short critical sections. The lock is
released before provider API calls and user queries, so those waits do not block
other sandbox updates.
"""

api_impl = os.getenv("AMSC_JOB_MANAGER_API_IMPL", "IRI")
"""Backend selector (str), read from AMSC_JOB_MANAGER_API_IMPL at module import.

Defaults to 'IRI'; 'SPOOF' selects the simulated implementation. API modules
choose their backend during import, so set the environment variable before
importing the library rather than changing this value on a running server.
"""

def checkSafePath(machine: str, path: str | pathlib.Path, *, allow_tmp: bool = True)->bool:
    """Check filesystem sandbox membership, validating registered roots before access.

    machine (str) is a case-insensitive target; path (str | pathlib.Path) is the
    filesystem location to authorize. Reject '~' and resolve symlinks here before
    comparing with roots; the caller's path is not modified. allow_tmp (bool,
    default True) permits /tmp without a configured root. Return bool: True for
    permitted paths, False outside the allowed roots.

    Local paths under /tmp are writable even without local configuration.
    Set allow_tmp=False to require a configured sandbox directory even for
    paths under /tmp.
    Synchronously validate the machine's outstanding directory registrations
    before comparing the path with its roots, even if the path is then rejected.
    Failed validation raises and remains retryable. Built-in /tmp access needs
    no directory probe.
    """
    machine = machine.lower()
    if "~" in str(path):
        raise ValueError("Paths containing '~' are not supported; use an explicit path")
    path_p = pathlib.Path(path).resolve()
    tmp_p = pathlib.Path("/tmp").resolve()
    # Local temporary storage is available even without a configured local root.
    if machine == "local" and allow_tmp and path_p.is_relative_to(tmp_p):
        return True
    if remote_workdir is None:
        raise Exception("setupWorkflowAgent has not been called")
    if machine not in remote_workdir:
        raise Exception("Unknown machine")
    
    if allow_tmp and path_p.is_relative_to(tmp_p):
        return True

    # Validate once, then read the current registrations so removals made during
    # authentication, directory probes, or user input are reflected in the check.
    validateSandboxDirs(machine)
    with _sandbox_lock:
        roots = list((remote_workdir or {}).get(machine, []))
    return any(root.validated and path_p.is_relative_to(pathlib.Path(root.path).resolve()) for root in roots)


def getSandboxDirs(machine: str | None = None) -> dict[str, list[str]]:
    """Return registered filesystem roots grouped by machine, without directory probes.

    machine (str | None, default None) optionally filters a case-insensitive
    machine name; None selects all registrations. Return dict[str, list[str]]
    with lowercase keys and exact root strings, including unvalidated roots and
    empty lists. An unmatched machine or uninitialized mapping returns {}.
    Returned dictionaries/lists are copies. Implicit /tmp and staging allowances
    are not registrations and are not added to this result.
    """
    selected = None if machine is None else machine.lower()
    with _sandbox_lock:
        return {name: [root.path for root in roots]
                for name, roots in (remote_workdir or {}).items()
                if selected is None or name == selected}


def addSandboxDirs(dirs: Dict[str,str | List[str]], warn_unavailable=True, *,
                   replace=False):
    """Register sandbox roots for directory validation on a subsequent sandbox check.

    dirs (dict[str, str | list[str]]) maps case-insensitive machines to root
    strings. Reject '~'; duplicate registrations are ignored. warn_unavailable
    (bool, default True) warns when a machine's IRI implementation is not
    configured; those machines are skipped. replace (bool, default False) clears
    prior registrations and cached validation. Return None without directory probes, creation
    queries, or authentication. Both local and remote roots await validation by
    a subsequent sandbox check or explicit validateSandboxDirs call.
    """
    global remote_workdir
    # Reject ambiguous roots before registering any paths or probing machines.
    for directories in dirs.values():
        for directory in ([directories] if isinstance(directories, str) else directories):
            if "~" in str(directory):
                raise ValueError("Sandbox paths containing '~' are not supported")
    configured_machines = None
    if api_impl == "IRI":
        from .iri_api.resources import getKnownMachines
        configured_machines = set(getKnownMachines())
    with _sandbox_lock:
        if replace:
            remote_workdir = {}
        if remote_workdir is None:
            remote_workdir = {}
    for machine, dir_or_dirs in dirs.items():
        assert isinstance(dir_or_dirs, (list,str))

        machine = machine.lower()
        if configured_machines is not None and machine not in configured_machines:
            if warn_unavailable:
                warnings.warn(
                    f"Ignoring sandbox directory for {machine!r}: its IRI "
                    "implementation is not configured",
                    RuntimeWarning,
                    stacklevel=2,
                )
            continue

        if not isinstance(dir_or_dirs, list):
            dir_or_dirs = [dir_or_dirs]
        with _sandbox_lock:
            roots = list(remote_workdir.get(machine, []))
            for directory in dir_or_dirs:
                if not any(root.path == directory for root in roots):
                    roots.append(SandboxDirectory(directory))
            remote_workdir = {**remote_workdir, machine: roots}


def removeSandboxDirs(dirs: Dict[str, str | List[str]]) -> None:
    """Unregister exact sandbox roots without deleting files or stopping work.

    dirs (dict[str, str | list[str]]) maps case-insensitive machines to registered
    path strings. Matching is exact, including trailing slashes; descendants and
    overlapping roots are not removed. Reject '~'. Missing roots are ignored.
    Return None. Remove the registrations and their validation state; retain /tmp access.
    Changes apply to later checks, not operations already authorized or running.
    """
    global remote_workdir
    # Validate the complete request before changing any machine's roots.
    for machine, directories in dirs.items():
        if not isinstance(machine, str) or not isinstance(directories, (str, list)):
            raise TypeError('Expected machine names mapped to a path or list of paths')
        for directory in ([directories] if isinstance(directories, str) else directories):
            if not isinstance(directory, str):
                raise TypeError('Sandbox paths must be strings')
            if '~' in directory:
                raise ValueError("Sandbox paths containing '~' are not supported")

    # Publish replacement lists so checks already iterating a root list remain
    # consistent. A validation in progress must not resurrect a removed root.
    with _sandbox_lock:
        for machine, directories in dirs.items():
            machine = machine.lower()
            removed = {directories} if isinstance(directories, str) else set(directories)
            if remote_workdir is not None and machine in remote_workdir:
                remote_workdir = {**remote_workdir,
                                  machine: [root for root in remote_workdir[machine] if root.path not in removed]}


def validateSandboxDirs(machine: str) -> None:
    """Validate pending sandbox paths for one machine, once successfully checked.

    machine (str) is a case-insensitive configured target. Return None when no
    registered roots remain unchecked. Concurrent callers for the same machine
    wait for validation; other machines and registration updates can proceed.
    Missing directories retain the existing interactive creation behavior.
    Each successful root is marked validated immediately; a later failure leaves
    only unsuccessful roots unchecked for retry.
    Client retrieval does not perform validation, and approved root creation
    bypasses path checks, so these blocking operations do not reenter this call.
    """
    global remote_workdir
    machine = machine.lower()
    from .api_general import isDirectory, remoteMkdir
    from .utils import queryYesNo

    with _sandbox_lock:
        if all(root.validated for root in (remote_workdir or {}).get(machine, [])):
            return
        validation_lock = _sandbox_validation_locks.setdefault(machine, Lock())

    # Only this machine's validation lock spans API calls and user waits. Check
    # again after acquiring it: another caller may already have done the work.
    with validation_lock:
        while True:
            with _sandbox_lock:
                pending = [root for root in (remote_workdir or {}).get(machine, []) if not root.validated]
            if not pending:
                return
            for registration in pending:
                if not isDirectory(machine, registration.path.rstrip("/") or "/"):
                    if queryYesNo(f"Directory {registration.path} on machine {machine} does not exist, do you want to create it?"):
                        remoteMkdir(machine, registration.path, allow_unsafe=True)
                    else:
                        raise Exception(f"Directory {registration.path} on machine {machine} does not exist")

                # Retain each success if a later probe fails. Match identity so
                # removal/re-registration of the same path requires a new probe;
                # never restore a removed record or overwrite concurrent additions.
                with _sandbox_lock:
                    if remote_workdir is not None and machine in remote_workdir:
                        remote_workdir = {**remote_workdir, machine: [
                            root._replace(validated=True) if root is registration else root
                            for root in remote_workdir[machine]]}
