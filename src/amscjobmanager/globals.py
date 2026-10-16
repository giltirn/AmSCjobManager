import os
import warnings
from typing import Dict, List
import pathlib

remote_workdir = None ##Dictionary mapping machine to one or more sandbox directories
_pending_sandbox_checks: dict[str, list[str]] = {}
_validating_sandboxes: set[str] = set()
api_impl = os.getenv("AMSC_JOB_MANAGER_API_IMPL", "IRI") #Control which API implementation is used

def checkSafePath(machine: str, path: str)->bool:
    """
    Check if a path is within the sandbox directory structure on the machine
    """
    machine = machine.lower()
    if remote_workdir is None:
        raise Exception("setupWorkflowAgent has not been called")
    if machine not in remote_workdir:
        raise Exception("Unknown machine")
    
    path_p = pathlib.Path(path).resolve()
    tmp_p = pathlib.Path("/tmp")
    if path_p.is_relative_to(tmp_p):
        return True

    for sp in remote_workdir[machine]:            
        safe_p = pathlib.Path(sp).resolve()
        if path_p.is_relative_to(safe_p):
            return True
    return False


def addSandboxDirs(dirs: Dict[str,str | List[str]], warn_unavailable=True, *,
                   defer_checks=False, replace=False):
    """
    Add paths to the sandbox directories. Expects a dictionary mapping machine to
    directories. Machines outside the implementations configured during setup
    are ignored without probing the remote API. With defer_checks, register
    remote paths immediately and validate them when validateSandboxDirs is called.
    Local paths are checked immediately because they have no IRI client lifecycle.
    replace clears previously registered paths and pending checks. Available
    IRI machines are determined from the current implementation configuration.
    """
    global remote_workdir
    configured_machines = None
    if api_impl == "IRI":
        from .iri_api.resources import getKnownMachines
        configured_machines = set(getKnownMachines())
    if replace:
        remote_workdir = {}
        _pending_sandbox_checks.clear()
        _validating_sandboxes.clear()
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
        if machine not in remote_workdir:
            remote_workdir[machine] = []
        for directory in dir_or_dirs:
            if directory not in remote_workdir[machine]:
                remote_workdir[machine].append(directory)
                _pending_sandbox_checks.setdefault(machine, []).append(directory)
        if not defer_checks or machine == "local":
            validateSandboxDirs(machine)


def validateSandboxDirs(machine: str) -> None:
    """Validate pending sandbox paths for one machine, once successfully checked.

    Missing directories retain the existing interactive creation behavior.
    Failed checks remain pending, so a later operation cannot bypass validation.
    Recursive requests made by the checks themselves do not restart validation.
    """
    machine = machine.lower()
    if machine not in _pending_sandbox_checks or machine in _validating_sandboxes:
        return
    from .api_general import isDirectory, remoteMkdir
    from .utils import queryYesNo

    _validating_sandboxes.add(machine)
    try:
        for directory in _pending_sandbox_checks[machine]:
            if not isDirectory(machine, directory.rstrip("/") or "/"):
                if queryYesNo(f"Directory {directory} on machine {machine} does not exist, do you want to create it?"):
                    remoteMkdir(machine, directory, allow_unsafe=True)
                else:
                    raise Exception(f"Directory {directory} on machine {machine} does not exist")
        del _pending_sandbox_checks[machine]
    finally:
        _validating_sandboxes.remove(machine)
