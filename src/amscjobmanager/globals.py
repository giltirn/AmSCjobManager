import os
from typing import Dict, List
import pathlib

remote_workdir = None ##Dictionary mapping machine to a sandbox directory
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


def addSandboxDirs(dirs: Dict[str,str | List[str]]):
    """
    Add paths to the sandbox directories. Expects a dictionary mapping machine to 
    """
    global remote_workdir
    if remote_workdir is None:
        remote_workdir = {}
    for machine, dir_or_dirs in dirs.items():
        assert isinstance(dir_or_dirs, (list,str))

        if not isinstance(dir_or_dirs, list):
            dir_or_dirs = [dir_or_dirs]
        machine = machine.lower()
        if machine not in remote_workdir:
            remote_workdir[machine] = []
        for dir in dir_or_dirs:            
            remote_workdir[machine].append(dir)
