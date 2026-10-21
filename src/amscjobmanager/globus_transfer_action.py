from dataclasses import dataclass
from typing import Literal
from .actions_base import replaceJobIdSubstring, TransferActionBase
from .api_general import (
    cancelGlobusTransfer, findGlobusTransferByLabel, globusCopy,
    globusTransferStatus,
)
from .action_manager import ActionManager, ActionStatus
import sqlite3

@dataclass
class GlobusTransfer(TransferActionBase):
    """Transfer between collections, using Globus-visible paths by default.

    source_path is a file or directory in the source collection; dest_path is
    a directory in the destination collection. Set either path type to
    "filesystem" to translate a configured machine path to a Globus path.
    """
    #The <JOBID> substring will be replaced by the job index if present in the path strings
    dest_endpoint: str
    dest_path: str
    source_endpoint: str 
    source_path: str
    source_path_type: Literal["filesystem", "globus"] = "globus"
    dest_path_type: Literal["filesystem", "globus"] = "globus"

    def initiateAction(self, job_id, name: str | None = None)-> str:
        source_path = replaceJobIdSubstring(self.source_path, job_id)
        dest_path = replaceJobIdSubstring(self.dest_path, job_id)
        if not dest_path.endswith("/"):
            dest_path += "/"
        
        return globusCopy(self.dest_endpoint, dest_path, self.source_endpoint, source_path,
                          allow_unsafe=False, block_until_complete=True, label=name,
                          source_path_type=self.source_path_type, dest_path_type=self.dest_path_type)

    def getInfo(self)->dict:
        """
        Return the transfer information in a common dictionary format with entries {"origin", "destination"}
        """
        return { "origin" : f"{self.source_endpoint}:{self.source_path}",  "destination" : f"{self.dest_endpoint}:{self.dest_path}" }

class GlobusDataTransfers(ActionManager):
    """Action manager for Globus transfers"""
    terminal_api_statuses = {"SUCCEEDED", "FAILED"}
    def __init__(self, connection : sqlite3.Connection, **kwargs):
        #"ACTIVE"  The task is in progress.
        #"INACTIVE" The task has been suspended and will not continue without intervention. Currently, only credential expiration will cause this state.
        #"SUCCEEDED"  The task completed successfully.
        #"FAILED"  The task or one of its subtasks failed, expired, or was canceled.
        smap = { "ACTIVE" : ActionStatus.ACTIVE, "INACTIVE" : ActionStatus.FAILED, "SUCCEEDED" : ActionStatus.COMPLETED, "FAILED" : ActionStatus.FAILED }    
        super().__init__(connection, "transfers", smap, **kwargs)
    def _queryStatusInternal(self, machine, api_key):
        return globusTransferStatus(api_key)

    def _findRemoteActionByName(self, machine, name):
        return findGlobusTransferByLabel(name)

    def _cancelActionInternal(self, machine, api_key):
        cancelGlobusTransfer(api_key)
