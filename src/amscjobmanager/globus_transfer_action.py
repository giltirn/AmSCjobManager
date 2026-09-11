from dataclasses import dataclass
from .actions_base import replaceJobIdSubstring, TransferActionBase
from .api_general import globusCopy, globusTransferStatus
from .action_manager import ActionManager, ActionStatus
import sqlite3

@dataclass
class GlobusTransfer(TransferActionBase):
    #The <JOBID> substring will be replaced by the job index if present in the path strings
    dest_endpoint: str
    dest_path: str
    source_endpoint: str 
    source_path: str

    def initiateAction(self, job_id)-> str:        
        source_path = replaceJobIdSubstring(self.source_path, job_id)
        dest_path = replaceJobIdSubstring(self.dest_path, job_id)
        
        return globusCopy(self.dest_endpoint, dest_path, self.source_endpoint, source_path, allow_unsafe=False, block_until_complete=True)

    def getInfo(self)->dict:
        """
        Return the transfer information in a common dictionary format with entries {"origin", "destination"}
        """
        return { "origin" : f"{self.source_endpoint}:{self.source_path}",  "destination" : f"{self.dest_endpoint}:{self.dest_path}" }

class GlobusDataTransfers(ActionManager):
    """Action manager for Globus transfers"""
    def __init__(self, connection : sqlite3.Connection):
        #"ACTIVE"  The task is in progress.
        #"INACTIVE" The task has been suspended and will not continue without intervention. Currently, only credential expiration will cause this state.
        #"SUCCEEDED"  The task completed successfully.
        #"FAILED"  The task or one of its subtasks failed, expired, or was canceled.
        smap = { "ACTIVE" : ActionStatus.ACTIVE, "INACTIVE" : ActionStatus.FAILED, "SUCCEEDED" : ActionStatus.COMPLETED, "FAILED" : ActionStatus.FAILED }    
        super().__init__(connection, "transfers", smap)
    def _queryStatusInternal(self, api_key):
        return globusTransferStatus(api_key)