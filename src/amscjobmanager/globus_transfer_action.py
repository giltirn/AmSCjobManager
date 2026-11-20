from dataclasses import field
from pydantic import ConfigDict, StrictStr
from pydantic.dataclasses import dataclass
from typing import Literal
from .actions_base import replaceWorkflowIdSubstring, TransferActionBase
from .api_general import (
    cancelGlobusTransfer, findGlobusTransferByLabel, globusCopy,
    globusTransferStatus,
)
from .action_manager import ActionStatus
from .async_action_manager import AsyncActionManager
import sqlite3

@dataclass(config=ConfigDict(extra="forbid"))
class GlobusTransfer(TransferActionBase):
    """Transfer between collections, using Globus-visible paths by default.

    source_path is a file or directory in the source collection; dest_path is
    a directory in the destination collection. Set either path type to
    "filesystem" to translate a configured machine path to a Globus path.
    Endpoint strings identify configured tags or UUIDs. Both path types default
    to 'globus'; paths support <WORKFLOWID>. type is always 'globus_transfer'.
    """
    #The <WORKFLOWID> substring will be replaced by the workflow ID if present in the path strings
    dest_endpoint: StrictStr
    dest_path: StrictStr
    source_endpoint: StrictStr
    source_path: StrictStr
    source_path_type: Literal["filesystem", "globus"] = "globus"
    dest_path_type: Literal["filesystem", "globus"] = "globus"
    type: Literal["globus_transfer"] = field(default="globus_transfer", kw_only=True)

    def initiateAction(self, workflow_id, name: str | None = None)-> str:
        """Expand workflow paths and submit an asynchronous Globus transfer.

        workflow_id (positive int) is the owning workflow database ID used for
        <WORKFLOWID> expansion. name (str | None) is the optional transfer label.
        Return str, the provider transfer ID; completion is monitored separately.
        """
        source_path = replaceWorkflowIdSubstring(self.source_path, workflow_id)
        dest_path = replaceWorkflowIdSubstring(self.dest_path, workflow_id)
        if not dest_path.endswith("/"):
            dest_path += "/"
        
        return globusCopy(self.dest_endpoint, dest_path, self.source_endpoint, source_path,
                          allow_unsafe=False, block_until_complete=False, label=name,
                          source_path_type=self.source_path_type, dest_path_type=self.dest_path_type)

    def getInfo(self)->dict:
        """
        Return the transfer information in a common dictionary format with entries {"origin", "destination"}
        """
        return { "origin" : f"{self.source_endpoint}:{self.source_path}",  "destination" : f"{self.dest_endpoint}:{self.dest_path}" }

class GlobusDataTransfers(AsyncActionManager):
    """Action manager for Globus transfers"""
    terminal_api_statuses = {"SUCCEEDED", "FAILED"}
    def __init__(self, connection : sqlite3.Connection, **kwargs):
        #"ACTIVE"  The task is in progress.
        #"INACTIVE" The task has been suspended and will not continue without intervention. Currently, only credential expiration will cause this state.
        #"SUCCEEDED"  The task completed successfully.
        #"FAILED"  The task or one of its subtasks failed, expired, or was canceled.
        smap = { "ACTIVE" : ActionStatus.ACTIVE, "INACTIVE" : ActionStatus.PROVIDER_HELD, "SUCCEEDED" : ActionStatus.COMPLETED, "FAILED" : ActionStatus.FAILED }
        super().__init__(connection, "transfers", smap, **kwargs)
    def _queryStatusInternal(self, machine, api_key):
        return globusTransferStatus(api_key)

    def _findRemoteActionByName(self, machine, name):
        return findGlobusTransferByLabel(name)

    def _cancelActionInternal(self, machine, api_key):
        cancelGlobusTransfer(api_key)
