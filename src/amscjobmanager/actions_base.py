import re
from pydantic import ConfigDict, StrictStr
from pydantic.dataclasses import dataclass
from enum import Enum

"""Actions are classes of operations that can be performed via the AmSC infrastructure"""

def replaceJobIdSubstring(in_str, job_id):
    """Replace instances of <JOBID> with the job index in path strings"""
    return re.sub(r'<JOBID>', str(job_id), in_str)

@dataclass(config=ConfigDict(extra="forbid"))
class TransferActionBase:
    """Base class of all transfer actions"""

    def queryResourceStatus(self) -> bool | None:
        """Return whether the transfer resource is up, if this can be queried.

        ``None`` means no availability query is available for this action.
        Transfer submission then relies on the normal API error handling.
        """
        return None

    def getInfo(self)->dict:
        """
        Return the transfer information in a common dictionary format with entries {"origin", "destination"}
        """
        raise NotImplementedError("Derived class must implement getInfo")

    def initiateAction(self, job_id, name: str | None = None)->str:
        """Initiate the action and return the API key; name identifies the action."""
        raise NotImplementedError("Derived class must implement initiateAction")

@dataclass(config=ConfigDict(extra="forbid"))
class ComputeActionBase:
    """Common target and scheduling attributes for compute action definitions.

    machine (str) selects a configured target or local; account/queue (str)
    select the project and scheduler queue. time (str) is walltime in seconds.
    Subclasses define execution fields and may implement provider operations.
    """

    machine : StrictStr
    account : StrictStr
    queue : StrictStr
    time : StrictStr

    def queryResourceStatus(self) -> bool:
        """Return whether this action's compute resource reports being up.

        API failures propagate so scheduling can defer without recording a
        remote submission attempt.
        """
        from .api_general import queryMachineStatus

        return queryMachineStatus(self.machine)

    def getInfo(self)->dict:
        """
        Return the transfer information in a common dictionary format with entries {"machine", "queue", "time"}

        NOTE: Derived classes can override this to provide any custom information they want
        """
        return {"machine" : self.machine, "queue" : self.queue, "time" : self.time}    

    def initiateAction(self, job_id, name: str | None = None)->str:
        """Initiate the action and return the API key; name identifies the action."""
        raise NotImplementedError("Derived class must implement initiateAction")

class ActionClass(Enum):
    NONE = 0
    TRANSFER = 1
    COMPUTE = 2
    API = 3
    
def actionClass(action):
    from .api_actions import APIAction

    if issubclass(type(action), TransferActionBase):
        return ActionClass.TRANSFER
    elif issubclass(type(action), ComputeActionBase):
        return ActionClass.COMPUTE
    elif isinstance(action, APIAction):
        return ActionClass.API
    else:
        raise Exception("Unknown action type",type(action))
