import re
from dataclasses import dataclass
from enum import Enum

"""Actions are classes of operations that can be performed via the AmSC infrastructure"""

def replaceJobIdSubstring(in_str, job_id):
    """Replace instances of <JOBID> with the job index in path strings"""
    return re.sub(r'<JOBID>', str(job_id), in_str)

@dataclass
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

@dataclass
class ComputeActionBase:
    machine : str
    account : str
    queue : str
    time : str

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
    
def actionClass(action):
    if issubclass(type(action), TransferActionBase):
        return ActionClass.TRANSFER
    elif issubclass(type(action), ComputeActionBase):
        return ActionClass.COMPUTE
    else:
        raise Exception("Unknown action type",type(action))
