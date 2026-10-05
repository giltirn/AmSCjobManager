from .action_manager import ActionManager, ActionStatus
import sqlite3
from .api_general import getJobState, findJobByName

class ComputeActions(ActionManager):
    """Action manager for all compute jobs submitted via the IRI API"""
    def __init__(self, connection : sqlite3.Connection, **kwargs):
        #IRI API: 
        #0"new"
        #1"queued"
        #2"active"
        #3"completed"
        #4"failed"
        #5"canceled"
        smap = {"new": ActionStatus.ACTIVE, "queued" : ActionStatus.ACTIVE, "active" : ActionStatus.ACTIVE,
                "completed" : ActionStatus.COMPLETED, "failed" : ActionStatus.FAILED, "canceled" : ActionStatus.FAILED }
        super().__init__(connection, "computes", smap, **kwargs)
    def _queryStatusInternal(self, machine, api_key):
        return getJobState(machine, api_key)

    def _findRemoteActionByName(self, machine, name):
        return findJobByName(machine, name)
