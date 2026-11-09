import pickle
from abc import ABC, abstractmethod
from enum import Enum

def _ser(obj):
    return pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
def _unser(ser):
    return pickle.loads(ser)

class ActionStatus(Enum):
    PENDING = 0 #not yet started
    SCHEDULING = 1 #submission is in progress and must be reconciled after a crash
    ACTIVE = 2 #a live action (any status not failed or completed, e.g. queued, new, etc)
    COMPLETED = 3 #action completed successfully
    FAILED = 4 #action failed
    CANCELED = 5 #cancellation was requested or the action was removed before submission
    CANCELING = 6 #cancellation intent is persisted but not yet reconciled


class SimulatedCrash(RuntimeError):
    """Test-only exception raised at an explicitly requested crash boundary."""

    def __init__(self, point):
        self.point = point
        super().__init__(f"Simulated crash at {point}")
    
class ActionManager(ABC):
    """Workflow-facing action-manager interface, independent of storage schema.

    Implementations own their records, execution and recovery. JobData always
    allocates action IDs and supplies them on initial execution and recovery;
    action managers never allocate or replace them.
    Construction and internal reconciliation hooks are implementation-specific.
    """

    @abstractmethod
    def startAction(self, action, job_id: int, action_id: int) -> bool:
        """Start or recover an action, retaining an existing record's state.

        Args:
            action (object): Serializable action definition from the workflow.
            job_id (int): Positive owning workflow ID.
            action_id (int): Positive global action ID allocated by JobData
                before the initial call. Supply the same ID during recovery.

        Returns:
            bool: True when this scheduling/recovery pass is resolved and the
                caller should read queryStatus using the supplied action_id;
                False when deferred and the workflow head should remain
                unchanged until a later attempt. True does not imply successful
                submission: the recorded status may also indicate failure.
                Repeated calls must safely recover existing work.
        """
        raise NotImplementedError

    @abstractmethod
    def cancelAction(self, action_id: int, *, recovery: tuple[object, int] | None = None) -> int | None:
        """Cancel an action without initiating new work.

        Args:
            action_id (int): Positive global action ID.
            recovery (tuple[object, int] | None): Optional stored action at index
                0 and positive owning workflow ID at index 1. Allows recovery
                of a missing record. Defaults to None.

        Returns:
            int | None: Action ID when cancellation is resolved, or None when
                recovery must wait. Failures must retain cancellation intent.
        """
        raise NotImplementedError

    @abstractmethod
    def hasAction(self, action_id: int) -> bool:
        """Return whether positive integer action_id has a persisted record."""
        raise NotImplementedError

    @abstractmethod
    def getMaxActionId(self) -> int | None:
        """Take no arguments; return the highest recorded int ID, or None if empty."""
        raise NotImplementedError

    @abstractmethod
    def queryStatus(self, action_id: int, update_freq=30, force_update=False) -> tuple[ActionStatus, str | None]:
        """Read an action's status, refreshing external state when appropriate.

        Args:
            action_id (int): Positive global action ID.
            update_freq (float): Minimum seconds between external polls;
                defaults to 30. Infinity requests cached state.
            force_update (bool): Refresh regardless of cache age; defaults to
                False. Managers without external state may return stored status.

        Returns:
            tuple[ActionStatus, str | None]: Action status at index 0 and raw
                provider status at index 1, or None when unavailable/inapplicable.
        """
        raise NotImplementedError

    @abstractmethod
    def updateStatuses(self) -> None:
        """Take no arguments; refresh all active statuses and return None.

        Managers with no externally tracked work may perform no operation.
        """
        raise NotImplementedError

    @abstractmethod
    def waitForAction(self, action_id: int, check_freq=30) -> ActionStatus:
        """Wait while an action is ACTIVE and return its resulting ActionStatus.

        Args:
            action_id (int): Positive global action ID.
            check_freq (float): Seconds between status checks; defaults to 30.
        """
        raise NotImplementedError

    @abstractmethod
    def getActiveActions(self) -> list[dict]:
        """Take no arguments; return information dictionaries for ACTIVE actions.

        Each dictionary contains job_id (positive int), api_key (str or None),
        api_status (str or None), and implementation-specific action details.
        """
        raise NotImplementedError

    @abstractmethod
    def getActionInfo(self, action_id: int) -> dict:
        """Return an information dictionary for positive integer action_id.

        Includes job_id (positive int), api_key (str or None), api_status
        (str or None), and implementation-specific action details.
        """
        raise NotImplementedError
