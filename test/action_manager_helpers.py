"""Deterministic unit adapters for existing reconciliation/crash-boundary tests.

These adapters call the production sequential reconciliation bodies directly.
They isolate database/retry logic from thread scheduling; test_action_workers
exercises the real asynchronous supervisors and process-restart recovery.
"""

import time

from amscjobmanager.action_manager import ActionStatus, ActionStatusGroup
from amscjobmanager.api_action_manager import APIActions
from amscjobmanager.compute_action_manager import ComputeActions
from amscjobmanager.globus_transfer_action import GlobusDataTransfers


class DirectAsyncActions:
    """Execute asynchronous-manager reconciliation on the unit-test caller thread."""

    def startOrResumeAction(self, action, workflow_id, action_id):
        """Record action/workflow_id/action_id and run one production reconciliation; return None."""
        self._recordSchedulingIntentIfNew(action, workflow_id, action_id)
        self._reconcileScheduledAction(action_id)

    def cancelOrResumeCancelingAction(self, action_id, *, recovery=None):
        """Record/cancel positive action_id with optional (action, workflow_id) recovery; return None."""
        if recovery is not None:
            action, workflow_id = recovery
            self._recordSchedulingIntentIfNew(action, workflow_id, action_id)
        self._recordCancelingIntentIfNotAlready(action_id)
        if self.queryRecordedStatus(action_id)[0] != ActionStatus.CANCELED:
            self._reconcileCancelingAction(action_id)

    def queryRecordedStatus(self, action_id, *, force_update=False):
        """Read/poll action_id using the old unit-test timing contract.

        force_update (bool, default False) forces a synchronous poll; otherwise
        use this manager's configured poll_freq delay in seconds. Return
        (ActionStatus, provider status).
        Production supervisor tests do not use this adapter.
        """
        status, api_status = super().queryRecordedStatus(action_id)
        with self.conn as conn:
            row = conn.execute(f'SELECT last_update FROM {self.table_name} WHERE action_id = ?', (action_id,)).fetchone()
        if status in ActionStatusGroup.LIVE.value and (force_update or int(time.time()) > row['last_update'] + self.poll_freq):
            self._pollAction(action_id)
            status, api_status = super().queryRecordedStatus(action_id)
        return status, api_status


class DirectComputeActions(DirectAsyncActions, ComputeActions):
    """Production compute reconciliation with deterministic unit-test dispatch."""


class DirectGlobusActions(DirectAsyncActions, GlobusDataTransfers):
    """Production transfer reconciliation with deterministic unit-test dispatch."""


class DirectAPIActions(APIActions):
    """Production API sequence execution without background scheduling in unit tests."""

    def startOrResumeAction(self, action, workflow_id, action_id):
        """Record action/workflow_id/action_id and execute its saved sequence; return None."""
        if self._recordAction(action, workflow_id, action_id, ActionStatus.SCHEDULING):
            self._maybeSimulateCrash('action_intent_persisted')
        self._executeAction(action_id)
