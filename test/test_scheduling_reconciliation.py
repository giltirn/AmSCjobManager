"""Crash-boundary tests for action scheduling reconciliation.

The harness substitutes only the remote Globus service.  WorkflowData and the
ActionManager implementation execute their normal database workflow.
"""



from dataclasses import dataclass
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from action_manager_helpers import DirectAsyncActions, DirectComputeActions
from amscjobmanager.action_manager import ActionManager, ActionStatus, SimulatedCrash
from amscjobmanager.async_action_manager import AsyncActionManager
from amscjobmanager.api_action_manager import APIActions
from amscjobmanager.actions_base import TransferActionBase
from amscjobmanager.globus_transfer_action import GlobusDataTransfers
from amscjobmanager.workflow_data import WorkflowData
from amscjobmanager.manager import WorkflowManager
from amscjobmanager.actions_base import ActionClass


class TransferHarness:
    """Small, deterministic model of the remote transfer service."""

    def __init__(self):
        self.by_name = {}
        self.status_by_api_key = {}
        self.submission_count = 0
        self.failed_submission_count = 0
        self.failed_status_lookup_count = 0
        self.failed_remote_lookup_count = 0
        self.hidden_remote_lookup_count = 0
        self.failed_cancellation_count = 0
        self.cancellation_count = 0

    def submit(self, name):
        if self.failed_submission_count:
            self.failed_submission_count -= 1
            raise RuntimeError("simulated remote submission failure")
        self.submission_count += 1
        api_key = f"transfer-{self.submission_count}"
        self.by_name[name] = api_key
        self.status_by_api_key[api_key] = "ACTIVE"
        return api_key

    def find(self, name):
        if self.failed_remote_lookup_count:
            self.failed_remote_lookup_count -= 1
            raise RuntimeError("simulated remote action lookup failure")
        if self.hidden_remote_lookup_count:
            self.hidden_remote_lookup_count -= 1
            return None
        return self.by_name.get(name)

    def status(self, api_key):
        if self.failed_status_lookup_count:
            self.failed_status_lookup_count -= 1
            raise RuntimeError("simulated remote status lookup failure")
        return self.status_by_api_key[api_key]

    def cancel(self, api_key):
        if self.failed_cancellation_count:
            self.failed_cancellation_count -= 1
            raise RuntimeError("simulated remote cancellation failure")
        self.cancellation_count += 1
        self.status_by_api_key[api_key] = "FAILED"


@dataclass
class HarnessGlobusTransfer(TransferActionBase):
    """Transfer action whose remote side effect is owned by the test harness."""

    harness = None

    def initiateAction(self, workflow_id, name=None):
        return self.harness.submit(name)

    def getInfo(self):
        return {}


@dataclass
class RaisingHarnessGlobusTransfer(HarnessGlobusTransfer):
    """Action-layer failure used to verify ActionManager exception handling."""

    def initiateAction(self, workflow_id, name=None):
        raise RuntimeError("simulated action-layer submission failure")


@dataclass
class AcceptedThenRaisingHarnessGlobusTransfer(HarnessGlobusTransfer):
    """Simulate a response failure after the remote service accepts work."""

    def initiateAction(self, workflow_id, name=None):
        self.harness.submit(name)
        raise RuntimeError("simulated response failure after acceptance")


class HarnessGlobusDataTransfers(DirectAsyncActions, GlobusDataTransfers):
    """The real transfer manager with only its remote operations overridden."""

    def __init__(self, connection, harness, scheduling_retry_delay=0, **kwargs):
        self.harness = harness
        super().__init__(
            connection, scheduling_retry_delay=scheduling_retry_delay, **kwargs
        )

    def _queryStatusInternal(self, machine, api_key):
        return self.harness.status(api_key)

    def _findRemoteActionByName(self, machine, name):
        return self.harness.find(name)

    def _cancelActionInternal(self, machine, api_key):
        self.harness.cancel(api_key)


class SchedulingReconciliationTests(unittest.TestCase):
    def test_reconciliation_preserves_supplied_action_id(self):
        """Defer a real scheduling pass, then resolve it under the supplied ID.

        Use the transfer harness and a resource-down response to exercise False
        without submitting work. Restore availability and verify that the
        nontrivial supplied ID is retained and recovery does not resubmit.
        """
        workflows = self._new_workflow_data()
        try:
            action = HarnessGlobusTransfer()
            workflow_id = workflows.enqueueWorkflow(action)
            manager = workflows.action_man[ActionClass.TRANSFER]
            action_id = 37
            with patch.object(HarnessGlobusTransfer, "queryResourceStatus", return_value=False):
                manager._recordSchedulingIntentIfNew(action, workflow_id, action_id)
                self.assertIsNone(manager._reconcileScheduledAction(action_id))
            self.assertEqual(manager.queryRecordedStatus(action_id)[0], ActionStatus.SCHEDULING)
            self.assertEqual(self.harness.submission_count, 0)

            self.assertEqual(manager._reconcileScheduledAction(action_id), action_id)
            self.assertEqual(manager.getMaxActionId(), action_id)
            self.assertEqual(manager.queryRecordedStatus(action_id)[0], ActionStatus.ACTIVE)
            self.assertEqual(manager._reconcileScheduledAction(action_id), action_id)
            self.assertEqual(self.harness.submission_count, 1)
        finally:
            workflows.conn.close()

    def test_action_manager_interface_requires_an_implementation(self):
        """The interface cannot be instantiated; registered managers implement it.

        Construct all registered managers through WorkflowData. Compute and transfer
        managers retain the asynchronous implementation, while APIActions owns
        its separate implementation behind the same interface. All start empty.
        """
        with self.assertRaises(TypeError):
            ActionManager()
        workflows = self._new_workflow_data()
        try:
            for manager in workflows.action_man.values():
                self.assertIsInstance(manager, ActionManager)
                if isinstance(manager, APIActions):
                    self.assertNotIsInstance(manager, AsyncActionManager)
                else:
                    self.assertIsInstance(manager, AsyncActionManager)
                self.assertIsNone(manager.getMaxActionId())
        finally:
            workflows.conn.close()

    def test_queued_action_ids_do_not_depend_on_manager_table_names(self):
        """Allocate IDs at enqueue and retain them with custom execution managers.

        Queue several unstarted stages, reopen storage, then queue another one.
        IDs must be distinct actions-table keys without inspecting or seeding
        execution-manager tables, which remain empty until scheduling.
        """
        factories = {
            ActionClass.COMPUTE: lambda conn, **kwargs: AsyncActionManager(
                conn, "custom_compute", {}, **kwargs
            ),
            ActionClass.TRANSFER: lambda conn, **kwargs: AsyncActionManager(
                conn, "custom_transfer", {}, **kwargs
            ),
        }
        workflows = WorkflowData(self.db_path, _test_action_manager_factories=factories)
        workflows.enqueueWorkflow([HarnessGlobusTransfer(), HarnessGlobusTransfer()])
        with workflows.conn as conn:
            ids = [row[0] for row in conn.execute('SELECT action_id FROM actions ORDER BY action_id')]
        self.assertEqual(len(set(ids)), 2)
        self.assertTrue(all(manager.getMaxActionId() is None for manager in workflows.action_man.values()))
        workflows.conn.close()

        recovered = WorkflowData(self.db_path, _test_action_manager_factories=factories)
        try:
            workflow_id = recovered.enqueueWorkflow(HarnessGlobusTransfer())
            self.assertGreater(recovered.workflowState(workflow_id)['head_action_id'], max(ids))
            self.assertTrue(all(manager.getMaxActionId() is None for manager in recovered.action_man.values()))
        finally:
            recovered.conn.close()

    def test_head_action_status_forced_poll_updates_both_records_once(self):
        """Refresh action/workflow records once without starting the next step.

        Use the real WorkflowData and action manager with a fake remote transfer
        service. Start the first of two steps, then mark it successful remotely.
        A cached read must not poll; a forced read must poll exactly once and
        expose COMPLETED/SUCCEEDED in the local records. The workflow remains
        ACTIVE because another step is outstanding. Listing by COMPLETED must
        therefore exclude it, while listing by ACTIVE includes it. This guards
        the overall-status filter used by MCP against intermediate completion.
        """
        workflow_data = self._new_workflow_data()
        self.addCleanup(workflow_data.conn.close)
        workflow_id = workflow_data.enqueueWorkflow([HarnessGlobusTransfer(), HarnessGlobusTransfer()])
        self.assertEqual(workflow_data.headActionStatus(workflow_id, return_api_status=True),
                         (ActionStatus.PENDING, None))
        workflow_data.startWorkflows([workflow_id])
        self.harness.status_by_api_key['transfer-1'] = 'SUCCEEDED'
        with patch.object(self.harness, 'status', wraps=self.harness.status) as poll:
            self.assertEqual(workflow_data.headActionStatus(workflow_id, return_api_status=True),
                             (ActionStatus.ACTIVE, 'ACTIVE'))
            poll.assert_not_called()
            self.assertEqual(workflow_data.headActionStatus(workflow_id, force_poll=True, return_api_status=True),
                             (ActionStatus.COMPLETED, 'SUCCEEDED'))
            poll.assert_called_once_with('transfer-1')
        self.assertEqual(workflow_data.workflowState(workflow_id)['head_action_status'], ActionStatus.COMPLETED)
        self.assertEqual(workflow_data.workflowStatus(workflow_id), ActionStatus.ACTIVE)
        self.assertEqual(self.harness.submission_count, 1)
        self.assertEqual(workflow_data.listWorkflows(statuses=ActionStatus.COMPLETED), [])
        self.assertEqual([row['workflow_id'] for row in workflow_data.listWorkflows(statuses=ActionStatus.ACTIVE)], [workflow_id])

    def test_head_action_status_failed_poll_returns_cached_state(self):
        """Return cached head/provider statuses when a forced remote poll fails.

        Start a transfer through the fake service, then make its next status
        lookup raise an exception. The public headActionStatus call must keep
        returning ACTIVE/ACTIVE rather than propagate the error or fail the
        workflow, and the log must include the simulated server failure reason.
        """
        workflow_data = self._new_workflow_data()
        self.addCleanup(workflow_data.conn.close)
        workflow_id = workflow_data.enqueueWorkflow(HarnessGlobusTransfer())
        workflow_data.startWorkflows([workflow_id])
        self.harness.failed_status_lookup_count = 1
        self.assertEqual(workflow_data.headActionStatus(workflow_id, force_poll=True, return_api_status=True),
                         (ActionStatus.ACTIVE, 'ACTIVE'))
        self.assertEqual(workflow_data.headActionStatus(workflow_id), ActionStatus.ACTIVE)
        self.assertTrue(any('simulated remote status lookup failure' in line for line in self.logs))

    def setUp(self):
        # These tests isolate the checked-in reconciliation bodies; real
        # asynchronous lifecycles and crash recovery have separate integration tests.
        adapter = patch('amscjobmanager.workflow_data.ComputeActions', DirectComputeActions)
        adapter.start()
        self.addCleanup(adapter.stop)
        self.logs = []
        log_output = patch("amscjobmanager.logging.wfman_log_func", side_effect=self.logs.append)
        log_output.start()
        self.addCleanup(log_output.stop)
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "manager.sqlite"
        self.harness = TransferHarness()
        HarnessGlobusTransfer.harness = self.harness

    def tearDown(self):
        HarnessGlobusTransfer.harness = None
        self.tmpdir.cleanup()

    def _new_workflow_data(self, crash_at=None, scheduling_retry_delay=0):
        return WorkflowData(
            self.db_path,
            _test_crash_at=crash_at,
            _test_action_manager_factories={
                ActionClass.TRANSFER: lambda conn, **kwargs: HarnessGlobusDataTransfers(
                    conn, self.harness,
                    scheduling_retry_delay=scheduling_retry_delay,
                    **kwargs,
                )
            },
        )

    def _crash_while_starting(self, point):
        # startWorkflows normally records the job head, records the action
        # intent, submits remotely, then updates both local records to ACTIVE.
        # The injected exception models a process dying immediately after one
        # of those durable boundaries.
        workflow_data = self._new_workflow_data(point)
        workflow_id = workflow_data.enqueueWorkflow(HarnessGlobusTransfer())
        with self.assertRaises(SimulatedCrash) as crash:
            workflow_data.startWorkflows([workflow_id])
        self.assertEqual(crash.exception.point, point)

        #job status (outer), action status (inner)
        crash_states = {
            "workflow_head_scheduling_persisted": (ActionStatus.SCHEDULING, None),
            "action_intent_persisted": (ActionStatus.SCHEDULING, ActionStatus.SCHEDULING),
            "before_remote_submission": (ActionStatus.SCHEDULING, ActionStatus.SCHEDULING),
            "remote_submission_completed": (ActionStatus.SCHEDULING, ActionStatus.SCHEDULING),
            "remote_status_obtained": (ActionStatus.SCHEDULING, ActionStatus.SCHEDULING),
            "action_state_persisted": (ActionStatus.SCHEDULING, ActionStatus.ACTIVE),
            "action_state_persisted_before_workflow_head_update": (
                ActionStatus.SCHEDULING,
                ActionStatus.ACTIVE,
            ),
        }
        self._assert_persisted_states(workflow_data, workflow_id, *crash_states[point])
        workflow_data.conn.close()
        return workflow_id

    def _reopen_and_reconcile(self, scheduling_retry_delay=0):
        # A new WorkflowData instance represents the restarted process.  Reconcile
        # must repair the local action/job records from the durable intent and
        # the remote action service before normal workflow progression can
        # continue.
        workflow_data = self._new_workflow_data(scheduling_retry_delay=scheduling_retry_delay)
        workflow_data.submitSchedulingActions()
        return workflow_data

    def _assert_persisted_states(self, workflow_data, workflow_id, workflow_status, action_status):
        """Check the workflow head and ActionManager agree on durable state."""
        state = workflow_data.workflowState(workflow_id)
        self.assertEqual(state["head_action_status"], workflow_status)

        action_id = state["head_action_id"]
        manager = workflow_data.action_man[ActionClass.TRANSFER]
        if action_status is None:
            self.assertFalse(manager.hasAction(action_id))
        else:
            self.assertTrue(manager.hasAction(action_id))
            self.assertEqual(manager.queryRecordedStatus(action_id)[0], action_status)

    def _assert_reconciled_active(self, workflow_data, workflow_id):
        self._assert_persisted_states(
            workflow_data, workflow_id, ActionStatus.ACTIVE, ActionStatus.ACTIVE
        )
        self.assertEqual(workflow_data.workflowStatus(workflow_id), ActionStatus.ACTIVE)

    def test_reconciliation_submits_when_crashing_after_workflow_head_persistence(self):
        """A SCHEDULING job with no action row must be submitted on recovery."""
        # The job head and its action ID have committed, but the corresponding
        # action row has not been created.  Reconciliation must recreate the
        # action intent and submit it because no remote action can exist.
        workflow_id = self._crash_while_starting("workflow_head_scheduling_persisted")
        
        workflow_data = self._reopen_and_reconcile()
        self.assertEqual(self.harness.submission_count, 1)    
        self._assert_reconciled_active(workflow_data, workflow_id)

    def test_reconciliation_submits_when_crashing_after_action_intent(self):
        """An action row with no remote action must be submitted on recovery."""
        # Both local records say SCHEDULING, but the crash occurs before the
        # remote submission step.  Reconciliation must detect the missing
        # remote action and retry submission from the persisted action row.
        workflow_id = self._crash_while_starting("action_intent_persisted")

        workflow_data = self._reopen_and_reconcile()
        self.assertEqual(self.harness.submission_count, 1)
        self._assert_reconciled_active(workflow_data, workflow_id)

    def test_reconciliation_submits_when_crashing_before_remote_submission(self):
        # This is the final crash boundary before initiateAction().  The local
        # action intent exists, but the harness has no action under its
        # deterministic action name, so reconciliation must submit it.
        workflow_id = self._crash_while_starting("before_remote_submission")

        workflow_data = self._reopen_and_reconcile()
        self.assertEqual(self.harness.submission_count, 1)
        self._assert_reconciled_active(workflow_data, workflow_id)

    def test_reconciliation_fails_after_exhausting_submission_attempts(self):
        # With no remote action and repeated submission failures, recovery
        # must eventually stop retrying and expose a FAILED workflow state.
        workflow_id = self._crash_while_starting("action_intent_persisted")
        self.harness.failed_submission_count = 3

        # Construction performs the first attempt; the helper's observation
        # and this additional pass complete the three failed submissions.
        workflow_data = self._reopen_and_reconcile()
        workflow_data.submitSchedulingActions()

        self.assertEqual(self.harness.submission_count, 0)
        self._assert_persisted_states(
            workflow_data, workflow_id, ActionStatus.SCHEDULING, ActionStatus.SCHEDULING
        )

        workflow_data.submitSchedulingActions()
        self.assertEqual(workflow_data.workflowStatus(workflow_id), ActionStatus.FAILED)
        self._assert_persisted_states(
            workflow_data, workflow_id, ActionStatus.FAILED, ActionStatus.FAILED
        )

    def test_final_attempt_is_reconciled_before_marking_failed(self):
        workflow_data = self._new_workflow_data()
        workflow_data.action_man[ActionClass.TRANSFER].max_submission_attempts = 1
        workflow_id = workflow_data.enqueueWorkflow(AcceptedThenRaisingHarnessGlobusTransfer())

        workflow_data.startWorkflows([workflow_id])
        self._assert_persisted_states(
            workflow_data, workflow_id, ActionStatus.SCHEDULING, ActionStatus.SCHEDULING
        )

        workflow_data.submitSchedulingActions()
        self.assertEqual(self.harness.submission_count, 1)
        self._assert_reconciled_active(workflow_data, workflow_id)

    def test_initial_submission_failure_leaves_action_scheduling_for_retry(self):
        # A normal remote error must not terminate startWorkflows.  The action
        # stays durable in SCHEDULING until a later reconciliation retry.
        self.harness.failed_submission_count = 1
        workflow_data = self._new_workflow_data()
        workflow_id = workflow_data.enqueueWorkflow(HarnessGlobusTransfer())

        workflow_data.startWorkflows([workflow_id])

        self.assertEqual(self.harness.submission_count, 0)
        self._assert_persisted_states(
            workflow_data, workflow_id, ActionStatus.SCHEDULING, ActionStatus.SCHEDULING
        )

        workflow_data.submitSchedulingActions()
        self.assertEqual(self.harness.submission_count, 1)
        self._assert_reconciled_active(workflow_data, workflow_id)

    def test_fresh_intent_submits_without_remote_lookup(self):
        # No submission attempt has occurred, so there is no remote action
        # to discover and a lookup outage should not block the first submit.
        self.harness.failed_remote_lookup_count = 1 #force a failure if the manager attempts to look up the job
        workflow_data = self._new_workflow_data()
        workflow_id = workflow_data.enqueueWorkflow(HarnessGlobusTransfer())

        workflow_data.startWorkflows([workflow_id])

        self.assertEqual(self.harness.submission_count, 1) #submission occurred
        self.assertEqual(self.harness.failed_remote_lookup_count, 1) #no lookup occured
        self._assert_reconciled_active(workflow_data, workflow_id)

    def test_action_layer_submission_exception_is_persisted_for_retry(self):
        # This exception originates in the action implementation itself, not
        # in the transfer harness/API.  startWorkflows must not propagate it.
        workflow_data = self._new_workflow_data()
        workflow_id = workflow_data.enqueueWorkflow(RaisingHarnessGlobusTransfer())

        workflow_data.startWorkflows([workflow_id])

        self._assert_persisted_states(
            workflow_data, workflow_id, ActionStatus.SCHEDULING, ActionStatus.SCHEDULING
        )
        with workflow_data.conn as conn:
            row = conn.execute(
                "SELECT submission_attempts, last_submission_error FROM transfers"
            ).fetchone()
        self.assertEqual(row["submission_attempts"], 1)
        self.assertIn("action-layer submission failure", row["last_submission_error"])
        submission_log = next(line for line in self.logs if "Submission failed" in line)
        self.assertIn(str(workflow_id), submission_log)
        self.assertIn("attempt 1/", submission_log)
        self.assertIn("RuntimeError: simulated action-layer submission failure", submission_log)

    def test_submission_status_lookup_error_leaves_action_scheduling(self):
        # Submission succeeds remotely, but the immediate status API call
        # fails before the local API key/status update.  This must leave a
        # recoverable SCHEDULING action instead of propagating an API error.
        self.harness.failed_status_lookup_count = 1
        workflow_data = self._new_workflow_data()
        workflow_id = workflow_data.enqueueWorkflow(HarnessGlobusTransfer())

        workflow_data.startWorkflows([workflow_id])

        self._assert_persisted_states(
            workflow_data, workflow_id, ActionStatus.SCHEDULING, ActionStatus.SCHEDULING
        )
        self.assertEqual(self.harness.submission_count, 1)

        poll_log = next(line for line in self.logs if "Status poll failed" in line)
        self.assertIn(str(workflow_id), poll_log)
        self.assertIn("transfer-1", poll_log)
        self.assertIn("simulated remote status lookup failure", poll_log)

        workflow_data.submitSchedulingActions()
        self.assertEqual(self.harness.submission_count, 1)
        self._assert_reconciled_active(workflow_data, workflow_id)

    def test_reconciliation_lookup_error_leaves_action_scheduling(self):
        # An outage while looking up a remote action must be deferred as a
        # retryable scheduling state after an ambiguous submission attempt.
        workflow_id = self._crash_while_starting("before_remote_submission")
        # Cover both constructor recovery and the following reconciliation pass.
        self.harness.failed_remote_lookup_count = 2
        workflow_data = self._new_workflow_data()

        workflow_data.submitSchedulingActions()

        self._assert_persisted_states(
            workflow_data, workflow_id, ActionStatus.SCHEDULING, ActionStatus.SCHEDULING
        )
        self.assertEqual(self.harness.submission_count, 0)
        lookup_log = next(line for line in self.logs if "Remote lookup failed" in line)
        self.assertIn(str(workflow_id), lookup_log)
        self.assertIn("simulated remote action lookup failure", lookup_log)

        workflow_data.submitSchedulingActions() #this one should succeed
        self.assertEqual(self.harness.submission_count, 1)
        self._assert_reconciled_active(workflow_data, workflow_id)

    def test_stale_scheduling_status_does_not_poll_without_api_key(self):
        # A previous submission error leaves no API key.  Even after the usual
        # cached-status interval has elapsed, querying that action must return
        # SCHEDULING rather than polling the remote API with None.
        self.harness.failed_submission_count = 1
        workflow_data = self._new_workflow_data()
        workflow_id = workflow_data.enqueueWorkflow(HarnessGlobusTransfer())
        workflow_data.startWorkflows([workflow_id])
        action_id = workflow_data.workflowState(workflow_id)["head_action_id"]
        with workflow_data.conn as conn:
            conn.execute("UPDATE transfers SET last_update = 0")

        action_status, _ = workflow_data.action_man[ActionClass.TRANSFER].queryRecordedStatus(
            action_id
        )

        self.assertEqual(action_status, ActionStatus.SCHEDULING)

    def test_failed_active_polls_preserve_state_and_allow_other_workflows_to_finish(self):
        # Both transfers have finished remotely, but one resource cannot be polled.
        # Its next workflow step must wait while the other workflow completes.
        workflow_data = self._new_workflow_data()
        self.addCleanup(workflow_data.conn.close)
        first = workflow_data.enqueueWorkflow([HarnessGlobusTransfer(), HarnessGlobusTransfer()])
        second = workflow_data.enqueueWorkflow(HarnessGlobusTransfer())
        workflow_data.startWorkflows([first, second])
        first_state = workflow_data.workflowState(first)
        action_id = first_state["head_action_id"]
        with workflow_data.conn as conn:
            first_action = dict(conn.execute(
                "SELECT * FROM transfers WHERE action_id = ?", (action_id,)
            ).fetchone())
        for api_key in self.harness.status_by_api_key:
            self.harness.status_by_api_key[api_key] = "SUCCEEDED"
        original_status = self.harness.status

        def poll(api_key):
            if api_key == first_action["api_key"]:
                raise RuntimeError("temporary status outage")
            return original_status(api_key)

        with patch.object(self.harness, "status", side_effect=poll):
            for _ in range(3):
                workflow_data.progressActiveState(force_poll=True)
                self.assertEqual(workflow_data.workflowState(first), first_state)
                with workflow_data.conn as conn:
                    self.assertEqual(dict(conn.execute(
                        "SELECT * FROM transfers WHERE action_id = ?", (action_id,)
                    ).fetchone()), first_action)
                self.assertEqual(workflow_data.workflowStatus(second), ActionStatus.COMPLETED)
                self.assertEqual(self.harness.submission_count, 2)

        # Once polling succeeds, the retained workflow advances exactly once.
        self.assertEqual(sum("temporary status outage" in line for line in self.logs), 3)
        workflow_data.progressActiveState(force_poll=True)
        self.assertEqual(workflow_data.workflowState(first)["workflow_stage"], 1)
        self.assertEqual(self.harness.submission_count, 3)

    def test_failed_poll_after_submission_is_reconciled_without_resubmission(self):
        # Submission has already saved its remote ID and action status. If the
        # workflow's follow-up poll fails, retain its scheduling head for recovery.
        workflow_data = self._new_workflow_data()
        self.addCleanup(workflow_data.conn.close)
        workflow_id = workflow_data.enqueueWorkflow(HarnessGlobusTransfer())
        manager = workflow_data.action_man[ActionClass.TRANSFER]
        with patch.object(manager, "queryRecordedStatus", side_effect=RuntimeError("status outage")):
            workflow_data.startWorkflows([workflow_id])
            workflow_data.submitSchedulingActions()
        self.assertEqual(workflow_data.workflowState(workflow_id)["head_action_status"], ActionStatus.SCHEDULING)
        self.assertEqual(self.harness.submission_count, 1)

        workflow_data.submitSchedulingActions()
        self._assert_reconciled_active(workflow_data, workflow_id)
        self.assertEqual(self.harness.submission_count, 1)

    def test_background_manager_survives_polling_failure(self):
        # Run the actual background loop: the failed action stays active while
        # another finishes, and the manager remains alive with an accessible lock.
        manager = WorkflowManager(filename=str(self.db_path), poll_freq=0, max_workflows_active=0)
        workflow_data = manager.workflow_data
        workflow_data.action_man[ActionClass.TRANSFER] = HarnessGlobusDataTransfers(
            workflow_data.conn, self.harness
        )
        first = workflow_data.enqueueWorkflow(HarnessGlobusTransfer())
        second = workflow_data.enqueueWorkflow(HarnessGlobusTransfer())
        workflow_data.startWorkflows([first, second])
        with workflow_data.conn as conn:
            first_api_key = conn.execute(
                "SELECT api_key FROM transfers WHERE action_id = ?",
                (workflow_data.workflowState(first)["head_action_id"],),
            ).fetchone()[0]
        for api_key in self.harness.status_by_api_key:
            self.harness.status_by_api_key[api_key] = "SUCCEEDED"
        with workflow_data.conn as conn:
            conn.execute("UPDATE transfers SET last_update = 0")
        other_polled = threading.Event()

        def poll(api_key):
            if api_key == first_api_key:
                raise RuntimeError("temporary status outage")
            other_polled.set()
            return self.harness.status_by_api_key[api_key]

        with patch.object(self.harness, "status", side_effect=poll):
            manager.start()
            try:
                self.assertTrue(other_polled.wait(timeout=5), "Other workflow was not polled")
                self.assertTrue(manager.isAlive())
                self.assertTrue(manager._lock.acquire(timeout=5), "Manager lock was left held")
                try:
                    self.assertEqual(workflow_data.workflowStatus(first), ActionStatus.ACTIVE)
                    self.assertEqual(workflow_data.workflowStatus(second), ActionStatus.COMPLETED)
                finally:
                    manager._lock.release()
            finally:
                manager.stop(wait_until_done=False, close_database=True)

    def test_polling_does_not_swallow_simulated_crashes(self):
        # Fault injection must still stop execution rather than be logged as
        # a routine remote outage.
        workflow_data = self._new_workflow_data()
        self.addCleanup(workflow_data.conn.close)
        workflow_id = workflow_data.enqueueWorkflow(HarnessGlobusTransfer())
        workflow_data.startWorkflows([workflow_id])
        with patch.object(self.harness, "status", side_effect=SimulatedCrash("status_poll")):
            with self.assertRaises(SimulatedCrash):
                workflow_data.progressActiveState(force_poll=True)

    def test_reconciliation_waits_for_retry_backoff_after_a_submission_attempt(self):
        # A crash immediately before submission is ambiguous: the process may
        # have reached the remote service even though it did not save an API
        # key.  Reconciliation searches first, then waits for the configured
        # delay before submitting again if indexing still finds no action.
        workflow_id = self._crash_while_starting("before_remote_submission")

        workflow_data = self._reopen_and_reconcile(scheduling_retry_delay=60)
        self.assertEqual(self.harness.submission_count, 0)
        self._assert_persisted_states(
            workflow_data, workflow_id, ActionStatus.SCHEDULING, ActionStatus.SCHEDULING
        )
        with workflow_data.conn as conn:
            row = conn.execute("SELECT submission_attempts FROM transfers").fetchone()
        self.assertEqual(row["submission_attempts"], 1)

        # Make the persisted attempt older than the configured retry delay.
        # The next reconciliation pass must now perform the deferred retry.
        with workflow_data.conn as conn:
            conn.execute("UPDATE transfers SET last_submission_attempt = 0")
        workflow_data.submitSchedulingActions()

        self.assertEqual(self.harness.submission_count, 1)
        self._assert_reconciled_active(workflow_data, workflow_id)

    def test_reconciliation_does_not_resubmit_after_remote_submission(self):
        # The remote action was created, but its API key was not persisted
        # locally.  Reconciliation must find it by action name and repair the
        # local row without issuing a duplicate action.
        workflow_id = self._crash_while_starting("remote_submission_completed")
        self.assertEqual(self.harness.submission_count, 1)

        workflow_data = self._reopen_and_reconcile()
        self.assertEqual(self.harness.submission_count, 1)
        self._assert_reconciled_active(workflow_data, workflow_id)

    def test_reconciliation_does_not_resubmit_after_remote_status_lookup(self):
        # Submission and the first status lookup succeeded, but local state
        # has not yet been updated.  The remote action is authoritative;
        # reconciliation must find it and persist the resulting status once.
        workflow_id = self._crash_while_starting("remote_status_obtained")
        self.assertEqual(self.harness.submission_count, 1)

        workflow_data = self._reopen_and_reconcile()
        self.assertEqual(self.harness.submission_count, 1)
        self._assert_reconciled_active(workflow_data, workflow_id)

    def test_reconciliation_does_not_resubmit_after_action_state_persistence(self):
        # The action row is ACTIVE, but the job head is still SCHEDULING.
        # Reconciliation must propagate the already-persisted action state to
        # the job head and must not submit another remote action.
        workflow_id = self._crash_while_starting("action_state_persisted")
        self.assertEqual(self.harness.submission_count, 1)

        workflow_data = self._reopen_and_reconcile()
        self.assertEqual(self.harness.submission_count, 1)
        self._assert_reconciled_active(workflow_data, workflow_id)

    def test_reconciliation_repairs_stale_workflow_head_after_action_persistence(self):
        # ActionManager completed normally, then the process died before
        # WorkflowData updated the job head.  Reconciliation reads the existing
        # local action row and repairs only the job-head status.
        workflow_id = self._crash_while_starting("action_state_persisted_before_workflow_head_update")
        self.assertEqual(self.harness.submission_count, 1)

        workflow_data = self._reopen_and_reconcile()
        self.assertEqual(self.harness.submission_count, 1)
        self._assert_reconciled_active(workflow_data, workflow_id)

    def test_cancellation_recovers_after_workflow_intent_crash(self):
        # The workflow records its cancellation intent before the action or
        # remote task is touched. A restarted manager must finish the request.
        workflow_data = self._new_workflow_data()
        workflow_id = workflow_data.enqueueWorkflow(HarnessGlobusTransfer())
        workflow_data.startWorkflows([workflow_id])
        workflow_data._test_crash_at = "workflow_head_canceling_persisted"

        with self.assertRaises(SimulatedCrash):
            workflow_data.cancelWorkflow(workflow_id)
        self.assertEqual(workflow_data.workflowStatus(workflow_id), ActionStatus.CANCELING)
        self.assertEqual(self.harness.cancellation_count, 0)
        workflow_data.conn.close()

        recovered = self._new_workflow_data()
        recovered.progressActiveState()
        self.assertEqual(recovered.workflowStatus(workflow_id), ActionStatus.CANCELED)
        self.assertEqual(self.harness.cancellation_count, 1)

    def test_cancellation_recovers_after_action_intent_crash(self):
        # The workflow and action manager record their intents before the
        # remote call. WorkflowData's action copy must retain ACTIVE until it can
        # observe the manager's status. Recovery must cancel the task only once.
        workflow_data = self._new_workflow_data()
        workflow_id = workflow_data.enqueueWorkflow(HarnessGlobusTransfer())
        workflow_data.startWorkflows([workflow_id])
        workflow_data.action_man[ActionClass.TRANSFER]._test_crash_at = (
            "action_canceling_persisted"
        )

        with self.assertRaises(SimulatedCrash):
            workflow_data.cancelWorkflow(workflow_id)
        self.assertEqual(workflow_data.workflowStatus(workflow_id), ActionStatus.CANCELING)
        self._assert_persisted_states(
            workflow_data, workflow_id, ActionStatus.ACTIVE, ActionStatus.CANCELING
        )
        self.assertEqual(workflow_data.workflowState(workflow_id)['head_action_status'], ActionStatus.ACTIVE)
        self.assertEqual(self.harness.cancellation_count, 0)
        workflow_data.conn.close()

        recovered = self._new_workflow_data()
        recovered.progressActiveState()
        self._assert_persisted_states(
            recovered, workflow_id, ActionStatus.CANCELED, ActionStatus.CANCELED
        )
        self.assertEqual(self.harness.cancellation_count, 1)

    def test_action_cancellation_intent_survives_without_workflow_intent(self):
        # A direct action cancellation changes only the action row. Retry it
        # through the public entry point after reopening the database, then
        # let workflow polling update the job head.
        workflow_data = self._new_workflow_data()
        workflow_id = workflow_data.enqueueWorkflow(HarnessGlobusTransfer())
        workflow_data.startWorkflows([workflow_id])
        action_id = workflow_data.workflowState(workflow_id)["head_action_id"]
        action_manager = workflow_data.action_man[ActionClass.TRANSFER]
        action_manager._test_crash_at = "action_canceling_persisted"

        with self.assertRaises(SimulatedCrash):
            action_manager.cancelOrResumeCancelingAction(action_id)
        self._assert_persisted_states(
            workflow_data, workflow_id, ActionStatus.ACTIVE, ActionStatus.CANCELING
        )
        workflow_data.conn.close()

        recovered = self._new_workflow_data()
        recovered.action_man[ActionClass.TRANSFER].cancelOrResumeCancelingAction(action_id)
        self.assertEqual(recovered.workflowStatus(workflow_id), ActionStatus.ACTIVE)
        recovered.progressActiveState()
        self.assertEqual(recovered.workflowStatus(workflow_id), ActionStatus.CANCELED)
        self.assertEqual(self.harness.cancellation_count, 1)

    def test_cancellation_recovers_after_remote_call_crash(self):
        # The remote service accepts cancellation before either database row
        # is finalized. Recovery must not issue a second remote call.
        workflow_data = self._new_workflow_data()
        workflow_id = workflow_data.enqueueWorkflow(HarnessGlobusTransfer())
        workflow_data.startWorkflows([workflow_id])
        workflow_data.action_man[ActionClass.TRANSFER]._test_crash_at = (
            "remote_cancellation_completed"
        )

        with self.assertRaises(SimulatedCrash):
            workflow_data.cancelWorkflow(workflow_id)
        self.assertEqual(workflow_data.workflowStatus(workflow_id), ActionStatus.CANCELING)
        self._assert_persisted_states(
            workflow_data, workflow_id, ActionStatus.ACTIVE, ActionStatus.CANCELING
        )
        self.assertEqual(self.harness.cancellation_count, 1)
        workflow_data.conn.close()

        recovered = self._new_workflow_data()
        recovered.progressActiveState()
        self.assertEqual(recovered.workflowStatus(workflow_id), ActionStatus.CANCELED)
        self.assertEqual(self.harness.cancellation_count, 1)

    def test_cancellation_recovers_after_action_state_crash(self):
        # The action manager is CANCELED, but WorkflowData still records an ACTIVE head
        # and a CANCELING workflow because its status copy has not committed.
        # Recovery should finish the workflow without canceling again.
        workflow_data = self._new_workflow_data()
        workflow_id = workflow_data.enqueueWorkflow(HarnessGlobusTransfer())
        workflow_data.startWorkflows([workflow_id])
        workflow_data._test_crash_at = "action_canceled_before_workflow_head_update"

        with self.assertRaises(SimulatedCrash):
            workflow_data.cancelWorkflow(workflow_id)
        self.assertEqual(workflow_data.workflowStatus(workflow_id), ActionStatus.CANCELING)
        self._assert_persisted_states(
            workflow_data, workflow_id, ActionStatus.ACTIVE, ActionStatus.CANCELED
        )
        self.assertEqual(self.harness.cancellation_count, 1)
        workflow_data.conn.close()

        recovered = self._new_workflow_data()
        recovered.progressActiveState()
        self.assertEqual(recovered.workflowStatus(workflow_id), ActionStatus.CANCELED)
        self.assertEqual(self.harness.cancellation_count, 1)

    def test_cancellation_api_failure_remains_retryable(self):
        # A failed remote cancellation leaves both intents persisted so the
        # next manager pass can retry the call.
        workflow_data = self._new_workflow_data()
        workflow_id = workflow_data.enqueueWorkflow(HarnessGlobusTransfer())
        workflow_data.startWorkflows([workflow_id])
        self.harness.failed_cancellation_count = 1

        with self.assertRaisesRegex(RuntimeError, "remote cancellation failure"):
            workflow_data.cancelWorkflow(workflow_id)
        self.assertEqual(workflow_data.workflowStatus(workflow_id), ActionStatus.CANCELING)
        self._assert_persisted_states(
            workflow_data, workflow_id, ActionStatus.ACTIVE, ActionStatus.CANCELING
        )
        self.assertEqual(self.harness.cancellation_count, 0)

        workflow_data.progressActiveState()
        self.assertEqual(workflow_data.workflowStatus(workflow_id), ActionStatus.CANCELED)
        self.assertEqual(self.harness.cancellation_count, 1)

    def test_cancellation_status_lookup_failure_recovers_after_restart(self):
        # A status lookup fails before cancellation reaches the remote task.
        # The persisted intents allow a restarted manager to retry.
        workflow_data = self._new_workflow_data()
        workflow_id = workflow_data.enqueueWorkflow(HarnessGlobusTransfer())
        workflow_data.startWorkflows([workflow_id])
        self.harness.failed_status_lookup_count = 1

        with self.assertRaisesRegex(RuntimeError, "status lookup failure"):
            workflow_data.cancelWorkflow(workflow_id)
        self.assertEqual(workflow_data.workflowStatus(workflow_id), ActionStatus.CANCELING)
        self._assert_persisted_states(
            workflow_data, workflow_id, ActionStatus.ACTIVE, ActionStatus.CANCELING
        )
        workflow_data.conn.close()

        recovered = self._new_workflow_data()
        recovered.progressActiveState()
        self.assertEqual(recovered.workflowStatus(workflow_id), ActionStatus.CANCELED)
        self.assertEqual(self.harness.cancellation_count, 1)

    def test_cancellation_of_unsubmitted_action_does_not_submit(self):
        """Cancel missing action intent before constructor recovery can submit it.

        Interrupt selection after the workflow head is saved, then cancel on
        that same WorkflowData instance. A new WorkflowData would instead recover and
        submit the pending head immediately during construction.
        """
        workflow_data = self._new_workflow_data(crash_at='workflow_head_scheduling_persisted')
        workflow_id = workflow_data.enqueueWorkflow(HarnessGlobusTransfer())
        with self.assertRaises(SimulatedCrash):
            workflow_data.startWorkflows([workflow_id])
        workflow_data._test_crash_at = None
        workflow_data.cancelWorkflow(workflow_id)
        self.assertEqual(workflow_data.workflowStatus(workflow_id), ActionStatus.CANCELED)
        self.assertEqual(self.harness.submission_count, 0)
        workflow_data.conn.close()

    def test_cancellation_finds_submitted_action_without_saved_remote_id(self):
        # The remote task was accepted, but its ID was not saved locally.
        # Cancellation must find it by name and stop it once.
        workflow_id = self._crash_while_starting("remote_submission_completed")
        workflow_data = self._new_workflow_data()

        workflow_data.cancelWorkflow(workflow_id)

        self.assertEqual(workflow_data.workflowStatus(workflow_id), ActionStatus.CANCELED)
        self.assertEqual(self.harness.submission_count, 1)
        self.assertEqual(self.harness.cancellation_count, 1)

    def test_cancellation_waits_for_remote_name_indexing(self):
        # The first name lookup cannot yet see an accepted remote task. Keep
        # the cancellation intent until a later lookup can find and stop it.
        workflow_id = self._crash_while_starting("remote_submission_completed")
        # Keep it hidden during constructor recovery and the cancel request.
        self.harness.hidden_remote_lookup_count = 2
        workflow_data = self._new_workflow_data(scheduling_retry_delay=60)

        workflow_data.cancelWorkflow(workflow_id)
        self._assert_persisted_states(
            workflow_data, workflow_id, ActionStatus.CANCELING, ActionStatus.CANCELING
        )
        # A normally returned pending request copies the manager's actual
        # CANCELING status, even though remote discovery has not finished.
        self.assertEqual(workflow_data.workflowState(workflow_id)['head_action_status'], ActionStatus.CANCELING)
        self.assertEqual(self.harness.cancellation_count, 0)
        workflow_data.conn.close()

        recovered = self._new_workflow_data(scheduling_retry_delay=60)
        recovered.progressActiveState()
        self.assertEqual(recovered.workflowStatus(workflow_id), ActionStatus.CANCELED)
        self.assertEqual(self.harness.submission_count, 1)
        self.assertEqual(self.harness.cancellation_count, 1)

    def test_cancellation_finishes_when_remote_stays_missing_after_delay(self):
        # Submission was attempted but never reached the remote service.
        # Wait for indexing, then finish cancellation without a remote call.
        workflow_id = self._crash_while_starting("before_remote_submission")
        workflow_data = self._new_workflow_data(scheduling_retry_delay=60)

        workflow_data.cancelWorkflow(workflow_id)
        self._assert_persisted_states(
            workflow_data, workflow_id, ActionStatus.CANCELING, ActionStatus.CANCELING
        )

        # Simulate the retry delay expiring without sleeping in the test.
        with workflow_data.conn as conn:
            conn.execute(
                "UPDATE transfers SET last_submission_attempt = 0 WHERE action_id = ?",
                (workflow_data.workflowState(workflow_id)["head_action_id"],),
            )
        workflow_data.progressActiveState()
        self.assertEqual(workflow_data.workflowStatus(workflow_id), ActionStatus.CANCELED)
        self.assertEqual(self.harness.submission_count, 0)
        self.assertEqual(self.harness.cancellation_count, 0)


if __name__ == "__main__":
    unittest.main()
