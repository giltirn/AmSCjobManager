"""Crash-boundary tests for action scheduling reconciliation.

The harness substitutes only the remote Globus service.  JobData and the
ActionManager implementation execute their normal database workflow.
"""

from dataclasses import dataclass
from pathlib import Path
import tempfile
import unittest

from amscjobmanager.action_manager import ActionStatus, SimulatedCrash
from amscjobmanager.actions_base import TransferActionBase
from amscjobmanager.globus_transfer_action import GlobusDataTransfers
from amscjobmanager.job_data import JobData
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
        return self.by_name.get(name)

    def status(self, api_key):
        if self.failed_status_lookup_count:
            self.failed_status_lookup_count -= 1
            raise RuntimeError("simulated remote status lookup failure")
        return self.status_by_api_key[api_key]


@dataclass
class HarnessGlobusTransfer(TransferActionBase):
    """Transfer action whose remote side effect is owned by the test harness."""

    harness = None

    def initiateAction(self, job_id, name=None):
        return self.harness.submit(name)

    def getInfo(self):
        return {}


@dataclass
class RaisingHarnessGlobusTransfer(HarnessGlobusTransfer):
    """Action-layer failure used to verify ActionManager exception handling."""

    def initiateAction(self, job_id, name=None):
        raise RuntimeError("simulated action-layer submission failure")


class HarnessGlobusDataTransfers(GlobusDataTransfers):
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


class SchedulingReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "manager.sqlite"
        self.harness = TransferHarness()
        HarnessGlobusTransfer.harness = self.harness

    def tearDown(self):
        HarnessGlobusTransfer.harness = None
        self.tmpdir.cleanup()

    def _new_job_data(self, crash_at=None, scheduling_retry_delay=0):
        return JobData(
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
        job_data = self._new_job_data(point)
        job_id = job_data.enqueueJob(HarnessGlobusTransfer())
        with self.assertRaises(SimulatedCrash) as crash:
            job_data.startWorkflows([job_id])
        self.assertEqual(crash.exception.point, point)
        crash_states = {
            "job_head_scheduling_persisted": (ActionStatus.SCHEDULING, None),
            "action_intent_persisted": (ActionStatus.SCHEDULING, ActionStatus.SCHEDULING),
            "before_remote_submission": (ActionStatus.SCHEDULING, ActionStatus.SCHEDULING),
            "remote_submission_completed": (ActionStatus.SCHEDULING, ActionStatus.SCHEDULING),
            "remote_status_obtained": (ActionStatus.SCHEDULING, ActionStatus.SCHEDULING),
            "action_state_persisted": (ActionStatus.SCHEDULING, ActionStatus.ACTIVE),
            "action_state_persisted_before_job_head_update": (
                ActionStatus.SCHEDULING,
                ActionStatus.ACTIVE,
            ),
        }
        self._assert_persisted_states(job_data, job_id, *crash_states[point])
        job_data.conn.close()
        return job_id

    def _reopen_and_reconcile(self, scheduling_retry_delay=0):
        # A new JobData instance represents the restarted process.  Reconcile
        # must repair the local action/job records from the durable intent and
        # the remote action service before normal workflow progression can
        # continue.
        job_data = self._new_job_data(scheduling_retry_delay=scheduling_retry_delay)
        job_data.reconcileSchedulingActions()
        return job_data

    def _assert_persisted_states(self, job_data, job_id, job_status, action_status):
        """Check the workflow head and ActionManager agree on durable state."""
        state = job_data.jobState(job_id)
        self.assertEqual(state["head_action_status"], job_status)

        action_id = state["head_action_id"]
        manager = job_data.action_man[ActionClass.TRANSFER]
        if action_status is None:
            self.assertFalse(manager.hasAction(action_id))
        else:
            self.assertTrue(manager.hasAction(action_id))
            self.assertEqual(manager.queryStatus(action_id)[0], action_status)

    def _assert_reconciled_active(self, job_data, job_id):
        self._assert_persisted_states(
            job_data, job_id, ActionStatus.ACTIVE, ActionStatus.ACTIVE
        )
        self.assertEqual(job_data.jobStatus(job_id), ActionStatus.ACTIVE)

    def test_reconciliation_submits_when_crashing_after_job_head_persistence(self):
        """A SCHEDULING job with no action row must be submitted on recovery."""
        # The job head and its action ID have committed, but the corresponding
        # action row has not been created.  Reconciliation must recreate the
        # action intent and submit it because no remote action can exist.
        job_id = self._crash_while_starting("job_head_scheduling_persisted")
        
        job_data = self._reopen_and_reconcile()        
        self.assertEqual(self.harness.submission_count, 1)    
        self._assert_reconciled_active(job_data, job_id)

    def test_reconciliation_submits_when_crashing_after_action_intent(self):
        """An action row with no remote action must be submitted on recovery."""
        # Both local records say SCHEDULING, but the crash occurs before the
        # remote submission step.  Reconciliation must detect the missing
        # remote action and retry submission from the persisted action row.
        job_id = self._crash_while_starting("action_intent_persisted")

        job_data = self._reopen_and_reconcile()
        self.assertEqual(self.harness.submission_count, 1)
        self._assert_reconciled_active(job_data, job_id)

    def test_reconciliation_submits_when_crashing_before_remote_submission(self):
        # This is the final crash boundary before initiateAction().  The local
        # action intent exists, but the harness has no action under its
        # deterministic action name, so reconciliation must submit it.
        job_id = self._crash_while_starting("before_remote_submission")

        job_data = self._reopen_and_reconcile()
        self.assertEqual(self.harness.submission_count, 1)
        self._assert_reconciled_active(job_data, job_id)

    def test_reconciliation_fails_after_exhausting_submission_attempts(self):
        # With no remote action and repeated submission failures, recovery
        # must eventually stop retrying and expose a FAILED workflow state.
        job_id = self._crash_while_starting("action_intent_persisted")
        self.harness.failed_submission_count = 3

        job_data = self._reopen_and_reconcile()
        job_data.reconcileSchedulingActions()
        job_data.reconcileSchedulingActions()

        self.assertEqual(self.harness.submission_count, 0)
        self.assertEqual(job_data.jobStatus(job_id), ActionStatus.FAILED)
        self._assert_persisted_states(
            job_data, job_id, ActionStatus.FAILED, ActionStatus.FAILED
        )

    def test_initial_submission_failure_leaves_action_scheduling_for_retry(self):
        # A normal remote error must not terminate startWorkflows.  The action
        # stays durable in SCHEDULING until a later reconciliation retry.
        self.harness.failed_submission_count = 1
        job_data = self._new_job_data()
        job_id = job_data.enqueueJob(HarnessGlobusTransfer())

        job_data.startWorkflows([job_id])

        self.assertEqual(self.harness.submission_count, 0)
        self._assert_persisted_states(
            job_data, job_id, ActionStatus.SCHEDULING, ActionStatus.SCHEDULING
        )

        job_data.reconcileSchedulingActions()
        self.assertEqual(self.harness.submission_count, 1)
        self._assert_reconciled_active(job_data, job_id)

    def test_action_layer_submission_exception_is_persisted_for_retry(self):
        # This exception originates in the action implementation itself, not
        # in the transfer harness/API.  startWorkflows must not propagate it.
        job_data = self._new_job_data()
        job_id = job_data.enqueueJob(RaisingHarnessGlobusTransfer())

        job_data.startWorkflows([job_id])

        self._assert_persisted_states(
            job_data, job_id, ActionStatus.SCHEDULING, ActionStatus.SCHEDULING
        )
        row = job_data.conn.execute(
            "SELECT submission_attempts, last_submission_error FROM transfers"
        ).fetchone()
        self.assertEqual(row["submission_attempts"], 1)
        self.assertIn("action-layer submission failure", row["last_submission_error"])

    @unittest.expectedFailure
    def test_submission_status_lookup_error_leaves_action_scheduling(self):
        # Submission succeeds remotely, but the immediate status API call
        # fails before the local API key/status update.  This must leave a
        # recoverable SCHEDULING action instead of propagating an API error.
        self.harness.failed_status_lookup_count = 1
        job_data = self._new_job_data()
        job_id = job_data.enqueueJob(HarnessGlobusTransfer())

        job_data.startWorkflows([job_id])

        self._assert_persisted_states(
            job_data, job_id, ActionStatus.SCHEDULING, ActionStatus.SCHEDULING
        )

    @unittest.expectedFailure
    def test_reconciliation_lookup_error_leaves_action_scheduling(self):
        # An outage while looking up a remote action must be deferred as a
        # retryable scheduling state, rather than terminating progression.
        job_id = self._crash_while_starting("action_intent_persisted")
        self.harness.failed_remote_lookup_count = 1
        job_data = self._new_job_data()

        job_data.reconcileSchedulingActions()

        self._assert_persisted_states(
            job_data, job_id, ActionStatus.SCHEDULING, ActionStatus.SCHEDULING
        )

    @unittest.expectedFailure
    def test_stale_scheduling_status_does_not_poll_without_api_key(self):
        # A previous submission error leaves no API key.  Even after the usual
        # cached-status interval has elapsed, querying that action must return
        # SCHEDULING rather than polling the remote API with None.
        self.harness.failed_submission_count = 1
        job_data = self._new_job_data()
        job_id = job_data.enqueueJob(HarnessGlobusTransfer())
        job_data.startWorkflows([job_id])
        action_id = job_data.jobState(job_id)["head_action_id"]
        job_data.conn.execute("UPDATE transfers SET last_update = 0")

        action_status, _ = job_data.action_man[ActionClass.TRANSFER].queryStatus(
            action_id
        )

        self.assertEqual(action_status, ActionStatus.SCHEDULING)

    def test_reconciliation_waits_for_retry_backoff_after_a_submission_attempt(self):
        # A crash immediately before submission is ambiguous: the process may
        # have reached the remote service even though it did not save an API
        # key.  Reconciliation searches first, then waits for the configured
        # delay before submitting again if indexing still finds no action.
        job_id = self._crash_while_starting("before_remote_submission")

        job_data = self._reopen_and_reconcile(scheduling_retry_delay=60)
        self.assertEqual(self.harness.submission_count, 0)
        self._assert_persisted_states(
            job_data, job_id, ActionStatus.SCHEDULING, ActionStatus.SCHEDULING
        )
        row = job_data.conn.execute("SELECT submission_attempts FROM transfers").fetchone()
        self.assertEqual(row["submission_attempts"], 1)

        # Make the persisted attempt older than the configured retry delay.
        # The next reconciliation pass must now perform the deferred retry.
        job_data.conn.execute("UPDATE transfers SET last_submission_attempt = 0")
        job_data.reconcileSchedulingActions()

        self.assertEqual(self.harness.submission_count, 1)
        self._assert_reconciled_active(job_data, job_id)

    def test_reconciliation_does_not_resubmit_after_remote_submission(self):
        # The remote action was created, but its API key was not persisted
        # locally.  Reconciliation must find it by action name and repair the
        # local row without issuing a duplicate action.
        job_id = self._crash_while_starting("remote_submission_completed")
        self.assertEqual(self.harness.submission_count, 1)

        job_data = self._reopen_and_reconcile()
        self.assertEqual(self.harness.submission_count, 1)
        self._assert_reconciled_active(job_data, job_id)

    def test_reconciliation_does_not_resubmit_after_remote_status_lookup(self):
        # Submission and the first status lookup succeeded, but local state
        # has not yet been updated.  The remote action is authoritative;
        # reconciliation must find it and persist the resulting status once.
        job_id = self._crash_while_starting("remote_status_obtained")
        self.assertEqual(self.harness.submission_count, 1)

        job_data = self._reopen_and_reconcile()
        self.assertEqual(self.harness.submission_count, 1)
        self._assert_reconciled_active(job_data, job_id)

    def test_reconciliation_does_not_resubmit_after_action_state_persistence(self):
        # The action row is ACTIVE, but the job head is still SCHEDULING.
        # Reconciliation must propagate the already-persisted action state to
        # the job head and must not submit another remote action.
        job_id = self._crash_while_starting("action_state_persisted")
        self.assertEqual(self.harness.submission_count, 1)

        job_data = self._reopen_and_reconcile()
        self.assertEqual(self.harness.submission_count, 1)
        self._assert_reconciled_active(job_data, job_id)

    def test_reconciliation_repairs_stale_job_head_after_action_persistence(self):
        # ActionManager completed normally, then the process died before
        # JobData updated the job head.  Reconciliation reads the existing
        # local action row and repairs only the job-head status.
        job_id = self._crash_while_starting("action_state_persisted_before_job_head_update")
        self.assertEqual(self.harness.submission_count, 1)

        job_data = self._reopen_and_reconcile()
        self.assertEqual(self.harness.submission_count, 1)
        self._assert_reconciled_active(job_data, job_id)


if __name__ == "__main__":
    unittest.main()
