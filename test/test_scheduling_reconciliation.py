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

    def initiateAction(self, job_id, name=None):
        return self.harness.submit(name)

    def getInfo(self):
        return {}


@dataclass
class RaisingHarnessGlobusTransfer(HarnessGlobusTransfer):
    """Action-layer failure used to verify ActionManager exception handling."""

    def initiateAction(self, job_id, name=None):
        raise RuntimeError("simulated action-layer submission failure")


@dataclass
class AcceptedThenRaisingHarnessGlobusTransfer(HarnessGlobusTransfer):
    """Simulate a response failure after the remote service accepts work."""

    def initiateAction(self, job_id, name=None):
        self.harness.submit(name)
        raise RuntimeError("simulated response failure after acceptance")


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

    def _cancelActionInternal(self, machine, api_key):
        self.harness.cancel(api_key)


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

        #job status (outer), action status (inner)
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
        self._assert_persisted_states(
            job_data, job_id, ActionStatus.SCHEDULING, ActionStatus.SCHEDULING
        )

        job_data.reconcileSchedulingActions()
        self.assertEqual(job_data.jobStatus(job_id), ActionStatus.FAILED)
        self._assert_persisted_states(
            job_data, job_id, ActionStatus.FAILED, ActionStatus.FAILED
        )

    def test_final_attempt_is_reconciled_before_marking_failed(self):
        job_data = self._new_job_data()
        job_data.action_man[ActionClass.TRANSFER].max_submission_attempts = 1
        job_id = job_data.enqueueJob(AcceptedThenRaisingHarnessGlobusTransfer())

        job_data.startWorkflows([job_id])
        self._assert_persisted_states(
            job_data, job_id, ActionStatus.SCHEDULING, ActionStatus.SCHEDULING
        )

        job_data.reconcileSchedulingActions()
        self.assertEqual(self.harness.submission_count, 1)
        self._assert_reconciled_active(job_data, job_id)

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

    def test_fresh_intent_submits_without_remote_lookup(self):
        # No submission attempt has occurred, so there is no remote action
        # to discover and a lookup outage should not block the first submit.
        self.harness.failed_remote_lookup_count = 1 #force a failure if the manager attempts to look up the job
        job_data = self._new_job_data()
        job_id = job_data.enqueueJob(HarnessGlobusTransfer())

        job_data.startWorkflows([job_id])

        self.assertEqual(self.harness.submission_count, 1) #submission occurred
        self.assertEqual(self.harness.failed_remote_lookup_count, 1) #no lookup occured
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
        self.assertEqual(self.harness.submission_count, 1)

        job_data.reconcileSchedulingActions()
        self.assertEqual(self.harness.submission_count, 1)
        self._assert_reconciled_active(job_data, job_id)

    def test_reconciliation_lookup_error_leaves_action_scheduling(self):
        # An outage while looking up a remote action must be deferred as a
        # retryable scheduling state after an ambiguous submission attempt.
        job_id = self._crash_while_starting("before_remote_submission")
        self.harness.failed_remote_lookup_count = 1
        job_data = self._new_job_data()

        job_data.reconcileSchedulingActions()

        self._assert_persisted_states(
            job_data, job_id, ActionStatus.SCHEDULING, ActionStatus.SCHEDULING
        )
        self.assertEqual(self.harness.submission_count, 0)

        job_data.reconcileSchedulingActions() #this one should succeed
        self.assertEqual(self.harness.submission_count, 1)
        self._assert_reconciled_active(job_data, job_id)

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

    def test_cancellation_recovers_after_job_intent_crash(self):
        # The workflow records its cancellation intent before the action or
        # remote task is touched. A restarted manager must finish the request.
        job_data = self._new_job_data()
        job_id = job_data.enqueueJob(HarnessGlobusTransfer())
        job_data.startWorkflows([job_id])
        job_data._test_crash_at = "job_head_canceling_persisted"

        with self.assertRaises(SimulatedCrash):
            job_data.removeJob(job_id)
        self.assertEqual(job_data.jobStatus(job_id), ActionStatus.CANCELING)
        self.assertEqual(self.harness.cancellation_count, 0)
        job_data.conn.close()

        recovered = self._new_job_data()
        recovered.progressActiveState()
        self.assertEqual(recovered.jobStatus(job_id), ActionStatus.CANCELED)
        self.assertEqual(self.harness.cancellation_count, 1)

    def test_cancellation_recovers_after_action_intent_crash(self):
        # Both database rows reach CANCELING before the remote call. Recovery
        # must cancel the task once and finalize both rows.
        job_data = self._new_job_data()
        job_id = job_data.enqueueJob(HarnessGlobusTransfer())
        job_data.startWorkflows([job_id])
        job_data.action_man[ActionClass.TRANSFER]._test_crash_at = (
            "action_canceling_persisted"
        )

        with self.assertRaises(SimulatedCrash):
            job_data.removeJob(job_id)
        self._assert_persisted_states(
            job_data, job_id, ActionStatus.CANCELING, ActionStatus.CANCELING
        )
        self.assertEqual(self.harness.cancellation_count, 0)
        job_data.conn.close()

        recovered = self._new_job_data()
        recovered.progressActiveState()
        self._assert_persisted_states(
            recovered, job_id, ActionStatus.CANCELED, ActionStatus.CANCELED
        )
        self.assertEqual(self.harness.cancellation_count, 1)

    def test_action_cancellation_intent_survives_without_job_intent(self):
        # A direct action cancellation changes only the action row. Reconcile
        # that row, then let workflow polling update the job head.
        job_data = self._new_job_data()
        job_id = job_data.enqueueJob(HarnessGlobusTransfer())
        job_data.startWorkflows([job_id])
        action_id = job_data.jobState(job_id)["head_action_id"]
        action_manager = job_data.action_man[ActionClass.TRANSFER]
        action_manager._test_crash_at = "action_canceling_persisted"

        with self.assertRaises(SimulatedCrash):
            action_manager.cancelAction(action_id)
        self._assert_persisted_states(
            job_data, job_id, ActionStatus.ACTIVE, ActionStatus.CANCELING
        )
        job_data.conn.close()

        recovered = self._new_job_data()
        recovered.action_man[ActionClass.TRANSFER].reconcileCancelingAction(action_id)
        self.assertEqual(recovered.jobStatus(job_id), ActionStatus.ACTIVE)
        recovered.progressActiveState()
        self.assertEqual(recovered.jobStatus(job_id), ActionStatus.CANCELED)
        self.assertEqual(self.harness.cancellation_count, 1)

    def test_cancellation_recovers_after_remote_call_crash(self):
        # The remote service accepts cancellation before either database row
        # is finalized. Recovery must not issue a second remote call.
        job_data = self._new_job_data()
        job_id = job_data.enqueueJob(HarnessGlobusTransfer())
        job_data.startWorkflows([job_id])
        job_data.action_man[ActionClass.TRANSFER]._test_crash_at = (
            "remote_cancellation_completed"
        )

        with self.assertRaises(SimulatedCrash):
            job_data.removeJob(job_id)
        self._assert_persisted_states(
            job_data, job_id, ActionStatus.CANCELING, ActionStatus.CANCELING
        )
        self.assertEqual(self.harness.cancellation_count, 1)
        job_data.conn.close()

        recovered = self._new_job_data()
        recovered.progressActiveState()
        self.assertEqual(recovered.jobStatus(job_id), ActionStatus.CANCELED)
        self.assertEqual(self.harness.cancellation_count, 1)

    def test_cancellation_recovers_after_action_state_crash(self):
        # The action is CANCELED, but the workflow head still says CANCELING.
        # Recovery should finish the workflow without canceling again.
        job_data = self._new_job_data()
        job_id = job_data.enqueueJob(HarnessGlobusTransfer())
        job_data.startWorkflows([job_id])
        job_data._test_crash_at = "action_canceled_before_job_head_update"

        with self.assertRaises(SimulatedCrash):
            job_data.removeJob(job_id)
        self._assert_persisted_states(
            job_data, job_id, ActionStatus.CANCELING, ActionStatus.CANCELED
        )
        self.assertEqual(self.harness.cancellation_count, 1)
        job_data.conn.close()

        recovered = self._new_job_data()
        recovered.progressActiveState()
        self.assertEqual(recovered.jobStatus(job_id), ActionStatus.CANCELED)
        self.assertEqual(self.harness.cancellation_count, 1)

    def test_cancellation_api_failure_remains_retryable(self):
        # A failed remote cancellation leaves both intents persisted so the
        # next manager pass can retry the call.
        job_data = self._new_job_data()
        job_id = job_data.enqueueJob(HarnessGlobusTransfer())
        job_data.startWorkflows([job_id])
        self.harness.failed_cancellation_count = 1

        with self.assertRaisesRegex(RuntimeError, "remote cancellation failure"):
            job_data.removeJob(job_id)
        self._assert_persisted_states(
            job_data, job_id, ActionStatus.CANCELING, ActionStatus.CANCELING
        )
        self.assertEqual(self.harness.cancellation_count, 0)

        job_data.progressActiveState()
        self.assertEqual(job_data.jobStatus(job_id), ActionStatus.CANCELED)
        self.assertEqual(self.harness.cancellation_count, 1)

    def test_cancellation_status_lookup_failure_recovers_after_restart(self):
        # A status lookup fails before cancellation reaches the remote task.
        # The persisted intents allow a restarted manager to retry.
        job_data = self._new_job_data()
        job_id = job_data.enqueueJob(HarnessGlobusTransfer())
        job_data.startWorkflows([job_id])
        self.harness.failed_status_lookup_count = 1

        with self.assertRaisesRegex(RuntimeError, "status lookup failure"):
            job_data.removeJob(job_id)
        self._assert_persisted_states(
            job_data, job_id, ActionStatus.CANCELING, ActionStatus.CANCELING
        )
        job_data.conn.close()

        recovered = self._new_job_data()
        recovered.progressActiveState()
        self.assertEqual(recovered.jobStatus(job_id), ActionStatus.CANCELED)
        self.assertEqual(self.harness.cancellation_count, 1)

    def test_cancellation_of_unsubmitted_action_does_not_submit(self):
        # Submission stopped after saving the workflow head, before creating
        # a remote task. Cancellation must not start that task.
        job_id = self._crash_while_starting("job_head_scheduling_persisted")
        job_data = self._new_job_data()

        job_data.removeJob(job_id)

        self.assertEqual(job_data.jobStatus(job_id), ActionStatus.CANCELED)
        self.assertEqual(self.harness.submission_count, 0)

    def test_cancellation_finds_submitted_action_without_saved_remote_id(self):
        # The remote task was accepted, but its ID was not saved locally.
        # Cancellation must find it by name and stop it once.
        job_id = self._crash_while_starting("remote_submission_completed")
        job_data = self._new_job_data()

        job_data.removeJob(job_id)

        self.assertEqual(job_data.jobStatus(job_id), ActionStatus.CANCELED)
        self.assertEqual(self.harness.submission_count, 1)
        self.assertEqual(self.harness.cancellation_count, 1)

    def test_cancellation_waits_for_remote_name_indexing(self):
        # The first name lookup cannot yet see an accepted remote task. Keep
        # the cancellation intent until a later lookup can find and stop it.
        job_id = self._crash_while_starting("remote_submission_completed")
        self.harness.hidden_remote_lookup_count = 1
        job_data = self._new_job_data(scheduling_retry_delay=60)

        job_data.removeJob(job_id)
        self._assert_persisted_states(
            job_data, job_id, ActionStatus.CANCELING, ActionStatus.CANCELING
        )
        self.assertEqual(self.harness.cancellation_count, 0)
        job_data.conn.close()

        recovered = self._new_job_data(scheduling_retry_delay=60)
        recovered.progressActiveState()
        self.assertEqual(recovered.jobStatus(job_id), ActionStatus.CANCELED)
        self.assertEqual(self.harness.submission_count, 1)
        self.assertEqual(self.harness.cancellation_count, 1)

    def test_cancellation_finishes_when_remote_stays_missing_after_delay(self):
        # Submission was attempted but never reached the remote service.
        # Wait for indexing, then finish cancellation without a remote call.
        job_id = self._crash_while_starting("before_remote_submission")
        job_data = self._new_job_data(scheduling_retry_delay=60)

        job_data.removeJob(job_id)
        self._assert_persisted_states(
            job_data, job_id, ActionStatus.CANCELING, ActionStatus.CANCELING
        )

        # Simulate the retry delay expiring without sleeping in the test.
        job_data.conn.execute(
            "UPDATE transfers SET last_submission_attempt = 0 WHERE action_id = ?",
            (job_data.jobState(job_id)["head_action_id"],),
        )
        job_data.progressActiveState()
        self.assertEqual(job_data.jobStatus(job_id), ActionStatus.CANCELED)
        self.assertEqual(self.harness.submission_count, 0)
        self.assertEqual(self.harness.cancellation_count, 0)


if __name__ == "__main__":
    unittest.main()
