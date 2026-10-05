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

    def submit(self, name):
        self.submission_count += 1
        api_key = f"transfer-{self.submission_count}"
        self.by_name[name] = api_key
        self.status_by_api_key[api_key] = "ACTIVE"
        return api_key

    def find(self, name):
        return self.by_name.get(name)

    def status(self, api_key):
        return self.status_by_api_key[api_key]


@dataclass
class HarnessGlobusTransfer(TransferActionBase):
    """Transfer action whose remote side effect is owned by the test harness."""

    harness = None

    def initiateAction(self, job_id, name=None):
        return self.harness.submit(name)

    def getInfo(self):
        return {}


class HarnessGlobusDataTransfers(GlobusDataTransfers):
    """The real transfer manager with only its remote operations overridden."""

    def __init__(self, connection, harness, **kwargs):
        self.harness = harness
        super().__init__(connection, **kwargs)

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

    def _new_job_data(self, crash_at=None):
        return JobData(
            self.db_path,
            _test_crash_at=crash_at,
            _test_action_manager_factories={
                ActionClass.TRANSFER: lambda conn, **kwargs: HarnessGlobusDataTransfers(
                    conn, self.harness, **kwargs
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
        job_data.conn.close()
        return job_id

    def _reopen_and_reconcile(self):
        # A new JobData instance represents the restarted process.  Reconcile
        # must repair the local action/job records from the durable intent and
        # the remote action service before normal workflow progression can
        # continue.
        job_data = self._new_job_data()
        job_data.reconcileSchedulingActions()
        return job_data

    @unittest.expectedFailure
    def test_reconciliation_submits_when_crashing_after_job_head_persistence(self):
        """A SCHEDULING job with no action row must be submitted on recovery."""
        # The job head and its action ID have committed, but the corresponding
        # action row has not been created.  Reconciliation must recreate the
        # action intent and submit it because no remote action can exist.
        job_id = self._crash_while_starting("job_head_scheduling_persisted")
        
        job_data = self._reopen_and_reconcile()        
        self.assertEqual(self.harness.submission_count, 1)    
        self.assertEqual(job_data.jobStatus(job_id), ActionStatus.ACTIVE)        

    @unittest.expectedFailure
    def test_reconciliation_submits_when_crashing_after_action_intent(self):
        """An action row with no remote action must be submitted on recovery."""
        # Both local records say SCHEDULING, but the crash occurs before the
        # remote submission step.  Reconciliation must detect the missing
        # remote action and retry submission from the persisted action row.
        job_id = self._crash_while_starting("action_intent_persisted")

        job_data = self._reopen_and_reconcile()
        self.assertEqual(self.harness.submission_count, 1)
        self.assertEqual(job_data.jobStatus(job_id), ActionStatus.ACTIVE)

    @unittest.expectedFailure
    def test_reconciliation_submits_when_crashing_before_remote_submission(self):
        # This is the final crash boundary before initiateAction().  The local
        # action intent exists, but the harness has no action under its
        # deterministic action name, so reconciliation must submit it.
        job_id = self._crash_while_starting("before_remote_submission")

        job_data = self._reopen_and_reconcile()
        self.assertEqual(self.harness.submission_count, 1)
        self.assertEqual(job_data.jobStatus(job_id), ActionStatus.ACTIVE)

    def test_reconciliation_does_not_resubmit_after_remote_submission(self):
        # The remote action was created, but its API key was not persisted
        # locally.  Reconciliation must find it by action name and repair the
        # local row without issuing a duplicate action.
        job_id = self._crash_while_starting("remote_submission_completed")
        self.assertEqual(self.harness.submission_count, 1)

        job_data = self._reopen_and_reconcile()
        self.assertEqual(self.harness.submission_count, 1)
        self.assertEqual(job_data.jobStatus(job_id), ActionStatus.ACTIVE)

    def test_reconciliation_does_not_resubmit_after_remote_status_lookup(self):
        # Submission and the first status lookup succeeded, but local state
        # has not yet been updated.  The remote action is authoritative;
        # reconciliation must find it and persist the resulting status once.
        job_id = self._crash_while_starting("remote_status_obtained")
        self.assertEqual(self.harness.submission_count, 1)

        job_data = self._reopen_and_reconcile()
        self.assertEqual(self.harness.submission_count, 1)
        self.assertEqual(job_data.jobStatus(job_id), ActionStatus.ACTIVE)

    def test_reconciliation_does_not_resubmit_after_action_state_persistence(self):
        # The action row is ACTIVE, but the job head is still SCHEDULING.
        # Reconciliation must propagate the already-persisted action state to
        # the job head and must not submit another remote action.
        job_id = self._crash_while_starting("action_state_persisted")
        self.assertEqual(self.harness.submission_count, 1)

        job_data = self._reopen_and_reconcile()
        self.assertEqual(self.harness.submission_count, 1)
        self.assertEqual(job_data.jobStatus(job_id), ActionStatus.ACTIVE)

    def test_reconciliation_repairs_stale_job_head_after_action_persistence(self):
        # ActionManager completed normally, then the process died before
        # JobData updated the job head.  Reconciliation reads the existing
        # local action row and repairs only the job-head status.
        job_id = self._crash_while_starting("action_state_persisted_before_job_head_update")
        self.assertEqual(self.harness.submission_count, 1)

        job_data = self._reopen_and_reconcile()
        self.assertEqual(self.harness.submission_count, 1)
        self.assertEqual(job_data.jobStatus(job_id), ActionStatus.ACTIVE)


if __name__ == "__main__":
    unittest.main()
