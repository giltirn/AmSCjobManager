"""Check compute resource gating and submission recovery across crashes."""

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from amscjobmanager import api_general, compute_action_manager, compute_actions
from amscjobmanager.action_manager import ActionStatus, SimulatedCrash
from amscjobmanager.actions_base import ActionClass
from amscjobmanager.compute_actions import ExecuteBatchScriptComputeAction
from amscjobmanager.job_data import JobData


class ComputeResourceSchedulingTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.db_path = Path(self.tmpdir.name) / "manager.sqlite"
        self.remote_jobs = {}

        def submit(*args, **kwargs):
            api_key = f"compute-{len(self.remote_jobs) + 1}"
            self.remote_jobs[kwargs["name"]] = api_key
            return api_key

        resource_patch = patch.object(api_general, "queryMachineStatus", return_value=True)
        submission_patch = patch.object(
            compute_actions, "executeBatchJobCompat", side_effect=submit
        )
        lookup_patch = patch.object(
            compute_action_manager, "findJobByName",
            side_effect=lambda machine, name: self.remote_jobs.get(name),
        )
        status_patch = patch.object(
            compute_action_manager, "getJobState", return_value="active"
        )
        self.resource_status = resource_patch.start()
        self.submit = submission_patch.start()
        self.find = lookup_patch.start()
        self.remote_status = status_patch.start()
        for active_patch in (resource_patch, submission_patch, lookup_patch, status_patch):
            self.addCleanup(active_patch.stop)

    def _new_job_data(self, crash_at=None, max_submission_attempts=3):
        job_data = JobData(self.db_path, _test_crash_at=crash_at)
        manager = job_data.action_man[ActionClass.COMPUTE]
        manager.scheduling_retry_delay = 0
        manager.max_submission_attempts = max_submission_attempts
        self.addCleanup(job_data.conn.close)
        return job_data

    @staticmethod
    def _action():
        return ExecuteBatchScriptComputeAction(
            machine="local", account="", queue="debug", time="60",
            script_body="true", rundir="/tmp", nodes=1, ranks_per_node=1,
            gpus_per_rank=0,
        )

    def _assert_scheduling_without_attempt(self, job_data, job_id):
        self.assertEqual(job_data.jobState(job_id)["head_action_status"], ActionStatus.SCHEDULING)
        action = job_data.conn.execute(
            "SELECT action_status, submission_attempts, api_key FROM computes"
        ).fetchone()
        self.assertEqual(action["action_status"], ActionStatus.SCHEDULING.name)
        self.assertEqual(action["submission_attempts"], 0)
        self.assertIsNone(action["api_key"])
        self.submit.assert_not_called()

    def test_down_resource_defers_without_consuming_an_attempt(self):
        self.resource_status.return_value = False
        job_data = self._new_job_data()
        job_id = job_data.enqueueJob(self._action())

        job_data.startWorkflows([job_id])
        self._assert_scheduling_without_attempt(job_data, job_id)

        self.resource_status.return_value = True
        job_data.reconcileSchedulingActions()
        self.assertEqual(self.submit.call_count, 1)
        self.assertEqual(job_data.jobState(job_id)["head_action_status"], ActionStatus.ACTIVE)
        self.assertEqual(
            job_data.conn.execute("SELECT submission_attempts FROM computes").fetchone()[0],
            1,
        )

    def test_status_query_error_defers_and_recovers_after_restart(self):
        self.resource_status.side_effect = RuntimeError("resource status unavailable")
        job_data = self._new_job_data()
        job_id = job_data.enqueueJob(self._action())
        job_data.startWorkflows([job_id])
        self._assert_scheduling_without_attempt(job_data, job_id)
        job_data.conn.close()

        self.resource_status.side_effect = None
        recovered = self._new_job_data()
        recovered.reconcileSchedulingActions()
        self.assertEqual(self.submit.call_count, 1)
        self.assertEqual(recovered.jobState(job_id)["head_action_status"], ActionStatus.ACTIVE)

    def test_crash_around_resource_check_preserves_unattempted_intent(self):
        for point in ("before_resource_status_check", "resource_status_checked"):
            with self.subTest(point=point):
                self.db_path = Path(self.tmpdir.name) / f"{point}.sqlite"
                self.submit.reset_mock()
                self.resource_status.reset_mock()
                job_data = self._new_job_data(crash_at=point)
                job_id = job_data.enqueueJob(self._action())

                with self.assertRaises(SimulatedCrash) as crash:
                    job_data.startWorkflows([job_id])
                self.assertEqual(crash.exception.point, point)
                self._assert_scheduling_without_attempt(job_data, job_id)
                job_data.conn.close()

                recovered = self._new_job_data()
                recovered.reconcileSchedulingActions()
                self.assertEqual(self.submit.call_count, 1)
                self.assertEqual(
                    recovered.jobState(job_id)["head_action_status"], ActionStatus.ACTIVE
                )
                recovered.conn.close()

    def test_recovery_finds_remote_job_before_checking_resource(self):
        job_data = self._new_job_data(crash_at="remote_submission_completed")
        job_id = job_data.enqueueJob(self._action())
        with self.assertRaises(SimulatedCrash):
            job_data.startWorkflows([job_id])
        self.assertEqual(self.submit.call_count, 1)
        job_data.conn.close()

        self.resource_status.reset_mock(side_effect=True)
        self.resource_status.side_effect = AssertionError("availability check must be skipped")
        recovered = self._new_job_data()
        recovered.reconcileSchedulingActions()
        self.resource_status.assert_not_called()
        self.assertEqual(self.submit.call_count, 1)
        self.assertEqual(recovered.jobState(job_id)["head_action_status"], ActionStatus.ACTIVE)

    def test_down_resource_does_not_finalize_exhausted_submission(self):
        self.submit.side_effect = RuntimeError("remote submission failure")
        job_data = self._new_job_data(max_submission_attempts=1)
        job_id = job_data.enqueueJob(self._action())
        job_data.startWorkflows([job_id])
        self.assertEqual(self.submit.call_count, 1)
        self.assertEqual(job_data.jobState(job_id)["head_action_status"], ActionStatus.SCHEDULING)

        self.resource_status.return_value = False
        job_data.reconcileSchedulingActions()
        self.assertEqual(job_data.jobState(job_id)["head_action_status"], ActionStatus.SCHEDULING)
        self.assertEqual(self.submit.call_count, 1)

        self.resource_status.return_value = True
        job_data.reconcileSchedulingActions()
        self.assertEqual(job_data.jobState(job_id)["head_action_status"], ActionStatus.FAILED)
        self.assertEqual(self.submit.call_count, 1)

    def test_crash_after_recording_attempt_keeps_name_recovery(self):
        job_data = self._new_job_data(crash_at="before_remote_submission")
        job_id = job_data.enqueueJob(self._action())
        with self.assertRaises(SimulatedCrash):
            job_data.startWorkflows([job_id])
        self.assertEqual(
            job_data.conn.execute("SELECT submission_attempts FROM computes").fetchone()[0],
            1,
        )
        self.submit.assert_not_called()
        job_data.conn.close()

        self.resource_status.return_value = False
        recovered = self._new_job_data()
        recovered.reconcileSchedulingActions()
        self.assertEqual(recovered.jobState(job_id)["head_action_status"], ActionStatus.SCHEDULING)
        self.assertEqual(
            recovered.conn.execute("SELECT submission_attempts FROM computes").fetchone()[0],
            1,
        )

        self.resource_status.return_value = True
        recovered.reconcileSchedulingActions()
        self.assertEqual(self.submit.call_count, 1)
        self.assertEqual(recovered.jobState(job_id)["head_action_status"], ActionStatus.ACTIVE)


if __name__ == "__main__":
    unittest.main()
