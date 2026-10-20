"""End-to-end mixed workflow tests against the deterministic SPOOF API."""

import os

# api_general selects its implementation at import time.  Set this before any
# amscjobmanager import so this test runs without IRI credentials or config.
os.environ["AMSC_JOB_MANAGER_API_IMPL"] = "SPOOF"

import unittest
from unittest.mock import call, patch

from amscjobmanager import spoof_api
from amscjobmanager.iri_api import state
from amscjobmanager.action_manager import ActionStatus
from amscjobmanager.actions_base import ActionClass
from amscjobmanager.compute_actions import ExecuteBatchScriptComputeAction
from amscjobmanager.globus_transfer_action import GlobusTransfer
from amscjobmanager.job_data import JobData


class SpoofWorkflowTests(unittest.TestCase):
    def setUp(self):
        # Zero delay makes each remote status query deterministically complete.
        spoof_api.resetSpoofActionState(completion_delay=0)
        self.job_data = JobData()

    def tearDown(self):
        self.job_data.conn.close()

    def test_default_compute_and_transfer_durations_are_random_and_not_immediate(self):
        spoof_api.resetSpoofActionState()
        with patch.object(spoof_api.random, "randint", side_effect=[1, 3]) as duration, patch.object(spoof_api.timemodule, "time", return_value=100) as clock:
            compute_id = spoof_api.executeJob(
                "perlmutter", "/bin/true", [], nodes=1, ranks_per_node=1,
                gpus_per_rank=1, time="300", queue="debug", account="my_proj1",
                job_run_dir="/fake/run",
            )
            transfer_id = spoof_api.globusCopy(
                dest_endpoint="perlmutter", dest_path="/fake/destination/",
                source_endpoint="polaris", source_path="/fake/source.txt",
            )
            self.assertEqual(duration.call_args_list, [call(1, 3), call(1, 3)])
            self.assertEqual(spoof_api.getJobState("perlmutter", compute_id), "active")
            self.assertEqual(spoof_api.globusTransferStatus(transfer_id), "ACTIVE")
            clock.return_value = 101
            self.assertEqual(spoof_api.getJobState("perlmutter", compute_id), "completed")
            self.assertEqual(spoof_api.globusTransferStatus(transfer_id), "ACTIVE")
            clock.return_value = 103
            self.assertEqual(spoof_api.globusTransferStatus(transfer_id), "SUCCEEDED")

    def test_personal_transfer_paths_follow_staging_rules(self):
        staging = {
            "endpoint_uuid": "personal-uuid",
            "staging_directory_local": "/local/staging",
            "staging_directory_globus": "/globus/staging",
        }
        with patch.object(state, "globus_personal_endpoint", staging), \
             patch.dict(state.special_globus_endpoints, {"personal": "personal-uuid"}):
            transfer_id = spoof_api.globusCopy(
                "personal", "/globus/staging/output", "external-uuid", "/input.txt",
            )
            self.assertEqual(spoof_api.transfer_details[transfer_id]["dest_path"], "/globus/staging/output")
            transfer_id = spoof_api.globusCopy(
                "personal-uuid", "/local/staging/output", "external-uuid", "/input.txt",
                dest_path_type="filesystem",
            )
            self.assertEqual(spoof_api.transfer_details[transfer_id]["dest_path"], "/globus/staging/output")
            with self.assertRaisesRegex(ValueError, "outside the staging directory"):
                spoof_api.globusCopy("personal", "/outside/output", "external-uuid", "/input.txt")

    @staticmethod
    def _compute_action(marker):
        return ExecuteBatchScriptComputeAction(
            machine="local",
            account="",
            queue="debug",
            time="1",
            script_body=f"echo {marker}",
            rundir="/tmp",
            nodes=1,
            ranks_per_node=1,
            gpus_per_rank=0,
        )

    @staticmethod
    def _transfer_action(marker):
        return GlobusTransfer(
            dest_endpoint="fake_endpoint2",
            dest_path=f"/tmp/destination-{marker}",
            source_endpoint="fake_endpoint1",
            source_path=f"/tmp/source-{marker}",
        )

    def _run_workflow(self, workflow):
        job_id = self.job_data.enqueueJob(workflow)
        self.job_data.startWorkflows([job_id])

        for _ in range(len(workflow) + 1):
            if self.job_data.jobStatus(job_id) == ActionStatus.COMPLETED:
                break
            self.job_data.progressActiveState(force_poll=True)
        else:
            self.fail("Workflow did not complete within one progression pass per action")
        return job_id

    def test_compute_transfer_compute_transfer_workflow(self):
        workflow = [
            self._compute_action("compute-one"),
            self._transfer_action("transfer-one"),
            self._compute_action("compute-two"),
            self._transfer_action("transfer-two"),
        ]
        job_id = self._run_workflow(workflow)

        state = self.job_data.jobState(job_id)
        self.assertEqual(self.job_data.jobStatus(job_id), ActionStatus.COMPLETED)
        self.assertEqual(state["workflow_stage"], len(workflow))
        self.assertEqual(state["head_action_class"], ActionClass.NONE)
        self.assertEqual(state["head_action_status"], ActionStatus.COMPLETED)

        events = spoof_api.getSpoofActionHistory()
        self.assertEqual(
            [(event["event"], event["action_type"]) for event in events],
            [
                ("submitted", "compute"),
                ("completed", "compute"),
                ("submitted", "transfer"),
                ("completed", "transfer"),
                ("submitted", "compute"),
                ("completed", "compute"),
                ("submitted", "transfer"),
                ("completed", "transfer"),
            ],
        )

        submissions = [event for event in events if event["event"] == "submitted"]
        self.assertEqual(len({event["name"] for event in submissions}), len(workflow))
        self.assertTrue(all(event["name"].startswith("amscjm:") for event in submissions))
        self.assertEqual(
            [
                event["details"]["script_body"]
                if event["action_type"] == "compute"
                else event["details"]["source_path"]
                for event in submissions
            ],
            [
                "echo compute-one",
                "/tmp/source-transfer-one",
                "echo compute-two",
                "/tmp/source-transfer-two",
            ],
        )

    def test_transfer_compute_transfer_workflow(self):
        workflow = [
            self._transfer_action("transfer-first"),
            self._compute_action("compute-middle"),
            self._transfer_action("transfer-last"),
        ]
        job_id = self._run_workflow(workflow)

        self.assertEqual(self.job_data.jobStatus(job_id), ActionStatus.COMPLETED)
        submissions = [
            event for event in spoof_api.getSpoofActionHistory()
            if event["event"] == "submitted"
        ]
        self.assertEqual(
            [
                event["details"]["script_body"]
                if event["action_type"] == "compute"
                else event["details"]["source_path"]
                for event in submissions
            ],
            [
                "/tmp/source-transfer-first",
                "echo compute-middle",
                "/tmp/source-transfer-last",
            ],
        )


if __name__ == "__main__":
    unittest.main()
