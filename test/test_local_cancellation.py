"""Real local processes exercise cancellation and workflow integration offline."""

import os
from pathlib import Path
import signal
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from types import SimpleNamespace

from amscjobmanager import api_general as api, local_api
from amscjobmanager.action_manager import ActionStatus
from amscjobmanager.compute_actions import ExecuteBatchScriptComputeAction
from amscjobmanager.iri_api import compute
from amscjobmanager.job_data import JobData


class LocalCancellationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)
        self.status = local_api.LocalJobStatusWrapper(None)
        self.patches = (
            patch.object(local_api, "status_man", self.status),
            patch.object(local_api, "_local_jobs", {}),
            patch.object(local_api, "_LOCAL_CANCEL_GRACE_SECONDS", 0.1),
            patch.object(compute, "getResourceID", side_effect=AssertionError("Remote lookup")),
        )
        for active_patch in self.patches:
            active_patch.start()
        self.jobs = []

    def tearDown(self):
        # Ensure failures in an assertion cannot leave a running test job behind.
        try:
            for jobid, job in self.jobs:
                local_api.cancelJob(jobid)
                thread = job["thread"] if job else None
                if thread is not None and thread.ident is not None:
                    thread.join(timeout=3)
                    self.assertFalse(thread.is_alive())
            self.assertFalse(local_api._local_jobs)
        finally:
            for active_patch in reversed(self.patches):
                active_patch.stop()
            self.status._status_man.conn.close()
            self.temporary.cleanup()

    def _submit(self, script):
        """Start an isolated local job and retain its worker for test cleanup."""
        directory = self.directory / str(len(self.jobs))
        directory.mkdir()
        jobid = local_api.executeJobScript(script, str(directory))
        with local_api._local_jobs_lock:
            job = local_api._local_jobs.get(jobid)
        self.jobs.append((jobid, job))
        return jobid, directory

    def _wait_for(self, predicate):
        """Wait with a deadline for a worker/process transition under test."""
        deadline = time.monotonic() + 5
        while not predicate():
            if time.monotonic() >= deadline:
                self.fail("Timed out waiting for local job")
            time.sleep(0.01)

    def test_running_job_and_child_are_stopped_through_public_api(self):
        jobid, directory = self._submit("sleep 60 &\necho $! > child.pid\nwait\n")
        self._wait_for(lambda: (directory / "child.pid").exists())
        child_pid = int((directory / "child.pid").read_text())
        self.assertIsNone(api.cancelJob("LOCAL", jobid))
        self.assertEqual(api.getJobState("local", jobid), "canceled")
        self.assertNotIn(jobid, local_api._local_jobs)
        # A killed child can briefly remain a zombie awaiting OS reaping.
        proc_stat = Path(f"/proc/{child_pid}/stat")
        self._wait_for(lambda: not proc_stat.exists() or proc_stat.read_text().split()[2] == "Z")

    def test_cancel_before_worker_launch_prevents_process_creation(self):
        real_thread = threading.Thread
        with patch.object(local_api.threading, "Thread") as thread, \
             patch.object(local_api.subprocess, "run", return_value=SimpleNamespace(stdout="queued-id")), \
             patch.object(local_api.subprocess, "Popen") as launch:
            jobid, directory = self._submit("touch executed")
            target = thread.call_args.kwargs["target"]
            api.cancelJob("local", jobid)
            # Run the delayed worker only after cancellation has completed.
            worker = real_thread(target=target)
            worker.start()
            worker.join(timeout=3)
            self.assertFalse(worker.is_alive())
            launch.assert_not_called()
            self.assertFalse((directory / "executed").exists())
            self.assertEqual(local_api.getJobState(jobid), "canceled")
            self.jobs[-1] = (jobid, None)

    def test_ignoring_sigterm_escalates_to_sigkill(self):
        jobid, directory = self._submit("trap '' TERM\necho ready > ready\nwhile :; do sleep 60; done\n")
        self._wait_for(lambda: (directory / "ready").exists())
        with patch.object(local_api.os, "killpg", wraps=os.killpg) as kill:
            local_api.cancelJob(jobid)
        self.assertTrue(any(call.args[1] == signal.SIGKILL for call in kill.call_args_list))
        self.assertEqual(local_api.getJobState(jobid), "canceled")

    def test_signaling_failure_preserves_handle_for_retry(self):
        jobid, directory = self._submit("echo ready > ready\nexec sleep 60\n")
        self._wait_for(lambda: (directory / "ready").exists())
        with patch.object(local_api.os, "killpg", side_effect=PermissionError("denied")):
            with self.assertRaises(PermissionError):
                local_api.cancelJob(jobid)
        self.assertEqual(local_api.getJobState(jobid), "active")
        self.assertIn(jobid, local_api._local_jobs)
        local_api.cancelJob(jobid)
        self.assertEqual(local_api.getJobState(jobid), "canceled")

    def test_completion_and_repeated_cancellation_preserve_terminal_status(self):
        for script, expected in (("true", "completed"), ("exit 1", "failed")):
            jobid, _ = self._submit(script)
            self._wait_for(lambda: jobid not in local_api._local_jobs)
            for _ in range(2):
                local_api.cancelJob(jobid)
                self.assertEqual(local_api.getJobState(jobid), expected)
        jobid, _ = self._submit("sleep 60")
        local_api.cancelJob(jobid)
        local_api.cancelJob(jobid)
        self.assertEqual(local_api.getJobState(jobid), "canceled")

    def test_cancel_racing_completion_never_records_failed(self):
        for _ in range(8):
            jobid, _ = self._submit("true")
            local_api.cancelJob(jobid)
            self.assertIn(local_api.getJobState(jobid), ("completed", "canceled"))

    def test_simultaneous_cancellations_finish_without_deadlock(self):
        jobid, directory = self._submit("echo ready > ready\nexec sleep 60\n")
        self._wait_for(lambda: (directory / "ready").exists())
        errors = []
        start = threading.Barrier(3)

        def cancel():
            try:
                start.wait(timeout=3)
                local_api.cancelJob(jobid)
            except Exception as exc:
                errors.append(exc)

        callers = [threading.Thread(target=cancel) for _ in range(2)]
        for caller in callers:
            caller.start()
        start.wait(timeout=3)
        for caller in callers:
            caller.join(timeout=3)
            self.assertFalse(caller.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(local_api.getJobState(jobid), "canceled")

    def test_thread_start_failure_records_failed_and_releases_handle(self):
        with patch.object(local_api.threading, "Thread") as thread, \
             patch.object(local_api.subprocess, "run", return_value=SimpleNamespace(stdout="thread-failed")):
            thread.return_value.start.side_effect = RuntimeError("thread failed")
            with self.assertRaisesRegex(RuntimeError, "thread failed"):
                local_api.executeJobScript("true", str(self.directory))
        self.assertEqual(local_api.getJobState("thread-failed"), "failed")
        self.assertNotIn("thread-failed", local_api._local_jobs)

    def test_worker_launch_failure_records_failed_and_cleans_registry(self):
        with patch.object(local_api.subprocess, "run", return_value=SimpleNamespace(stdout="failed-id")), \
             patch.object(local_api.subprocess, "Popen", side_effect=OSError("launch failed")):
            jobid, _ = self._submit("true")
            self._wait_for(lambda: jobid not in local_api._local_jobs)
        self.assertEqual(local_api.getJobState(jobid), "failed")

    def test_unknown_job_and_missing_runtime_handle_raise(self):
        with self.assertRaisesRegex(Exception, "Unknown job"):
            local_api.cancelJob("unknown")
        with self.status as status:
            status.enqueueJob("untracked")
        with self.assertRaisesRegex(ValueError, "runtime handle"):
            local_api.cancelJob("untracked")

    def test_workflow_cancellation_stops_local_action_and_retains_records(self):
        jobs = JobData()
        try:
            workflow_id = jobs.enqueueJob(ExecuteBatchScriptComputeAction(
                machine="local", account="", queue="", time="60",
                script_body="echo ready > ready\nexec sleep 60\n", rundir=str(self.directory),
                nodes=1, ranks_per_node=1, gpus_per_rank=0,
            ))
            jobs.startWorkflows([workflow_id])
            jobid = jobs.conn.execute("SELECT api_key FROM computes").fetchone()[0]
            with local_api._local_jobs_lock:
                self.jobs.append((jobid, local_api._local_jobs.get(jobid)))
            self._wait_for(lambda: (self.directory / "ready").exists())
            # A failed signal preserves both cancellation intents and the handle.
            with patch.object(local_api.os, "killpg", side_effect=PermissionError("denied")):
                with self.assertRaises(PermissionError):
                    jobs.removeJob(workflow_id)
            self.assertEqual(jobs.jobState(workflow_id)["head_action_status"], ActionStatus.CANCELING)
            self.assertEqual(jobs.conn.execute("SELECT action_status FROM computes").fetchone()[0], "CANCELING")
            self.assertEqual(local_api.getJobState(jobid), "active")
            # The normal reconciliation path then finishes the same cancellation.
            jobs.reconcileCancelingActions()
            self.assertEqual(jobs.jobState(workflow_id)["head_action_status"], ActionStatus.CANCELED)
            self.assertEqual(local_api.getJobState(jobid), "canceled")
            self.assertEqual(jobs.conn.execute("SELECT action_status FROM computes").fetchone()[0], "CANCELED")
            self.assertEqual(jobs.conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0], 1)
        finally:
            jobs.conn.close()


if __name__ == "__main__":
    unittest.main()
