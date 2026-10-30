"""Exercise local executable jobs, hook semantics, and the shared job runner."""

import json
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from amscjobmanager import api_general as api, globals as manager_globals, local_api
from amscjobmanager.iri_api import compute


class LocalComputeTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.status = local_api.LocalJobStatusWrapper(None)
        self.addCleanup(self.status._status_man.conn.close)
        for active_patch in (
            patch.object(local_api, "status_man", self.status),
            patch.object(local_api, "_local_jobs", {}),
            patch.object(local_api, "_LOCAL_CANCEL_GRACE_SECONDS", 0.1),
            patch.object(manager_globals, "remote_workdir", {"local": [str(self.directory)]}),
            patch.object(compute, "getResourceID", side_effect=AssertionError("Remote lookup")),
        ):
            active_patch.start()
            self.addCleanup(active_patch.stop)
        self.jobs = []

    def tearDown(self):
        # Terminate jobs before fixture cleanup, including after failed assertions.
        for jobid, worker in self.jobs:
            local_api.cancelJob(jobid)
            if worker is not None:
                worker.join(timeout=3)
                self.assertFalse(worker.is_alive())
        self.assertFalse(local_api._local_jobs)

    def _submit(self, executable, arguments, **kwargs):
        """Submit a local executable and retain its worker for bounded cleanup."""
        directory = self.directory / str(len(self.jobs))
        directory.mkdir()
        jobid = local_api.executeJob(executable, arguments, str(directory), **kwargs)
        with local_api._local_jobs_lock:
            job = local_api._local_jobs.get(jobid)
        self.jobs.append((jobid, job["thread"] if job else None))
        return jobid, directory

    def _finish(self, jobid, directory):
        """Wait for registry cleanup; return the job's terminal state and log."""
        deadline = time.monotonic() + 5
        while jobid in local_api._local_jobs:
            if time.monotonic() >= deadline:
                self.fail("Timed out waiting for local execution")
            time.sleep(0.01)
        return local_api.getJobState(jobid), (directory / f"run.{jobid}.log").read_text()

    def test_hooks_preserve_environment_and_literal_executable_arguments(self):
        arguments = ["a b", "", "$(touch injected)", "'quoted'", "--flag"]
        application = (
            "import json, os, sys; "
            "print(json.dumps([os.environ['LOCAL_JOB_TEST_VALUE'], os.getcwd(), sys.argv[1:]]))"
        )
        jobid, directory = self._submit(
            sys.executable, ["-c", application, *arguments], name="local-executable",
            pre_launch="export LOCAL_JOB_TEST_VALUE='from pre'\nmkdir nested\ncd nested\necho pre",
            post_launch="echo post\nprintf '%s' \"$LOCAL_JOB_TEST_VALUE\" > post.txt",
        )
        status, log = self._finish(jobid, directory)
        self.assertEqual(status, "completed", log)
        lines = log.splitlines()
        self.assertEqual(lines[0], "pre")
        self.assertEqual(json.loads(lines[1]), ["from pre", str(directory / "nested"), arguments])
        self.assertEqual(lines[2], "post")
        self.assertEqual((directory / "nested" / "post.txt").read_text(), "from pre")
        self.assertFalse((directory / "nested" / "injected").exists())
        self.assertEqual(local_api.findJobByName("local-executable"), jobid)

    def test_hook_and_application_failures_determine_job_status(self):
        cases = (
            ("exit 9", "0", "echo post", "failed", False, False),
            ("false", "0", "echo post", "failed", False, False),
            (None, "7", "echo post", "failed", True, False),
            (None, "0", "echo post; exit 8", "failed", True, True),
            (None, "0", None, "completed", True, False),
            ("set -e", "7", "echo post", "failed", True, False),
        )
        for pre, exitcode, post, expected, app_runs, post_runs in cases:
            with self.subTest(pre=pre, exitcode=exitcode, post=post):
                jobid, directory = self._submit(
                    sys.executable, ["-c", f"print('application'); raise SystemExit({exitcode})"],
                    pre_launch=pre, post_launch=post,
                )
                status, log = self._finish(jobid, directory)
                self.assertEqual(status, expected, log)
                self.assertEqual("application" in log, app_runs)
                self.assertEqual("post" in log, post_runs)

    def test_missing_executable_skips_post_launch_and_fails(self):
        jobid, directory = self._submit("/missing-local-executable", [], post_launch="echo post")
        status, log = self._finish(jobid, directory)
        self.assertEqual(status, "failed", log)
        self.assertNotIn("post", log)

    def test_cancellation_interrupts_executable_and_skips_post_launch(self):
        jobid, directory = self._submit(
            sys.executable, ["-c", "import time; time.sleep(60)"],
            pre_launch="echo ready > ready", post_launch="touch post-ran",
        )
        deadline = time.monotonic() + 5
        while not (directory / "ready").exists():
            if time.monotonic() >= deadline:
                self.fail("Executable did not start")
            time.sleep(0.01)
        api.cancelJob("local", jobid)
        worker = self.jobs[-1][1]
        worker.join(timeout=3)
        self.assertFalse(worker.is_alive())
        self.assertEqual(local_api.getJobState(jobid), "canceled")
        self.assertFalse((directory / "post-ran").exists())

    def test_iri_dispatch_passes_structured_execution_to_local_api(self):
        with patch.object(local_api, "executeJob", return_value="local-id") as submit, \
             patch.object(local_api, "executeJobScript") as script:
            result = api.executeJob(
                "LOCAL", "/bin/echo", ["a b"], nodes=2, ranks_per_node=4,
                gpus_per_rank=0, time="60", queue="debug", account="test",
                job_run_dir=str(self.directory), allow_unsafe=True, name="job-name",
                pre_launch="echo pre", post_launch="echo post",
            )
            self.assertEqual(result, "local-id")
            submit.assert_called_once_with(
                "/bin/echo", ["a b"], str(self.directory), allow_unsafe=True,
                name="job-name", pre_launch="echo pre", post_launch="echo post",
            )
            script.assert_not_called()

    def test_executable_paths_reject_tilde_and_check_shared_outputs(self):
        with patch.object(local_api.subprocess, "Popen") as launch:
            with self.assertRaisesRegex(ValueError, "~"):
                local_api.executeJob("~/program", [], str(self.directory), allow_unsafe=True)
            with self.assertRaisesRegex(ValueError, "sandbox"):
                local_api.executeJob("true", [], "/unconfigured-local-output")
            launch.assert_not_called()


if __name__ == "__main__":
    unittest.main()
