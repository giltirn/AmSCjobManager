"""Exercise local executable jobs, hook semantics, and the shared job runner."""

import json
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from pydantic import ValidationError

from amscjobmanager import api_general as api, globals as manager_globals, local_api
from amscjobmanager.iri_api import compute, state
from amscjobmanager.manager_config_models import ManagerConfig


class LocalComputeTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.status = local_api.LocalJobStatusWrapper(None)
        self.addCleanup(self.status._status_man.conn.close)
        for active_patch in (
            patch.object(local_api, "status_man", self.status),
            patch.object(local_api, "_mpi_launcher", None),
            patch.object(local_api, "_local_jobs", {}),
            patch.object(local_api, "_LOCAL_CANCEL_GRACE_SECONDS", 0.1),
            patch.object(manager_globals, "remote_workdir", {"local": [str(self.directory)]}),
            patch.object(compute, "getResourceID", side_effect=AssertionError("Remote lookup")),
        ):
            active_patch.start()
            self.addCleanup(active_patch.stop)
        self.jobs = []
        # A local launcher proxy exercises rank counts, child processes and
        # failure propagation without requiring an installed MPI distribution.
        self.launcher = self.directory / "fake mpi launcher"
        self.launcher.write_text(f"#!{sys.executable}\n" + '''
import json, os, subprocess, sys
from pathlib import Path
Path("launcher-arguments.json").write_text(json.dumps(sys.argv[1:]))
if os.environ.get("FAKE_MPI_FAIL"):
    raise SystemExit(17)
assert sys.argv[1] == "-n"
ranks = int(sys.argv[2])
children = []
for rank in range(ranks):
    environment = dict(os.environ, FAKE_MPI_RANK=str(rank), FAKE_MPI_SIZE=str(ranks))
    children.append(subprocess.Popen(sys.argv[3:], env=environment))
statuses = [child.wait() for child in children]
raise SystemExit(0 if all(status == 0 for status in statuses) else 1)
''')
        self.launcher.chmod(0o700)

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

    def test_mpi_launches_requested_ranks_with_literal_arguments_and_single_hooks(self):
        """Hooks run once while all ranks inherit pre-launch environment and arguments."""
        local_api.configureMPILauncher(str(self.launcher))
        arguments = ["a b", "", "$(touch injected)", "'quoted'", "--flag"]
        application = (
            "import json, os, sys; from pathlib import Path; "
            "rank = os.environ['FAKE_MPI_RANK']; "
            "Path('rank-' + rank + '.json').write_text(json.dumps(["
            "os.environ['FAKE_MPI_SIZE'], os.environ['MPI_TEST_VALUE'], sys.argv[1:]]))"
        )
        jobid, directory = self._submit(
            sys.executable, ["-c", application, *arguments], ranks=3,
            pre_launch="export MPI_TEST_VALUE=from-pre\necho pre >> hooks",
            post_launch="echo post >> hooks",
        )
        status, log = self._finish(jobid, directory)
        self.assertEqual(status, "completed", log)
        self.assertEqual((directory / "hooks").read_text().splitlines(), ["pre", "post"])
        for rank in range(3):
            self.assertEqual(json.loads((directory / f"rank-{rank}.json").read_text()), ["3", "from-pre", arguments])
        self.assertFalse((directory / "injected").exists())
        self.assertEqual(json.loads((directory / "launcher-arguments.json").read_text())[:2], ["-n", "3"])

    def test_one_rank_uses_mpi_and_launcher_is_found_after_pre_launch(self):
        """A command-name launcher can be supplied by pre-launch PATH setup."""
        local_api.configureMPILauncher(self.launcher.name)
        jobid, directory = self._submit(
            sys.executable, ["-c", "print('one MPI rank')"], ranks=1,
            pre_launch=f"export PATH={local_api.shlex.quote(str(self.launcher.parent))}:\"$PATH\"",
        )
        status, log = self._finish(jobid, directory)
        self.assertEqual(status, "completed", log)
        self.assertIn("one MPI rank", log)
        self.assertEqual(json.loads((directory / "launcher-arguments.json").read_text())[:2], ["-n", "1"])

    def test_mpi_launcher_and_rank_failures_skip_post_launch(self):
        """Missing launchers, launcher failure, and failing ranks mark the job failed."""
        for launcher, pre_launch, exitcode in (
            (str(self.directory / "missing-launcher"), None, 0),
            (str(self.launcher), "export FAKE_MPI_FAIL=1", 0),
            (str(self.launcher), None, 7),
        ):
            with self.subTest(launcher=launcher, pre_launch=pre_launch, exitcode=exitcode):
                local_api.configureMPILauncher(launcher)
                jobid, directory = self._submit(
                    sys.executable, ["-c", f"raise SystemExit({exitcode})"], ranks=2,
                    pre_launch=pre_launch, post_launch="touch post-ran",
                )
                status, log = self._finish(jobid, directory)
                self.assertEqual(status, "failed", log)
                self.assertFalse((directory / "post-ran").exists())

    def test_mpi_pre_launch_failure_prevents_launcher_start(self):
        """Failed setup must prevent both MPI launching and postprocessing."""
        local_api.configureMPILauncher(str(self.launcher))
        jobid, directory = self._submit("true", [], ranks=2, pre_launch="false", post_launch="touch post-ran")
        status, log = self._finish(jobid, directory)
        self.assertEqual(status, "failed", log)
        self.assertFalse((directory / "launcher-arguments.json").exists())
        self.assertFalse((directory / "post-ran").exists())

    def test_invalid_ranks_are_rejected_before_creating_a_job(self):
        """No launcher means exactly one rank; rank counts must be positive integers."""
        with patch.object(local_api, "_startLocalJob") as start:
            for ranks in (2, 0, -1, 1.5, True, "2"):
                with self.subTest(ranks=ranks), self.assertRaises(ValueError):
                    local_api.executeJob("true", [], str(self.directory), ranks=ranks)
            local_api.configureMPILauncher(str(self.launcher))
            for nodes, ranks_per_node in ((0, 1), (2, 1), (2, 4), (1, 0), (True, 1), (1.0, 1), (1, 1.5)):
                with self.subTest(nodes=nodes, ranks_per_node=ranks_per_node), self.assertRaises(ValueError):
                    api.executeJob("local", "true", [], nodes, ranks_per_node, 0, "60", "unused", "unused", str(self.directory))
            start.assert_not_called()

    def test_mpi_cancellation_terminates_launcher_children(self):
        """Cancellation stops all local ranks and prevents postprocessing."""
        local_api.configureMPILauncher(str(self.launcher))
        application = (
            "import os, time; from pathlib import Path; "
            "Path('rank-' + os.environ['FAKE_MPI_RANK'] + '.pid').write_text(str(os.getpid())); "
            "time.sleep(60)"
        )
        jobid, directory = self._submit(sys.executable, ["-c", application], ranks=2, post_launch="touch post-ran")
        deadline = time.monotonic() + 5
        while len(list(directory.glob("rank-*.pid"))) != 2:
            if time.monotonic() >= deadline:
                self.fail("MPI ranks did not start")
            time.sleep(0.01)
        local_api.cancelJob(jobid)
        status, log = self._finish(jobid, directory)
        self.assertEqual(status, "canceled", log)
        self.assertFalse((directory / "post-ran").exists())
        for pid_file in directory.glob("rank-*.pid"):
            process_state = Path(f"/proc/{int(pid_file.read_text())}/stat")
            if process_state.exists():
                self.assertEqual(process_state.read_text().rsplit(")", 1)[1].split()[0], "Z")

    def test_configure_launcher_can_reset_to_serial_and_reject_invalid_values(self):
        """Reconfiguration disables MPI explicitly, and invalid launcher values fail."""
        local_api.configureMPILauncher(str(self.launcher))
        local_api.configureMPILauncher(None)
        jobid, directory = self._submit(sys.executable, ["-c", "print('serial')"])
        status, log = self._finish(jobid, directory)
        self.assertEqual(status, "completed", log)
        self.assertFalse((directory / "launcher-arguments.json").exists())
        for launcher in ("", " ", "~/mpiexec", 5):
            with self.subTest(launcher=launcher), self.assertRaises(ValueError):
                local_api.configureMPILauncher(launcher)

    def test_agent_configuration_installs_launcher_and_resets_when_omitted(self):
        """Manager setup installs MPI configuration and later default setup disables it."""
        config = ManagerConfig(iri_implementations={}, sandbox_directories={}, local_mpi_launcher=str(self.launcher))
        with patch.object(state, "_close_compute_clients"), patch.object(state, "addSandboxDirs"), \
             patch.object(state, "_configure_personal_globus_endpoint"), \
             patch.dict(state._implementation_credentials), patch.dict(state._implementation_accounts), \
             patch.dict(state.tokens), patch.object(state, "_transfer_token_path", None), \
             patch.object(state, "_configuration_cache_directory", None):
            state.setupWorkflowAgentFromConfig(config)
            self.assertEqual(local_api._mpi_launcher, str(self.launcher))
            state.setupWorkflowAgent({}, None, {})
            self.assertIsNone(local_api._mpi_launcher)

    def test_configuration_accepts_null_and_rejects_blank_or_tilde_launchers(self):
        """The optional executable is validated without requiring MPI installation."""
        self.assertIsNone(ManagerConfig(iri_implementations={}, sandbox_directories={}).local_mpi_launcher)
        for launcher in ("", " ", "~/bin/mpiexec", 5):
            with self.subTest(launcher=launcher), self.assertRaises(ValidationError):
                ManagerConfig(iri_implementations={}, sandbox_directories={}, local_mpi_launcher=launcher)

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
                "LOCAL", "/bin/echo", ["a b"], nodes=1, ranks_per_node=4,
                gpus_per_rank=0, time="60", queue="debug", account="test",
                job_run_dir=str(self.directory), allow_unsafe=True, name="job-name",
                pre_launch="echo pre", post_launch="echo post",
            )
            self.assertEqual(result, "local-id")
            submit.assert_called_once_with(
                "/bin/echo", ["a b"], str(self.directory), allow_unsafe=True,
                name="job-name", pre_launch="echo pre", post_launch="echo post",
                ranks=4,
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
