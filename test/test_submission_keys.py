"""Optional workflow submission keys, persistence and concurrent retry handling."""

from concurrent.futures import ThreadPoolExecutor
import multiprocessing
import os
from pathlib import Path
import tempfile
from threading import Barrier
import unittest

from amscjobmanager.action_manager import ActionStatus
from amscjobmanager.actions_base import ActionClass
from amscjobmanager.api_actions import APIAction, WriteFile
from amscjobmanager.compute_actions import ExecuteJobComputeAction
from amscjobmanager.job_data import JobData
from amscjobmanager.manager import JobManager


def submit_then_exit(database, output):
    """Commit a workflow and exit before delivering any submission response.

    database/output (str) are temporary SQLite/output filenames. Return no value:
    exit with code 23 immediately after enqueueJob returns, without graceful
    manager cleanup. The parent retries the same key after reopening storage.
    """
    jobs = JobData(database)
    jobs.enqueueJob(APIAction([WriteFile('local', output, 'original')]),
                    job_group='original', submission_key='lost-response')
    os._exit(23)


class SubmissionKeyTests(unittest.TestCase):
    def setUp(self):
        """Create isolated workflow storage and a compute definition that is never run."""
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.database = self.root / 'workflows.sqlite'
        self.jobs = JobData(self.database)
        self.addCleanup(self.jobs.conn.close)
        self.addCleanup(self.jobs.stopWorkers)
        self.action = ExecuteJobComputeAction(
            machine='local', account='', queue='', time='60', executable='/bin/true',
            arguments=[], rundir=str(self.root), nodes=1, ranks_per_node=1, gpus_per_rank=0,
        )

    def test_unkeyed_submissions_always_create_new_workflows(self):
        """Preserve existing behavior for omitted and explicitly None submission keys."""
        first = self.jobs.enqueueJob(self.action, job_group='campaign')
        second = self.jobs.enqueueJob(self.action, job_group='campaign')
        third = self.jobs.enqueueJob(self.action, submission_key=None)
        self.assertEqual(len({first, second, third}), 3)
        self.assertTrue(all(row['submission_key'] is None for row in self.jobs.listWorkflows()))

    def test_existing_key_preserves_original_workflow_and_group(self):
        """Return the original ID without replacing payload, metadata or progress.

        Supply a different stage list and group on retry. Compare public metadata
        and persisted action rows so payloads, IDs, and timestamps remain unchanged.
        """
        first = self.jobs.enqueueJob(self.action, job_group='original', submission_key='request')
        original = self.jobs.jobState(first)
        with self.jobs.conn as conn:
            actions = conn.execute('SELECT * FROM actions WHERE job_id = ? ORDER BY workflow_stage', (first,)).fetchall()
        retry = self.jobs.enqueueJob([self.action, self.action], job_group='changed', submission_key='request')
        self.assertEqual(retry, first)
        self.assertEqual(self.jobs.jobState(first), original)
        with self.jobs.conn as conn:
            self.assertEqual(conn.execute('SELECT * FROM actions WHERE job_id = ? ORDER BY workflow_stage',
                                          (first,)).fetchall(), actions)
        self.assertEqual(len(self.jobs.listWorkflows()), 1)

    def test_keys_are_exact_optional_strings(self):
        """Keep case, whitespace and empty strings distinct; reject non-string keys."""
        keys = ['', 'run', 'Run', ' run ', 'campaign/\u03b1']
        ids = [self.jobs.enqueueJob(self.action, submission_key=key) for key in keys]
        self.assertEqual(len(set(ids)), len(keys))
        for key, job_id in zip(keys, ids):
            self.assertEqual(self.jobs.enqueueJob(self.action, submission_key=key), job_id)
        for invalid in (True, 3, b'run', ['run']):
            with self.subTest(key=invalid), self.assertRaises(TypeError):
                self.jobs.enqueueJob(self.action, submission_key=invalid)
        self.assertEqual(len(self.jobs.listWorkflows()), len(keys))

    def test_completed_and_canceled_keys_survive_reopening(self):
        """Retain terminal workflows and their keys across a database restart.

        Complete an empty API stage and cancel a pending compute stage through
        public methods. Reopen storage and retry changed work under both keys;
        neither workflow may return to pending or acquire a new database ID.
        """
        completed = self.jobs.enqueueJob(APIAction([]), submission_key='complete')
        canceled = self.jobs.enqueueJob(self.action, submission_key='cancel')
        self.jobs.cancelJob(canceled)
        self.jobs.startWorkflows()
        action_id = self.jobs.jobState(completed)['head_action_id']
        self.jobs.action_man[ActionClass.API].waitForAction(action_id, .01)
        self.jobs.progressActiveState()
        self.assertEqual(self.jobs.jobStatus(completed), ActionStatus.COMPLETED)
        self.assertEqual(self.jobs.jobStatus(canceled), ActionStatus.CANCELED)
        original = {job_id: self.jobs.jobState(job_id) for job_id in (completed, canceled)}
        self.jobs.stopWorkers()
        self.jobs.conn.close()

        reopened = JobData(self.database)
        self.addCleanup(reopened.conn.close)
        self.addCleanup(reopened.stopWorkers)
        for key, job_id in (('complete', completed), ('cancel', canceled)):
            self.assertEqual(reopened.enqueueJob(self.action, submission_key=key), job_id)
            self.assertEqual(reopened.jobState(job_id), original[job_id])
        self.assertEqual(len(reopened.listWorkflows()), 2)

    def test_concurrent_callers_share_one_job_data_instance(self):
        """Serialize duplicate submissions through one JobData connection lock.

        Release eight callers together, bypassing JobManager's separate lock.
        Every caller must receive the same workflow ID and only one record may
        exist, exercising the supported concurrency model directly.
        """
        ready = Barrier(8)

        def submit(index):
            """Submit for caller index (zero-based int) and return the workflow database ID."""
            ready.wait(timeout=3)
            return self.jobs.enqueueJob(self.action, job_group=f'caller-{index}',
                                        submission_key='concurrent')

        with ThreadPoolExecutor(max_workers=8) as callers:
            ids = list(callers.map(submit, range(8)))
        self.assertEqual(len(set(ids)), 1)
        self.assertEqual(len(self.jobs.listWorkflows()), 1)

    def test_job_manager_locked_interfaces_share_keys_and_persist_them(self):
        """Retry concurrent submissions through a running JobManager, then reopen it.

        Automatic starts are disabled so no compute command executes. Exercise
        both its callable interface and context manager, with the same key used
        by eight callers. A new manager must retrieve that same persisted ID.
        """
        path = self.root / 'manager.sqlite'
        manager = JobManager(path, max_workflows_active=0)
        ready = Barrier(8)

        def submit(index):
            """Submit for caller index (zero-based int) and return the workflow database ID."""
            ready.wait(timeout=3)
            return manager(lambda jobs: jobs.enqueueJob(
                self.action, job_group=f'caller-{index}', submission_key='manager-request',
            ))

        try:
            manager.start()
            with ThreadPoolExecutor(max_workers=8) as callers:
                ids = list(callers.map(submit, range(8)))
            self.assertEqual(len(set(ids)), 1)
            with manager as jobs:
                self.assertEqual(jobs.enqueueJob(self.action, submission_key='manager-request'), ids[0])
                self.assertEqual(len(jobs.listWorkflows()), 1)
        finally:
            manager.stop(wait_until_done=False, close_database=True)

        reopened = JobManager(path, max_workflows_active=0)
        try:
            self.assertEqual(reopened(lambda jobs: jobs.enqueueJob(
                self.action, submission_key='manager-request')), ids[0])
        finally:
            reopened.stop(wait_until_done=False, close_database=True)

    def test_crash_after_commit_before_response_does_not_resubmit(self):
        """Kill a submitting process after commit, then retry and execute the original.

        The child sends no workflow ID back. After reopening SQLite, retry with
        changed output work under the same key. Exactly the original workflow
        must remain and write its original artifact when progressed.
        """
        path, output = self.root / 'crash.sqlite', self.root / 'original'
        child = multiprocessing.get_context('spawn').Process(target=submit_then_exit, args=(str(path), str(output)))
        child.start()
        try:
            child.join(10)
            self.assertFalse(child.is_alive(), 'Submitting child did not exit')
            self.assertEqual(child.exitcode, 23)
        finally:
            if child.is_alive():
                child.kill()
                child.join(3)
            child.close()

        recovered = JobData(path)
        try:
            original = recovered.listWorkflows()
            self.assertEqual(len(original), 1)
            changed = self.root / 'changed'
            retry = recovered.enqueueJob(APIAction([WriteFile('local', str(changed), 'changed')]),
                                         job_group='changed', submission_key='lost-response')
            self.assertEqual(retry, original[0]['job_id'])
            self.assertEqual(recovered.listWorkflows(), original)
            recovered.startWorkflows()
            action_id = recovered.jobState(retry)['head_action_id']
            recovered.action_man[ActionClass.API].waitForAction(action_id, .01)
            recovered.progressActiveState()
            self.assertEqual(recovered.jobStatus(retry), ActionStatus.COMPLETED)
            self.assertEqual(output.read_text(), 'original')
            self.assertFalse(changed.exists())
        finally:
            recovered.stopWorkers()
            recovered.conn.close()


if __name__ == '__main__':
    unittest.main()
