"""Background failures must release manager state and permit shutdown/recovery."""

import threading
import unittest
from unittest.mock import patch

from amscjobmanager.action_manager import ActionStatus
from amscjobmanager.compute_actions import ExecuteJobComputeAction
from amscjobmanager.manager import JobManager


class ManagerFailureTests(unittest.TestCase):
    def test_background_failure_releases_lock_and_logs_reason(self):
        """Crash submission/progression while the real worker holds the manager lock.

        Replace each progression entry point in turn with a failing API call,
        start the actual background thread, and wait with a deadline for exit.
        Verify failed health, the original error in wfmanLog, and successful
        acquisition of the lock. No workflows or remote jobs are submitted.

        Args:
            self (ManagerFailureTests): unittest-created test instance; no
                caller-supplied parameters.

        Returns:
            None: Checks report failures through unittest assertions.
        """
        for operation in ('startWorkflows', 'progressActiveState'):
            with self.subTest(operation=operation):
                manager = JobManager(max_workflows_active=0)
                try:
                    with patch.object(manager.job_data, operation, side_effect=RuntimeError('unexpected worker failure')), \
                         patch('amscjobmanager.manager.wfmanLog') as log:
                        manager.start()
                        manager._thread.join(timeout=3)
                        self.assertFalse(manager._thread.is_alive())
                        self.assertFalse(manager.isAlive())
                        self.assertTrue(manager._lock.acquire(timeout=1), 'Failed worker retained its lock')
                        manager._lock.release()
                        log.assert_called_once()
                        self.assertIn('RuntimeError: unexpected worker failure', log.call_args.args[0])
                finally:
                    manager.stop(wait_until_done=False, close_database=True)

    def test_default_stop_does_not_wait_for_dead_worker_to_finish_pending_work(self):
        """A crashed worker cannot drain queued workflows, so default stop must return.

        Enqueue a harmless action with automatic starts disabled, then fail
        workflow progression. Invoke default stop in a bounded helper thread
        to detect a shutdown hang without blocking the test suite. The pending
        record must remain intact; shutdown does not cancel or execute it.

        Args:
            self (ManagerFailureTests): unittest-created test instance; no
                caller-supplied parameters.

        Returns:
            None: Checks report failures through unittest assertions.
        """
        manager = JobManager(max_workflows_active=0)
        try:
            job_id = manager(lambda jobs: jobs.enqueueJob(ExecuteJobComputeAction(
                machine='local', executable='/bin/true', arguments=[], account='', queue='', time='60',
                rundir='/tmp', nodes=1, ranks_per_node=1, gpus_per_rank=0,
            )))
            with patch.object(manager.job_data, 'startWorkflows', side_effect=RuntimeError('worker stopped')), \
                 patch('amscjobmanager.manager.wfmanLog'):
                manager.start()
                manager._thread.join(timeout=3)
                self.assertFalse(manager._thread.is_alive())
            stopper = threading.Thread(target=manager.stop, daemon=True)
            stopper.start()
            stopper.join(timeout=2)
            self.assertFalse(stopper.is_alive(), 'stop waited for a dead worker to finish workflows')
            self.assertEqual(manager(lambda jobs: jobs.jobStatus(job_id)), ActionStatus.PENDING)
        finally:
            manager.stop(wait_until_done=False, close_database=True)


if __name__ == '__main__':
    unittest.main()
