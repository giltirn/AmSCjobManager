"""Background failures must release manager state and permit shutdown/recovery."""

import threading
import unittest
from unittest.mock import Mock, patch

from amscjobmanager.action_manager import ActionStatus
from amscjobmanager.compute_actions import ExecuteJobComputeAction
from amscjobmanager.manager import WorkflowManager


class ManagerFailureTests(unittest.TestCase):
    def test_context_errors_preserve_original_exception_and_release_lock(self):
        """Catch original block errors without stopping the real background manager.

        Raise both a validation-style error and a BaseException inside the
        context. Verify object identity, lock release, and successful reentry;
        the caller catches the errors while the manager continues running.
        """
        manager = WorkflowManager(max_workflows_active=0)
        try:
            manager.start()
            for error in (ValueError('invalid workflow'), KeyboardInterrupt()):
                with self.subTest(error=type(error).__name__):
                    with self.assertRaises(type(error)) as caught:
                        with manager:
                            raise error
                    self.assertIs(caught.exception, error)
                    self.assertTrue(manager._lock.acquire(timeout=1), 'Context error retained its lock')
                    manager._lock.release()
                    with manager as workflows:
                        self.assertEqual(workflows.listWorkflows(), [])
                    self.assertTrue(manager.isAlive())
        finally:
            manager.stop(wait_until_done=False, close_database=True)

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
                manager = WorkflowManager(max_workflows_active=0)
                try:
                    with patch.object(manager.workflow_data, operation, side_effect=RuntimeError('unexpected worker failure')), \
                         patch('amscjobmanager.manager.wfmanLog') as log:
                        manager.start()
                        manager._thread.join(timeout=3)
                        self.assertFalse(manager._thread.is_alive())
                        self.assertFalse(manager.isAlive())
                        self.assertTrue(manager._lock.acquire(timeout=1), 'Failed worker retained its lock')
                        manager._lock.release()
                        log.assert_called_once()
                        self.assertIn('RuntimeError: unexpected worker failure', log.call_args.args[0])
                        with self.assertRaisesRegex(RuntimeError, 'cannot be restarted'):
                            manager.start()
                finally:
                    manager.stop(wait_until_done=False, close_database=True)

    def test_shutdown_is_idempotent_and_permanently_prevents_start(self):
        """Retire a manager on stop, both before and after its first start.

        Starting a running manager is harmless. After stop, including repeated
        calls after storage closes, restart must fail without replacing its
        thread or clearing shutdown state.
        """
        for started in (False, True):
            with self.subTest(started=started):
                manager = WorkflowManager(max_workflows_active=0)
                try:
                    if started:
                        manager.start()
                        original = manager._thread
                        manager.start()
                        self.assertIs(manager._thread, original)
                    manager.stop(wait_until_done=False, close_database=True)
                    manager.stop()
                    with self.assertRaisesRegex(RuntimeError, 'create a new WorkflowManager'):
                        manager.start()
                    self.assertFalse(manager.isAlive())
                finally:
                    manager.stop(wait_until_done=False, close_database=True)

    def test_thread_launch_failure_permanently_prevents_restart(self):
        """Retain terminal failure when launching the manager thread itself fails.

        Substitute only Thread construction, then check that a second start is
        rejected and shutdown does not try to join a thread that never started.
        """
        manager = WorkflowManager(max_workflows_active=0)
        thread = Mock()
        thread.start.side_effect = RuntimeError('launch denied')
        try:
            with patch('amscjobmanager.manager.threading.Thread', return_value=thread) as create:
                with self.assertRaisesRegex(RuntimeError, 'launch denied'):
                    manager.start()
                with self.assertRaisesRegex(RuntimeError, 'cannot be restarted'):
                    manager.start()
                create.assert_called_once()
            manager.stop(wait_until_done=False, close_database=True)
            thread.join.assert_not_called()
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
        manager = WorkflowManager(max_workflows_active=0)
        try:
            workflow_id = manager(lambda workflows: workflows.enqueueWorkflow(ExecuteJobComputeAction(
                machine='local', executable='/bin/true', arguments=[], account='', queue='', time='60',
                rundir='/tmp', nodes=1, ranks_per_node=1, gpus_per_rank=0,
            )))
            with patch.object(manager.workflow_data, 'startWorkflows', side_effect=RuntimeError('worker stopped')), \
                 patch('amscjobmanager.manager.wfmanLog'):
                manager.start()
                manager._thread.join(timeout=3)
                self.assertFalse(manager._thread.is_alive())
            stopper = threading.Thread(target=manager.stop, daemon=True)
            stopper.start()
            stopper.join(timeout=2)
            self.assertFalse(stopper.is_alive(), 'stop waited for a dead worker to finish workflows')
            self.assertEqual(manager(lambda workflows: workflows.workflowStatus(workflow_id)), ActionStatus.PENDING)
        finally:
            manager.stop(wait_until_done=False, close_database=True)


if __name__ == '__main__':
    unittest.main()
