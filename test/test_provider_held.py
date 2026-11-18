"""Provider-held compute/transfer lifecycles with real storage and worker recovery.

Only provider API calls are mocked. Deterministic adapters cover state transitions;
production workers exercise continued polling, waits, and recovery while held.
"""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

from action_manager_helpers import DirectAPIActions, DirectComputeActions, DirectGlobusActions
from amscjobmanager import actions_base, compute_action_manager, compute_actions, globus_transfer_action
from amscjobmanager.action_manager import ActionStatus, SimulatedCrash
from amscjobmanager.actions_base import ActionClass
from amscjobmanager.api_actions import APIAction, WriteFile
from amscjobmanager.compute_actions import ExecuteJobComputeAction
from amscjobmanager.globus_transfer_action import GlobusTransfer
from amscjobmanager.job_data import JobData


class ProviderHeldTests(unittest.TestCase):
    def setUp(self):
        """Isolate storage and replace provider submission, polling, lookup, and cancellation."""
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.next_database = 0
        self.factories = {ActionClass.COMPUTE: DirectComputeActions,
                          ActionClass.TRANSFER: DirectGlobusActions, ActionClass.API: DirectAPIActions}
        self.enterContext(patch('amscjobmanager.logging.wfman_log_func', Mock()))
        self.enterContext(patch('amscjobmanager.api_general.queryMachineStatus', return_value=True))
        self.submit = {
            ActionClass.COMPUTE: self.enterContext(patch.object(compute_actions, 'executeJob', return_value='compute-1')),
            ActionClass.TRANSFER: self.enterContext(patch.object(globus_transfer_action, 'globusCopy', return_value='transfer-1')),
        }
        self.status = {
            ActionClass.COMPUTE: self.enterContext(patch.object(compute_action_manager, 'getJobState', return_value='held')),
            ActionClass.TRANSFER: self.enterContext(patch.object(globus_transfer_action, 'globusTransferStatus', return_value='INACTIVE')),
        }
        self.cancel = {
            ActionClass.COMPUTE: self.enterContext(patch.object(compute_action_manager, 'cancelJob')),
            ActionClass.TRANSFER: self.enterContext(patch.object(globus_transfer_action, 'cancelGlobusTransfer')),
        }
        self.enterContext(patch.object(compute_action_manager, 'findJobByName', return_value=None))
        self.enterContext(patch.object(globus_transfer_action, 'findGlobusTransferByLabel', return_value=None))
        self.actions = [ExecuteJobComputeAction('local', '', '', '60', '/bin/true', [], '/tmp', 1, 0, nodes=1),
                        GlobusTransfer('dest', '/output', 'source', '/input')]

    def _newJobs(self, *, filename=None, workers=False):
        """Open isolated JobData and register cleanup, optionally reopening a prior file.

        filename (Path | None) chooses storage; None creates a new temporary
        path. workers (bool, default False) selects production workers instead
        of deterministic adapters. Return JobData with infinite automatic poll
        delay; tests explicitly wake or poll it without relying on wall time.
        """
        if filename is None:
            self.next_database += 1
            filename = self.root / f'workflow-{self.next_database}.sqlite'
        jobs = JobData(filename, poll_freq=float('inf'),
                       _test_action_manager_factories=None if workers else self.factories)
        self.addCleanup(jobs.conn.close)
        self.addCleanup(jobs.stopWorkers)
        return jobs

    def _waitUntil(self, condition):
        """Wait up to three seconds for condition (zero-argument bool callable); return None."""
        deadline = time.monotonic() + 3
        while not condition():
            if time.monotonic() >= deadline:
                self.fail('Provider-held worker did not reach the expected state')
            time.sleep(.005)

    def test_held_heads_keep_workflows_active_and_resume_without_resubmission(self):
        """Exercise both initial held mappings, listing, and release into normal execution.

        A later file-writing stage must wait for successful completion, and
        repeated held polls or resumed execution must never submit another job.
        """
        for action in self.actions:
            action_class = actions_base.actionClass(action)
            with self.subTest(action_class=action_class):
                jobs = self._newJobs()
                output = self.root / action_class.name
                job_id = jobs.enqueueJob([action, APIAction([WriteFile('local', str(output), 'done')])])
                jobs.startWorkflows([job_id])
                manager = jobs.getManager(action_class)
                self.assertEqual(jobs.headActionStatus(job_id), ActionStatus.PROVIDER_HELD)
                self.assertEqual(jobs.jobStatus(job_id), ActionStatus.ACTIVE)
                self.assertEqual(len(jobs.listWorkflows(statuses=ActionStatus.ACTIVE)), 1)
                self.assertEqual(len(manager.getLiveActions()), 1)
                for _ in range(2):
                    jobs.progressActiveState(force_poll=True)
                self.assertFalse(output.exists())
                self.status[action_class].return_value = 'active' if action_class == ActionClass.COMPUTE else 'ACTIVE'
                jobs.progressActiveState(force_poll=True)
                self.assertEqual(jobs.headActionStatus(job_id), ActionStatus.ACTIVE)
                self.status[action_class].return_value = 'completed' if action_class == ActionClass.COMPUTE else 'SUCCEEDED'
                jobs.progressActiveState(force_poll=True)
                jobs.progressActiveState()
                self.assertEqual(jobs.jobStatus(job_id), ActionStatus.COMPLETED)
                self.assertEqual(output.read_text(), 'done')
                self.submit[action_class].assert_called_once()

    def test_active_action_can_be_held_and_poll_failure_preserves_held_state(self):
        """Suspend already active jobs, tolerate a failed poll, then observe terminal failure."""
        for action in self.actions:
            action_class = actions_base.actionClass(action)
            with self.subTest(action_class=action_class):
                self.status[action_class].return_value = 'active' if action_class == ActionClass.COMPUTE else 'ACTIVE'
                jobs = self._newJobs()
                job_id = jobs.enqueueJob(action)
                jobs.startWorkflows([job_id])
                self.status[action_class].return_value = 'held' if action_class == ActionClass.COMPUTE else 'INACTIVE'
                jobs.progressActiveState(force_poll=True)
                self.assertEqual(jobs.headActionStatus(job_id), ActionStatus.PROVIDER_HELD)
                self.status[action_class].side_effect = RuntimeError('temporary polling outage')
                jobs.progressActiveState(force_poll=True)
                self.assertEqual(jobs.headActionStatus(job_id), ActionStatus.PROVIDER_HELD)
                self.assertEqual(jobs.jobStatus(job_id), ActionStatus.ACTIVE)
                self.status[action_class].side_effect = None
                self.status[action_class].return_value = 'failed' if action_class == ActionClass.COMPUTE else 'FAILED'
                jobs.progressActiveState(force_poll=True)
                self.assertEqual(jobs.jobStatus(job_id), ActionStatus.FAILED)

    def test_held_jobs_are_cancellable_and_failed_transfers_need_no_remote_cancel(self):
        """Cancel held work normally, but leave a genuinely failed transfer terminal."""
        for action in self.actions:
            action_class = actions_base.actionClass(action)
            with self.subTest(action_class=action_class):
                jobs = self._newJobs()
                job_id = jobs.enqueueJob(action)
                jobs.startWorkflows([job_id])
                jobs.cancelJob(job_id)
                self.assertEqual(jobs.jobStatus(job_id), ActionStatus.CANCELED)
                self.assertEqual(jobs.headActionStatus(job_id), ActionStatus.CANCELED)
                self.cancel[action_class].assert_called_once()
        self.status[ActionClass.TRANSFER].return_value = 'FAILED'
        jobs = self._newJobs()
        job_id = jobs.enqueueJob(self.actions[1])
        jobs.startWorkflows([job_id])
        self.assertEqual(jobs.jobStatus(job_id), ActionStatus.FAILED)
        self.status[ActionClass.TRANSFER].reset_mock()
        self.cancel[ActionClass.TRANSFER].reset_mock()
        jobs.cancelJob(job_id)
        self.assertEqual(jobs.jobStatus(job_id), ActionStatus.CANCELED)
        self.assertEqual(jobs.headActionStatus(job_id), ActionStatus.FAILED)
        self.status[ActionClass.TRANSFER].assert_not_called()
        self.cancel[ActionClass.TRANSFER].assert_not_called()

    def test_cancellation_intent_for_held_work_recovers_after_crash(self):
        """Persist cancellation before contacting either provider, then reopen and finish it."""
        for action in self.actions:
            action_class = actions_base.actionClass(action)
            with self.subTest(action_class=action_class):
                jobs = self._newJobs()
                with jobs.conn as conn:
                    filename = conn.execute('PRAGMA database_list').fetchone()['file']
                job_id = jobs.enqueueJob(action)
                jobs.startWorkflows([job_id])
                jobs._test_crash_at = 'job_head_canceling_persisted'
                with self.assertRaises(SimulatedCrash):
                    jobs.cancelJob(job_id)
                self.assertEqual(jobs.jobStatus(job_id), ActionStatus.CANCELING)
                self.cancel[action_class].assert_not_called()
                jobs.conn.close()
                recovered = self._newJobs(filename=filename)
                recovered.progressActiveState()
                self.assertEqual(recovered.jobStatus(job_id), ActionStatus.CANCELED)
                self.cancel[action_class].assert_called_once()
                self.submit[action_class].assert_called_once()

    def test_reopening_held_action_restores_real_worker_and_provider_polling(self):
        """Reopen durable held records using production supervisors, without resubmitting.

        Explicitly wake a recovered worker and observe held-to-completed state
        through normal JobData polling. Startup must supervise the existing ID.
        """
        for action in self.actions:
            action_class = actions_base.actionClass(action)
            with self.subTest(action_class=action_class):
                jobs = self._newJobs()
                with jobs.conn as conn:
                    filename = conn.execute('PRAGMA database_list').fetchone()['file']
                job_id = jobs.enqueueJob(action)
                jobs.startWorkflows([job_id])
                action_id = jobs.jobState(job_id)['head_action_id']
                jobs.conn.close()
                recovered = self._newJobs(filename=filename, workers=True)
                manager = recovered.action_man[action_class]
                before = self.status[action_class].call_count
                manager.updateStatuses()
                self._waitUntil(lambda: self.status[action_class].call_count > before)
                self.assertEqual(manager.queryRecordedStatus(action_id)[0], ActionStatus.PROVIDER_HELD)
                self.status[action_class].return_value = 'completed' if action_class == ActionClass.COMPUTE else 'SUCCEEDED'
                manager.updateStatuses()
                self._waitUntil(lambda: manager.queryRecordedStatus(action_id)[0] == ActionStatus.COMPLETED)
                recovered.progressActiveState()
                self.assertEqual(recovered.jobStatus(job_id), ActionStatus.COMPLETED)
                self.submit[action_class].assert_called_once()
                recovered.stopWorkers()

    def test_wait_for_action_and_supervisor_continue_while_provider_holds_it(self):
        """Keep real execution and waiter threads alive across a held-to-success transition."""
        jobs = self._newJobs(workers=True)
        job_id = jobs.enqueueJob(self.actions[0])
        action_id = jobs.jobState(job_id)['head_action_id']
        manager = jobs.action_man[ActionClass.COMPUTE]
        manager.poll_freq = .01
        jobs.startWorkflows([job_id])
        self._waitUntil(lambda: manager.queryRecordedStatus(action_id)[0] == ActionStatus.PROVIDER_HELD)
        with ThreadPoolExecutor(max_workers=1) as executor:
            waiting = executor.submit(manager.waitForAction, action_id, .001)
            try:
                self.assertFalse(waiting.done())
                self.status[ActionClass.COMPUTE].return_value = 'completed'
                # The ordinary polling timer must observe release from hold;
                # do not explicitly wake the supervisor on this success path.
                self.assertEqual(waiting.result(timeout=3), ActionStatus.COMPLETED)
            finally:
                # A failed assertion must still release a held supervisor/waiter.
                self.status[ActionClass.COMPUTE].return_value = 'completed'
                manager.updateStatuses()
        jobs.progressActiveState()
        self.assertEqual(jobs.jobStatus(job_id), ActionStatus.COMPLETED)
        self.submit[ActionClass.COMPUTE].assert_called_once()


if __name__ == '__main__':
    unittest.main()
