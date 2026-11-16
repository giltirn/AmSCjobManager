"""Database-driven dependency readiness, split/join execution, and cancellation recovery.

Only the remote transfer service is replaced by the existing scheduling harness.
API stages perform real local file writes, and subprocess recovery loses all
in-memory state before reopening the durable workflow/action records.
"""

from functools import partial
import multiprocessing
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import Mock, patch

from action_manager_helpers import DirectAPIActions
from test_scheduling_reconciliation import HarnessGlobusTransfer, HarnessGlobusDataTransfers, TransferHarness
from amscjobmanager.action_manager import ActionStatus, SimulatedCrash
from amscjobmanager.actions_base import ActionClass
from amscjobmanager.api_actions import APIAction, WriteFile
from amscjobmanager.job_data import JobData


def crash_after_dependent_scheduling(database: str, directory: str) -> None:
    """Commit producer success and dependent intent, then exit without worker cleanup.

    database/directory (str) select temporary SQLite storage and output paths.
    Return no value: exit code 29 represents death after the dependent's
    scheduling transaction, before its execution-manager record exists.
    """
    jobs = JobData(database, _test_action_manager_factories={ActionClass.API: DirectAPIActions})
    producer = jobs.enqueueJob(APIAction([WriteFile('local', directory + '/producer', 'prepared')], producer_key='ready'))
    dependent = jobs.enqueueJob(APIAction([WriteFile('local', directory + '/consumer', 'processed')], depends_on='ready'))
    jobs.startWorkflows([producer])
    jobs.progressActiveState()
    jobs._test_crash_at = 'job_head_scheduling_persisted'
    try:
        jobs.startWorkflows([dependent])
    except SimulatedCrash:
        os._exit(29)
    os._exit(30)


class WorkflowDependencyTests(unittest.TestCase):
    def setUp(self):
        """Create real temporary storage, deterministic dispatch, and a remote-task harness."""
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.database = self.root / 'workflows.sqlite'
        self.harness = TransferHarness()
        self.enterContext(patch.object(HarnessGlobusTransfer, 'harness', self.harness))
        self.enterContext(patch('amscjobmanager.logging.wfman_log_func', Mock()))
        self.factories = {ActionClass.API: DirectAPIActions,
                          ActionClass.TRANSFER: partial(HarnessGlobusDataTransfers, harness=self.harness)}
        self.jobs = self._openJobs()

    def _openJobs(self, filename=None):
        """Open JobData with deterministic API/transfer managers and register cleanup.

        filename (Path | None) selects SQLite storage, default self.database.
        Return JobData with immediate forced fake-provider polls and real DB
        transactions. Reopening restores recorded work through normal recovery.
        """
        jobs = JobData(self.database if filename is None else filename,
                       _test_action_manager_factories=self.factories)
        self.addCleanup(jobs.conn.close)
        self.addCleanup(jobs.stopWorkers)
        return jobs

    def test_submission_requires_known_earlier_producers_and_is_atomic(self):
        """Reject unknown, self, and later-stage references without partial rows or keys.

        Every operand is checked, even in an OR with a known alternative. Earlier
        stages within the same submission and producers in older workflows work.
        """
        self.jobs.enqueueJob(APIAction(producer_key='known'))
        invalid = [APIAction(depends_on='unknown'),
                   APIAction(depends_on='self', producer_key='self'),
                   [APIAction(depends_on='later'), APIAction(producer_key='later')],
                   [APIAction(producer_key='rollback'), APIAction(depends_on='missing')],
                   APIAction(depends_on=['known', 'OR', 'unknown'])]
        for index, workflow in enumerate(invalid):
            with self.subTest(workflow=workflow):
                with self.jobs.conn as conn:
                    before = (conn.execute('SELECT COUNT(*) FROM jobs').fetchone()[0],
                              conn.execute('SELECT COUNT(*) FROM actions').fetchone()[0])
                with self.assertRaisesRegex(ValueError, 'Unknown dependency key'):
                    self.jobs.enqueueJob(workflow, submission_key=f'request-{index}')
                with self.jobs.conn as conn:
                    self.assertEqual(before, (conn.execute('SELECT COUNT(*) FROM jobs').fetchone()[0],
                                              conn.execute('SELECT COUNT(*) FROM actions').fetchone()[0]))
                # Failed validation must not reserve its submission key.
                self.jobs.enqueueJob(APIAction(depends_on='known'), submission_key=f'request-{index}')
        job_id = self.jobs.enqueueJob([APIAction(producer_key='earlier'), APIAction(depends_on='earlier')])
        self.jobs.startWorkflows([job_id])
        self.jobs.progressActiveState()
        self.jobs.progressActiveState()
        self.assertEqual(self.jobs.jobStatus(job_id), ActionStatus.COMPLETED)

    def test_canceled_references_respect_boolean_alternatives_and_success_history(self):
        """Reject required canceled keys while allowing OR alternatives and completed producers."""
        canceled = self.jobs.enqueueJob(APIAction(producer_key='canceled'))
        self.jobs.cancelJob(canceled)
        self.jobs.enqueueJob(APIAction(producer_key='alternative'))
        with self.jobs.conn as conn:
            self.assertTrue(self.jobs._dependencyKeyState(conn, 'canceled')['depends_on_canceled_action'])
            self.assertFalse(self.jobs._dependencyKeyState(conn, 'alternative')['depends_on_canceled_action'])
            self.assertEqual(self.jobs._dependencyKeyState(conn, 'unknown'), {
                'producer_is_known': False, 'producer_has_completed': False, 'depends_on_canceled_action': False})
        for condition in ('canceled', ['alternative', 'AND', 'canceled']):
            with self.subTest(condition=condition), self.assertRaisesRegex(ValueError, 'canceled dependency'):
                self.jobs.enqueueJob(APIAction(depends_on=condition))
        alternative = self.jobs.enqueueJob(APIAction(depends_on=['canceled', 'OR', 'alternative']))
        self.jobs.startWorkflows([alternative])
        self.assertEqual(self.jobs.jobStatus(alternative), ActionStatus.PENDING)
        successful = self.jobs.enqueueJob(APIAction(producer_key='successful'))
        self.jobs.startWorkflows([successful])
        self.jobs.cancelJob(successful)
        with self.jobs.conn as conn:
            self.assertEqual(self.jobs._dependencyKeyState(conn, 'successful'), {
                'producer_is_known': True, 'producer_has_completed': True, 'depends_on_canceled_action': False})
        consumer = self.jobs.enqueueJob(APIAction(depends_on='successful'))
        self.jobs.startWorkflows([consumer])
        self.jobs.progressActiveState()
        self.assertEqual(self.jobs.jobStatus(consumer), ActionStatus.COMPLETED)

    def test_split_and_grouped_join_wait_for_success_while_branches_run_concurrently(self):
        """Fan out into three live remote tasks and join after a AND (b OR c).

        A pending consumer has no execution-manager record. Completing only b
        cannot start the join; completing a then releases its real file write
        while c remains live. No remote status calls are made by readiness checks.
        """
        producer = self.jobs.enqueueJob(APIAction(producer_key='ready'))
        branches = [self.jobs.enqueueJob(HarnessGlobusTransfer(depends_on='ready', producer_key=key))
                    for key in ('a', 'b', 'c')]
        output = self.root / 'joined'
        join = self.jobs.enqueueJob(APIAction([WriteFile('local', str(output), 'joined')],
                                              depends_on=['a', 'AND', ['b', 'OR', 'c']]))
        self.jobs.startWorkflows(branches + [join])
        self.assertEqual(self.harness.submission_count, 0)
        self.assertTrue(all(self.jobs.jobStatus(job) == ActionStatus.PENDING for job in branches + [join]))
        join_action = self.jobs.jobState(join)['head_action_id']
        self.assertFalse(self.jobs.action_man[ActionClass.API].hasAction(join_action))
        self.jobs.startWorkflows([producer])
        self.jobs.progressActiveState()
        self.jobs.startWorkflows()
        self.assertEqual(self.harness.submission_count, 3)
        self.assertTrue(all(self.jobs.jobStatus(job) == ActionStatus.ACTIVE for job in branches))
        self.harness.status_by_api_key['transfer-2'] = 'SUCCEEDED'
        self.jobs.progressActiveState(force_poll=True)
        with patch.object(self.harness, 'status', side_effect=AssertionError('Readiness queried provider')):
            self.jobs.startWorkflows([join])
        self.assertFalse(output.exists())
        self.harness.status_by_api_key['transfer-1'] = 'SUCCEEDED'
        self.jobs.progressActiveState(force_poll=True)
        self.jobs.startWorkflows([join])
        self.jobs.progressActiveState()
        self.assertEqual(output.read_text(), 'joined')
        self.assertEqual(self.jobs.jobStatus(join), ActionStatus.COMPLETED)
        self.assertEqual(self.jobs.jobStatus(branches[2]), ActionStatus.ACTIVE)
        self.assertEqual(self.harness.submission_count, 3)

    def test_waiting_between_stages_releases_capacity_without_starving_ready_work(self):
        """Skip a blocked lower-ID workflow before filling the last available slot.

        Retain the first stage's success while its next stage is PENDING. An
        unrelated ready workflow must start, then producer success releases the
        blocked stage without replaying setup or skipping its stage index.
        """
        self.jobs.max_workflows_active = 2
        producer = self.jobs.enqueueJob(HarnessGlobusTransfer(producer_key='ready'))
        setup, output, unrelated = self.root / 'setup', self.root / 'output', self.root / 'unrelated'
        dependent = self.jobs.enqueueJob([APIAction([WriteFile('local', str(setup), 'setup')]),
                                         APIAction([WriteFile('local', str(output), 'output')], depends_on='ready')])
        independent = self.jobs.enqueueJob(APIAction([WriteFile('local', str(unrelated), 'independent')]))
        self.jobs.startWorkflows([producer, dependent])
        self.jobs.progressActiveState(force_poll=True)
        self.assertEqual(self.jobs.jobStatus(dependent), ActionStatus.PENDING)
        self.assertEqual(self.jobs.jobState(dependent)['workflow_stage'], 1)
        blocked_id = self.jobs.jobState(dependent)['head_action_id']
        self.assertFalse(self.jobs.action_man[ActionClass.API].hasAction(blocked_id))
        setup.write_text('preserve successful setup')
        self.jobs.startWorkflows()
        self.assertTrue(unrelated.exists())
        self.assertFalse(output.exists())
        self.harness.status_by_api_key['transfer-1'] = 'SUCCEEDED'
        self.jobs.progressActiveState(force_poll=True)
        self.jobs.startWorkflows()
        self.jobs.progressActiveState()
        self.assertEqual(self.jobs.jobStatus(dependent), ActionStatus.COMPLETED)
        self.assertEqual(setup.read_text(), 'preserve successful setup')
        self.assertEqual(output.read_text(), 'output')

    def test_duplicate_producers_allow_alternatives_until_last_one_is_canceled(self):
        """Require only one producer per key; cancel consumers only when all are canceled."""
        first = self.jobs.enqueueJob(APIAction(producer_key='shared'))
        second = self.jobs.enqueueJob(APIAction(producer_key='shared'))
        consumer = self.jobs.enqueueJob(APIAction(depends_on='shared', producer_key='downstream'))
        last = self.jobs.enqueueJob(APIAction(depends_on='downstream'))
        self.jobs.cancelJob(first)
        self.assertEqual(self.jobs.jobStatus(consumer), ActionStatus.PENDING)
        self.jobs.cancelJob(second)
        self.assertEqual(self.jobs.jobStatus(consumer), ActionStatus.CANCELED)
        self.assertEqual(self.jobs.jobStatus(last), ActionStatus.CANCELED)
        self.assertGreater(self.jobs.enqueueJob(APIAction(producer_key='shared'), submission_key='new-producer'), 0)
        self.assertEqual(self.jobs.jobStatus(consumer), ActionStatus.CANCELED)

    def test_cascade_checks_future_stages_and_cancels_only_required_branches(self):
        """Cancel an executing chain that requires a lost producer later, retaining OR branches."""
        root = self.jobs.enqueueJob(APIAction(producer_key='a'))
        self.jobs.enqueueJob(APIAction(producer_key='b'))
        required = self.jobs.enqueueJob(APIAction(depends_on=['a', 'AND', 'b'], producer_key='required'))
        alternative = self.jobs.enqueueJob(APIAction(depends_on=['a', 'OR', 'b']))
        active = self.jobs.enqueueJob([HarnessGlobusTransfer(), APIAction(depends_on='required', producer_key='tail')])
        descendant = self.jobs.enqueueJob(APIAction(depends_on='tail'))
        unrelated = self.jobs.enqueueJob(HarnessGlobusTransfer())
        self.jobs.startWorkflows([active, unrelated])
        self.assertEqual(self.harness.submission_count, 2)
        self.jobs.cancelJob(root)
        for job_id in (root, required, active, descendant):
            self.assertEqual(self.jobs.jobStatus(job_id), ActionStatus.CANCELED)
        self.assertEqual(self.jobs.jobStatus(alternative), ActionStatus.PENDING)
        self.assertEqual(self.jobs.jobStatus(unrelated), ActionStatus.ACTIVE)
        self.assertEqual(self.harness.cancellation_count, 1)
        with self.assertRaisesRegex(ValueError, 'canceled dependency'):
            self.jobs.enqueueJob(APIAction(depends_on='tail'))

    def test_reused_keys_cannot_supply_their_own_missing_prerequisites(self):
        """Cancel circular fallback producers after their real starting producer is canceled.

        Keys are nonunique, so otherwise valid submissions can form a cycle
        after earlier producers vanish. Pending actions must not count their
        own output, a later stage, or a mutually waiting workflow as an escape.
        """
        root = self.jobs.enqueueJob(APIAction(producer_key='key'))
        self_republishing = self.jobs.enqueueJob(APIAction(depends_on='key', producer_key='key'))
        forward = self.jobs.enqueueJob([APIAction(depends_on='key'), APIAction(producer_key='key')])
        first = self.jobs.enqueueJob(APIAction(depends_on='key', producer_key='other'))
        second = self.jobs.enqueueJob(APIAction(depends_on='other', producer_key='key'))
        self.jobs.cancelJob(root)
        self.assertTrue(all(self.jobs.jobStatus(job) == ActionStatus.CANCELED
                            for job in (root, self_republishing, forward, first, second)))

    def test_failed_producer_leaves_consumer_pending_without_cancellation(self):
        """Failure alone never satisfies a key or triggers cancellation propagation."""
        producer = self.jobs.enqueueJob(HarnessGlobusTransfer(producer_key='failed'))
        consumer = self.jobs.enqueueJob(APIAction(depends_on='failed'))
        self.jobs.startWorkflows([producer])
        self.harness.status_by_api_key['transfer-1'] = 'FAILED'
        self.jobs.progressActiveState(force_poll=True)
        for _ in range(2):
            self.jobs.startWorkflows()
            self.jobs.progressActiveState()
        self.assertEqual(self.jobs.jobStatus(producer), ActionStatus.FAILED)
        self.assertEqual(self.jobs.jobStatus(consumer), ActionStatus.PENDING)
        self.assertEqual(self.harness.cancellation_count, 0)
        with self.jobs.conn as conn:
            self.assertEqual(self.jobs._dependencyKeyState(conn, 'failed'), {
                'producer_is_known': True, 'producer_has_completed': False, 'depends_on_canceled_action': False})

    def test_external_action_cancellation_also_records_the_dependency_cascade(self):
        """Project a direct action cancellation and cancel its waiting consumers."""
        producer = self.jobs.enqueueJob([HarnessGlobusTransfer(producer_key='ready'),
                                         APIAction(producer_key='tail')])
        consumer = self.jobs.enqueueJob(APIAction(depends_on=['ready', 'AND', 'tail']))
        self.jobs.startWorkflows([producer])
        action_id = self.jobs.jobState(producer)['head_action_id']
        self.jobs.action_man[ActionClass.TRANSFER].cancelOrResumeCancelingAction(action_id)
        self.jobs.progressActiveState(force_poll=True)
        self.assertEqual(self.jobs.jobStatus(producer), ActionStatus.CANCELED)
        self.assertEqual(self.jobs.jobStatus(consumer), ActionStatus.CANCELED)
        with self.jobs.conn as conn:
            self.assertEqual([row[0] for row in conn.execute(
                'SELECT action_status FROM actions WHERE job_id = ? ORDER BY workflow_stage', (producer,))],
                ['CANCELED', 'CANCELED'])

    def test_cascade_intent_survives_crash_before_remote_cancellation(self):
        """Commit a pending root and live descendant's full cascade, then crash before cancel.

        Reopening must resume the live cancellation once without submitting
        work again; pending descendants must already have cancellation intent.
        """
        root = self.jobs.enqueueJob(APIAction(producer_key='root'))
        active = self.jobs.enqueueJob([HarnessGlobusTransfer(), APIAction(depends_on='root', producer_key='tail')])
        final = self.jobs.enqueueJob(APIAction(depends_on='tail'))
        self.jobs.startWorkflows([active])
        self.jobs._test_crash_at = 'workflow_cancellation_cascade_persisted'
        with self.assertRaises(SimulatedCrash):
            self.jobs.cancelJob(root)
        self.assertEqual(self.jobs.jobStatus(root), ActionStatus.CANCELING)
        self.assertEqual(self.jobs.jobStatus(active), ActionStatus.CANCELING)
        self.assertEqual(self.jobs.jobStatus(final), ActionStatus.CANCELING)
        self.assertEqual(self.harness.cancellation_count, 0)
        self.jobs.conn.close()
        recovered = self._openJobs()
        recovered.progressActiveState()
        self.assertTrue(all(recovered.jobStatus(job) == ActionStatus.CANCELED for job in (root, active, final)))
        self.assertEqual(self.harness.cancellation_count, 1)
        self.assertEqual(self.harness.submission_count, 1)

    def test_process_death_after_dependency_ready_intent_does_not_replay_producer(self):
        """Kill the submitting process at a committed dependency-ready scheduling boundary.

        Inspect durable state before reopening. Recovery must complete only the
        dependent action using its existing ID, preserving the producer's effect.
        """
        database = self.root / 'process-crash.sqlite'
        process = multiprocessing.get_context('spawn').Process(
            target=crash_after_dependent_scheduling, args=(str(database), str(self.root)))
        process.start()
        process.join(10)
        if process.is_alive():
            process.kill()
            process.join(3)
        self.assertEqual(process.exitcode, 29)
        self.assertEqual((self.root / 'producer').read_text(), 'prepared')
        self.assertFalse((self.root / 'consumer').exists())
        conn = sqlite3.connect(database)
        try:
            self.assertEqual(conn.execute('SELECT workflow_status FROM jobs ORDER BY job_id').fetchall(),
                             [('COMPLETED',), ('ACTIVE',)])
            action_id = conn.execute('SELECT action_id FROM actions WHERE job_id = 2').fetchone()[0]
        finally:
            conn.close()
        (self.root / 'producer').write_text('preserve completed effect')
        recovered = self._openJobs(database)
        self.assertEqual(recovered.jobState(2)['head_action_id'], action_id)
        recovered.progressActiveState()
        self.assertEqual(recovered.jobStatus(2), ActionStatus.COMPLETED)
        self.assertEqual((self.root / 'consumer').read_text(), 'processed')
        self.assertEqual((self.root / 'producer').read_text(), 'preserve completed effect')

    def test_provider_failure_retains_all_live_descendant_cancellation_intents(self):
        """Fail the first remote cancellation, then recover both descendants after reopening.

        The pending root is already CANCELED when the API fails. Both live
        consumers must still have durable CANCELING intent, so recovery stops
        them without another submission or relying on the original caller.
        """
        root = self.jobs.enqueueJob(APIAction(producer_key='root'))
        descendants = [self.jobs.enqueueJob([HarnessGlobusTransfer(), APIAction(depends_on='root')])
                       for _ in range(2)]
        self.jobs.startWorkflows(descendants)
        self.harness.failed_cancellation_count = 1
        with self.assertRaisesRegex(RuntimeError, 'remote cancellation failure'):
            self.jobs.cancelJob(root)
        self.assertEqual(self.jobs.jobStatus(root), ActionStatus.CANCELED)
        self.assertTrue(all(self.jobs.jobStatus(job) == ActionStatus.CANCELING for job in descendants))
        self.assertEqual(self.harness.cancellation_count, 0)
        self.jobs.conn.close()
        recovered = self._openJobs()
        recovered.progressActiveState()
        self.assertTrue(all(recovered.jobStatus(job) == ActionStatus.CANCELED for job in descendants))
        self.assertEqual(self.harness.cancellation_count, 2)
        self.assertEqual(self.harness.submission_count, 2)

    def test_every_head_state_recovers_through_workflow_cancellation_reconciliation(self):
        """Crash before reconciliation for pending, live, intermediate, final, and failed heads.

        Every workflow must record CANCELING while action rows remain unchanged.
        Reopening must finish local cases without provider calls, cancel live
        work once, preserve completed/failed history, and never run queued writes.
        """
        for state in ('pending', 'executing', 'intermediate', 'completed', 'failed'):
            with self.subTest(state=state):
                # Stop each representative workflow after intent commits, before
                # reconciliation has changed any action's recorded outcome.
                jobs = self._openJobs(self.root / f'{state}.sqlite')
                first, later = self.root / f'{state}-first', self.root / f'{state}-later'
                initial = (HarnessGlobusTransfer() if state == 'executing' else
                           APIAction([WriteFile('local', str(self.root if state == 'failed' else first), 'first')]))
                workflow = [initial] if state == 'completed' else [initial, APIAction([WriteFile('local', str(later), 'unexpected')])]
                job_id = jobs.enqueueJob(workflow)
                if state != 'pending':
                    jobs.startWorkflows([job_id])
                if state == 'completed':
                    jobs.progressActiveState()
                with jobs.conn as conn:
                    recorded = conn.execute('SELECT action_id, action_status FROM actions ORDER BY workflow_stage').fetchall()
                jobs._test_crash_at = 'workflow_cancellation_cascade_persisted'
                with self.assertRaises(SimulatedCrash):
                    jobs.cancelJob(job_id)
                self.assertEqual(jobs.jobStatus(job_id), ActionStatus.CANCELING)
                with jobs.conn as conn:
                    self.assertEqual(conn.execute('SELECT action_id, action_status FROM actions ORDER BY workflow_stage').fetchall(), recorded)
                jobs.conn.close()

                # Reopening uses ordinary cancellation recovery. Only the live
                # head may contact the provider; queued writes must never run.
                with patch.object(self.harness, 'submit', wraps=self.harness.submit) as submit, \
                     patch.object(self.harness, 'find', wraps=self.harness.find) as find, \
                     patch.object(self.harness, 'status', wraps=self.harness.status) as status, \
                     patch.object(self.harness, 'cancel', wraps=self.harness.cancel) as cancel:
                    recovered = self._openJobs(self.root / f'{state}.sqlite')
                    self.assertEqual(recovered.jobStatus(job_id), ActionStatus.CANCELED)
                    submit.assert_not_called()
                    find.assert_not_called()
                    if state == 'executing':
                        status.assert_called_once()
                        cancel.assert_called_once()
                    else:
                        status.assert_not_called()
                        cancel.assert_not_called()
                self.assertFalse(later.exists())
                if state in ('intermediate', 'completed'):
                    self.assertEqual(first.read_text(), 'first')
                with recovered.conn as conn:
                    terminal = [row[0] for row in conn.execute('SELECT action_status FROM actions ORDER BY workflow_stage')]
                expected_head = ('COMPLETED' if state in ('intermediate', 'completed') else
                                 'FAILED' if state == 'failed' else 'CANCELED')
                self.assertEqual(terminal, [expected_head] + ([] if state == 'completed' else ['CANCELED']))

    def test_repeated_cancellation_intents_recover_without_submission(self):
        """Repeat cancellation at its intent crash boundary, then recover a missing record.

        Action rows must remain unchanged until recovery creates the cancellation
        record and checks for accepted remote work without submitting a new task.
        """
        # Leave an uncertain submission, then interrupt repeated cancellation
        # calls after the workflow intent commits but before action processing.
        job_id = self.jobs.enqueueJob([HarnessGlobusTransfer(), APIAction()])
        self.jobs._test_crash_at = 'job_head_scheduling_persisted'
        with self.assertRaises(SimulatedCrash):
            self.jobs.startWorkflows([job_id])
        self.jobs._test_crash_at = 'workflow_cancellation_cascade_persisted'
        for _ in range(2):
            with self.assertRaises(SimulatedCrash):
                self.jobs.cancelJob(job_id)
            self.assertEqual(self.jobs.jobStatus(job_id), ActionStatus.CANCELING)
            with self.jobs.conn as conn:
                self.assertEqual([row[0] for row in conn.execute('SELECT action_status FROM actions ORDER BY workflow_stage')],
                                 ['SCHEDULING', 'PENDING'])
        action_id = self.jobs.jobState(job_id)['head_action_id']
        self.jobs.conn.close()

        # No submission was accepted. Recovery checks by name and completes
        # cancellation without submitting, polling, or canceling a remote task.
        with patch.object(self.harness, 'submit') as submit, patch.object(self.harness, 'find', return_value=None) as find, \
             patch.object(self.harness, 'status') as status, patch.object(self.harness, 'cancel') as cancel:
            recovered = self._openJobs()
            self.assertEqual(recovered.jobStatus(job_id), ActionStatus.CANCELED)
            self.assertEqual(recovered.action_man[ActionClass.TRANSFER].queryRecordedStatus(action_id)[0], ActionStatus.CANCELED)
            submit.assert_not_called()
            find.assert_called_once()
            status.assert_not_called()
            cancel.assert_not_called()


if __name__ == '__main__':
    unittest.main()
