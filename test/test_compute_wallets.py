"""Compute budget admission, estimated usage, and crash-safe settlement.

Run: PYTHONPATH=test python -m unittest test_compute_wallets -v
The deterministic tests use the existing synchronous supervision adapter and a
fake provider. Integration coverage separately uses real action-worker threads.
"""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import patch

from action_manager_helpers import DirectComputeActions
from amscjobmanager.action_manager import ActionStatus, SimulatedCrash, _unser
from amscjobmanager.actions_base import ActionClass
from amscjobmanager.compute_actions import ExecuteJobComputeAction
from amscjobmanager.job_data import JobData
from amscjobmanager.manager import JobManager


class ComputeWalletTests(unittest.TestCase):
    """Exercise real persistence and accounting against controlled provider effects."""

    def setUp(self):
        """Create an isolated database, virtual clock, and named remote-job registry."""
        temporary = self.enterContext(tempfile.TemporaryDirectory())
        self.path = Path(temporary) / 'wallet.sqlite'
        self.now = 1000
        self.remote = {}
        self.status = 'queued'
        self.enterContext(patch('amscjobmanager.job_data.ComputeActions', DirectComputeActions))
        self.enterContext(patch('amscjobmanager.compute_action_manager.time.time', side_effect=lambda: self.now))
        self.enterContext(patch('amscjobmanager.api_general.queryMachineStatus', return_value=True))
        self.submit = self.enterContext(patch('amscjobmanager.compute_actions.executeJob', side_effect=self.submitJob))
        self.enterContext(patch('amscjobmanager.compute_action_manager.findJobByName', side_effect=lambda machine, name: self.remote.get(name)))
        self.poll = self.enterContext(patch('amscjobmanager.compute_action_manager.getJobState', side_effect=lambda *args: self.status))
        self.cancel = self.enterContext(patch('amscjobmanager.compute_action_manager.cancelJob'))
        self.jobs = self.openDatabase()

    def submitJob(self, *args, **kwargs):
        """Accept provider args/kwargs, save its deterministic name, and return a job ID str."""
        key = f'job-{len(self.remote) + 1}'
        self.remote[kwargs['name']] = key
        return key

    def openDatabase(self, crash_at=None):
        """Recover self.path and refresh the compute-manager fixture.

        crash_at (str | None) selects a simulated crash. Return JobData with
        registered cleanup; set self.computes to its recovered compute manager.
        """
        jobs = JobData(self.path, _test_crash_at=crash_at, poll_freq=float('inf'))
        self.computes = jobs.getManager(ActionClass.COMPUTE)
        self.computes.scheduling_retry_delay = 0
        self.addCleanup(jobs.conn.close)
        return jobs

    def enqueue(self, nodes=2, seconds=100):
        """Queue a fake compute request with nodes/seconds (positive int); return workflow ID."""
        return self.jobs.enqueueJob(ExecuteJobComputeAction(
            machine='local', account='', queue='', time=str(seconds), nodes=nodes,
            executable='/bin/true', arguments=[], rundir='/tmp', ranks_per_node=1,
            gpus_per_rank=0))

    def test_shortage_is_visible_and_recharge_resumes_without_attempts(self):
        """Keep unfunded work SCHEDULING, expose its reason, then submit after recharge."""
        self.computes.setWalletEnabled('LOCAL', True)
        job = self.enqueue()
        self.jobs.startWorkflows([job])
        for _ in range(3):
            self.jobs.submitSchedulingActions()
        self.assertEqual(self.jobs.headActionStatus(job, return_api_status=True),
                         (ActionStatus.SCHEDULING, 'INSUFFICIENT_WALLET_BALANCE'))
        with self.jobs.conn as conn:
            self.assertEqual(conn.execute('SELECT submission_attempts FROM computes').fetchone()[0], 0)
        self.submit.assert_not_called()
        # A restart must recheck funding, preserving the reason and zero attempts.
        self.jobs.conn.close()
        self.jobs = self.openDatabase()
        self.assertEqual(self.jobs.headActionStatus(job, return_api_status=True)[1],
                         'INSUFFICIENT_WALLET_BALANCE')
        self.computes.rechargeWallet('local', 1)
        self.jobs.submitSchedulingActions()
        self.assertEqual(self.jobs.headActionStatus(job, return_api_status=True), (ActionStatus.ACTIVE, 'queued'))
        self.assertAlmostEqual(self.computes.getWallets('local')['local']['available_node_hours'], 3400 / 3600)
        self.assertEqual(self.submit.call_count, 1)
        # The two-node, 100-second request is stored directly in node-hours.
        with self.jobs.conn as conn:
            accounting = conn.execute('SELECT requested_node_hours, reserved_node_hours, used_node_hours FROM compute_wallet_actions').fetchone()
            self.assertAlmostEqual(accounting['requested_node_hours'], 200 / 3600)
            self.assertAlmostEqual(accounting['reserved_node_hours'], 200 / 3600)
            self.assertEqual(accounting['used_node_hours'], 0)
        self.computes._reconcileScheduledAction(1)
        self.assertAlmostEqual(self.computes.getWallets('local')['local']['available_node_hours'], 3400 / 3600)

    def test_machine_isolation_and_concurrent_admission(self):
        """Race two requests for one reservation; only one can consume the available funds."""
        self.computes.setWalletEnabled('local', True)
        self.computes.rechargeWallet('local', 200 / 3600)
        self.computes.setWalletEnabled('other', True)
        first, second = self.enqueue(), self.enqueue()
        manager = self.computes
        # Persist both intents first so admission itself is raced across threads.
        for job in (first, second):
            with self.jobs.conn as conn:
                row = conn.execute('SELECT details, action_id FROM actions WHERE job_id = ?', (job,)).fetchone()
            manager._recordSchedulingIntentIfNew(_unser(row['details']), job, row['action_id'])
        with ThreadPoolExecutor(2) as pool:
            list(pool.map(manager._reconcileScheduledAction, (1, 2)))
        states = [manager.queryRecordedStatus(i) for i in (1, 2)]
        self.assertEqual(sum(status == ActionStatus.ACTIVE for status, _ in states), 1)
        self.assertAlmostEqual(self.computes.getWallets('local')['local']['available_node_hours'], 0)
        self.assertEqual(self.submit.call_count, 1)
        self.assertEqual(self.computes.getWallets('other')['other']['available_node_hours'], 0)

    def test_queue_and_held_time_are_free_and_terminal_states_refund(self):
        """Exclude waiting time, then charge observed execution on completion or failure."""
        for terminal in ('completed', 'failed', 'canceled'):
            with self.subTest(terminal=terminal):
                self.computes.setWalletEnabled('local', True)
                self.computes.rechargeWallet('local', 1)
                initial = self.computes.getWallets('local')['local']['available_node_hours']
                job = self.enqueue()
                self.status = 'queued'
                self.jobs.startWorkflows([job])
                action_id = self.jobs.jobState(job)['head_action_id']
                manager = self.computes
                self.now += 1000
                self.status = 'held'
                manager._pollAction(action_id)
                self.now += 1000
                self.status = 'queued'
                manager._pollAction(action_id)
                self.now += 5
                self.status = 'active'
                manager._pollAction(action_id)
                self.now += 10
                self.status = terminal
                manager._pollAction(action_id)
                self.assertAlmostEqual(self.computes.getWallets('local')['local']['available_node_hours'], initial - 30 / 3600)
                manager._pollAction(action_id)
                self.assertAlmostEqual(self.computes.getWallets('local')['local']['available_node_hours'], initial - 30 / 3600)

    def test_cancel_acknowledgment_refunds_and_api_failure_retains_reservation(self):
        """Retry a failed cancellation with funds reserved, then settle on its acknowledgment."""
        self.computes.setWalletEnabled('local', True)
        self.computes.rechargeWallet('local', 1)
        self.status = 'active'
        job = self.enqueue()
        self.jobs.startWorkflows([job])
        self.now += 10
        self.cancel.side_effect = RuntimeError('gateway unavailable')
        with self.assertRaisesRegex(RuntimeError, 'gateway unavailable'):
            self.jobs.cancelJob(job)
        self.assertAlmostEqual(self.computes.getWallets('local')['local']['available_node_hours'], 3400 / 3600)
        self.assertEqual(self.jobs.jobStatus(job), ActionStatus.CANCELING)
        self.now += 2
        self.cancel.side_effect = None
        self.jobs.performPendingCancellations()
        self.assertEqual(self.jobs.jobStatus(job), ActionStatus.CANCELED)
        self.assertAlmostEqual(self.computes.getWallets('local')['local']['available_node_hours'], 3576 / 3600)
        self.jobs.performPendingCancellations()
        self.assertAlmostEqual(self.computes.getWallets('local')['local']['available_node_hours'], 3576 / 3600)

    def test_disabled_wallet_and_later_enable_do_not_charge_existing_work(self):
        """Leave unbudgeted submissions uncharged and settle old reservations while disabled."""
        job = self.enqueue()
        self.jobs.startWorkflows([job])
        self.assertEqual(self.computes.getWallets(), {})
        with self.jobs.conn as conn:
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM compute_wallet_actions').fetchone()[0], 0)
        self.computes.setWalletEnabled('local', True)
        self.computes.rechargeWallet('local', 1)
        self.now += 20
        self.status = 'completed'
        manager = self.computes
        manager._pollAction(1)
        self.assertAlmostEqual(self.computes.getWallets('local')['local']['available_node_hours'], 1)
        self.status = 'active'
        next_job = self.enqueue()
        self.jobs.startWorkflows([next_job])
        self.computes.setWalletEnabled('local', False)
        self.now += 10
        self.status = 'completed'
        manager._pollAction(2)
        self.assertAlmostEqual(self.computes.getWallets('local')['local']['available_node_hours'], 3580 / 3600)

    def test_enabling_wallet_applies_to_existing_workflows_and_submission_retries(self):
        """Apply the current wallet policy to pending work and a previously rejected attempt.

        Both workflows are queued before budgeting is enabled. One has already
        attempted submission while disabled; the other has not started. Neither
        should retain a zero-cost exemption when enforcement is enabled.
        """
        self.computes.setWalletEnabled('local', False)
        retrying, pending = self.enqueue(), self.enqueue()
        self.submit.side_effect = RuntimeError('submission rejected')
        self.jobs.startWorkflows([retrying])
        with self.jobs.conn as conn:
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM compute_wallet_actions').fetchone()[0], 0)
        self.computes.setWalletEnabled('local', True)
        self.submit.side_effect = self.submitJob
        self.jobs.submitSchedulingActions()
        self.jobs.startWorkflows([pending])
        for job in (retrying, pending):
            self.assertEqual(self.jobs.headActionStatus(job, return_api_status=True),
                             (ActionStatus.SCHEDULING, 'INSUFFICIENT_WALLET_BALANCE'))
        self.assertEqual(self.submit.call_count, 1)
        self.computes.rechargeWallet('local', 1)
        self.jobs.submitSchedulingActions()
        self.assertEqual(self.submit.call_count, 3)
        self.assertAlmostEqual(self.computes.getWallets()['local']['available_node_hours'], 1 - 400 / 3600)

    def test_enabling_does_not_charge_a_previously_accepted_uncertain_submission(self):
        """Recover an accepted job after enabling budgeting without reserving funds retroactively."""
        def acceptThenFail(*args, **kwargs):
            """Accept provider args/kwargs, then raise to simulate a lost submission response."""
            self.submitJob(*args, **kwargs)
            raise RuntimeError('response lost')

        self.submit.side_effect = acceptThenFail
        job = self.enqueue()
        self.jobs.startWorkflows([job])
        self.computes.setWalletEnabled('local', True)
        self.jobs.submitSchedulingActions()
        self.assertEqual(self.jobs.headActionStatus(job), ActionStatus.ACTIVE)
        self.assertEqual(self.submit.call_count, 1)
        self.assertEqual(self.computes.getWallets()['local']['available_node_hours'], 0)
        with self.jobs.conn as conn:
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM compute_wallet_actions').fetchone()[0], 0)

    def test_unsubmitted_cancellation_and_exhausted_retries_refund_all(self):
        """Release admission when no remote job exists, including cancellation before an attempt."""
        self.computes.setWalletEnabled('local', True)
        self.computes.rechargeWallet('local', 1)
        self.computes._test_crash_at = 'wallet_reserved'
        job = self.enqueue()
        with self.assertRaises(SimulatedCrash):
            self.jobs.startWorkflows([job])
        self.assertAlmostEqual(self.computes.getWallets('local')['local']['available_node_hours'], 3400 / 3600)
        manager = self.computes
        manager._test_crash_at = None
        self.jobs.cancelJob(job)
        self.assertAlmostEqual(self.computes.getWallets('local')['local']['available_node_hours'], 1)
        self.submit.assert_not_called()
        self.submit.side_effect = RuntimeError('submission rejected')
        manager.max_submission_attempts = 1
        second = self.enqueue()
        self.jobs.startWorkflows([second])
        self.assertAlmostEqual(self.computes.getWallets('local')['local']['available_node_hours'], 3400 / 3600)
        self.jobs.submitSchedulingActions()
        self.assertEqual(self.jobs.jobStatus(second), ActionStatus.FAILED)
        self.assertAlmostEqual(self.computes.getWallets('local')['local']['available_node_hours'], 1)

    def test_reservation_and_accepted_submission_recover_without_duplicate_effects(self):
        """Reopen at each admission/submission boundary and check one debit and one remote job."""
        for point in ('wallet_reservation_recorded', 'wallet_reserved', 'before_remote_submission',
                      'remote_submission_completed', 'remote_id_persisted'):
            with self.subTest(point=point):
                self.jobs.conn.close()
                self.path = self.path.with_name(point + '.sqlite')
                self.remote.clear()
                self.submit.reset_mock()
                self.jobs = self.openDatabase(point)
                self.computes.setWalletEnabled('local', True)
                self.computes.rechargeWallet('local', 1)
                job = self.enqueue()
                with self.assertRaises(SimulatedCrash):
                    self.jobs.startWorkflows([job])
                self.assertAlmostEqual(self.computes.getWallets('local')['local']['available_node_hours'], 1 if point == 'wallet_reservation_recorded' else 1 - 200 / 3600)
                self.jobs.conn.close()
                self.jobs = self.openDatabase()
                self.jobs.submitSchedulingActions()
                self.assertAlmostEqual(self.computes.getWallets('local')['local']['available_node_hours'], 3400 / 3600)
                self.assertEqual(self.submit.call_count, 1)
                self.assertEqual(len(self.remote), 1)
                self.assertEqual(self.jobs.headActionStatus(job), ActionStatus.ACTIVE)

    def test_settlement_crashes_recover_without_double_refund(self):
        """Crash before/after terminal accounting commits, reopen, and check exactly one refund."""
        for point in ('before_status_settlement', 'wallet_settlement_recorded', 'status_settled',
                      'remote_cancellation_completed', 'before_cancellation_settlement', 'cancellation_settled'):
            with self.subTest(point=point):
                self.jobs.conn.close()
                self.path = self.path.with_name(point + '.sqlite')
                self.remote.clear()
                self.jobs = self.openDatabase()
                self.computes.setWalletEnabled('local', True)
                self.computes.rechargeWallet('local', 1)
                self.status = 'active'
                job = self.enqueue()
                self.jobs.startWorkflows([job])
                manager = self.computes
                manager._test_crash_at = point
                self.now += 10
                if 'status' in point or point == 'wallet_settlement_recorded':
                    self.status = 'completed'
                    with self.assertRaises(SimulatedCrash):
                        manager._pollAction(1)
                else:
                    # Model cancellation acceptance persisting at the provider
                    # even if its local status/settlement write never happened.
                    self.cancel.side_effect = lambda *args: setattr(self, 'status', 'canceled')
                    with self.assertRaises(SimulatedCrash):
                        self.jobs.cancelJob(job)
                self.jobs.conn.close()
                self.jobs = self.openDatabase()
                self.jobs.progressActiveActions(force_poll=True)
                self.jobs.progressActiveState()
                self.assertAlmostEqual(self.computes.getWallets('local')['local']['available_node_hours'], 3580 / 3600)
                self.jobs.performPendingCancellations()
                self.assertAlmostEqual(self.computes.getWallets('local')['local']['available_node_hours'], 3580 / 3600)

    def test_uncertain_submission_and_failed_polls_do_not_release_funds(self):
        """Retain a reservation after a lost submission response or failed status query."""
        self.computes.setWalletEnabled('local', True)
        self.computes.rechargeWallet('local', 1)
        def acceptThenFail(*args, **kwargs):
            """Accept captured provider args/kwargs then simulate a lost response; never return."""
            self.submitJob(*args, **kwargs)
            raise RuntimeError('response lost')
        self.submit.side_effect = acceptThenFail
        job = self.enqueue()
        self.jobs.startWorkflows([job])
        self.assertAlmostEqual(self.computes.getWallets('local')['local']['available_node_hours'], 3400 / 3600)
        self.poll.side_effect = RuntimeError('poll unavailable')
        self.jobs.submitSchedulingActions()
        self.assertAlmostEqual(self.computes.getWallets('local')['local']['available_node_hours'], 3400 / 3600)
        self.assertEqual(self.submit.call_count, 1)
        self.poll.side_effect = lambda *args: self.status
        self.jobs.submitSchedulingActions()
        self.assertEqual(self.jobs.headActionStatus(job), ActionStatus.ACTIVE)

    def test_usage_is_capped_and_local_worker_failure_does_not_refund(self):
        """Cap estimated use at the reservation and retain funds for uncertain remote work."""
        self.computes.setWalletEnabled('local', True)
        self.computes.rechargeWallet('local', 1)
        self.status = 'active'
        first = self.enqueue()
        self.jobs.startWorkflows([first])
        self.now += 10000
        self.status = 'completed'
        manager = self.computes
        manager._pollAction(1)
        self.assertAlmostEqual(self.computes.getWallets('local')['local']['available_node_hours'], 3400 / 3600)
        second = self.enqueue()
        self.status = 'active'
        self.jobs.startWorkflows([second])
        manager._recordWorkerFailure(2, RuntimeError('local supervision failed'))
        self.assertAlmostEqual(self.computes.getWallets('local')['local']['available_node_hours'], 3200 / 3600)
        self.assertEqual(manager.queryRecordedStatus(2)[0], ActionStatus.FAILED)
        self.assertAlmostEqual(self.computes.getWallets()['local']['reserved_node_hours'], 200 / 3600)

    def test_fractional_grants_and_large_balances_persist_in_node_hours(self):
        """Preserve sub-second grants and realistic large balances through reserve/refund recovery.

        Work directly in node-hours, including a fractional recharge smaller
        than one node-second. Reopen the database after a completed job and
        verify its available balance and zero outstanding reservation.
        """
        self.computes.rechargeWallet('local', .00001)
        self.assertEqual(self.computes.getWallets()['local']['available_node_hours'], .00001)
        self.computes.rechargeWallet('local', 100000)
        self.computes.setWalletEnabled('local', True)
        initial = self.computes.getWallets()['local']['available_node_hours']
        self.status = 'active'
        job = self.enqueue()
        self.jobs.startWorkflows([job])
        self.assertAlmostEqual(self.computes.getWallets()['local']['available_node_hours'], initial - 200 / 3600)
        self.now += 3
        self.status = 'completed'
        self.computes._pollAction(1)
        self.jobs.conn.close()
        self.jobs = self.openDatabase()
        self.assertAlmostEqual(self.computes.getWallets()['local']['available_node_hours'], initial - 6 / 3600)
        self.assertEqual(self.computes.getWallets()['local']['reserved_node_hours'], 0)

    def test_database_amounts_are_real_and_reject_negative_values(self):
        """Enforce nonnegative node-hour amounts independently of Python input checks.

        Create a funded reservation, confirm SQLite stores REAL values, and try
        negative updates to every monetary column. Each failed transaction must
        preserve the available balance and outstanding reservation.
        """
        self.computes.setWalletEnabled('local', True)
        self.computes.rechargeWallet('local', 1)
        job = self.enqueue()
        self.jobs.startWorkflows([job])
        before = self.computes.getWallets()
        for table, column in (('compute_wallets', 'available_node_hours'),
                              ('compute_wallet_actions', 'requested_node_hours'),
                              ('compute_wallet_actions', 'reserved_node_hours'),
                              ('compute_wallet_actions', 'used_node_hours')):
            with self.subTest(column=column):
                with self.jobs.conn as conn:
                    self.assertEqual(conn.execute(f'SELECT typeof({column}) FROM {table}').fetchone()[0], 'real')
                with self.assertRaises(sqlite3.IntegrityError):
                    with self.jobs.conn as conn:
                        conn.execute(f'UPDATE {table} SET {column} = -1')
        self.assertEqual(self.computes.getWallets(), before)

    def test_wallet_validation_and_query_copy(self):
        """Reject invalid grants and return independent query records without config defaults."""
        for amount in (0, -1, float('nan'), float('inf'), True, '1'):
            with self.subTest(amount=amount), self.assertRaises(ValueError):
                self.computes.rechargeWallet('local', amount)
        self.computes.rechargeWallet('local', 1)
        copied = self.computes.getWallets()
        copied['local']['enabled'] = True
        self.assertFalse(self.computes.getWallets()['local']['enabled'])
        self.assertEqual(self.computes.getWallets('missing'), {})


class ComputeWalletWorkerTests(unittest.TestCase):
    """Use real manager/action threads to exercise a wallet wait and automatic resumption."""

    def test_background_worker_resumes_after_recharge(self):
        """Observe the public wait marker, recharge, then settle completed work without manual steps."""
        with patch('amscjobmanager.api_general.queryMachineStatus', return_value=True), \
             patch('amscjobmanager.compute_actions.executeJob', return_value='remote-id') as submit, \
             patch('amscjobmanager.compute_action_manager.getJobState', return_value='completed'):
            manager = JobManager(poll_freq=.02)
            self.addCleanup(manager.stop, False, close_database=True)
            manager(lambda jobs: jobs.getManager(ActionClass.COMPUTE).setWalletEnabled('local', True))
            workflow = manager(lambda jobs: jobs.enqueueJob(ExecuteJobComputeAction(
                machine='local', account='', queue='', time='60', nodes=1,
                executable='/bin/true', arguments=[], rundir='/tmp', ranks_per_node=1, gpus_per_rank=0)))
            manager.start()
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline:
                status = manager(lambda jobs: jobs.headActionStatus(workflow, return_api_status=True))
                if status[1] == 'INSUFFICIENT_WALLET_BALANCE':
                    break
                time.sleep(.02)
            self.assertEqual(status, (ActionStatus.SCHEDULING, 'INSUFFICIENT_WALLET_BALANCE'))
            submit.assert_not_called()
            manager(lambda jobs: jobs.getManager(ActionClass.COMPUTE).rechargeWallet('local', 1))
            while time.monotonic() < deadline:
                if manager(lambda jobs: jobs.jobStatus(workflow)) == ActionStatus.COMPLETED:
                    break
                time.sleep(.02)
            self.assertEqual(manager(lambda jobs: jobs.jobStatus(workflow)), ActionStatus.COMPLETED)
            self.assertEqual(submit.call_count, 1)
            self.assertEqual(manager(lambda jobs: jobs.getManager(ActionClass.COMPUTE).getWallets()['local']['reserved_node_hours']), 0)


if __name__ == '__main__':
    unittest.main()
