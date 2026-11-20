"""In-memory query delivery, global query-hook routing and asynchronous server startup.

Run: python -m unittest discover -s test -p test_server_queries.py -v
No MCP dependency, real site credentials or network listener is required.
"""

import json
from pathlib import Path
import tempfile
from threading import Event
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from amscjobmanager import logging as manager_logging
from amscjobmanager.action_manager import ActionStatus
from amscjobmanager.api_actions import APIAction, WriteFile
from amscjobmanager.database import DatabaseClosed
from amscjobmanager.iri_api import auth
from amscjobmanager.manager import JobManager
from amscjobmanager.user_queries import QueryResponseError, UserQueryQueue
from amscjobmanager.server.runtime import ServerRuntime
from amscjobmanager.workers import Workers, WorkerInterrupted


class ServerQueryTests(unittest.TestCase):
    def setUp(self):
        """Isolate local artifacts and capture log calls without changing query routing."""
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.log = Mock()
        for name in ('wfman_log_func', 'api_log_func'):
            hook = patch.object(manager_logging, name, self.log)
            hook.start()
            self.addCleanup(hook.stop)

    def _waitPending(self, queue, count=1):
        """Wait for count pending questions and return their public dictionaries.

        queue (UserQueryQueue) is inspected without consuming requests; count
        (int, default 1) is the expected length. Fail after three seconds.
        """
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            pending = queue.listPending()
            if len(pending) == count:
                return pending
            time.sleep(.005)
        self.fail(f'Expected {count} pending questions, got {queue.listPending()}')

    def test_concurrent_questions_and_duplicate_responses(self):
        """Answer two real workers out of order without consuming either listing.

        Verify session-local IDs, exact response delivery, empty-string answers,
        rejection of duplicate replies, and absence of answer values from logs.
        """
        queue, workers = UserQueryQueue(), Workers()
        self.addCleanup(queue.close)
        self.addCleanup(workers.stop)
        first = workers.start('first', queue.ask, 'first', 'First prompt')
        second = workers.start('second', queue.ask, 'second', 'Second prompt')
        questions = self._waitPending(queue, 2)
        self.assertEqual(questions, queue.listPending())
        self.assertEqual([item['query_id'] for item in questions], [1, 2])
        by_title = {item['title']: item for item in questions}
        question = by_title['second']
        queue.respond(question['session_id'], question['query_id'], '')
        self.assertEqual(second.future.result(timeout=2), '')
        self.assertFalse(first.future.done())
        with self.assertRaises(QueryResponseError):
            queue.respond(question['session_id'], question['query_id'], 'duplicate')
        question = by_title['first']
        queue.respond(question['session_id'], question['query_id'], 'private-answer')
        self.assertEqual(first.future.result(timeout=2), 'private-answer')
        self.assertEqual(queue.listPending(), [])
        self.assertNotIn('private-answer', str(self.log.call_args_list))

    def test_immediate_replies_are_not_lost(self):
        """Reply as soon as each request becomes visible and ensure its worker finishes.

        Repeating across fresh worker contexts exercises the publish/wait race;
        replies must not depend on the worker already waiting for notification.
        """
        queue, workers = UserQueryQueue(), Workers()
        self.addCleanup(queue.close)
        self.addCleanup(workers.stop)
        for index in range(10):
            worker = workers.start(index, queue.ask, 'question', str(index))
            question = self._waitPending(queue)[0]
            queue.respond(question['session_id'], question['query_id'], str(index))
            self.assertEqual(worker.future.result(timeout=2), str(index))

    def test_worker_interruption_invalidates_a_pending_query(self):
        """Reject a reply immediately after interrupt, before the waiter consumes it."""
        queue, workers = UserQueryQueue(), Workers()
        self.addCleanup(queue.close)
        self.addCleanup(workers.stop)
        worker = workers.start('question', queue.ask, 'operation', 'Proceed?')
        question = self._waitPending(queue)[0]
        workers.interrupt('question')
        with self.assertRaises(QueryResponseError) as error:
            queue.respond(question['session_id'], question['query_id'], 'too late')
        self.assertEqual(error.exception.code, 'query_not_pending')
        with self.assertRaises(WorkerInterrupted):
            worker.future.result(timeout=2)
        self.assertEqual(queue.listPending(), [])

    def test_closed_queue_releases_waiters_and_rejects_new_questions(self):
        """Close a pending queue, then verify all entry/reply paths reject reuse."""
        queue, workers = UserQueryQueue(), Workers()
        self.addCleanup(workers.stop)
        worker = workers.start('question', queue.ask, 'operation', 'Proceed?')
        question = self._waitPending(queue)[0]
        queue.close()
        queue.close()
        with self.assertRaises(WorkerInterrupted):
            worker.future.result(timeout=2)
        with self.assertRaises(WorkerInterrupted):
            queue.ask('late', 'Cannot publish')
        with self.assertRaises(QueryResponseError) as error:
            queue.respond(question['session_id'], question['query_id'], 'late')
        self.assertEqual(error.exception.code, 'query_queue_closed')
        self.assertEqual(queue.listPending(), [])

    def test_recreated_queue_rejects_previous_session_ids(self):
        """Prevent query-number reuse after restart from accepting an old response."""
        old, new, workers = UserQueryQueue(), UserQueryQueue(), Workers()
        self.addCleanup(old.close)
        self.addCleanup(new.close)
        self.addCleanup(workers.stop)
        old_worker = workers.start('old', old.ask, 'old', 'Old session')
        stale = self._waitPending(old)[0]
        old.close()
        with self.assertRaises(WorkerInterrupted):
            old_worker.future.result(timeout=2)
        current = workers.start('new', new.ask, 'new', 'New session')
        fresh = self._waitPending(new)[0]
        self.assertEqual(stale['query_id'], fresh['query_id'])
        with self.assertRaises(QueryResponseError) as error:
            new.respond(stale['session_id'], stale['query_id'], 'old answer')
        self.assertEqual(error.exception.code, 'stale_session')
        self.assertFalse(current.future.done())
        new.respond(fresh['session_id'], fresh['query_id'], 'fresh answer')
        self.assertEqual(current.future.result(timeout=2), 'fresh answer')

    def test_authentication_retry_creates_a_new_query_and_saves_tokens(self):
        """Run the actual OAuth retry loop through the queue with mocked provider I/O.

        A rejected authorization code gets a new query number in the same flow.
        The accepted reply returns to the authentication worker, which persists
        fake tokens. No code is logged or included in pending query metadata.
        """
        queue, callers = UserQueryQueue(), Workers()
        self.addCleanup(queue.close)
        self.addCleanup(callers.stop)
        client = Mock()
        client.oauth2_get_authorize_url.return_value = 'https://example.invalid/authorize'
        granted = {'access_token': 'fake-token', 'refresh_token': 'fake-refresh', 'scope': 'test-scope'}
        client.oauth2_exchange_code_for_tokens.side_effect = [RuntimeError('invalid code'),
            SimpleNamespace(by_resource_server={'test-resource': granted})]
        path = self.root / 'token.json'
        with patch.dict(auth.IRI_IMPLEMENTATIONS, {'query-test': {'resource_server': 'test-resource',
                        'required_scopes': ['test-scope'], 'session_policy': 'test-policy'}}), \
             patch.object(auth.globus_sdk, 'NativeAppAuthClient', return_value=client), \
             patch.object(auth, 'IRIComputeClient', return_value='authenticated'), \
             patch('builtins.input', side_effect=AssertionError('Terminal input was used')), \
             patch.object(manager_logging, 'api_query_user_func', queue.ask):
            worker = callers.start('auth', auth.setupIRIapiCompute, 'query-test', str(path))
            question = self._waitPending(queue)[0]
            queue.respond(question['session_id'], question['query_id'], 'rejected-private-code')
            next_question = self._waitPending(queue)[0]
            self.assertGreater(next_question['query_id'], question['query_id'])
            queue.respond(next_question['session_id'], next_question['query_id'], 'accepted-private-code')
            self.assertEqual(worker.future.result(timeout=2), 'authenticated')
        self.assertEqual(json.loads(path.read_text()), granted)
        client.oauth2_start_flow.assert_called_once()
        self.assertNotIn('private-code', str(self.log.call_args_list))

    def test_shared_authentication_question_survives_one_callers_interruption(self):
        """Keep one authentication question valid when one dependent caller stops.

        Authentication runs on a shared worker, separately from the two callers.
        Interrupting the first caller must not invalidate the authenticator's
        query or prevent the second caller from receiving the accepted answer.
        """
        queue, parents, authentication = UserQueryQueue(), Workers(), Workers(blocking=True)
        self.addCleanup(queue.close)
        self.addCleanup(parents.stop)
        self.addCleanup(authentication.stop)
        calls = []

        def authenticate():
            """Ask once on the shared authenticator and return its response string."""
            calls.append('authentication')
            return manager_logging.wfapiUserQuery('shared authentication', 'Authorization code')

        def caller():
            """Wait for the shared authentication result and return its response string."""
            _, response = authentication.run('credentials', authenticate)
            return response

        self.enterContext(patch.object(manager_logging, 'api_query_user_func', queue.ask))
        first = parents.start('first', caller)
        question = self._waitPending(queue)[0]
        second = parents.start('second', caller)
        parents.interrupt('first')
        with self.assertRaises(WorkerInterrupted):
            first.future.result(timeout=2)
        self.assertEqual(self._waitPending(queue)[0]['query_id'], question['query_id'])
        queue.respond(question['session_id'], question['query_id'], 'shared-answer')
        self.assertEqual(second.future.result(timeout=2), 'shared-answer')
        self.assertEqual(calls, ['authentication'])

    def test_first_callers_shutdown_leaves_shared_authentication_running(self):
        """Keep authentication independent of the first caller's registry lifetime.

        Stop the first caller's registry while a second registry still waits on
        the same authentication context. The original question must remain valid
        and its answer must reach the surviving caller without another login.
        """
        queue = UserQueryQueue()
        first_callers, second_callers = Workers(), Workers()
        authentication = Workers(blocking=True)
        for registry in (first_callers, second_callers, authentication):
            self.addCleanup(registry.stop)
        self.addCleanup(queue.close)

        def authenticate():
            """Ask once for the shared authentication context and return its answer."""
            return queue.ask('authentication', 'Code?')

        def caller():
            """Wait for shared authentication and return its response string."""
            return authentication.run('credentials', authenticate)[1]

        first = first_callers.start('caller', caller)
        question = self._waitPending(queue)[0]
        second = second_callers.start('caller', caller)
        first_callers.stop()
        with self.assertRaises(WorkerInterrupted):
            first.future.result(timeout=2)
        self.assertEqual(self._waitPending(queue)[0], question)
        queue.respond(question['session_id'], question['query_id'], 'accepted')
        self.assertEqual(second.future.result(timeout=2), 'accepted')

    def test_server_shutdown_discards_late_authentication_and_next_session_reasks(self):
        """Ignore a late OAuth response and start fresh authentication after shutdown.

        Answer a real queued login, then block its mocked provider exchange.
        Shutdown must stop authentication without waiting for that exchange.
        Release the old response after restoring terminal input: no credentials
        may be saved or client constructed. A new server using the same token
        path must ask again and persist only its newly accepted credentials.
        """
        entered, release = Event(), Event()
        self.addCleanup(release.set)
        self.addCleanup(auth.stopAuthenticationWorkers)
        token_path = self.root / 'late-token.json'
        granted = {'access_token': 'fake-token', 'scope': 'test-scope'}
        client = Mock()
        client.oauth2_get_authorize_url.return_value = 'https://example.invalid/authorize'

        def exchange(code):
            """Delay the old code's provider reply; return fake OAuth tokens for either code."""
            if code == 'old-code':
                entered.set()
                if not release.wait(3):
                    raise TimeoutError('Old exchange was not released')
            return SimpleNamespace(by_resource_server={'test-resource': granted})

        client.oauth2_exchange_code_for_tokens.side_effect = exchange

        def initialize():
            """Authenticate normally and return a new memory-only workflow manager."""
            auth.setupIRIapiCompute('query-test', str(token_path))
            return JobManager(max_workflows_active=0)

        runtime = ServerRuntime(initialize)
        self.addCleanup(runtime.stop)
        with patch.dict(auth.IRI_IMPLEMENTATIONS, {'query-test': {'resource_server': 'test-resource',
                        'required_scopes': ['test-scope'], 'session_policy': 'test-policy'}}), \
             patch.object(auth.globus_sdk, 'NativeAppAuthClient', return_value=client), \
             patch.object(auth, 'IRIComputeClient', return_value='authenticated') as build, \
             patch('builtins.input', side_effect=AssertionError('Late terminal prompt')):
            try:
                with patch.object(manager_logging, 'api_query_user_func', runtime.queries.ask):
                    runtime.start()
                    question = self._waitPending(runtime.queries)[0]
                    runtime.queries.respond(question['session_id'], question['query_id'], 'old-code')
                    self.assertTrue(entered.wait(1))
                    context = ('compute', 'query-test', str(token_path.resolve()), 'globus')
                    with auth._authentication_workers._lock:
                        old_worker = auth._authentication_workers._tasks[context]
                    runtime.stop()
                    runtime._startup_worker.future.result(timeout=2)
            finally:
                release.set()
            with self.assertRaises(WorkerInterrupted):
                old_worker.future.result(timeout=2)
            self.assertFalse(token_path.exists())
            build.assert_not_called()
            with self.assertRaises(QueryResponseError):
                runtime.queries.respond(question['session_id'], question['query_id'], 'stale')

            fresh = ServerRuntime(initialize)
            self.addCleanup(fresh.stop)
            with patch.object(manager_logging, 'api_query_user_func', fresh.queries.ask):
                fresh.start()
                new_question = self._waitPending(fresh.queries)[0]
                self.assertNotEqual(new_question['session_id'], question['session_id'])
                # Repeated cleanup of the old server must not stop the new flow.
                runtime.stop()
                fresh.queries.respond(new_question['session_id'], new_question['query_id'], 'new-code')
                fresh._startup_worker.future.result(timeout=2)
                self.assertEqual(fresh.state, 'running')
                self.assertEqual(json.loads(token_path.read_text()), granted)
                build.assert_called_once()
                fresh.stop()

    def test_configured_query_hook_receives_the_prompt(self):
        """Send the prompt directly to the configured hook and return its exact answer."""
        with patch.object(manager_logging, 'api_query_user_func', return_value='custom answer') as hook:
            self.assertEqual(manager_logging.wfapiUserQuery('title', 'prompt'), 'custom answer')
        hook.assert_called_once_with('title', 'prompt')

    def test_startup_uses_installed_hook_and_python_prompts_resume_after_restore(self):
        """Serve startup through the installed hook, then restore terminal prompting."""
        def initialize():
            """Wait for setup approval and return a new memory-only JobManager."""
            self.assertEqual(manager_logging.wfapiUserQuery('setup', 'Authorize startup?'), 'approved')
            return JobManager(max_workflows_active=0)

        runtime = ServerRuntime(initialize)
        self.addCleanup(runtime.stop)
        with patch('builtins.input', return_value='direct-python') as direct:
            with patch.object(manager_logging, 'api_query_user_func', runtime.queries.ask):
                runtime.start()
                question = self._waitPending(runtime.queries)[0]
                self.assertEqual(runtime.state, 'starting')
                self.assertIsNone(runtime.manager)
                runtime.queries.respond(question['session_id'], question['query_id'], 'approved')
                runtime._startup_worker.future.result(timeout=2)
                self.assertEqual(runtime.state, 'running')
                self.assertTrue(runtime.manager.isAlive())
                runtime.stop()
            self.assertEqual(manager_logging.wfapiUserQuery('outside', 'Normal Python?'), 'direct-python')
            direct.assert_called_once_with('Normal Python?')
        runtime.stop()
        self.assertEqual(runtime.state, 'stopped')
        with self.assertRaises(RuntimeError):
            runtime.start()

    def test_manager_thread_routes_new_action_queries_without_blocking_other_work(self):
        """Route stages through the installed server hook after initialization returns.

        The first API stage waits for an answer while an independent workflow
        finishes. Answering the queue allows the original worker to complete without replaying the write.
        """
        blocked, independent = self.root / 'blocked', self.root / 'independent'
        ids = {}
        original = WriteFile.execute

        def execute(operation, job_id):
            """Ask for the blocked WriteFile operation, then perform its normal write."""
            if operation.path == str(blocked):
                manager_logging.wfapiUserQuery('file write', 'Allow this write?')
            original(operation, job_id)

        def initialize():
            """Prepare two API workflows and return a manager for normal progression."""
            manager = JobManager(self.root / 'workflow.sqlite', poll_freq=.01)
            ids['blocked'] = manager.job_data.enqueueJob(APIAction([WriteFile('local', str(blocked), 'done')]))
            ids['other'] = manager.job_data.enqueueJob(APIAction([WriteFile('local', str(independent), 'done')]))
            return manager

        runtime = ServerRuntime(initialize)
        self.addCleanup(runtime.stop)
        with patch.object(WriteFile, 'execute', execute), \
             patch('builtins.input', side_effect=AssertionError('Terminal input was used')), \
             patch.object(manager_logging, 'api_query_user_func', runtime.queries.ask):
            runtime.start()
            question = self._waitPending(runtime.queries)[0]
            deadline = time.monotonic() + 4
            while runtime.manager(lambda jobs: jobs.jobStatus(ids['other'])) != ActionStatus.COMPLETED:
                if time.monotonic() > deadline:
                    self.fail('Independent workflow did not finish')
                time.sleep(.01)
            self.assertFalse(blocked.exists())
            runtime.queries.respond(question['session_id'], question['query_id'], 'yes')
            deadline = time.monotonic() + 4
            while runtime.manager(lambda jobs: jobs.jobStatus(ids['blocked'])) != ActionStatus.COMPLETED:
                if time.monotonic() > deadline:
                    self.fail('Answered workflow did not finish')
                time.sleep(.01)
            self.assertEqual(blocked.read_text(), 'done')

    def test_shutdown_during_initialization_releases_queries_and_late_managers(self):
        """Stop a queried initializer and a slow initializer before publication.

        Queue closure releases the first worker. The second returns a manager
        only after shutdown; that late manager must be stopped and storage closed.
        """
        def ask_during_setup():
            """Ask during initialization and return a manager only if answered."""
            manager_logging.wfapiUserQuery('setup', 'Wait for approval')
            return JobManager()

        runtime = ServerRuntime(ask_during_setup)
        self.enterContext(patch.object(manager_logging, 'api_query_user_func', runtime.queries.ask))
        runtime.start()
        self._waitPending(runtime.queries)
        runtime.stop()
        runtime._startup_worker.future.result(timeout=2)
        self.assertEqual(runtime.state, 'stopped')
        self.assertIsNone(runtime.manager)

        entered, release = Event(), Event()
        constructed = []

        def slow_setup():
            """Simulate an uninterruptible setup call and return a late JobManager."""
            entered.set()
            if not release.wait(2):
                raise TimeoutError('Test did not release initializer')
            manager = JobManager()
            constructed.append(manager)
            return manager

        runtime = ServerRuntime(slow_setup)
        self.addCleanup(runtime.stop)
        try:
            runtime.start()
            self.assertTrue(entered.wait(1))
            runtime.stop()
        finally:
            release.set()
        runtime._startup_worker.future.result(timeout=2)
        self.assertIsNone(runtime.manager)
        self.assertFalse(constructed[0].isAlive())
        with self.assertRaises(DatabaseClosed):
            with constructed[0].job_data.conn:
                self.fail('Late manager storage remained open')


if __name__ == '__main__':
    unittest.main()
