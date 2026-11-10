"""Offline terminal authentication checks using mocked OAuth exchanges.

Exercise the default query hook, actual worker execution, token-file persistence and cache updates.
No browser, real credentials or provider requests are involved.
"""

import io
import json
from pathlib import Path
import tempfile
from concurrent.futures import ThreadPoolExecutor
from threading import Event, get_ident
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, call, patch

from amscjobmanager import logging as manager_logging
from amscjobmanager.iri_api import auth, state
from amscjobmanager.workers import Workers


class AuthenticationWorkerTests(unittest.TestCase):
    def setUp(self):
        """Set up fake provider scopes and an isolated token path; return None."""
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.token_path = Path(temporary.name) / 'credentials' / 'token.json'
        self.server = 'test-resource-server'
        self.granted = {'access_token': 'fake-access', 'refresh_token': 'fake-refresh', 'scope': 'test-scope'}
        specification = patch.dict(auth.IRI_IMPLEMENTATIONS, {
            'test': {'resource_server': self.server, 'required_scopes': ['test-scope'], 'session_policy': 'test-policy'},
        })
        specification.start()
        self.addCleanup(specification.stop)
        # Test the default blocking path even when an embedding test suite has
        # configured its own global hook. Capture the intentional prompt output.
        for hook in [patch.object(manager_logging, 'api_query_user_func', manager_logging.defaultAPIuserQueryFunc),
                     patch('sys.stdout', new=io.StringIO()),
                     patch.object(manager_logging, 'api_log_func', Mock())]:
            hook.start()
            self.addCleanup(hook.stop)

    def test_plain_query_and_legacy_hook_still_return_strings(self):
        """Default input strips whitespace; plain calls preserve two-argument hooks."""
        with patch('builtins.input', return_value=' answer '):
            self.assertEqual(manager_logging.wfapiUserQuery('Title', 'Question'), 'answer')
        hook = Mock(return_value='custom')
        with patch.object(manager_logging, 'api_query_user_func', hook):
            self.assertEqual(manager_logging.wfapiUserQuery('Title', 'Question'), 'custom')
        hook.assert_called_once_with('Title', 'Question')

    def test_concurrent_client_initialization_is_shared_per_implementation(self):
        """Two action workers requesting one implementation initialize it once.

        Block the first initialization on a query, then start another caller.
        Both must receive the same cached client without replacing/closing it.
        """
        workers = Workers()
        entered, answer = Event(), Event()
        client = Mock()

        def initialize(*args, **kwargs):
            """Wait for a query answer and return the prepared fake client object."""
            manager_logging.wfapiUserQuery('login', 'code?')
            return client

        def prompt(title, query):
            """Wait for the fake answer to title/query (str), returning a code string."""
            entered.set()
            if not answer.wait(3):
                raise RuntimeError('Test did not answer login')
            return 'code'

        try:
            with patch.dict(state.iri_compute_clients, {}, clear=True), \
                 patch.dict(state._implementation_credentials, {'test': {'token_path': str(self.token_path)}}, clear=True), \
                 patch.dict(auth.IRI_IMPLEMENTATIONS, {'test': {'machines': {}}}), \
                 patch.object(state, 'setupIRIapiCompute', side_effect=initialize) as setup, \
                 patch.object(manager_logging, 'api_query_user_func', prompt):
                self.assertEqual(workers.run('first', state.getImplementationClient, 'test'), (False, None))
                self.assertTrue(entered.wait(1))
                self.assertEqual(workers.run('second', state.getImplementationClient, 'test'), (False, None))
                answer.set()
                completed = {}
                deadline = time.monotonic() + 2
                while len(completed) < 2:
                    if time.monotonic() > deadline:
                        self.fail('Shared initialization did not complete')
                    for key in ('first', 'second'):
                        if key not in completed:
                            ready, result = workers.run(key, state.getImplementationClient, 'test')
                            if ready:
                                completed[key] = result
                    time.sleep(.005)
                self.assertIs(completed['first'], client)
                self.assertIs(completed['second'], client)
                setup.assert_called_once()
                client.close.assert_not_called()
        finally:
            answer.set()
            workers.stop()

    def test_authenticator_waits_on_worker_without_continuations(self):
        """Keep the OAuth stack on a worker while its normal query hook blocks.

        Release a simulated answer and verify the original flow completes and
        saves credentials. The public Python call waits instead of raising a
        condition exception or requiring a completion callback.
        """
        entered, answer = Event(), Event()
        query_threads = []
        client = Mock()
        client.oauth2_exchange_code_for_tokens.return_value = SimpleNamespace(
            by_resource_server={self.server: self.granted})

        def prompt(question):
            """Wait for a fake answer to question (str), returning its code string."""
            query_threads.append(get_ident())
            entered.set()
            if not answer.wait(3):
                raise RuntimeError('Test did not answer the query')
            return 'code'

        with patch.object(auth.globus_sdk, 'NativeAppAuthClient', return_value=client), \
             patch.object(auth, 'IRIComputeClient', return_value='client'), \
             patch('builtins.input', side_effect=prompt) as query, \
             ThreadPoolExecutor(max_workers=1) as callers:
            result = callers.submit(auth.setupIRIapiCompute, 'test', str(self.token_path))
            try:
                self.assertTrue(entered.wait(1))
                self.assertFalse(result.done())
                self.assertNotEqual(query_threads, [get_ident()])
            finally:
                answer.set()
            self.assertEqual(result.result(timeout=2), 'client')
            query.assert_called_once()
            client.oauth2_start_flow.assert_called_once()

    def test_compute_interactive_retries_codes_then_persists_before_client(self):
        """Reuse one OAuth flow after a rejected code, then save before construction.

        Input and exchanges are mocked; the worker writes fake tokens to
        a private temporary file. The client factory observes the saved contents.
        """
        client = Mock()
        client.oauth2_get_authorize_url.return_value = 'https://example.invalid/authorize'
        client.oauth2_exchange_code_for_tokens.side_effect = [RuntimeError('invalid code'),
            SimpleNamespace(by_resource_server={self.server: self.granted})]
        def build(implementation, access_token):
            """Check saved credentials before returning a mock IRI client.

            implementation/access_token (str) identify the requested client.
            Return an opaque object standing in for IRIComputeClient.
            """
            self.assertEqual(implementation, 'test')
            self.assertEqual(access_token, self.granted['access_token'])
            self.assertEqual(json.loads(self.token_path.read_text()), self.granted)
            return 'initialized-client'

        with patch.object(auth.globus_sdk, 'NativeAppAuthClient', return_value=client), \
             patch.object(auth, 'IRIComputeClient', side_effect=build), \
             patch('builtins.input', side_effect=[' bad ', ' good ']) as prompt:
            result = auth.setupIRIapiCompute('test', str(self.token_path))
        self.assertEqual(result, 'initialized-client')
        self.assertEqual(prompt.call_count, 2)
        self.assertEqual(client.oauth2_exchange_code_for_tokens.call_args_list, [call('bad'), call('good')])
        client.oauth2_start_flow.assert_called_once_with(requested_scopes='test-scope', refresh_tokens=True)
        client.oauth2_get_authorize_url.assert_called_once_with(query_params={
            'prompt': 'login', 'session_required_policies': 'test-policy'})
        self.assertEqual(self.token_path.stat().st_mode & 0o777, 0o600)

    def test_compute_refresh_completes_without_prompting(self):
        """Successful refresh takes the same persistence/client path without input."""
        with patch.object(auth.globus_sdk, 'NativeAppAuthClient'), \
             patch.object(auth, 'load_globus_tokens', return_value={'refresh_token': 'old'}), \
             patch.object(auth, 'refresh_globus_tokens', return_value=self.granted), \
             patch.object(auth, 'IRIComputeClient', return_value='client') as build, \
             patch('builtins.input', side_effect=AssertionError('Unexpected prompt')):
            result = auth.setupIRIapiCompute('test', str(self.token_path))
        self.assertEqual(result, 'client')
        self.assertEqual(json.loads(self.token_path.read_text()), self.granted)
        build.assert_called_once_with('test', 'fake-access')

    def test_failed_refresh_falls_back_to_terminal_login(self):
        """A transport failure during refresh still permits interactive recovery.

        Fail the mocked refresh before a token response exists, then complete
        the normal prompt/exchange/save path. The refresh error must not itself
        fail while logging an uninitialized response variable.
        """
        client = Mock()
        client.oauth2_refresh_token.side_effect = RuntimeError('refresh unavailable')
        client.oauth2_exchange_code_for_tokens.return_value = SimpleNamespace(
            by_resource_server={self.server: self.granted})
        with patch.object(auth.globus_sdk, 'NativeAppAuthClient', return_value=client), \
             patch.object(auth, 'load_globus_tokens', return_value={'refresh_token': 'old'}), \
             patch.object(auth, 'IRIComputeClient', return_value='client'), \
             patch('builtins.input', return_value='code') as prompt:
            self.assertEqual(auth.setupIRIapiCompute('test', str(self.token_path)), 'client')
        client.oauth2_refresh_token.assert_called_once_with('old')
        prompt.assert_called_once()
        self.assertEqual(json.loads(self.token_path.read_text()), self.granted)

    def test_compute_missing_scopes_does_not_prompt_again_or_save(self):
        """A completion failure propagates instead of exchanging more single-use codes."""
        client = Mock()
        client.oauth2_exchange_code_for_tokens.return_value = SimpleNamespace(
            by_resource_server={self.server: {**self.granted, 'scope': ''}})
        with patch.object(auth.globus_sdk, 'NativeAppAuthClient', return_value=client), \
             patch.object(auth, 'save_globus_tokens') as save, \
             patch('builtins.input', return_value='code') as prompt:
            with self.assertRaisesRegex(RuntimeError, 'Missing required scopes'):
                auth.setupIRIapiCompute('test', str(self.token_path))
        prompt.assert_called_once()
        client.oauth2_exchange_code_for_tokens.assert_called_once_with('code')
        save.assert_not_called()

    def test_transfer_interactive_retries_then_installs_both_token_sets(self):
        """The authentication worker retries exchange and persists both services before caching."""
        iri = {'access_token': 'iri-new', 'refresh_token': 'iri-refresh'}
        globus = {'access_token': 'globus-new', 'refresh_token': 'globus-refresh'}
        client = Mock()
        client.oauth2_exchange_code_for_tokens.side_effect = [RuntimeError('invalid'),
            SimpleNamespace(by_resource_server={auth.IRI_TRANSFER_RESOURCE_SERVER: iri, 'transfer.api.globus.org': globus})]
        with patch.dict(state.tokens, {'iriapi_transfer_base': 'old', 'globus_transfer': 'old'}), \
             patch.object(auth.globus_sdk, 'NativeAppAuthClient', return_value=client), \
             patch.object(auth, '_globus_data_access_collections', return_value=set()), \
             patch('builtins.input', side_effect=['bad', 'good']) as prompt:
            self.assertIsNone(auth.setupIRIapiGlobusTransfer(str(self.token_path)))
            self.assertEqual(state.tokens['iriapi_transfer_base'], 'iri-new')
            self.assertEqual(state.tokens['globus_transfer'], 'globus-new')
        self.assertEqual(json.loads(self.token_path.read_text()), {**iri, 'globus_transfer': globus})
        self.assertEqual(prompt.call_count, 2)
        client.oauth2_start_flow.assert_called_once()

    def test_transfer_refresh_completes_without_prompting(self):
        """Refreshed transfer tokens are saved/installed using the same sequential path."""
        granted = ({'access_token': 'iri-new'}, {'access_token': 'globus-new'})
        with patch.dict(state.tokens, {'iriapi_transfer_base': 'old', 'globus_transfer': 'old'}), \
             patch.object(auth.globus_sdk, 'NativeAppAuthClient'), \
             patch.object(auth, 'load_globus_tokens', return_value={'refresh_token': 'old'}), \
             patch.object(auth, 'refresh_globus_transfer_tokens', return_value=granted), \
             patch('builtins.input', side_effect=AssertionError('Unexpected prompt')):
            auth.setupIRIapiGlobusTransfer(str(self.token_path))
            self.assertEqual(state.tokens['iriapi_transfer_base'], 'iri-new')
            self.assertEqual(state.tokens['globus_transfer'], 'globus-new')
        self.assertEqual(json.loads(self.token_path.read_text()), {**granted[0], 'globus_transfer': granted[1]})

    def test_save_failure_propagates_without_new_prompt_or_cache_update(self):
        """A valid code followed by disk failure must not be re-exchanged or installed."""
        client = Mock()
        client.oauth2_exchange_code_for_tokens.return_value = SimpleNamespace(by_resource_server={
            auth.IRI_TRANSFER_RESOURCE_SERVER: {'access_token': 'iri-new'},
            'transfer.api.globus.org': {'access_token': 'globus-new'}})
        old = {'iriapi_transfer_base': 'old', 'globus_transfer': 'old'}
        with patch.dict(state.tokens, old), \
             patch.object(auth.globus_sdk, 'NativeAppAuthClient', return_value=client), \
             patch.object(auth, '_globus_data_access_collections', return_value=set()), \
             patch.object(auth, 'save_globus_tokens', side_effect=OSError('disk error')), \
             patch('builtins.input', return_value='valid-code') as prompt:
            with self.assertRaisesRegex(OSError, 'disk error'):
                auth.setupIRIapiGlobusTransfer(str(self.token_path))
            self.assertEqual(state.tokens, old)
        prompt.assert_called_once()
        client.oauth2_exchange_code_for_tokens.assert_called_once()

    def test_transfer_eof_remains_a_noninteractive_error(self):
        """Missing terminal input still exits without saving or updating credentials."""
        with patch.object(auth, '_globus_data_access_collections', return_value=set()), \
             patch('builtins.input', side_effect=EOFError):
            with self.assertRaisesRegex(RuntimeError, 'requires an interactive terminal'):
                auth.interactive_globus_transfer_login(Mock())


if __name__ == '__main__':
    unittest.main()
