"""Offline checks for Globus collection session reauthorization challenges."""

from pathlib import Path
from types import SimpleNamespace
import json
import unittest
from unittest.mock import Mock, call, patch

import globus_sdk
import requests

from amscjobmanager.iri_api import auth, globus, state


class GlobusAuthorizationTests(unittest.TestCase):
    def setUp(self):
        self.parameters = {
            "session_message": "Session reauthentication required (Globus Transfer)",
            "session_required_single_domain": ["sso.ccs.ornl.gov", "clients.auth.globus.org"],
            "session_required_identities": [], "session_required_mfa": False,
        }
        response = requests.Response()
        response.status_code = 403
        response.request = requests.Request("GET", "https://transfer.api.globus.org/operation/stat").prepare()
        response._content = json.dumps({"code": "EndpointPermissionDenied", "message": "login failed",
                                       "authorization_parameters": self.parameters}).encode()
        response.headers["Content-Type"] = "application/json"
        self.challenge = globus_sdk.TransferAPIError(response)

    def test_collection_challenge_retries_with_replacement_token(self):
        """An OLCF-style error requests the required domain before retrying stat."""
        old_client, new_client = Mock(), Mock()
        old_client.operation_stat.side_effect = self.challenge
        new_client.operation_stat.return_value = {"type": "dir", "size": 0}
        with patch.object(globus, "getTransferToken", side_effect=["old-token", "new-token"]) as token, \
             patch.object(globus_sdk, "TransferClient", side_effect=[old_client, new_client]) as clients:
            self.assertEqual(globus.pathStatGlobus("frontier", "/safe")["type"], "dir")
        self.assertEqual(token.call_args_list, [call("globus_transfer"),
                         call("globus_transfer", authorization_parameters=self.parameters)])
        self.assertEqual(clients.call_args_list[0].kwargs["authorizer"].access_token, "old-token")
        self.assertEqual(clients.call_args_list[1].kwargs["authorizer"].access_token, "new-token")
        self.assertEqual(old_client.operation_stat.call_args, new_client.operation_stat.call_args)

    def test_second_challenge_propagates_without_another_login(self):
        """Repeated authorization failure cannot create an endless interactive loop."""
        client = Mock()
        client.operation_ls.side_effect = self.challenge
        with patch.object(globus, "getTransferToken", side_effect=["old", "new"]) as token, \
             patch.object(globus_sdk, "TransferClient", return_value=client):
            with self.assertRaises(globus_sdk.TransferAPIError):
                globus.remoteLsGlobus("frontier", "/safe")
        self.assertEqual(client.operation_ls.call_count, 2)
        self.assertEqual(token.call_count, 2)

    def test_plain_permission_denial_does_not_trigger_reauthorization(self):
        """Ordinary file permissions are not treated as a session-login challenge."""
        self.challenge.raw_json.pop("authorization_parameters")
        client = Mock()
        client.operation_stat.side_effect = self.challenge
        with patch.object(globus, "getTransferToken", return_value="token") as token, \
             patch.object(globus_sdk, "TransferClient", return_value=client):
            with self.assertRaises(globus_sdk.TransferAPIError):
                globus.pathStatGlobus("frontier", "/safe")
        token.assert_called_once_with("globus_transfer")
        client.operation_stat.assert_called_once()

    def test_authorize_url_forwards_domain_requirements_and_omits_empty_identities(self):
        """The actual authorize URL receives requirements, not merely prompt=login."""
        client = Mock()
        client.oauth2_get_authorize_url.return_value = "https://auth.globus.org/authorize"
        client.oauth2_exchange_code_for_tokens.return_value = SimpleNamespace(by_resource_server={
            auth.IRI_TRANSFER_RESOURCE_SERVER: {"access_token": "iri-new"},
            "transfer.api.globus.org": {"access_token": "globus-new"},
        })
        parameters = {**self.parameters, "required_scopes": ["urn:test:additional-scope"]}
        with patch.object(auth, "_globus_data_access_collections", return_value=set()), \
             patch.object(auth, "wfapiUserQuery", return_value="test-code"):
            result = auth.interactive_globus_transfer_login(client, authorization_parameters=parameters)
        client.oauth2_get_authorize_url.assert_called_once_with(
            prompt="login", session_message=self.parameters["session_message"],
            session_required_single_domain=self.parameters["session_required_single_domain"],
            session_required_mfa=False,
        )
        self.assertIn("urn:test:additional-scope", client.oauth2_start_flow.call_args.kwargs["requested_scopes"])
        self.assertEqual(result, ({"access_token": "iri-new"}, {"access_token": "globus-new"}))

    def test_challenge_bypasses_refresh_and_persists_both_new_tokens(self):
        """Refreshing the old session is bypassed; new tokens are saved and installed."""
        granted = ({"access_token": "iri-new", "refresh_token": "iri-refresh"},
                   {"access_token": "globus-new", "refresh_token": "globus-refresh"})
        with patch.dict(state.tokens, {"iriapi_transfer_base": "iri-old", "globus_transfer": "globus-old"}), \
             patch.object(globus_sdk, "NativeAppAuthClient") as client, \
             patch.object(auth, "load_globus_tokens", return_value={"refresh_token": "old"}), \
             patch.object(auth, "refresh_globus_transfer_tokens") as refresh, \
             patch.object(auth, "interactive_globus_transfer_login", return_value=granted) as login, \
             patch.object(auth, "save_globus_tokens") as save:
            auth.setupIRIapiGlobusTransfer("/fake/transfer.json", authorization_parameters=self.parameters)
            refresh.assert_not_called()
            login.assert_called_once_with(client.return_value, authorization_parameters=self.parameters)
            save.assert_called_once_with(Path("/fake/transfer.json"), {**granted[0], "globus_transfer": granted[1]})
            self.assertEqual(state.tokens["iriapi_transfer_base"], "iri-new")
            self.assertEqual(state.tokens["globus_transfer"], "globus-new")

    def test_failed_authorization_leaves_existing_tokens_unchanged(self):
        """A failed login must not overwrite saved credentials or cached tokens."""
        old = {"iriapi_transfer_base": "iri-old", "globus_transfer": "globus-old"}
        with patch.dict(state.tokens, old), patch.object(globus_sdk, "NativeAppAuthClient"), \
             patch.object(auth, "load_globus_tokens", return_value=None), \
             patch.object(auth, "interactive_globus_transfer_login", side_effect=RuntimeError("login failed")), \
             patch.object(auth, "save_globus_tokens") as save:
            with self.assertRaisesRegex(RuntimeError, "login failed"):
                auth.setupIRIapiGlobusTransfer("/fake/transfer.json", authorization_parameters=self.parameters)
            save.assert_not_called()
            self.assertEqual(state.tokens, old)

    def test_state_forwards_challenge_even_when_token_is_cached(self):
        """A configured cached token cannot prevent explicit session reauthorization."""
        def install(*args, **kwargs):
            state.tokens.update(iriapi_transfer_base="iri-new", globus_transfer="globus-new")

        with patch.dict(state.tokens, {"globus_transfer": "old"}), \
             patch.object(state, "_transfer_token_path", "/fake/transfer.json"), \
             patch.object(state, "setupIRIapiGlobusTransfer", side_effect=install) as setup:
            token = state.getTransferToken("globus_transfer", authorization_parameters=self.parameters)
            self.assertEqual(token, "globus-new")
            setup.assert_called_once_with("/fake/transfer.json", authorization_parameters=self.parameters)


if __name__ == "__main__":
    unittest.main()
