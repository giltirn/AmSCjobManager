"""Internal IRI/Globus authentication, token persistence, and client factories.

Normal operations obtain clients through state.getImplementationClient;
these explicit factories also support bootstrap and authorization-only tests.
"""

import json
import os
import stat
import time
from pathlib import Path

import globus_sdk
from globus_sdk.exc import GlobusAPIError
from globus_sdk.scopes import TransferScopes
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from ..logging import wfapiLog, wfapiUserQuery
from .config import IRI_IMPLEMENTATIONS
from .state import IRIComputeClient, getConfiguredImplementations, tokens
from .state import special_globus_endpoints
from .filesystem import getMountedFilesystems

def load_globus_tokens(token_file: Path) -> dict | None:
    if "~" in str(token_file):
        raise ValueError("Credential paths containing '~' are not supported")
    if not token_file.exists():
        return None
    with token_file.open("r", encoding="utf-8") as f:
        return json.load(f)

def save_globus_tokens(token_file: Path, tokens: dict) -> None:
    if "~" in str(token_file):
        raise ValueError("Credential paths containing '~' are not supported")
    token_file.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(token_file.parent, 0o700)
    tmp = token_file.with_suffix(".tmp")
    with os.fdopen(
        os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600),
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(tokens, f, indent=2)
    os.replace(tmp, token_file)
    os.chmod(token_file, stat.S_IRUSR | stat.S_IWUSR)

def refresh_globus_tokens(client: globus_sdk.NativeAppAuthClient, refresh_token: str, server: str) -> dict | None:
    try:
        token_response = client.oauth2_refresh_token(refresh_token)
        return token_response.by_resource_server[server]
    except GlobusAPIError as exc:
        wfapiLog(
            f"IRI token refresh failed ({exc.http_status}); switching to interactive login."
        )
        return None
    except Exception as exc:
        wfapiLog(
            f"IRI token refresh failed other error (Exception: {exc}, Server response: {token_response}, Resource server: {server}); switching to interactive login."
        )
        return None

############################################################
####IRI API SETUP
#############################################################

GLOBUS_CLIENT_ID = "eb18f0bb-4c76-43b5-88f1-750782be30ad" #Femtomeas, ckelly@bnl.gov

RESOURCE_SERVER = "auth.globus.org"

def interactive_globus_login(client: globus_sdk.NativeAppAuthClient, implementation: str) -> dict:
    resource_server = IRI_IMPLEMENTATIONS[implementation]["resource_server"]
    required_scopes = set(IRI_IMPLEMENTATIONS[implementation]["required_scopes"])
    client.oauth2_start_flow(
        requested_scopes=" ".join(sorted(required_scopes)),
        refresh_tokens=True,
    )
    query_parameters = {"prompt": "login"}
    session_policy = IRI_IMPLEMENTATIONS[implementation].get("session_policy")
    if session_policy:
        query_parameters["session_required_policies"] = session_policy
    query = f"""Open this URL, login, and consent:
    { client.oauth2_get_authorize_url(query_params=query_parameters) }

    Enter authorization code: """

    accept = False
    while(not accept):
        try:
            code = wfapiUserQuery("IRI API login", query)
            token_response = client.oauth2_exchange_code_for_tokens(code)
            accept = True
        except Exception as e:
            continue

    assert resource_server in token_response.by_resource_server.keys()
        
    return token_response.by_resource_server[resource_server]

class AMSCPersonalAccessToken(BaseModel):
    """The JSON credential produced by the AMSC PAT generator."""

    model_config = ConfigDict(strict=True, hide_input_in_errors=True)

    project: str
    description: str
    token_id: str
    token: str = Field(pattern=r"\S", repr=False)


def _load_amsc_pat(key_path: str) -> dict[str, str]:
    """Read a PAT without exposing its contents in validation errors."""
    if "~" in str(key_path):
        raise ValueError("Credential paths containing '~' are not supported")
    path = Path(key_path)
    try:
        data = path.read_text(encoding="utf-8")
    except (UnicodeError, OSError) as exc:
        raise ValueError(
            f"Cannot read AMSC PAT file {str(path)!r}: {type(exc).__name__}"
        ) from None

    try:
        return AMSCPersonalAccessToken.model_validate_json(data).model_dump()
    except ValidationError as exc:
        raise ValueError(
            f"Invalid AMSC PAT file {str(path)!r}: {exc}"
        ) from None


def setupIRIapiCompute(
    implementation: str, key_path: str, token_type: str = "globus"
) -> IRIComputeClient:
    """Create an IRI client using the selected authentication method."""
    if implementation not in IRI_IMPLEMENTATIONS:
        raise ValueError(f"Unknown IRI implementation: {implementation!r}")

    client_factories = {
        "globus": _create_globus_client,
        "amsc_pat": _create_amsc_pat_client,
    }
    try:
        factory = client_factories[token_type]
    except KeyError:
        raise ValueError(f"Unknown IRI token type: {token_type!r}") from None
    return factory(implementation, key_path)


def _create_amsc_pat_client(implementation: str, key_path: str) -> IRIComputeClient:
    """Create an IRI client from an AMSC personal access token file."""
    pat = _load_amsc_pat(key_path)
    return IRIComputeClient(
        implementation,
        pat["token"],
        token_type="amsc_pat",
        project=pat["project"],
        description=pat["description"],
        token_id=pat["token_id"],
    )


def _create_globus_client(implementation: str, key_path: str) -> IRIComputeClient:
    """Create an IRI client through Globus login or token refresh."""
    auth_client = globus_sdk.NativeAppAuthClient(GLOBUS_CLIENT_ID)
    resource_server = IRI_IMPLEMENTATIONS[implementation]["resource_server"]
    required_scopes = set(IRI_IMPLEMENTATIONS[implementation]["required_scopes"])

    wfapiLog(f"setupIRIapi({implementation}) checking stored tokens at", key_path)
    stored = load_globus_tokens(Path(key_path))
    auth_data = None
    if stored and stored.get("refresh_token"):
        auth_data = refresh_globus_tokens(auth_client, stored["refresh_token"], resource_server)

    if auth_data == None:
        auth_data = interactive_globus_login(auth_client, implementation)

    granted = set((auth_data.get("scope") or "").split())
    missing = required_scopes - granted
    if missing:
        raise RuntimeError(f"Missing required scopes: {sorted(missing)}")

    save_globus_tokens(Path(key_path), auth_data)

    expires_at = auth_data.get("expires_at_seconds")
    if expires_at:
        ttl = int(expires_at - time.time())
        print(f"\nAccess token valid for ~{max(ttl, 0)} seconds.")

    wfapiLog(f"Saved {implementation} token data to {key_path}")
    wfapiLog(f"Granted scopes: {auth_data.get('scope', '')}")

    return IRIComputeClient(implementation, auth_data['access_token'])
    

#############################
#### IRI TRANSFER API
#############################

IRI_TRANSFER_RESOURCE_SERVER = "08da012c-6998-46f9-9375-a6985ebe3f2b"
IRI_TRANSFER_DEFAULT_SCOPE = f"https://auth.globus.org/scopes/{IRI_TRANSFER_RESOURCE_SERVER}/transfer"

def _globus_data_access_collections() -> set[str]:
    """Return every collection for which the transfer login needs access."""
    collections = set()
    for implementation in getConfiguredImplementations():
        specification = IRI_IMPLEMENTATIONS[implementation]
        filesystems = [
            globus_filesystem
            for machine in specification["machines"]
            for filesystem in getMountedFilesystems(machine).values()
            for globus_filesystem in filesystem.get("globus_filesystems", [])
        ]
        for filesystem in filesystems:
            if not filesystem.get("requires_data_access", False):
                continue
            endpoint = filesystem["endpoint"]
            collections.add(special_globus_endpoints.get(endpoint, endpoint))
    return collections

def interactive_globus_transfer_login(client: globus_sdk.NativeAppAuthClient) -> dict:
    scope = globus_sdk.Scope(IRI_TRANSFER_DEFAULT_SCOPE)
    mapped_collections = _globus_data_access_collections()
    
    data_access = [globus_sdk.scopes.GCSCollectionScopes(mc).data_access for mc in mapped_collections] #data access scope required for mapped collections
    transfer_scope = TransferScopes.all.with_dependencies(data_access)
    scope = scope.with_dependency(transfer_scope)
    print(f"Logging in with scope: {scope}")

    client.oauth2_start_flow(
        # The IRI service needs Transfer as a dependency, while this client
        # also needs a first-party Transfer API token for task-list lookup.
        requested_scopes=[scope, transfer_scope],
        refresh_tokens=True,
    )
    query = f"""Open this URL, login, and consent:
    { client.oauth2_get_authorize_url(query_params={"prompt": "login"}) }

    Enter authorization code: """

    accept = False
    while(not accept):
        try:
            code = wfapiUserQuery("IRI Transfer API login", query)
            token_response = client.oauth2_exchange_code_for_tokens(code)
            accept = True
        except EOFError as exc:
            raise RuntimeError(
                "Globus Transfer API authorization requires an interactive terminal"
            ) from exc
        except Exception as e:
            continue

    return _globus_transfer_tokens_from_response(token_response)

def _globus_transfer_tokens_from_response(token_response):
    """Extract the IRI transfer-service and Globus Transfer API tokens."""
    resource_tokens = token_response.by_resource_server
    if IRI_TRANSFER_RESOURCE_SERVER not in resource_tokens:
        raise RuntimeError(
            "IRI transfer-service token was not granted; received tokens for "
            f"{sorted(resource_tokens)}"
        )
    if "transfer.api.globus.org" not in resource_tokens:
        raise RuntimeError(
            "Globus Transfer API token was not granted; received tokens for "
            f"{sorted(resource_tokens)}"
        )
    return (
        resource_tokens[IRI_TRANSFER_RESOURCE_SERVER],
        resource_tokens["transfer.api.globus.org"],
    )

def refresh_globus_transfer_tokens(client: globus_sdk.NativeAppAuthClient, stored_tokens: dict):
    """Refresh the separately persisted IRI and Globus Transfer API tokens."""
    globus_auth_data = stored_tokens.get("globus_transfer")
    if not globus_auth_data or not globus_auth_data.get("refresh_token"):
        return None

    iri_auth_data = refresh_globus_tokens(
        client, stored_tokens["refresh_token"], IRI_TRANSFER_RESOURCE_SERVER
    )
    globus_auth_data = refresh_globus_tokens(
        client, globus_auth_data["refresh_token"], "transfer.api.globus.org"
    )
    if iri_auth_data is None or globus_auth_data is None:
        return None
    return iri_auth_data, globus_auth_data

def setupIRIapiGlobusTransfer(key_path):
    client =  globus_sdk.NativeAppAuthClient(GLOBUS_CLIENT_ID)

    wfapiLog("setupIRIapiGlobusTransfer checking stored tokens at",key_path)
    stored = load_globus_tokens(Path(key_path))
    auth_data = None
    globus_auth_data = None
    if stored and stored.get("refresh_token"):
        refreshed = refresh_globus_transfer_tokens(client, stored)
        if refreshed is not None:
            auth_data, globus_auth_data = refreshed

    if auth_data == None:
        auth_data, globus_auth_data = interactive_globus_transfer_login(client)

    stored_tokens = dict(auth_data)
    stored_tokens["globus_transfer"] = globus_auth_data
    save_globus_tokens(Path(key_path), stored_tokens)

    expires_at = auth_data.get("expires_at_seconds")
    if expires_at:
        ttl = int(expires_at - time.time())
        print(f"\nAccess token valid for ~{max(ttl, 0)} seconds.")

    wfapiLog(f"Saved token data to {key_path}")
    wfapiLog(f"Granted scopes: {auth_data.get('scope', '')}")

    tokens['iriapi_transfer_base'] = auth_data['access_token']
    tokens['globus_transfer'] = globus_auth_data['access_token']



