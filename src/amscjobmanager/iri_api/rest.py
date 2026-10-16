"""Shared authenticated IRI REST and asynchronous-task helpers."""

import json
import time
from typing import Callable

import httpx

from ..logging import wfapiLog
from .config import IRI_IMPLEMENTATIONS
from .state import getImplementationClient, resolveImplementation

IRI_GET_RETRYABLE_STATUS_CODES = {502, 503, 504, 500}
IRI_GET_MAX_ATTEMPTS = 4
IRI_GET_INITIAL_RETRY_DELAY_SECONDS = 1
IRI_TASK_MAX_ATTEMPTS = 4
IRI_TASK_INITIAL_RETRY_DELAY_SECONDS = 1

def _request_target(machine, implementation, suburl):
    implementation = resolveImplementation(machine=machine, implementation=implementation)
    if not isinstance(suburl, str) or not suburl:
        raise ValueError("An IRI request path is required")
    client = getImplementationClient(implementation)
    url = IRI_IMPLEMENTATIONS[implementation]["api_base"].rstrip("/") + '/' + suburl.lstrip("/")
    return client, url


def get(machine=None, suburl=None, params=None, *, implementation=None):
    """Authenticated GET by machine or implementation; positional machine remains supported."""
    client, url = _request_target(machine, implementation, suburl)
    retry_errors = []
    for attempt in range(IRI_GET_MAX_ATTEMPTS):
        try:
            resp = client.http_client.get(
                url,
                headers={"accept": "application/json", "Authorization": f"Bearer {client.access_token}"},
                params=params,
                timeout=300,
            )
        except httpx.TransportError as exc:
            retryable_error = exc
        else:
            if resp.status_code == 200:
                return json.loads(resp.text)
            if resp.status_code not in IRI_GET_RETRYABLE_STATUS_CODES:
                raise Exception(
                    f"Get operation failed with code {resp.status_code} and text "
                    f"{resp.text} (full response: {resp})"
                )
            retryable_error = RuntimeError(
                f"HTTP {resp.status_code}: {resp.text}"
            )

        retry_errors.append(f"attempt {attempt + 1}: {retryable_error}")

        if attempt == IRI_GET_MAX_ATTEMPTS - 1:
            raise RuntimeError(
                f"GET {url} failed after {IRI_GET_MAX_ATTEMPTS} attempts: "
                + "; ".join(retry_errors)
            ) from retryable_error

        delay = IRI_GET_INITIAL_RETRY_DELAY_SECONDS * (2 ** attempt)
        wfapiLog(
            f"Transient GET failure for {client.implementation}:{suburl} ({retryable_error}); "
            f"retrying in {delay} seconds"
        )
        time.sleep(delay)


def put(machine=None, suburl=None, data=None, params=None, *, implementation=None):
    client, url = _request_target(machine, implementation, suburl)
    headers={ "accept" : "application/json", "Authorization" : f"Bearer {client.access_token}" }

    resp = client.http_client.put(
        url,
        headers=headers,
        json=data,
        params=params,
        timeout=300,
    )
    return json.loads(resp.text), resp.status_code
        

def post(machine=None, suburl=None, data=None, params=None, files=None, data_is_json=True, *, implementation=None):
    client, url = _request_target(machine, implementation, suburl)
    headers={ "accept" : "application/json", "Authorization" : f"Bearer {client.access_token}" }

    resp = client.http_client.post(
        url,
        headers=headers,
        json=data if data_is_json else None,
        data=data if not data_is_json else None,
        params=params,
        files=files,
        timeout=300,
    )
    return json.loads(resp.text), resp.status_code


def delete(machine=None, suburl=None, params=None, *, implementation=None):
    client, url = _request_target(machine, implementation, suburl)
    resp = client.http_client.delete(
        url,
        headers={"accept": "*/*", "Authorization": f"Bearer {client.access_token}"},
        params=params,
        timeout=300,
    )
    return {} if resp.text == "" else resp.json(), resp.status_code

def waitTask(
    machine: str | None = None,
    task_id: str | None = None,
    poll_freq: int = 4,
    *, implementation: str | None = None,
):
    """Wait for one task to reach a terminal state and return its response."""
    implementation = resolveImplementation(machine=machine, implementation=implementation)
    if not task_id:
        raise ValueError("An IRI task ID is required")
    j = get(suburl=f"task/{task_id}", implementation=implementation)

    while j["status"] in {"active", "pending"}:
        time.sleep(poll_freq)
        j = get(suburl=f"task/{task_id}", implementation=implementation)
    return j


def submitTask(
    machine: str | None = None,
    action: Callable[[], str] | None = None,
    poll_freq: int = 4,
    *, implementation: str | None = None,
):
    """Submit an asynchronous action and retry unsuccessful attempts.

    ``action`` must submit the request and return the IRI task ID. A failed
    IRI task cannot be restarted, so retrying resubmits the original action.
    Both submission/polling exceptions and all non-completed terminal task
    states are retried.
    """
    implementation = resolveImplementation(machine=machine, implementation=implementation)
    if not callable(action):
        raise ValueError("A task submission action is required")
    retry_errors = []
    for attempt in range(IRI_TASK_MAX_ATTEMPTS):
        try:
            task_id = action()
            j = waitTask(task_id=task_id, poll_freq=poll_freq, implementation=implementation)
        except Exception as exc:
            retry_errors.append(
                f"attempt {attempt + 1}, submission/polling error: {exc}"
            )
            last_error = exc
        else:
            if j["status"] == "completed":
                return j
            retry_errors.append(
                f"attempt {attempt + 1}, task {task_id}: "
                f"{json.dumps(j, indent=2)}"
            )
            last_error = None

        if attempt == IRI_TASK_MAX_ATTEMPTS - 1:
            error = RuntimeError(
                "Task failed after "
                f"{IRI_TASK_MAX_ATTEMPTS} submission attempts: "
                + "; ".join(retry_errors)
            )
            if last_error is not None:
                raise error from last_error
            raise error

        delay = IRI_TASK_INITIAL_RETRY_DELAY_SECONDS * (2 ** attempt)
        wfapiLog(
            f"Unsuccessful task submission for {implementation}; "
            f"resubmitting in {delay} seconds"
        )
        time.sleep(delay)


