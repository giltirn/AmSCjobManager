"""Shared authenticated IRI REST and asynchronous-task helpers."""

import json
import time
from typing import Callable

import httpx

from ..logging import wfapiLog
from .config import IRI_IMPLEMENTATIONS
from .state import _get_compute_client

IRI_GET_RETRYABLE_STATUS_CODES = {502, 503, 504, 500}
IRI_GET_MAX_ATTEMPTS = 4
IRI_GET_INITIAL_RETRY_DELAY_SECONDS = 1
IRI_TASK_MAX_ATTEMPTS = 4
IRI_TASK_INITIAL_RETRY_DELAY_SECONDS = 1

def get(machine, suburl, params = None):
    machine = machine.lower()
    client, _ = _get_compute_client(machine)
    url = IRI_IMPLEMENTATIONS[client.implementation]["api_base"] + '/' + suburl
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
            f"Transient GET failure for {machine}:{suburl} ({retryable_error}); "
            f"retrying in {delay} seconds"
        )
        time.sleep(delay)


def put(machine, suburl, data = None, params=None):
    machine = machine.lower()
    client, _ = _get_compute_client(machine)
    headers={ "accept" : "application/json", "Authorization" : f"Bearer {client.access_token}" }

    resp = client.http_client.put(
        IRI_IMPLEMENTATIONS[client.implementation]["api_base"] + '/' + suburl,
        headers=headers,
        json=data,
        params=params,
        timeout=300,
    )
    return json.loads(resp.text), resp.status_code
        

def post(machine, suburl, data = None, params=None, files=None, data_is_json=True):
    machine = machine.lower()
    client, _ = _get_compute_client(machine)
    headers={ "accept" : "application/json", "Authorization" : f"Bearer {client.access_token}" }

    resp = client.http_client.post(
        IRI_IMPLEMENTATIONS[client.implementation]["api_base"] + '/' + suburl,
        headers=headers,
        json=data if data_is_json else None,
        data=data if not data_is_json else None,
        params=params,
        files=files,
        timeout=300,
    )
    return json.loads(resp.text), resp.status_code


def delete(machine, suburl, params = None):
    machine = machine.lower()
    client, _ = _get_compute_client(machine)
    resp = client.http_client.delete(
        IRI_IMPLEMENTATIONS[client.implementation]["api_base"] + '/' + suburl,
        headers={"accept": "*/*", "Authorization": f"Bearer {client.access_token}"},
        params=params,
        timeout=300,
    )
    return {} if resp.text == "" else resp.json(), resp.status_code

def waitTask(
    machine: str,
    task_id: str,
    poll_freq: int = 4,
):
    """Wait for one task to reach a terminal state and return its response."""
    machine = machine.lower()
    j = get(machine, f"task/{task_id}")

    while j["status"] in {"active", "pending"}:
        time.sleep(poll_freq)
        j = get(machine, f"task/{task_id}")
    return j


def submitTask(
    machine: str,
    action: Callable[[], str],
    poll_freq: int = 4,
):
    """Submit an asynchronous action and retry unsuccessful attempts.

    ``action`` must submit the request and return the IRI task ID. A failed
    IRI task cannot be restarted, so retrying resubmits the original action.
    Both submission/polling exceptions and all non-completed terminal task
    states are retried.
    """
    machine = machine.lower()
    retry_errors = []
    for attempt in range(IRI_TASK_MAX_ATTEMPTS):
        try:
            task_id = action()
            j = waitTask(machine, task_id, poll_freq)
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
            f"Unsuccessful task submission for {machine}; "
            f"resubmitting in {delay} seconds"
        )
        time.sleep(delay)


