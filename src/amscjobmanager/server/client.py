"""Generic MCP requests with ordered, JSON-serializable result envelopes."""

import asyncio
import math
from typing import Any

import httpx2
from mcp import Client
from mcp.client.streamable_http import streamable_http_client
from mcp.types import Tool
from pydantic import BaseModel, ConfigDict, Field


class ToolRequest(BaseModel):
    """Validated input record for one tool call; unknown fields are rejected.

    Fields:
        tool (str): Nonempty MCP tool name, for example 'list_workflows'.
        arguments (dict[str, Any]): JSON object passed unchanged as tool
            arguments, defaulting to {}. The server validates its contents.
    """

    model_config = ConfigDict(extra="forbid", strict=True)
    tool: str = Field(min_length=1)
    arguments: dict[str, Any] = Field(default_factory=dict)


def _exception_message(error: Exception) -> str:
    """Return leaf error types/reasons from a transport exception or nested group.

    Args:
        error (Exception): Connection/call failure, possibly a nested
            ExceptionGroup containing several underlying failures.

    Returns:
        str: Unique leaf messages formatted as 'ExceptionType: reason', joined
            by '; ' in traversal order. No traceback is included.

    Async transport task groups
    can wrap the useful network cause several levels deep. Flatten those
    groups and omit duplicate messages so the returned string explains the
    failure without exposing a traceback or only reporting a TaskGroup error.
    """
    pending = [error]
    messages = []
    while pending:
        current = pending.pop(0)
        if isinstance(current, BaseExceptionGroup):
            pending.extend(current.exceptions)
        else:
            message = f'{type(current).__name__}: {current}'
            if message not in messages:
                messages.append(message)
    return '; '.join(messages)


async def _list_tools(client: Client) -> list[Tool]:
    """Return all discovered SDK Tool models from an already-connected client.

    client (mcp.Client) owns the connection and timeouts. Follow zero or more
    next_cursor tokens until all pages are read; propagate discovery errors.
    Return list[Tool] in server order without closing or replacing the client.
    """
    tools = []
    cursor = None
    while True:
        page = await client.list_tools(cursor=cursor)
        tools.extend(page.tools)
        cursor = page.next_cursor
        if cursor is None:
            return tools


async def _execute_on_client(
    client: Client, requests: list[ToolRequest], *, list_tools=False,
    continue_on_error=False, timeout=30.0,
) -> dict:
    """Execute typed operations on an existing connection and return an envelope.

    Args:
        client (mcp.Client): Connected SDK client; caller owns its lifetime.
        requests (list[ToolRequest]): Nonempty ordered, validated operations.
        list_tools (bool): Discover schemas instead of making tool calls.
        continue_on_error (bool): Continue after tool errors, never transport errors.
        timeout (float): Positive per-operation limit in seconds.

    Returns:
        dict: execute_requests' success/results envelope; indices are zero-based
            positions in requests. Preserve earlier results and skip remaining
            calls after a transport failure. No retries or new connections.
    """
    results = []
    stopped = None
    for index, request in enumerate(requests):
        entry = {"index": index, "tool": request.tool}
        if stopped is not None:
            results.append({**entry, "status": "skipped", "reason": stopped})
            continue
        try:
            async with asyncio.timeout(timeout):
                if list_tools:
                    tools = await _list_tools(client)
                    tools = [tool.model_dump(mode="json", by_alias=True) for tool in tools]
                    results.append({**entry, "status": "succeeded", "result": tools})
                    continue
                response = await client.call_tool(request.tool, request.arguments)
            if response.is_error:
                message = "\n".join(block.text for block in response.content if block.type == "text")
                error = {"code": "tool_error", "message": message}
                # Anticipated server errors carry stable codes separately
                # from the human-readable MCP content blocks.
                supplied = (response.structured_content or {}).get('error')
                if isinstance(supplied, dict) and isinstance(supplied.get('code'), str) and isinstance(supplied.get('message'), str):
                    error = supplied
                results.append({**entry, "status": "failed", "error": error})
                if not continue_on_error:
                    stopped = f"Stopped after operation {index} failed"
            else:
                result = response.structured_content
                # The SDK wraps non-object structured returns in result.
                if isinstance(result, dict) and set(result) == {"result"}:
                    result = result['result']
                if result is None:
                    result = [block.model_dump(mode="json", by_alias=True) for block in response.content]
                results.append({**entry, "status": "succeeded", "result": result})
        except Exception as exc:
            results.append({**entry, "status": "failed", "error": {
                "code": "request_error", "message": _exception_message(exc),
                "outcome_unknown": True,
            }})
            stopped = f"Connection or protocol failure during operation {index}; no further calls attempted"
    return {"success": all(entry['status'] == 'succeeded' for entry in results), "results": results}


async def execute_requests(
    url: str, requests: ToolRequest | list[ToolRequest] | None = None, *,
    list_tools=False, continue_on_error=False, timeout=30.0,
) -> dict:
    """Execute validated ToolRequest objects over one MCP connection.

    Args:
        url (str): Streamable HTTP endpoint, including its path, for example
            'http://127.0.0.1:8765/mcp'.
        requests (ToolRequest | list[ToolRequest] | None): One validated request
            or a nonempty ordered list. Raw dictionaries are not accepted.
            Parse JSON into these models before calling, validating the whole
            batch first. None is valid only with list_tools=True. Individual
            tool arguments are subsequently validated by the server.
        list_tools (bool): Discover tool descriptions/schemas instead of
            executing requests, which are ignored. Defaults to False.
        continue_on_error (bool): Continue after tool-reported failures if True;
            otherwise skip later operations. Defaults to False. Connection or
            protocol failures always stop the sequence, regardless of this flag.
        timeout (float | int): Finite positive limit in seconds for HTTP
            connection/read operations and each tool call. Defaults to 30.0.

    Returns:
        dict: JSON-serializable envelope with success (bool) and results
        (list[dict]). success is True only if every operation succeeded.
        Each results entry contains index (int, zero-based input position),
        tool (str), and status (str):
            'succeeded': result contains the tool's JSON-compatible value.
                SDK {'result': value} wrappers are unwrapped. Tools without
                structured output return a list of content-block dictionaries.
            'failed': error is {'code': str, 'message': str}, optionally with
                outcome_unknown (bool, True for an interrupted request).
            'skipped': reason (str) explains why no call was attempted.
        Discovery produces one entry at index 0, tool='list_tools', whose
        result is a list of SDK tool-description dictionaries with JSON schemas.
        Validation/setup errors add a top-level error with code/message and
        results=[]; connection teardown errors can retain earlier results.
        A single request still produces a one-element results array. Workflow
        IDs inside results are database keys, unrelated to operation indices.

    No operation is retried. Earlier operations remain effective if a later
    one fails: the sequence is not a transaction.
    """
    # Check the typed calling contract without reparsing caller-validated models.
    try:
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("timeout must be a finite positive number")
        if list_tools:
            requests = [ToolRequest(tool="list_tools")]
        else:
            if isinstance(requests, ToolRequest):
                requests = [requests]
            if not isinstance(requests, list) or any(not isinstance(request, ToolRequest) for request in requests):
                raise TypeError("requests must be a ToolRequest or a list of ToolRequest objects")
            if not requests:
                raise ValueError("Request array must not be empty")
    except (ValueError, TypeError) as exc:
        return {"success": False, "results": [],
                "error": {"code": "invalid_input", "message": str(exc)}}

    response = {"success": False, "results": []}
    try:
        async with httpx2.AsyncClient(timeout=timeout, trust_env=False) as http_client, Client(
            streamable_http_client(url, http_client=http_client),
            read_timeout_seconds=timeout, cache=None,
        ) as client:
            response = await _execute_on_client(
                client, requests, list_tools=list_tools,
                continue_on_error=continue_on_error, timeout=timeout,
            )
    except Exception as exc:
        return {"success": False, "results": response['results'], "error": {
            "code": "connection_error", "message": _exception_message(exc),
        }}
    return response


async def execute_command(url: str, tool_name: str | None, arguments: list[str], *, timeout=30.0) -> dict:
    """Discover, parse, and execute a traditional CLI command on one connection.

    Args:
        url (str): MCP Streamable HTTP endpoint, including /mcp.
        tool_name (str | None): Exact advertised tool name, e.g. 'list_workflows'.
            None requests a list of command names and brief server descriptions.
        arguments (list[str]): Tool-specific CLI tokens, e.g. ['--refresh'].
            Pass global connection options separately through url/timeout.
        timeout (float | int): Finite positive HTTP and operation limit in seconds.

    Returns:
        dict: execute_requests' JSON envelope for one operation at index zero.
            Unknown tools or invalid options return invalid_input with no calls.
            Tool help returns success=True, results=[], and help (str); the
            frontend prints that text instead of a JSON envelope. Discovery and
            execution share one connection; no call occurs on a parsing error.
    """
    from .cli import parse_tool_arguments

    if not math.isfinite(timeout) or timeout <= 0:
        return {'success': False, 'results': [], 'error': {
            'code': 'invalid_input', 'message': 'timeout must be a finite positive number',
        }}
    response = {'success': False, 'results': []}
    try:
        async with httpx2.AsyncClient(timeout=timeout, trust_env=False) as http_client, Client(
            streamable_http_client(url, http_client=http_client),
            read_timeout_seconds=timeout, cache=None,
        ) as client:
            async with asyncio.timeout(timeout):
                tools = await _list_tools(client)
            if tool_name is None:
                lines = ['Commands (from server):']
                width = max((len(tool.name) for tool in tools), default=0)
                for tool in tools:
                    brief = (tool.description or tool.title or 'No description provided.').splitlines()[0]
                    lines.append(f'  {tool.name:<{width}}  {brief}')
                if not tools:
                    lines.append('  No commands are advertised by this server.')
                return {'success': True, 'results': [], 'help': '\n'.join(lines) + '\n'}
            tool = next((tool for tool in tools if tool.name == tool_name), None)
            if tool is None:
                return {'success': False, 'results': [], 'error': {
                    'code': 'invalid_input', 'message': f'Unknown tool {tool_name!r}; use --list-tools to discover available commands',
                }}
            try:
                request = parse_tool_arguments(tool, arguments)
            except ValueError as exc:
                return {'success': False, 'results': [], 'error': {'code': 'invalid_input', 'message': str(exc)}}
            if isinstance(request, str):
                return {'success': True, 'results': [], 'help': request}
            response = await _execute_on_client(client, [request], timeout=timeout)
    except Exception as exc:
        return {'success': False, 'results': response['results'], 'error': {
            'code': 'connection_error', 'message': _exception_message(exc),
        }}
    return response
