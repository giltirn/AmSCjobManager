"""Optional MCP imports and request/response diagnostics shared by server tests."""

import json
from pathlib import Path
import shlex
import subprocess
import sys

from amscjobmanager.logging import wfmanLog
from support import REPOSITORY_ROOT

try:
    from mcp import Client
    from mcp.server import MCPServer
    import uvicorn
except ImportError as exc:
    Client = None
    wfmanLog(
        '\nMCP SERVER TESTS CANNOT RUN: optional server dependencies are missing or incompatible.\n'
        f'Import error: {exc}\n'
        f'Python interpreter: {sys.executable}\n'
        'Install the server extra (official MCP SDK >=2.3,<3 and its dependencies):\n  '
        + shlex.join([sys.executable, '-m', 'pip', 'install', '-e',
                      str(REPOSITORY_ROOT) + '[server]'])
        + '\nThen rerun: python test/run_tests.py integration --pattern test_mcp_server.py\n',
        file=sys.stderr, flush=True,
    )


async def call_mcp(client, tool: str | None = None, arguments: dict | None = None):
    """Log an actual SDK request/response, returning the original result model.

    Args:
        client (mcp.Client): Connected in-process MCP client used by the test.
        tool (str | None): Tool name for tools/call; None sends tools/list.
        arguments (dict | None): Tool parameters; None means an empty object.

    Returns:
        CallToolResult | ListToolsResult: Unmodified SDK response, also logged
            in full as JSON. Exceptions are logged and re-raised for assertions.
    """
    request = {'method': 'tools/list'} if tool is None else {
        'method': 'tools/call', 'params': {'name': tool, 'arguments': arguments or {}},
    }
    wfmanLog('\nMCP REQUEST (in-process):\n' + json.dumps(request, indent=2), file=sys.stderr, flush=True)
    try:
        response = await client.list_tools() if tool is None else await client.call_tool(tool, arguments or {})
    except Exception as exc:
        wfmanLog(f'MCP REQUEST EXCEPTION: {type(exc).__name__}: {exc}', file=sys.stderr, flush=True)
        raise
    wfmanLog('MCP RESPONSE:\n' + response.model_dump_json(indent=2, by_alias=True), file=sys.stderr, flush=True)
    return response


def run_client(command: list[str], **options) -> subprocess.CompletedProcess:
    """Run a CLI subprocess and log its command, request, exit code, and output.

    Args:
        command (list[str]): Full argument vector, beginning with the Python
            executable and client script. Printed using shell-safe quoting.
        **options: subprocess.run keyword arguments, forwarded unchanged. Tests
            use capture_output/text (bool), timeout (seconds), and optional
            input (str) for stdin. A --file request is read for display only.

    Returns:
        subprocess.CompletedProcess: Original captured process result. JSON
            stdout is pretty-printed separately from stderr; timeout or launch
            errors are logged and re-raised. Test diagnostics use stderr and
            do not alter the child's stdout or the assertions against it.
    """
    wfmanLog('\nCLI COMMAND:\n' + shlex.join(command), file=sys.stderr, flush=True)
    if '--request' in command:
        payload = command[command.index('--request') + 1]
    elif '--file' in command:
        source = command[command.index('--file') + 1]
        payload = options.get('input', '') if source == '-' else Path(source).read_text()
    elif '--list-tools' in command:
        payload = '(tool discovery: tools/list)'
    else:
        payload = '(traditional tool command; options are parsed using the server schema)'
    wfmanLog('CLI INPUT (sent if validation succeeds):\n' + payload, file=sys.stderr, flush=True)
    try:
        completed = subprocess.run(command, **options)
    except Exception as exc:
        wfmanLog(f'CLI EXCEPTION: {type(exc).__name__}: {exc}', file=sys.stderr, flush=True)
        raise
    wfmanLog(f'CLI EXIT CODE: {completed.returncode}', file=sys.stderr, flush=True)
    try:
        response = json.dumps(json.loads(completed.stdout), indent=2)
    except (ValueError, TypeError):
        response = completed.stdout or '(empty stdout)'
    wfmanLog('CLI RESPONSE:\n' + response, file=sys.stderr, flush=True)
    if completed.stderr:
        wfmanLog('CLI STDERR:\n' + completed.stderr, file=sys.stderr, flush=True)
    return completed


