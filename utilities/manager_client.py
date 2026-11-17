"""Call an MCP tool using command-line options or JSON, returning JSON results."""

import argparse
import asyncio
from functools import partial
import json
from pathlib import Path
import sys
from typing import Annotated


def main(argv=None) -> int:
    """Run a traditional tool command, JSON request/batch, or tool discovery.

    Args:
        argv (list[str] | None): CLI tokens excluding the executable name,
            or None to use sys.argv[1:]. Supply a tool name followed by its
            schema-derived options, or exactly one input option:
            --request supplies JSON text (str), --file supplies a JSON filename
            (str, '-' means stdin), or --list-tools is a discovery flag (bool).
            JSON must be a ToolRequest object or nonempty array, parsed and
            validated by Pydantic before calling execute_requests with typed
            models. --url is the endpoint string
            (default 'http://127.0.0.1:8765/mcp'). --continue-on-error is a bool
            flag, default False. --timeout is a finite positive float in seconds,
            default 30, used for HTTP connection/read operations and tool calls.
            Global options precede a tool name; subsequent tokens are passed
            to that tool's parser. -h/--help discovers command names and brief
            descriptions from the server. -h TOOL and TOOL -h show that tool's
            description, inputs and outputs from server metadata. Help requires
            a reachable server and makes no tool calls.

    Returns:
        int: Process exit code, 0 only if every requested operation succeeds,
            otherwise 1. Print one JSON envelope (success: bool, results:
            list[dict], optional error: dict) to stdout, using execute_requests'
            schema. Operation indices start at zero; they are not workflow IDs.
            Tool help prints text and returns 0 without executing the tool.
            Malformed JSON, file errors, and missing optional dependencies also
            produce failure envelopes. argparse usage errors raise SystemExit.

    Diagnostic log hooks use stderr; their previous callbacks are restored
    after the request sequence. stdout is reserved for the JSON document.
    """
    parser = argparse.ArgumentParser(
        description=__doc__, allow_abbrev=False, add_help=False,
        epilog='Example: manager_client.py --url http://127.0.0.1:8765/mcp list_workflows --statuses FAILED COMPLETED',
    )
    source = parser.add_mutually_exclusive_group()
    parser.add_argument('-h', '--help', action='store_true',
                        help='Show the main help page; -h COMMAND or --help COMMAND shows help for that command')
    source.add_argument('--request', help='JSON tool request object or array')
    source.add_argument('--file', help='JSON request file, or - for stdin')
    source.add_argument('--list-tools', action='store_true', help='Return tool descriptions and schemas')
    parser.add_argument('--url', default='http://127.0.0.1:8765/mcp',
                        help='MCP server endpoint URL, including /mcp (default: %(default)s); place before the command name')
    parser.add_argument('--continue-on-error', action='store_true',
                        help='Continue a JSON batch after a tool error (default: stop); connection/protocol failures still stop. '
                             'Earlier operations remain effective; any failure gives a nonzero exit status')
    parser.add_argument('--timeout', type=float, default=30, help='Connection/call timeout in seconds')
    parser.add_argument('command', nargs='?', help='MCP tool name; use --list-tools to discover commands')
    parser.add_argument('arguments', nargs=argparse.REMAINDER, help='Tool options; TOOL --help shows schema-derived help')
    args = parser.parse_args(argv)
    json_mode = args.request is not None or args.file is not None or args.list_tools
    if args.command and json_mode:
        parser.error('A tool command cannot be combined with --request, --file, or --list-tools')
    if args.help and json_mode:
        parser.error('Help cannot be combined with --request, --file, or --list-tools')
    if not args.command and not json_mode and not args.help:
        parser.error('Provide a tool command, --request, --file, or --list-tools')

    requests = None
    try:
        if not args.list_tools and not args.command and not args.help:
            text = args.request if args.request is not None else (
                sys.stdin.read() if args.file == '-' else Path(args.file).read_text()
            )
        from amscjobmanager import logging as manager_logging

        # Route diagnostics using the existing hooks; stdout remains available
        # for the result envelope without changing process-wide stream routing.
        previous_hooks = (manager_logging.wfman_log_func, manager_logging.api_log_func)
        manager_logging.wfman_log_func = partial(print, file=sys.stderr, flush=True)
        manager_logging.api_log_func = partial(print, file=sys.stderr, flush=True)
        try:
            from pydantic import Field, TypeAdapter
            from amscjobmanager.server.client import ToolRequest, execute_requests, execute_command
            if not args.list_tools and not args.command and not args.help:
                # Validate single requests and entire batches at the input
                # boundary; the execution layer receives only typed models.
                request_type = TypeAdapter(ToolRequest | Annotated[list[ToolRequest], Field(min_length=1)])
                requests = request_type.validate_python(json.loads(text))
                # json.loads accepts NaN/Infinity; reject these nonstandard
                # constants even when nested inside untyped tool arguments.
                json.dumps(request_type.dump_python(requests), allow_nan=False)
            if args.command or args.help:
                tokens = args.arguments + (['--help'] if args.help else [])
                response = asyncio.run(execute_command(args.url, args.command, tokens, timeout=args.timeout))
                if args.help and args.command is None and 'help' in response:
                    response['help'] = parser.format_help() + '\n' + response['help']
            else:
                response = asyncio.run(execute_requests(
                    args.url, requests, list_tools=args.list_tools,
                    continue_on_error=args.continue_on_error, timeout=args.timeout,
                ))
        finally:
            manager_logging.wfman_log_func, manager_logging.api_log_func = previous_hooks
    except (ValueError, OSError) as exc:
        response = {'success': False, 'results': [],
                    'error': {'code': 'invalid_input', 'message': str(exc)}}
    except ImportError as exc:
        response = {'success': False, 'results': [], 'error': {
            'code': 'dependency_error', 'message': f'{exc}; install amsc-job-manager[server]',
        }}
    if 'help' in response:
        print(response['help'], end='')
    else:
        print(json.dumps(response, indent=2, allow_nan=False))
    return 0 if response['success'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
