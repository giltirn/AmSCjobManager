"""Discover and execute password-protected administration through a local Unix socket."""

import argparse
import getpass
import sys
import warnings

from amscjobmanager.server.admin import AdminCallRequest, AdminDiscoveryRequest, execute_admin_request
from amscjobmanager.server.cli import format_function_list, parse_function_arguments


def build_parser(*, command_selection=False) -> argparse.ArgumentParser:
    """Build one stage of the connection/command parser.

    command_selection (bool, default False) selects parsing command/global
    options after the socket; False parses the socket and preserves later tokens.
    Return argparse.ArgumentParser. socket (str) is required except for offline -h/--help. command (str | None)
    selects a discovered operation; None lists commands. --timeout is seconds,
    default 120. Command-specific inputs/help come only from server schemas.
    """
    parser = argparse.ArgumentParser(description=__doc__, add_help=False,
        epilog='Use SOCKET -h for available commands, or SOCKET -h COMMAND for command help. Help requires a running initialized server but no password.')
    parser.add_argument('-h', '--help', action='store_true',
                        help='Show connection help; with SOCKET, list commands or show help for COMMAND')
    if not command_selection:
        parser.add_argument('socket', help='Unix socket on the server host; relative paths use the working directory')
    parser.add_argument('--timeout', type=float, default=None if command_selection else 120, help='Socket operation timeout in seconds (default: 120); place before COMMAND')
    if command_selection:
        parser.add_argument('command', nargs='?', help='Server-advertised command name')
    parser.add_argument('arguments', nargs=argparse.REMAINDER, help='Command inputs; required values are positional, optional values use flags')
    return parser


def main(argv: list[str] | None = None) -> int:
    """Discover schemas, display help or parse inputs, then privately authorize one execution.

    argv (list[str] | None) supplies CLI tokens, or None uses sys.argv[1:].
    -h/--help alone shows offline connection help. SOCKET -h COMMAND and SOCKET
    COMMAND -h retrieve detailed help without prompting. Actual execution prompts
    for a password only after discovery and input validation. Return 0 for success
    or help, 1 on request/transport/authentication failure; argparse exits with 2
    for connection syntax errors. Success/help uses stdout; errors use stderr.
    No execution retries, saved passwords or exposed password input are supported.
    """
    tokens = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser()
    if tokens in (['-h'], ['--help']):
        parser.print_help()
        return 0
    args = parser.parse_args(tokens)
    # Parse connection flags after SOCKET before consuming command arguments.
    # Once COMMAND is reached, its flags belong exclusively to its schema.
    selection = build_parser(command_selection=True).parse_args(args.arguments)
    args.command, args.arguments = selection.command, selection.arguments
    args.help = args.help or selection.help
    if selection.timeout is not None:
        args.timeout = selection.timeout
    try:
        # Metadata discovery precedes password input and contains no credentials.
        discovery = execute_admin_request(args.socket, AdminDiscoveryRequest(), timeout=args.timeout)
        if not discovery.success:
            print(discovery.message, file=sys.stderr)
            return 1
        if args.command is None:
            print(format_function_list(discovery.functions or []), end='')
            return 0
        function = next((item for item in discovery.functions or [] if item.name == args.command), None)
        if function is None:
            raise ValueError('Unknown command; use SOCKET -h to discover available commands.')
        arguments = parse_function_arguments(
            function, args.arguments + (['--help'] if args.help else []),
            required_positionals=True, program='manager_admin.py', connection_arguments='SOCKET',
        )
        if isinstance(arguments, str):
            print(arguments, end='')
            return 0

        # The frontend owns password input; it never enters the server query queue.
        with warnings.catch_warnings():
            warnings.simplefilter('error', getpass.GetPassWarning)
            password = getpass.getpass('Admin password: ')
        request = AdminCallRequest(command=function.name, arguments=arguments, password=password)
        password = None
        response = execute_admin_request(args.socket, request, timeout=args.timeout)
    except (getpass.GetPassWarning, EOFError):
        print('A terminal with hidden password input is required.', file=sys.stderr)
        return 1
    except OSError:
        print('Admin connection failed or timed out. Check the socket and server; an execution request may already have taken effect.', file=sys.stderr)
        return 1
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print('Admin command interrupted; an already submitted operation may still complete.', file=sys.stderr)
        return 1
    print(response.message, file=sys.stdout if response.success else sys.stderr)
    return 0 if response.success else 1


if __name__ == '__main__':
    raise SystemExit(main())
