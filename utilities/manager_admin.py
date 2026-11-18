"""Administer a running manager's sandbox roots and compute wallets through its private local admin socket."""

import argparse
import getpass
import sys
import warnings

from amscjobmanager.server.admin import execute_admin_command


def build_parser(*, help_command: str | None = None) -> argparse.ArgumentParser:
    """Build the main admin parser or select one command's parser for help.

    help_command (str | None, default None) selects a known command's parser;
    None returns the main parser. Unknown names produce an argparse usage error.
    Return argparse.ArgumentParser without parsing input or making a connection.
    socket (str) selects the server's Unix socket. Subcommands add/remove
    sandboxes, enable/disable wallets, or recharge a wallet. Each requires machine
    (str); sandbox commands require path (str), recharge requires node_hours
    (positive float).
    --timeout is the positive socket timeout in seconds, default 120.
    """
    # Define connection arguments and the main help page.
    parser = argparse.ArgumentParser(description=__doc__, add_help=False)
    parser.add_argument('-h', '--help', action='help',
                        help='Show the main help page; -h COMMAND or --help COMMAND shows help for that command')
    parser.add_argument('socket',
                        help='Unix socket on the server host (relative paths use the working directory)')
    parser.add_argument('--timeout', type=float, default=120, help='Socket operation timeout in seconds (default: 120)')

    # Share command descriptions and argument definitions across both help forms.
    descriptions = {
        'add_sandbox': 'Add a sandbox directory for this server session; validation occurs on first use.',
        'remove_sandbox': 'Remove an exact sandbox registration for this session without deleting files.',
        'enable_wallet': 'Enable persistent compute budgeting for a machine; a new wallet starts at zero.',
        'disable_wallet': 'Disable compute budgeting for new submissions while retaining existing reservations.',
        'recharge_wallet': 'Add node-hours to a persistent compute wallet without changing whether it is enabled.',
    }
    commands = parser.add_subparsers(dest='command', required=True, title='commands', metavar='COMMAND')
    selected_parser = None
    for command, description in descriptions.items():
        subparser = commands.add_parser(
            command, help=description, description=description, add_help=False,
            epilog=('Connection: supply the Unix socket path before the command. '
                    'Global --timeout SECONDS defaults to 120 and precedes the command. '
                    'Execution prompts privately for the admin password. '
                    'Outputs: success message on stdout and exit code 0; '
                    'operation errors on stderr and exit code 1; syntax errors exit with 2. '
                    f'Help: manager_admin.py -h {command} or manager_admin.py --help {command}. '
                    'Help requires no socket connection or password.'),
        )
        subparser.add_argument('-h', '--help', action='help', help="Show this command's help")
        if command == help_command:
            selected_parser = subparser
        subparser.add_argument('machine', help='Configured machine name, or local')
        if command in ('add_sandbox', 'remove_sandbox'):
            subparser.add_argument('path', help='Sandbox root; removal matches registered text exactly')
        elif command == 'recharge_wallet':
            subparser.add_argument('node_hours', type=float, help='Positive node-hour grant; additive, do not blindly retry')

    # Command help uses the same parser without requiring connection arguments.
    if help_command is not None:
        if selected_parser is None:
            parser.error(f"Unknown command {help_command!r}; choose from {', '.join(descriptions)}")
        return selected_parser
    return parser


def main(argv: list[str] | None = None) -> int:
    """Prompt privately, send one local admin command, and report its outcome.

    argv (list[str] | None) supplies CLI tokens, or None uses sys.argv[1:].
    Required positionals are socket, command, machine, followed by path for
    sandbox commands or node_hours for recharge;
    --timeout is optional and measured in seconds. -h/--help alone shows main
    help; -h COMMAND/--help COMMAND shows command help without a socket argument,
    password prompt, or connection. SOCKET COMMAND -h/--help also works.
    Return exit code 0 on success, 1 on authentication/transport/operation error;
    argparse syntax errors exit with code 2. Print successes to stdout and
    errors to stderr. Never accept a password argument, environment variable,
    file, or exposed stdin fallback. No credential or login is saved.
    """
    # Select command help before parsing the otherwise required socket argument.
    tokens = list(sys.argv[1:] if argv is None else argv)
    help_command = tokens[1] if len(tokens) > 1 and tokens[0] in ('-h', '--help') else None
    parser = build_parser(help_command=help_command)
    if help_command is not None:
        parser.print_help()
        return 0
    args = parser.parse_args(tokens)
    try:
        # The frontend owns password input; it must never enter MCP's query queue.
        with warnings.catch_warnings():
            warnings.simplefilter('error', getpass.GetPassWarning)
            password = getpass.getpass('Admin password: ')
        response = execute_admin_command(args.socket, args.command, args.machine, getattr(args, 'path', None),
                                         password, node_hours=getattr(args, 'node_hours', None), timeout=args.timeout)
        password = None
    except (getpass.GetPassWarning, EOFError):
        print('A terminal with hidden password input is required.', file=sys.stderr)
        return 1
    except OSError:
        print('Admin connection failed or timed out. Check the socket and server; the operation may already have taken effect.', file=sys.stderr)
        return 1
    except ValueError:
        # Pydantic errors can contain submitted input, so do not print them.
        print('Invalid admin request, response, socket permissions, or timeout.', file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print('Admin command interrupted; an already submitted operation may still complete.', file=sys.stderr)
        return 1
    print(response.message, file=sys.stdout if response.success else sys.stderr)
    return 0 if response.success else 1


if __name__ == '__main__':
    raise SystemExit(main())
