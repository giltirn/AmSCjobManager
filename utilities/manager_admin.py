"""Change a running manager's sandbox roots through its private local admin socket."""

import argparse
import getpass
import sys
import warnings

from amscjobmanager.server.admin import execute_admin_command


def build_parser() -> argparse.ArgumentParser:
    """Take no arguments; return the parser for one password-protected admin command.

    socket (str) selects the server's Unix socket. command is add_sandbox or
    remove_sandbox; machine/path identify a target and exact registered root.
    --timeout is the positive socket timeout in seconds, default 120.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('socket',
                        help='Unix socket on the server host (relative paths use the working directory)')
    parser.add_argument('--timeout', type=float, default=120, help='Socket operation timeout in seconds (default: 120)')
    parser.add_argument('command', choices=('add_sandbox', 'remove_sandbox'))
    parser.add_argument('machine', help='Configured machine name, or local')
    parser.add_argument('path', help='Sandbox root; removal matches the registered path text exactly')
    return parser


def main(argv: list[str] | None = None) -> int:
    """Prompt privately, send one local admin command, and report its outcome.

    argv (list[str] | None) supplies CLI tokens, or None uses sys.argv[1:].
    Required positionals are socket, command, machine, and path, in that order;
    --timeout is optional and measured in seconds.
    Return exit code 0 on success, 1 on authentication/transport/operation error;
    argparse syntax errors exit with code 2. Print successes to stdout and
    errors to stderr. Never accept a password argument, environment variable,
    file, or exposed stdin fallback. No credential or login is saved.
    """
    args = build_parser().parse_args(argv)
    try:
        # The frontend owns password input; it must never enter MCP's query queue.
        with warnings.catch_warnings():
            warnings.simplefilter('error', getpass.GetPassWarning)
            password = getpass.getpass('Admin password: ')
        response = execute_admin_command(args.socket, args.command, args.machine, args.path,
                                         password, timeout=args.timeout)
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
