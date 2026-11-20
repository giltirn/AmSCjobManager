"""Run MCP workflow management and user-query routing for a persistent WorkflowManager."""

import argparse
import getpass
from functools import partial
from pathlib import Path
import warnings


def main(argv=None) -> int:
    """Load CLI config/database settings, serve on loopback, and stop cleanly.

    Args:
        argv (list[str] | None): Command-line tokens excluding the executable
            name, or None to use sys.argv[1:]. The positional config argument
            is a manager JSON filename (str). --database is a SQLite filename
            (str), default 'workflows.sqlite' in the current working directory.
            --port is an int in 1..65535 (default 8765).
            --poll-interval is a finite positive float in seconds (default 20).
            --log-file is an optional filename (str) for append-only manager/API
            logs; its default is the database filename with '.log' appended.
            Log and database files must be different paths.
            --admin-socket is an optional private Unix socket pathname. When
            enabled, --admin-password supplies its password (str), otherwise
            prompt without echo. Literal passwords can appear in process lists
            and shell history. Neither setting is part of manager configuration.

    Returns:
        int: 0 after normal listener shutdown, including Ctrl-C. Invalid CLI arguments raise
            argparse's SystemExit. Listener/file failures propagate. Manager
            initialization failures are logged and keep the listener available
            with manager_unavailable responses and query tools.

    The runtime initializes its manager after listener startup and closes its
    database when the listener exits; shutdown does not cancel remote workflows.
    Restore the original logging and query hooks before closing the log file. Only one server should own this database.
    Readiness and termination notices are also written to stdout for the admin;
    routine request logging stays in the configured log destinations.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('config', help='Manager configuration JSON')
    parser.add_argument('--database', default='workflows.sqlite',
                        help='Persistent workflow SQLite database (default: workflows.sqlite in the working directory)')
    parser.add_argument('--port', type=int, default=8765)
    parser.add_argument('--poll-interval', type=float, default=20)
    parser.add_argument('--log-file', help='Append workflow/API logs here (default: DATABASE.log)')
    parser.add_argument('--admin-socket',
                        help='Enable local administration at this Unix socket path (relative paths use the working directory)')
    parser.add_argument('--admin-password', help='Admin password (prefer omitting this option to prompt without echo)')
    args = parser.parse_args(argv)
    if not 1 <= args.port <= 65535:
        parser.error('--port must be between 1 and 65535')
    if not 0 < args.poll_interval < float('inf'):
        parser.error('--poll-interval must be finite and positive')
    if args.admin_password is not None and args.admin_socket is None:
        parser.error('--admin-password requires --admin-socket')

    # Password entry stays in the frontend, outside the MCP user-query hook.
    # Fail instead of getpass's exposed-input fallback when no terminal exists.
    password = args.admin_password
    args.admin_password = None
    if args.admin_socket is not None:
        if password is None:
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter('error', getpass.GetPassWarning)
                    password = getpass.getpass('Set admin password: ')
            except (getpass.GetPassWarning, EOFError, OSError):
                parser.error('A terminal with hidden password input is required')
        if not 1 <= len(password.encode('utf-8')) <= 4096:
            parser.error('Admin password must contain 1..4096 UTF-8 bytes')

    log_path = Path(args.log_file or (args.database + '.log'))
    if log_path.resolve() == Path(args.database).resolve():
        parser.error('--log-file must differ from --database')

    from amscjobmanager import logging as manager_logging

    # Install the library's logging hooks before configuration or background
    # activity. Query routing is selected explicitly below for server mode;
    # stdout remains available for administrator lifecycle notices.
    with log_path.open('a', buffering=1) as log:
        previous_hooks = (manager_logging.wfman_log_func, manager_logging.api_log_func,
                          manager_logging.api_query_user_func)
        manager_logging.wfman_log_func = partial(print, file=log, flush=True)
        manager_logging.api_log_func = partial(print, file=log, flush=True)
        try:
            manager_logging.wfmanLog('Starting AmSC Job Manager MCP server')
            from amscjobmanager.manager_config import readManagerConfigFile, setupManager
            from amscjobmanager.manager import WorkflowManager
            from amscjobmanager.server.runner import ManagerHTTPServer

            def initialize_manager() -> WorkflowManager:
                """Configure the API and construct a manager on the startup worker.

                Take no arguments; use the parsed config/database/poll settings.
                Return WorkflowManager. Configuration and constructor recovery can
                ask questions while the already-running listener serves replies.
                """
                setupManager(readManagerConfigFile(args.config))
                return WorkflowManager(args.database, poll_freq=args.poll_interval)

            server = ManagerHTTPServer(initialize_manager, port=args.port, log_path=log_path,
                                       admin_socket=args.admin_socket, admin_password=password)
            password = None
            manager_logging.api_query_user_func = server.runtime.queries.ask
            try:
                server.run()
            except KeyboardInterrupt:
                # Uvicorn/asyncio may re-raise Ctrl-C after graceful HTTP
                # shutdown. It is an admin stop request, not a server failure.
                pass
            finally:
                server.runtime.stop()
                manager_logging.wfmanLog('Stopped AmSC Job Manager MCP server')
        finally:
            (manager_logging.wfman_log_func, manager_logging.api_log_func,
             manager_logging.api_query_user_func) = previous_hooks
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
