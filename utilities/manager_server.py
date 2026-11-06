"""Run a read-only MCP listener backed by a persistent JobManager."""

import argparse
from functools import partial
from pathlib import Path


def main(argv=None) -> int:
    """Load CLI config/database settings, serve on loopback, and stop cleanly.

    Args:
        argv (list[str] | None): Command-line tokens excluding the executable
            name, or None to use sys.argv[1:]. The positional config argument
            is a manager JSON filename (str). --database is a required SQLite
            filename (str). --port is an int in 1..65535 (default 8765).
            --poll-interval is a finite positive float in seconds (default 20).
            --log-file is an optional filename (str) for append-only manager/API
            logs; its default is the database filename with '.log' appended.
            Log and database files must be different paths.

    Returns:
        int: 0 after normal listener shutdown, including Ctrl-C. Invalid CLI arguments raise
            argparse's SystemExit; configuration, file, or startup failures
            propagate rather than being returned as an exit-code integer.

    The server owns its manager and closes the database when the listener
    exits; shutdown does not cancel remote jobs. Restore the original logging
    hooks before closing the log file. Only one server should own this database.
    Readiness and termination notices are also written to stdout for the admin;
    routine request logging stays in the configured log destinations.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('config', help='Manager configuration JSON')
    parser.add_argument('--database', required=True, help='Persistent workflow SQLite database')
    parser.add_argument('--port', type=int, default=8765)
    parser.add_argument('--poll-interval', type=float, default=20)
    parser.add_argument('--log-file', help='Append workflow/API logs here (default: DATABASE.log)')
    args = parser.parse_args(argv)
    if not 1 <= args.port <= 65535:
        parser.error('--port must be between 1 and 65535')
    if not 0 < args.poll_interval < float('inf'):
        parser.error('--poll-interval must be finite and positive')

    log_path = Path(args.log_file or (args.database + '.log'))
    if log_path.resolve() == Path(args.database).resolve():
        parser.error('--log-file must differ from --database')

    from amscjobmanager import logging as manager_logging

    # Install the library's logging hooks before configuration or background
    # activity. Leave stdout and the separate user-query hook untouched.
    with log_path.open('a', buffering=1) as log:
        previous_hooks = (manager_logging.wfman_log_func, manager_logging.api_log_func)
        manager_logging.wfman_log_func = partial(print, file=log, flush=True)
        manager_logging.api_log_func = partial(print, file=log, flush=True)
        try:
            manager_logging.wfmanLog('Starting AmSC Job Manager MCP server')
            from amscjobmanager.manager_config import readManagerConfigFile, setupManager
            from amscjobmanager.manager import JobManager
            from amscjobmanager.server.runner import ManagerHTTPServer

            setupManager(readManagerConfigFile(args.config))
            manager = JobManager(args.database, poll_freq=args.poll_interval)
            try:
                manager.start()
                server = ManagerHTTPServer(manager, port=args.port, log_path=log_path)
                server.run()
            except KeyboardInterrupt:
                # Uvicorn/asyncio may re-raise Ctrl-C after graceful HTTP
                # shutdown. It is an admin stop request, not a server failure.
                pass
            finally:
                manager.stop(wait_until_done=False, close_database=True)
                manager_logging.wfmanLog('Stopped AmSC Job Manager MCP server')
        finally:
            manager_logging.wfman_log_func, manager_logging.api_log_func = previous_hooks
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
