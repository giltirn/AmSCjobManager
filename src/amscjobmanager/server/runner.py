"""HTTP listener with an administrator-visible readiness announcement."""

import os
from pathlib import Path
import socket
import sys

import uvicorn

from ..logging import wfmanLog
from ..manager import JobManager
from .app import createServer


class ManagerHTTPServer(uvicorn.Server):
    """Serve MCP on loopback and announce readiness only after socket startup."""

    def __init__(self, manager: JobManager, *, port: int = 8765, log_path: str | Path):
        """Construct the listener without starting it or the manager.

        Args:
            manager (JobManager): Existing manager; caller starts/stops it.
            port (int): Loopback TCP port, normally 1..65535; 0 lets the OS pick
                a port for embedded callers. Defaults to 8765.
            log_path (str | Path): Manager/API log filename shown to the admin.
                Logging hooks must already be configured by the caller.

        Returns:
            None: Initializes the server. endpoint (str) is its MCP URL and is
            updated to the actual bound port after startup. Call inherited run()
            to serve. Routine access logs remain suppressed.
        """
        self.endpoint = f'http://127.0.0.1:{port}/mcp'
        self.log_path = Path(log_path).resolve()
        app = createServer(manager).streamable_http_app(json_response=True)
        super().__init__(uvicorn.Config(app, host='127.0.0.1', port=port, log_level='warning'))

    async def startup(self, sockets: list[socket.socket] | None = None) -> None:
        """Start HTTP sockets and print a readiness notice on successful startup.

        Args:
            sockets (list[socket.socket] | None): Optional already-bound sockets
                supplied by an embedding caller. None binds the configured port.

        Returns:
            None when awaited: If Uvicorn reports started=True, update endpoint
            from the bound socket and emit its URL, PID, and log path through
            wfmanLog to both its configured sink and stdout. Binding/lifespan
            failures propagate without printing a successful-start message.
        """
        await super().startup(sockets=sockets)
        if self.started:
            port = self.servers[0].sockets[0].getsockname()[1]
            self.endpoint = f'http://127.0.0.1:{port}/mcp'
            message = f'Server started: {self.endpoint} (PID {os.getpid()}, log: {self.log_path})'
            wfmanLog(message)
            wfmanLog(message, file=sys.stdout, flush=True)

    async def shutdown(self, sockets: list[socket.socket] | None = None) -> None:
        """Announce graceful termination before Uvicorn re-raises captured signals.

        Args:
            sockets (list[socket.socket] | None): Optional externally supplied
                listening sockets, forwarded to Uvicorn for shutdown.

        Returns:
            None when awaited: Emit the endpoint through wfmanLog to its sink
            and stdout if startup succeeded, then await normal HTTP shutdown.
            This handles Ctrl-C and SIGTERM; SIGKILL cannot run Python cleanup.
        """
        if self.started:
            message = f'Server terminating: {self.endpoint}'
            wfmanLog(message)
            wfmanLog(message, file=sys.stdout, flush=True)
        await super().shutdown(sockets=sockets)
