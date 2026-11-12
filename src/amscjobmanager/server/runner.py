"""MCP HTTP listener construction, lifecycle and readiness announcements."""

import os
from pathlib import Path
import socket
import sys
from typing import Callable

import uvicorn
from mcp.server import MCPServer

from ..logging import wfmanLog
from ..manager import JobManager
from .runtime import ServerRuntime
from .tools import registerTools


class ManagerHTTPServer(uvicorn.Server):
    """Serve MCP on loopback and announce readiness only after socket startup."""

    def __init__(self, initialize_manager: Callable[[], JobManager], *, port: int = 8765, log_path: str | Path):
        """Construct the listener without starting it or the manager.

        Args:
            initialize_manager (Callable[[], JobManager]): No-argument factory
                that configures the API and returns a new manager. Called on a
                worker after HTTP readiness; it can issue user queries. The
                caller configures logging and query hooks before running.
            port (int): Loopback TCP port, normally 1..65535; 0 lets the OS pick
                a port for embedded callers. Defaults to 8765.
            log_path (str | Path): Manager/API log filename shown to the admin.
                Logging hooks must already be configured by the caller.

        Returns:
            None: Initializes the server. endpoint (str) is its MCP URL and is
            updated to the actual bound port after startup. runtime
            (ServerRuntime) owns initialization, the manager and query queue;
            mcp_server (MCPServer) exposes tools for in-process SDK clients.
            Call inherited run() to serve. Routine access logs stay suppressed.
        """
        self.runtime = ServerRuntime(initialize_manager)
        self.endpoint = f'http://127.0.0.1:{port}/mcp'
        self.log_path = Path(log_path).resolve()
        # Keep routine HTTP/access logs out of machine-readable client output.
        self.mcp_server = MCPServer("AmSC Job Manager", log_level="WARNING")
        registerTools(self.mcp_server, self.runtime)
        app = self.mcp_server.streamable_http_app(json_response=True)
        super().__init__(uvicorn.Config(app, host='127.0.0.1', port=port, log_level='warning'))

    async def startup(self, sockets: list[socket.socket] | None = None) -> None:
        """Start HTTP sockets and the runtime, then announce listener readiness.

        Args:
            sockets (list[socket.socket] | None): Optional already-bound sockets
                supplied by an embedding caller. None binds the configured port.

        Returns:
            None when awaited: If Uvicorn reports started=True, start runtime
            initialization, update endpoint from the bound socket and emit its
            URL, PID, and log path through
            wfmanLog to both its configured sink and stdout. Binding/lifespan
            failures propagate without printing a successful-start message.
        """
        await super().startup(sockets=sockets)
        if self.started:
            self.runtime.start()
            port = self.servers[0].sockets[0].getsockname()[1]
            self.endpoint = f'http://127.0.0.1:{port}/mcp'
            message = f'Server started: {self.endpoint} (PID {os.getpid()}, log: {self.log_path})'
            wfmanLog(message)
            wfmanLog(message, file=sys.stdout, flush=True)

    async def shutdown(self, sockets: list[socket.socket] | None = None) -> None:
        """Stop the runtime and announce graceful listener termination.

        Args:
            sockets (list[socket.socket] | None): Optional externally supplied
                listening sockets, forwarded to Uvicorn for shutdown.

        Returns:
            None when awaited: Stop the runtime without canceling remote jobs.
            Emit the endpoint through wfmanLog to its sink
            and stdout if startup succeeded, then await normal HTTP shutdown.
            This handles Ctrl-C and SIGTERM; SIGKILL cannot run Python cleanup.
        """
        self.runtime.stop()
        if self.started:
            message = f'Server terminating: {self.endpoint}'
            wfmanLog(message)
            wfmanLog(message, file=sys.stdout, flush=True)
        await super().shutdown(sockets=sockets)
