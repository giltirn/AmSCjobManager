"""MCP HTTP listener construction, lifecycle and readiness announcements."""

import os
from pathlib import Path
import socket
import sys
from typing import Callable

import uvicorn

from ..logging import wfmanLog
from ..manager import WorkflowManager
from .runtime import ServerRuntime
from .admin import AdminServer
from .tools import ManagerMCPServer


class ManagerHTTPServer(uvicorn.Server):
    """Serve MCP on loopback and announce readiness only after socket startup."""

    def __init__(self, initialize_manager: Callable[[], WorkflowManager], *, port: int = 8765, log_path: str | Path,
                 admin_socket: str | Path | None = None, admin_password: str | None = None):
        """Construct the listener without starting it or the manager.

        Args:
            initialize_manager (Callable[[], WorkflowManager]): No-argument factory
                that configures the API and returns a new manager. Called on a
                worker after HTTP readiness; it can issue user queries. The
                caller configures logging and query hooks before running.
            port (int): Loopback TCP port, normally 1..65535; 0 lets the OS pick
                a port for embedded callers. Defaults to 8765.
            log_path (str | Path): Manager/API log filename shown to the admin.
                Logging hooks must already be configured by the caller.
            admin_socket (str | Path | None): Optional private Unix socket path.
                None disables administration; no admin tools are exposed by MCP.
            admin_password (str | None): In-memory admin password required with
                admin_socket. Neither value is read from manager configuration.

        Returns:
            None: Initializes the server. endpoint (str) is its MCP URL and is
            updated to the actual bound port after startup. runtime
            (ServerRuntime) owns initialization, the manager and query queue;
            mcp_server (MCPServer) exposes tools for in-process SDK clients.
            Call inherited run() to serve. Routine access logs stay suppressed.
        """
        self.runtime = ServerRuntime(initialize_manager)
        if (admin_socket is None) != (admin_password is None):
            raise ValueError('Admin socket and password must be supplied together')
        self.admin_server = None if admin_socket is None else AdminServer(admin_socket, admin_password, self.runtime)
        self.endpoint = f'http://127.0.0.1:{port}/mcp'
        self.log_path = Path(log_path).resolve()
        # Keep routine HTTP/access logs out of machine-readable client output.
        self.mcp_server = ManagerMCPServer(self.runtime)
        app = self.mcp_server.streamable_http_app(json_response=True)
        super().__init__(uvicorn.Config(app, host='127.0.0.1', port=port, log_level='warning'))

    async def startup(self, sockets: list[socket.socket] | None = None) -> None:
        """Start HTTP sockets and the runtime, then announce listener readiness.

        Args:
            sockets (list[socket.socket] | None): Optional already-bound sockets
                supplied by an embedding caller. None binds the configured port.

        Returns:
            None when awaited: If Uvicorn reports started=True, start runtime
            initialization after any requested admin listener binds, update endpoint and emit its
            URL, PID, and log path through
            wfmanLog to both its configured sink and stdout. Binding/lifespan
            failures propagate without printing a successful-start message.
        """
        await super().startup(sockets=sockets)
        if self.started:
            try:
                if self.admin_server is not None:
                    await self.admin_server.start()
            except BaseException:
                # Do not leave a partially started HTTP service running when
                # its requested administration listener could not be created.
                if self.admin_server is not None:
                    await self.admin_server.stop()
                self.runtime.stop()
                await super().shutdown(sockets=sockets)
                raise
            self.runtime.start()
            port = self.servers[0].sockets[0].getsockname()[1]
            self.endpoint = f'http://127.0.0.1:{port}/mcp'
            message = f'Server started: {self.endpoint} (PID {os.getpid()}, log: {self.log_path})'
            if self.admin_server is not None:
                message += f' (admin socket: {self.admin_server.socket_path})'
            wfmanLog(message)
            wfmanLog(message, file=sys.stdout, flush=True)

    async def shutdown(self, sockets: list[socket.socket] | None = None) -> None:
        """Stop the runtime and announce graceful listener termination.

        Args:
            sockets (list[socket.socket] | None): Optional externally supplied
                listening sockets, forwarded to Uvicorn for shutdown.

        Returns:
            None when awaited: Stop the runtime without canceling remote workflows.
            First close the optional admin listener and its active connections.
            Emit the endpoint through wfmanLog to its sink
            and stdout if startup succeeded, then await normal HTTP shutdown.
            This handles Ctrl-C and SIGTERM; SIGKILL cannot run Python cleanup.
        """
        if self.admin_server is not None:
            await self.admin_server.stop()
        self.runtime.stop()
        if self.started:
            message = f'Server terminating: {self.endpoint}'
            wfmanLog(message)
            wfmanLog(message, file=sys.stdout, flush=True)
        await super().shutdown(sockets=sockets)
