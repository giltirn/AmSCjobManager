"""Password-protected local administration, independent of MCP and its user queries."""

import asyncio
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import secrets
import socket
import stat
import sys
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr, StrictStr, ValidationError

from .. import globals as sandbox
from ..logging import wfmanLog
from ..workers import Workers, WorkerInterrupted, checkForThreadCurrentWorkerInterruption
from .runtime import ServerRuntime


_MAX_MESSAGE_BYTES = 16384
_READ_TIMEOUT = 10
_MAX_CONNECTIONS = 4


class AdminRequest(BaseModel):
    """One authenticated operation; no field represents Python code or a method name.

    password (SecretStr) is the per-request credential, never returned or logged.
    command selects add_sandbox/remove_sandbox. machine (str) is a configured
    target, and path (str) is a sandbox root; removal matches its registered text
    exactly. Requests reject extra fields. Credentials are not persisted.
    """

    model_config = ConfigDict(extra='forbid', hide_input_in_errors=True)
    password: SecretStr
    command: Literal['add_sandbox', 'remove_sandbox']
    machine: StrictStr = Field(min_length=1)
    path: StrictStr = Field(min_length=1)


class AdminResponse(BaseModel):
    """Result of one operation, excluding the request and its credentials.

    success (bool) means the command finished successfully. code (str) is 'ok'
    or a stable error category; message (str) is safe frontend text. A transport
    error after dispatch can leave an uncertain outcome; both commands are
    idempotent. Changes affect only this server session's sandbox registration.
    """

    model_config = ConfigDict(extra='forbid')
    success: bool
    code: Literal['ok', 'authentication_failed', 'invalid_request', 'manager_unavailable',
                  'operation_failed', 'busy', 'server_stopping']
    message: str


class AdminServer:
    """Serve one password-checked sandbox operation per local socket connection."""

    def __init__(self, socket_path: str | Path, password: str, runtime: ServerRuntime):
        """Prepare the listener and retain only a salted password verifier.

        socket_path (str | Path) is a Unix socket pathname without '~'. Relative
        paths are made absolute using the current working directory, without
        resolving symlinks, so existing socket/symlink entries remain detectable.
        password (str) must contain 1..4096 UTF-8 bytes. runtime (ServerRuntime)
        supplies the running manager. Return None without opening the socket.
        start()/stop() run on the same asyncio loop. No credentials reach MCP,
        configuration, database, query queues, or application logs.
        """
        self.socket_path = Path(os.path.abspath(socket_path))
        if '~' in str(self.socket_path):
            raise ValueError("Admin socket paths containing '~' are not supported")
        if not hasattr(socket, 'AF_UNIX') or not hasattr(os, 'geteuid'):
            raise ValueError('Local administration requires Unix-domain sockets and Unix permissions')
        if len(os.fsencode(self.socket_path)) >= 104:
            raise ValueError('Admin socket path must be shorter than 104 bytes')
        if not isinstance(password, str) or not 1 <= len(password.encode('utf-8')) <= 4096:
            raise ValueError('Admin password must contain 1..4096 UTF-8 bytes')
        self._salt = secrets.token_bytes(16)
        self._verifier = hashlib.scrypt(password.encode('utf-8'), salt=self._salt, n=16384, r=8, p=1)
        self.runtime = runtime
        self._server = None
        self._socket_identity = None
        self._connections = set()
        self._tasks = set()
        self._workers = Workers()
        self._closed = True

    async def start(self) -> None:
        """Bind the private Unix socket; take no arguments and return None when awaited.

        Create missing parent directories. Require a nonsymlinked directory,
        but only warn if it is shared or owned by another user. Recommend a
        separate user-owned directory with mode 0700; keep the socket mode 0600.
        Never overwrite an existing entry, including a stale socket or symlink.
        Partial startup cleans up only this instance's socket.
        """
        parent = self.socket_path.parent
        parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        info = parent.lstat()
        if not stat.S_ISDIR(info.st_mode):
            raise ValueError('Admin socket parent must be a directory, not a symlink')
        if info.st_uid != os.geteuid() or info.st_mode & 0o077:
            message = (f'Warning: admin socket parent {parent} is shared or owned by another user. '
                       'Prefer a separate directory you own with mode 0700 to reduce socket replacement risk.')
            wfmanLog(message)
            wfmanLog(message, file=sys.stderr, flush=True)
        if os.path.lexists(self.socket_path):
            raise FileExistsError('Admin socket path already exists; choose another path or remove the stale socket manually')

        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            # Bind ourselves: asyncio must not automatically unlink an existing
            # socket. Restrict the socket itself independently of parent mode.
            listener.bind(str(self.socket_path))
            info = self.socket_path.lstat()
            self._socket_identity = (info.st_dev, info.st_ino)
            self.socket_path.chmod(0o600)
            listener.setblocking(False)
            self._server = await asyncio.start_unix_server(
                self._handleConnection, sock=listener, limit=_MAX_MESSAGE_BYTES + 1,
                **({'cleanup_socket': False} if sys.version_info >= (3, 13) else {}),
            )
            self._closed = False
        except BaseException:
            listener.close()
            await self.stop()
            raise

    async def stop(self) -> None:
        """Close the listener, interrupt admin workers and remove this instance's socket.

        Take no arguments; return None when awaited. Repeated stop is harmless.
        A slow provider call may finish later; workers and user waits receive
        cooperative shutdown signals. Do not remove a replacement filesystem entry.
        """
        self._closed = True
        if self._server is not None:
            self._server.close()
        self._workers.stop()
        for writer in list(self._connections):
            writer.close()
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self._server is not None:
            # Recent asyncio versions wait for accepted streams too. Close
            # those streams before waiting for the listening server to finish.
            await self._server.wait_closed()
        if self._socket_identity is not None:
            try:
                info = self.socket_path.lstat()
                if stat.S_ISSOCK(info.st_mode) and (info.st_dev, info.st_ino) == self._socket_identity:
                    self.socket_path.unlink()
            except FileNotFoundError:
                pass
            self._socket_identity = None

    async def _handleConnection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Authenticate and handle one bounded JSON line on reader/writer, then disconnect.

        reader/writer are asyncio Unix-stream objects supplied by the listener.
        Return None when awaited. Invalid inputs receive generic errors without
        echoing secrets. Password hashing and API operations run off the event
        loop; neither manager locks nor sandbox locks span interactive waits.
        """
        task = asyncio.current_task()
        context = object()
        self._tasks.add(task)
        admitted = not self._closed and len(self._connections) < _MAX_CONNECTIONS
        if admitted:
            self._connections.add(writer)
        response = AdminResponse(success=False, code='busy', message='Admin listener is busy; retry later.')
        try:
            if self._closed:
                response = AdminResponse(success=False, code='server_stopping', message='Server is stopping.')
            elif admitted:
                # Bound both incomplete clients and authentication work. Do not
                # format raw JSON or Pydantic errors: they may contain secrets.
                try:
                    line = await asyncio.wait_for(reader.readline(), timeout=_READ_TIMEOUT)
                    if not line.endswith(b'\n') or len(line) > _MAX_MESSAGE_BYTES:
                        raise ValueError('Invalid message boundary')
                    document = json.loads(line)
                    password = document.get('password') if isinstance(document, dict) else None
                    if not isinstance(password, str) or not 1 <= len(password.encode('utf-8')) <= 4096:
                        response = AdminResponse(success=False, code='authentication_failed', message='Admin authentication failed.')
                    else:
                        verifier = await asyncio.to_thread(
                            hashlib.scrypt, password.encode('utf-8'), salt=self._salt, n=16384, r=8, p=1)
                        if not hmac.compare_digest(verifier, self._verifier):
                            response = AdminResponse(success=False, code='authentication_failed', message='Admin authentication failed.')
                        else:
                            request = AdminRequest.model_validate(document)
                            command, machine, path = request.command, request.machine.lower(), request.path
                            del request

                            def apply() -> AdminResponse:
                                """Apply the captured authenticated command; return a credential-free result.

                                Take no arguments; command/machine/path (str)
                                are captured from the validated request. Check
                                readiness and use the sandbox owner's API. The
                                worker can wait for existing API/user queries
                                without blocking MCP or manager progression.
                                """
                                checkForThreadCurrentWorkerInterruption()
                                manager = self.runtime.manager
                                if manager is None or not manager.isAlive() or self._closed:
                                    return AdminResponse(success=False, code='manager_unavailable', message='Workflow manager is not running.')
                                from ..api_general import getKnownMachines
                                if machine not in getKnownMachines():
                                    return AdminResponse(success=False, code='invalid_request', message='Machine is not configured.')
                                try:
                                    if command == 'add_sandbox':
                                        sandbox.addSandboxDirs({machine: path})
                                        message = 'Sandbox directory added.'
                                    else:
                                        sandbox.removeSandboxDirs({machine: path})
                                        message = 'Sandbox directory removed.'
                                except WorkerInterrupted:
                                    raise
                                except Exception as exc:
                                    # Only sandbox operations enter this block;
                                    # they receive no password or wire document.
                                    wfmanLog(f'Admin {command} failed: {type(exc).__name__}: {exc}')
                                    return AdminResponse(success=False,
                                                         code='invalid_request' if isinstance(exc, ValueError) else 'operation_failed',
                                                         message=str(exc))
                                wfmanLog(f'Admin {command} completed for {machine}:{path}')
                                return AdminResponse(success=True, code='ok', message=message)

                            # Forget the credential-bearing document before API
                            # work. Authorization belongs only to this operation.
                            document.clear()
                            del password
                            worker = self._workers.start(context, apply)
                            response = await asyncio.wrap_future(worker.future)
                except (ValueError, TimeoutError):
                    response = AdminResponse(success=False, code='invalid_request', message='Invalid or incomplete admin request.')
                except Exception as exc:
                    # Request validation is deliberately generic. Provider errors
                    # retain their normal API logs; do not log credential input.
                    wfmanLog(f'Admin operation failed ({type(exc).__name__})')
                    response = AdminResponse(success=False, code='operation_failed', message='Sandbox operation failed; check server logs or pending user queries.')
            if self._closed:
                return
            writer.write(response.model_dump_json().encode('utf-8') + b'\n')
            await writer.drain()
        except (ConnectionError, OSError):
            pass
        finally:
            self._workers.discard(context)
            self._connections.discard(writer)
            self._tasks.discard(task)
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass


def execute_admin_command(socket_path: str | Path, command: str, machine: str, path: str,
                          password: str, *, timeout: float = 120) -> AdminResponse:
    """Send one authenticated local command and return its AdminResponse.

    socket_path (str | Path) selects a private Unix socket owned by this OS user.
    Relative paths use the current working directory; '~' is rejected, not expanded.
    Shared or other-owned parent directories produce advice on stderr, not errors.
    command (str) is add_sandbox/remove_sandbox; machine/path (str) select the
    target and registered root. password (str) is used for this request only.
    timeout (finite positive float) bounds socket operations in seconds, default
    120. Transport errors propagate; the operation may already have taken effect.
    No credentials are printed or persisted. Run on the server host.
    """
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError('Admin timeout must be finite and positive')
    request = AdminRequest(password=password, command=command, machine=machine, path=path)
    # SecretStr deliberately redacts ordinary serialization; reveal it only for
    # the local wire message. Never return this document as part of an error.
    document = request.model_dump(mode='json')
    document['password'] = request.password.get_secret_value()
    message = json.dumps(document, ensure_ascii=True).encode('utf-8') + b'\n'
    if len(message) > _MAX_MESSAGE_BYTES:
        raise ValueError('Admin request is too large')
    address = Path(os.path.abspath(socket_path))
    if '~' in str(address):
        raise ValueError("Admin socket paths containing '~' are not supported")
    info = address.lstat()
    parent = address.parent.lstat()
    if (not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077
            or not stat.S_ISDIR(parent.st_mode)):
        raise ValueError('Admin socket must be private and owned by this user; its parent must be a nonsymlinked directory')
    if parent.st_uid != os.geteuid() or parent.st_mode & 0o077:
        wfmanLog(f'Warning: admin socket parent {address.parent} is shared or owned by another user. '
                 'Prefer a separate directory you own with mode 0700 to reduce socket replacement risk.',
                 file=sys.stderr, flush=True)
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(timeout)
        connection.connect(str(address))
        connection.sendall(message)
        with connection.makefile('rb') as stream:
            response = stream.readline(_MAX_MESSAGE_BYTES + 1)
    if len(response) > _MAX_MESSAGE_BYTES or not response.endswith(b'\n'):
        raise ValueError('Invalid admin response')
    try:
        return AdminResponse.model_validate_json(response)
    except ValidationError:
        raise ValueError('Invalid admin response') from None
