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
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, SecretStr, StrictStr, ValidationError

from ..manager_functions import FunctionDescription
from .functions import FunctionError
from ..logging import wfmanLog
from ..workers import Workers, WorkerInterrupted, checkForThreadCurrentWorkerInterruption
from .runtime import ServerRuntime


# Bound execution inputs independently of the larger schema-discovery response.
_MAX_MESSAGE_BYTES = 16384
_MAX_RESPONSE_BYTES = 1024 * 1024
_READ_TIMEOUT = 10
_MAX_CONNECTIONS = 4


class AdminDiscoveryRequest(BaseModel):
    """Unauthenticated metadata-only request; type is always 'discover'."""

    model_config = ConfigDict(extra='forbid', hide_input_in_errors=True)
    type: Literal['discover'] = 'discover'


class AdminCallRequest(BaseModel):
    """One password-checked command; its arguments are validated by the registry.

    type is 'call'; command (nonempty str) is an advertised name, arguments
    (dict[str, JSON value]) supplies inputs, and password (SecretStr) is used
    for this connection only. Unknown envelope fields are rejected.
    """

    model_config = ConfigDict(extra='forbid', hide_input_in_errors=True, strict=True)
    type: Literal['call'] = 'call'
    command: StrictStr = Field(min_length=1)
    arguments: dict[str, JsonValue] = Field(default_factory=dict)
    password: SecretStr


AdminRequest = Annotated[AdminDiscoveryRequest | AdminCallRequest, Field(discriminator='type')]


class AdminResponse(BaseModel):
    """Credential-free result or metadata response for a local request.

    success (bool), code (str), and message (str) report completion or failure.
    functions (list[FunctionDescription] | None) is populated by discovery;
    result (JSON value | None) holds an operation return value. Recharge is
    additive: an uncertain transport outcome must never trigger automatic retry.
    Wallet state persists; sandbox registration changes last for this session.
    """

    model_config = ConfigDict(extra='forbid')
    success: bool
    code: Literal['ok', 'authentication_failed', 'invalid_request', 'manager_unavailable',
                  'server_initializing', 'operation_failed', 'busy', 'server_stopping']
    message: str
    functions: list[FunctionDescription] | None = None
    result: JsonValue = None


class AdminServer:
    """Serve one password-checked administration operation per local connection."""

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
                    if isinstance(document, dict) and document.get('type') == 'discover':
                        AdminDiscoveryRequest.model_validate(document)
                        command, arguments = None, None
                    else:
                        # Authenticate execution before interpreting command data.
                        password = document.get('password') if isinstance(document, dict) else None
                        if not isinstance(password, str) or not 1 <= len(password.encode('utf-8')) <= 4096:
                            raise PermissionError('Admin authentication failed.')
                        verifier = await asyncio.to_thread(
                            hashlib.scrypt, password.encode('utf-8'), salt=self._salt, n=16384, r=8, p=1)
                        if not hmac.compare_digest(verifier, self._verifier):
                            raise PermissionError('Admin authentication failed.')
                        request = AdminCallRequest.model_validate(document)
                        command, arguments = request.command, request.arguments
                        del request, password

                    def apply() -> AdminResponse:
                        """Dispatch captured discovery or authenticated inputs; return AdminResponse.

                        Take no arguments. command (str | None) is None for
                        discovery; arguments (dict | None) contains no password.
                        Metadata/operations use the runtime registry off the event
                        loop. No connection or password is retained by managers.
                        """
                        checkForThreadCurrentWorkerInterruption()
                        if command is None:
                            functions = self.runtime.describeFunctions('admin')
                            return AdminResponse(success=True, code='ok', message='Available admin commands.', functions=functions)
                        result, message = self.runtime.invokeFunction('admin', command, arguments)
                        wfmanLog(f'Admin {command} completed')
                        return AdminResponse(success=True, code='ok', message=message, result=result['result'] if set(result) == {'result'} else result)

                    # Forget the credential-bearing envelope before dispatch.
                    document.clear()
                    worker = self._workers.start(context, apply)
                    try:
                        response = await asyncio.wrap_future(worker.future)
                    except FunctionError as exc:
                        response = AdminResponse(success=False,
                                                 code='invalid_request' if exc.code == 'invalid_input' else exc.code,
                                                 message=str(exc))
                    except ValidationError:
                        response = AdminResponse(success=False, code='invalid_request', message='Invalid command arguments.')
                    except WorkerInterrupted:
                        raise
                    except ValueError as exc:
                        response = AdminResponse(success=False, code='invalid_request', message=str(exc))
                    except Exception as exc:
                        wfmanLog(f'Admin operation failed: {type(exc).__name__}: {exc}')
                        response = AdminResponse(success=False, code='operation_failed', message='Admin operation failed; check the server log.')
                except PermissionError:
                    response = AdminResponse(success=False, code='authentication_failed', message='Admin authentication failed.')
                except (ValueError, TimeoutError):
                    response = AdminResponse(success=False, code='invalid_request', message='Invalid or incomplete admin request.')
                except Exception as exc:
                    # Request validation is deliberately generic. Provider errors
                    # retain their normal API logs; do not log credential input.
                    wfmanLog(f'Admin operation failed ({type(exc).__name__})')
                    response = AdminResponse(success=False, code='operation_failed', message='Admin operation failed; check server logs or pending user queries.')
            if self._closed:
                return
            payload = response.model_dump_json(exclude_none=True).encode('utf-8') + b'\n'
            if len(payload) > _MAX_RESPONSE_BYTES:
                payload = AdminResponse(success=False, code='operation_failed', message='Admin response is too large.').model_dump_json().encode() + b'\n'
            writer.write(payload)
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


def execute_admin_request(socket_path: str | Path, request: AdminRequest, *, timeout: float = 120) -> AdminResponse:
    """Send one typed discovery or execution request from the client to the server over the owner's Unix socket.

    socket_path (str | Path) may be relative but contains no '~'. request is
    AdminDiscoveryRequest or AdminCallRequest, never a raw dictionary. timeout
    (finite positive float) bounds socket operations in seconds, default 120.
    Return AdminResponse. Retain ownership/mode checks, warn about shared parent
    directories, reveal SecretStr only on the wire, and never retry a request.
    Transport errors propagate and may leave an uncertain execution outcome.
    """
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError('Admin timeout must be finite and positive')
    if not isinstance(request, (AdminDiscoveryRequest, AdminCallRequest)):
        raise TypeError('request must be a typed admin request')
    document = request.model_dump(mode='json')
    if isinstance(request, AdminCallRequest):
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
            response = stream.readline(_MAX_RESPONSE_BYTES + 1)
    if len(response) > _MAX_RESPONSE_BYTES or not response.endswith(b'\n'):
        raise ValueError('Invalid admin response')
    try:
        return AdminResponse.model_validate_json(response)
    except ValidationError:
        raise ValueError('Invalid admin response') from None
