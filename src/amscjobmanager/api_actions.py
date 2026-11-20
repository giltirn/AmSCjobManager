"""Replay-safe synchronous operations used as distinct workflow stages."""

from dataclasses import field
from abc import ABC, abstractmethod
import base64
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, SerializeAsAny, StrictBool, StrictStr, field_serializer, field_validator
from pydantic_core import core_schema
from pydantic.dataclasses import dataclass
import io
from pathlib import Path
import subprocess
import tempfile

from . import api_general as api
from .actions_base import ActionDefinition, replaceJobIdSubstring
from .globals import checkSafePath
from .local_api import localCopyFile


class APIActionStageBase(ABC):
    """An in-process, replay-safe operation in an APIAction sequence.

    Store recovery inputs in pickleable instance fields and define subclasses
    in importable modules. Workers share process globals, but those globals are
    not persisted. Return values are ignored; exceptions fail the action.
    """

    @abstractmethod
    def execute(self, job_id: int) -> None:
        """Perform work for workflow database ID job_id; tolerate replay."""


class BinaryContent(BaseModel):
    """JSON representation of bytes: base64 (str) is standard padded Base64 data."""

    model_config = ConfigDict(extra='forbid', strict=True)
    base64: str = Field(description='Standard Base64-encoded binary file contents.')


@dataclass(config=ConfigDict(extra="forbid"))
class MakeDirectory(APIActionStageBase):
    """Ensure a directory exists; successful replay leaves it unchanged.

    machine (str) is a configured target or 'local'; path (str) is the directory
    path and supports <JOBID>. create_parents (bool) defaults to True.
    The JSON discriminator type is 'make_directory'.
    """

    machine: StrictStr
    path: StrictStr
    create_parents: StrictBool = True

    type: Literal["make_directory"] = field(default="make_directory", kw_only=True)

    def execute(self, job_id: int) -> None:
        """Ensure the directory for positive workflow job_id (int); return None."""
        path = replaceJobIdSubstring(self.path, job_id)
        if not checkSafePath(self.machine, path):
            raise ValueError('Directory is outside the configured sandbox')
        if api.isDirectory(self.machine, path, check_exists=True):
            return
        api.remoteMkdir(self.machine, path,
                        create_parents=self.create_parents, allow_unsafe=False)


@dataclass(config=ConfigDict(extra="forbid"))
class WriteFile(APIActionStageBase):
    """Write complete contents, overwriting a partial destination on replay.

    machine (str) selects a target or 'local'; path (str) is the complete
    destination filename and supports <JOBID>. content (str | bytes) is stored
    verbatim, with str encoded as UTF-8. Content placeholders are not expanded.
    The target API's upload-size limit applies. JSON bytes use an object with
    a base64 string; Python callers may supply raw bytes. type is 'write_file'.
    """

    machine: StrictStr
    path: StrictStr
    content: StrictStr | bytes | BinaryContent

    type: Literal["write_file"] = field(default="write_file", kw_only=True)

    @field_validator('content')
    @classmethod
    def decodeBinaryContent(cls, value):
        """Convert validated BinaryContent to bytes; preserve str/bytes input.

        cls is WriteFile; value (str | bytes | BinaryContent) is file data.
        Return str or bytes. Invalid Base64 raises a validation error before
        workflow enqueueing; Python callers may continue supplying raw bytes.
        """
        if isinstance(value, BinaryContent):
            return base64.b64decode(value.base64, validate=True)
        return value

    @field_serializer('content', when_used='json', return_type=str | BinaryContent)
    def encodeBinaryContent(self, value):
        """Return JSON-safe contents, preserving the distinction between text and bytes.

        value (str | bytes) is the validated content. Return str for text or
        BinaryContent for bytes. Python-mode serialization retains raw bytes.
        """
        if isinstance(value, bytes):
            return BinaryContent(base64=base64.b64encode(value).decode('ascii'))
        return value

    def execute(self, job_id: int) -> None:
        """Write contents for positive workflow job_id (int); return None."""
        content = self.content.encode('utf-8') if isinstance(self.content, str) else self.content
        api.uploadBytes(self.machine, replaceJobIdSubstring(self.path, job_id),
                        io.BytesIO(content), allow_unsafe=False)


@dataclass(config=ConfigDict(extra="forbid"))
class UploadFile(APIActionStageBase):
    """Upload a small manager-host file, overwriting its destination on replay.

    machine (str) selects the destination target; dest_path (str) is its full
    filename, source_path (str) is a manager-host filename. Both support <JOBID>.
    The source must remain available and stable until the operation completes.
    The target API's upload-size limit applies; use a transfer stage for bulk data.
    The JSON discriminator type is 'upload_file'.
    """

    machine: StrictStr
    dest_path: StrictStr
    source_path: StrictStr

    type: Literal["upload_file"] = field(default="upload_file", kw_only=True)

    def execute(self, job_id: int) -> None:
        """Upload the file for positive workflow job_id (int); return None."""
        api.uploadSmallFile(self.machine, replaceJobIdSubstring(self.dest_path, job_id),
                            replaceJobIdSubstring(self.source_path, job_id),
                            allow_unsafe=False, definitely_is_file=True)


@dataclass(config=ConfigDict(extra="forbid"))
class DownloadFile(APIActionStageBase):
    """Download a file synchronously to a checked manager-host destination.

    machine (str) selects the source target; dest_path (str) is a local filename
    or existing directory; source_path (str) is the source filename. Both support
    <JOBID>. Replay overwrites the destination; use a separate asynchronous
    transfer stage for long-running or large transfers.
    The JSON discriminator type is 'download_file'.
    """

    machine: StrictStr
    dest_path: StrictStr
    source_path: StrictStr

    type: Literal["download_file"] = field(default="download_file", kw_only=True)

    def execute(self, job_id: int) -> None:
        """Download the file for positive workflow job_id (int); return None."""
        api.downloadFile(self.machine, replaceJobIdSubstring(self.dest_path, job_id),
                         replaceJobIdSubstring(self.source_path, job_id), allow_unsafe=False)


@dataclass(config=ConfigDict(extra="forbid"))
class CopyFile(APIActionStageBase):
    """Copy a file on the manager host using the local API's sandbox checks.

    dest_path (str) is a local filename or existing directory; source_path (str)
    is a local filename. Both support <JOBID>. Replay overwrites the destination.
    Remote or cross-machine copies belong in a separate transfer stage.
    The JSON discriminator type is 'copy_file'.
    """

    dest_path: StrictStr
    source_path: StrictStr

    type: Literal["copy_file"] = field(default="copy_file", kw_only=True)

    def execute(self, job_id: int) -> None:
        """Copy the file for positive workflow job_id (int); return None."""
        localCopyFile(replaceJobIdSubstring(self.dest_path, job_id),
                      replaceJobIdSubstring(self.source_path, job_id), allow_unsafe=False)


@dataclass(config=ConfigDict(extra="forbid"))
class RunLocal(APIActionStageBase):
    """Run a short, trusted, replay-safe program on the manager host.

    executable (str) is a command name or path, arguments (list[str]) contains
    literal argv entries, and cwd (str) is an existing local working directory.
    All support <JOBID>. timeout (float) is a positive maximum duration in
    seconds, default 60. Execution uses no shell. The cwd is checked against
    local sandboxes, but the child program's writes are NOT sandboxed. Use local
    compute actions for long-running work. Programs must tolerate replay after
    success if a crash prevents the operation checkpoint from being committed.
    The JSON discriminator type is 'run_local'.
    """

    executable: StrictStr
    arguments: list[StrictStr]
    cwd: StrictStr
    timeout: Annotated[float, Field(strict=True, gt=0, allow_inf_nan=False)] = 60

    type: Literal["run_local"] = field(default="run_local", kw_only=True)

    def execute(self, job_id: int) -> None:
        """Run for positive workflow job_id (int); return None on exit code zero.

        Nonzero exit raises RuntimeError with the last 4096 output bytes.
        Timeout raises subprocess.TimeoutExpired. No success checkpoint is
        recorded by this operation; the API action manager owns that transition.
        """
        import math

        if not math.isfinite(self.timeout) or self.timeout <= 0:
            raise ValueError('RunLocal timeout must be finite and positive')
        cwd = replaceJobIdSubstring(self.cwd, job_id)
        if not checkSafePath('local', cwd):
            raise ValueError('RunLocal working directory is outside the local sandbox')
        executable = replaceJobIdSubstring(self.executable, job_id)
        if '~' in executable:
            raise ValueError("Executable paths containing '~' are not supported")
        arguments = [replaceJobIdSubstring(value, job_id) for value in self.arguments]
        # Capture output on disk rather than retaining an unbounded stream in memory.
        with tempfile.TemporaryFile() as output:
            result = subprocess.run([executable, *arguments], cwd=Path(cwd).resolve(),
                                    stdout=output, stderr=subprocess.STDOUT, timeout=self.timeout)
            if result.returncode:
                output.seek(0, 2)
                output.seek(max(0, output.tell() - 4096))
                reason = output.read().decode('utf-8', errors='replace')
                raise RuntimeError(f'Local command {executable!r} exited with {result.returncode}: {reason}')


BuiltinAPIOperation = Annotated[
    MakeDirectory | WriteFile | UploadFile | DownloadFile | CopyFile | RunLocal,
    Field(discriminator='type', json_schema_extra={'required': ['type']}),
]


class _APIOperationSchema:
    """Accept Python extensions while retaining the built-in JSON contract."""

    @classmethod
    def __get_pydantic_core_schema__(cls, source_type, handler):
        """Build a schema for base-class instances or built-in operation data.

        source_type is the annotated base; handler generates the built-in
        discriminated union. Return a core schema with built-in-only JSON input.
        """
        builtin = handler.generate_schema(BuiltinAPIOperation)
        return core_schema.json_or_python_schema(
            json_schema=builtin,
            python_schema=core_schema.union_schema([
                core_schema.is_instance_schema(APIActionStageBase), builtin,
            ]),
        )


APIOperation = SerializeAsAny[Annotated[APIActionStageBase, _APIOperationSchema]]


@dataclass(config=ConfigDict(extra="forbid"))
class APIAction(ActionDefinition):
    """An ordered sequence of synchronous, replay-safe workflow operations.

    operations (list) contains APIActionStageBase instances, defaulting to an
    empty sequence. JSON input supports built-in operations only. Indices are
    zero-based. Each operation must tolerate replay after partial execution or
    success before its checkpoint commits. Completed operations are skipped on
    recovery. Local paths and commands refer to the manager host, not a client.
    The JSON discriminator type is 'api'.
    """

    operations: list[APIOperation] = field(default_factory=list)
    type: Literal["api"] = field(default="api", kw_only=True)

    def getInfo(self) -> dict:
        """Take no arguments; return operation_count (int) and operation type names."""
        return {'operation_count': len(self.operations),
                'operations': [type(operation).__name__ for operation in self.operations]}
