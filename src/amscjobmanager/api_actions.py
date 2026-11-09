"""Replay-safe synchronous operations used as distinct workflow stages."""

from dataclasses import dataclass, field
import io
from pathlib import Path
import subprocess
import tempfile

from . import api_general as api
from .actions_base import replaceJobIdSubstring
from .globals import checkSafePath
from .local_api import localCopyFile


@dataclass
class MakeDirectory:
    """Ensure a directory exists; successful replay leaves it unchanged.

    machine (str) is a configured target or 'local'; path (str) is the directory
    path and supports <JOBID>. create_parents (bool) defaults to True.
    """

    machine: str
    path: str
    create_parents: bool = True

    def execute(self, job_id: int) -> None:
        """Ensure the directory for positive workflow job_id (int); return None."""
        path = replaceJobIdSubstring(self.path, job_id)
        if not checkSafePath(self.machine, path):
            raise ValueError('Directory is outside the configured sandbox')
        if api.isDirectory(self.machine, path, check_exists=True):
            return
        api.remoteMkdir(self.machine, path,
                        create_parents=self.create_parents, allow_unsafe=False)


@dataclass
class WriteFile:
    """Write complete contents, overwriting a partial destination on replay.

    machine (str) selects a target or 'local'; path (str) is the complete
    destination filename and supports <JOBID>. content (str | bytes) is stored
    verbatim, with str encoded as UTF-8. Content placeholders are not expanded.
    The target API's upload-size limit applies.
    """

    machine: str
    path: str
    content: str | bytes

    def execute(self, job_id: int) -> None:
        """Write contents for positive workflow job_id (int); return None."""
        content = self.content.encode('utf-8') if isinstance(self.content, str) else self.content
        api.uploadBytes(self.machine, replaceJobIdSubstring(self.path, job_id),
                        io.BytesIO(content), allow_unsafe=False)


@dataclass
class UploadFile:
    """Upload a small manager-host file, overwriting its destination on replay.

    machine (str) selects the destination target; dest_path (str) is its full
    filename, source_path (str) is a manager-host filename. Both support <JOBID>.
    The source must remain available and stable until the operation completes.
    The target API's upload-size limit applies; use a transfer stage for bulk data.
    """

    machine: str
    dest_path: str
    source_path: str

    def execute(self, job_id: int) -> None:
        """Upload the file for positive workflow job_id (int); return None."""
        api.uploadSmallFile(self.machine, replaceJobIdSubstring(self.dest_path, job_id),
                            replaceJobIdSubstring(self.source_path, job_id),
                            allow_unsafe=False, definitely_is_file=True)


@dataclass
class DownloadFile:
    """Download a file synchronously to a checked manager-host destination.

    machine (str) selects the source target; dest_path (str) is a local filename
    or existing directory; source_path (str) is the source filename. Both support
    <JOBID>. Replay overwrites the destination; use a separate asynchronous
    transfer stage for long-running or large transfers.
    """

    machine: str
    dest_path: str
    source_path: str

    def execute(self, job_id: int) -> None:
        """Download the file for positive workflow job_id (int); return None."""
        api.downloadFile(self.machine, replaceJobIdSubstring(self.dest_path, job_id),
                         replaceJobIdSubstring(self.source_path, job_id), allow_unsafe=False)


@dataclass
class CopyFile:
    """Copy a file on the manager host using the local API's sandbox checks.

    dest_path (str) is a local filename or existing directory; source_path (str)
    is a local filename. Both support <JOBID>. Replay overwrites the destination.
    Remote or cross-machine copies belong in a separate transfer stage.
    """

    dest_path: str
    source_path: str

    def execute(self, job_id: int) -> None:
        """Copy the file for positive workflow job_id (int); return None."""
        localCopyFile(replaceJobIdSubstring(self.dest_path, job_id),
                      replaceJobIdSubstring(self.source_path, job_id), allow_unsafe=False)


@dataclass
class RunLocal:
    """Run a short, trusted, replay-safe program on the manager host.

    executable (str) is a command name or path, arguments (list[str]) contains
    literal argv entries, and cwd (str) is an existing local working directory.
    All support <JOBID>. timeout (float) is a positive maximum duration in
    seconds, default 60. Execution uses no shell. The cwd is checked against
    local sandboxes, but the child program's writes are NOT sandboxed. Use local
    compute actions for long-running work. Programs must tolerate replay after
    success if a crash prevents the operation checkpoint from being committed.
    """

    executable: str
    arguments: list[str]
    cwd: str
    timeout: float = 60

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


@dataclass
class APIAction:
    """An ordered sequence of synchronous, replay-safe workflow operations.

    operations (list) contains MakeDirectory, WriteFile, UploadFile, DownloadFile,
    CopyFile or RunLocal instances, defaulting to an empty sequence. Indices are
    zero-based. Each operation must tolerate replay after partial execution or
    success before its checkpoint commits. Completed operations are skipped on
    recovery. Local paths and commands refer to the manager host, not a client.
    """

    operations: list[MakeDirectory | WriteFile | UploadFile | DownloadFile | CopyFile | RunLocal] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Validate the operation list after construction; return None."""
        allowed = (MakeDirectory, WriteFile, UploadFile, DownloadFile, CopyFile, RunLocal)
        if not isinstance(self.operations, list) or any(type(op) not in allowed for op in self.operations):
            raise TypeError('operations must be a list of supported API operation instances')

    def getInfo(self) -> dict:
        """Take no arguments; return operation_count (int) and operation type names."""
        return {'operation_count': len(self.operations),
                'operations': [type(operation).__name__ for operation in self.operations]}
