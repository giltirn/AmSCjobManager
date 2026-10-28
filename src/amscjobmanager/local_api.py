#Implement local versions of API functions
from .globals import checkSafePath
import subprocess
import os
import shlex
from .logging import wfapiLog, wfapiUserQuery
import io
from pathlib import Path
import sqlite3
import threading

def localUsername() -> str:
    """Return the local operating-system username by executing whoami.

    Command execution failures propagate as subprocess exceptions.
    """
    result = subprocess.run(["whoami"], text=True, capture_output=True, check=True)
    return result.stdout.strip()


def localMkdir(path: str, create_parents = True, allow_unsafe = False):        
    path = getAbsoluteLocalPath(path)
    if not allow_unsafe and not checkSafePath("local", path):
        raise Exception("Path is not a subdirectory of the sandbox path")

    if os.path.isdir(path):
        return 1        

    args = [
        "mkdir"        
    ]
    if create_parents:
        args.append("-p")
    args.append(path)

    result = subprocess.run(
        args,
        text=True,
        capture_output=True,        
        check=False,
    )
    
    if result.returncode != 0:
        raise Exception(f"Directory creation failed: {result.stderr}")        


def localPathType(path: str):
    if "~" in str(path):
        raise ValueError("Paths containing '~' are not supported; use an explicit path")
    args = [ "file", path]

    result = subprocess.run(
        args,
        text=True,
        capture_output=True,        
        check=False,
    )
    
    if result.returncode != 0:
        raise Exception(f"Failed to run 'file' on {path}: {result.stderr}")      
        
    return result.stdout.strip()

def writeBytes(path: str, content: io.BytesIO, allow_unsafe = False):
    """
    Write contents as bytes
    Args:
       path - The path to write to
       content - The file contents as binary
       allow_unsafe - Allow copying to directories other than within the sandbox    
    """
    
    path = getAbsoluteLocalPath(path)
    if not allow_unsafe and not checkSafePath("local", path):
        raise Exception("Path is not a subdirectory of the sandbox path")

    try:
        with open(path, "wb") as f:
            f.write(content.getbuffer())
    except Exception as e:
         raise Exception(f"File write to path {path} failed: {e}")

def localCopyFile(path_to: str, path_from: str, allow_unsafe = False):
    """Copy a local source file into a checked destination file or directory.

    Resolve directory destinations and symlinks before checking the final file.
    Configured local sandboxes and /tmp are allowed unless allow_unsafe bypasses
    the check. The source is not sandbox-restricted. Return None on success.
    """
    from .api_general import isDirectory

    if "~" in str(path_from) or "~" in str(path_to):
        raise ValueError("Paths containing '~' are not supported; use an explicit path")
    source = Path(path_from)
    destination = Path(path_to)
    if isDirectory("local", str(destination), check_exists=True):
        destination /= source.name
    source = source.resolve()
    destination = destination.resolve()
    if not allow_unsafe and not checkSafePath("local", destination):
        raise Exception("Path is not a subdirectory of the sandbox path")

    if destination == source:
        return

    result = subprocess.run(
        ["cp", str(source), str(destination)],
        text=True,
        capture_output=True,        
        check=False,
    )
    
    if result.returncode != 0:
        raise Exception(f"File copy failed: {result.stderr}")      

def getAbsoluteLocalPath(path: str | Path) -> str:
    """Return a resolved local path, rejecting '~' rather than guessing a user.

    Accept a string or Path, resolve relative paths and symlinks on the manager
    host, and return an absolute string. No filesystem mutation is performed.
    """
    if "~" in str(path):
        raise ValueError("Paths containing '~' are not supported; use an explicit path")
    return str(Path(path).resolve())

class LocalJobStatus:
    """The primary component of the workflow manager. It provides submission, tracking and updating of workflows backed by a database. State is updated upon calls to its functions."""

    def __init__(self, filename: str | None = None):
        if filename is not None and "~" in str(filename):
            raise ValueError("Database paths containing '~' are not supported")
        db_path = ":memory:" if filename is None else str(Path(filename))
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

        with self.conn as conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS localjobs (
            job_id TEXT PRIMARY KEY,
            status TEXT,
            name TEXT
            )
            """)

    def enqueueJob(self, job_id: str, name: str | None = None):
        with self.conn as conn:        
            conn.execute(
                "INSERT INTO localjobs(job_id, status, name) VALUES (?,?,?)",
                (job_id, "queued", name),
            )

    def startJob(self, job_id : str):
        with self.conn as conn:
            conn.execute("UPDATE localjobs SET status = ? where job_id = ?", ("active", job_id) )        

    def finishJob(self, job_id : str, status : str):
        with self.conn as conn:
            conn.execute("UPDATE localjobs SET status = ? where job_id = ?", (status, job_id) )

    def jobStatus(self, job_id: str):
        with self.conn as conn:
            r = conn.execute("SELECT status FROM localjobs WHERE job_id = ?", (job_id,)).fetchone()
            if r is None:
                raise Exception(f"Unknown job {job_id}")
            row = dict(r)
            return row["status"]

class LocalJobStatusWrapper:
    """Enwrap the status manager in a mutex"""
    def __init__(self, filename: str | None = None):
        self._status_man = LocalJobStatus(filename)
        self._lock = threading.Lock()

    def __enter__(self):        
        self._lock.acquire()
        return self._status_man
            
    def __exit__(self,exc_type, exc_val, exc_tb):
        self._lock.release() #unlock before exception!
        if exc_type:
            raise Exception("Caught exception",exc_type,exc_val,exc_tb)

status_man = None
status_man_db_path = "localjobs.db"

def setStatusManagerDatabasePath(path: str | None):
    """Override the database path for the status manager. Must be called before the status manager is initialized. Setting it to None will use an in-memory database."""
    if status_man is not None:
        raise Exception("Can only call prior to initialization")
    global status_man_db_path
    if path is not None and "~" in str(path):
        raise ValueError("Database paths containing '~' are not supported")
    status_man_db_path = path

def statusMan():
    global status_man
    if status_man == None:
        status_man = LocalJobStatusWrapper(status_man_db_path)
    return status_man

def getJobState(jobid: str) -> str:
    with statusMan() as m:
        return m.jobStatus(jobid)

def findJobByName(name: str) -> str | None:
    with statusMan() as m:
        row = m.conn.execute(
            "SELECT job_id FROM localjobs WHERE name = ? ORDER BY rowid DESC LIMIT 1",
            (name,),
        ).fetchone()
        return None if row is None else row["job_id"]

def executeJobScript(
    script_body: str,
    job_run_dir: str,
    allow_unsafe=False,
    name: str | None = None,
) -> str:
    """
    Execute a job script on the local machine
    script_body: The content of the batch script. If you are executing an existing script, use "source /path/to/script"    
    name: Optional job name recorded with the local job metadata.
    allow_unsafe: Bypass local output path checks; otherwise the run directory,
        wrapper and log must resolve within a local sandbox or /tmp.
    """    

    job_run_dir = getAbsoluteLocalPath(job_run_dir)
    wrapper_path = Path(job_run_dir) / "exec_wrap.sh"
    # Reject unsafe outputs before recording a job or starting its worker.
    if not allow_unsafe and (
        not checkSafePath("local", job_run_dir)
        or not checkSafePath("local", wrapper_path)
    ):
        raise ValueError("Local job output is not below a sandbox directory or /tmp")
    jobid = subprocess.run(["cat", "/proc/sys/kernel/random/uuid"], capture_output=True, text=True).stdout.strip()
    logfile = Path(job_run_dir) / f"run.{jobid}.log"
    if not allow_unsafe and not checkSafePath("local", logfile):
        raise ValueError("Local job log is not below a sandbox directory or /tmp")
    with statusMan() as m:
        m.enqueueJob(jobid, name=name)

    def _run():
        wfapiLog(f"Executing local job {jobid}")
        with statusMan() as m:
            m.startJob(jobid)
        
        with open(wrapper_path,'w') as f:
            f.write(script_body)

        cmd = f"cd {shlex.quote(job_run_dir)} && chmod u+x exec_wrap.sh && ./exec_wrap.sh"
        print(f"Executing: {cmd}")
        with open(logfile, "w") as f:
            result = subprocess.run(
                cmd,
                shell=True,
                text=True,
                stdout=f, 
                stderr=subprocess.STDOUT, 
                check=False,
                executable="/bin/bash"
            )

        if result.returncode != 0:
            print(f"Execution FAILED: see {logfile} for details")
            with statusMan() as m:
                m.finishJob(jobid, "failed")
        else:
            print(f"Execution completed: see {logfile} for details")
            with statusMan() as m:
                m.finishJob(jobid, "completed")    
    threading.Thread(target=_run).start()

    return jobid
