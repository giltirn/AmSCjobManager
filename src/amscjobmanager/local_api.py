#Implement local versions of API functions
from .globals import checkSafePath
import subprocess
import os
from .logging import wfapiLog, wfapiUserQuery
import io
from pathlib import Path
import sqlite3
import threading

def localMkdir(path: str, create_parents = True, allow_unsafe = False):        
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
    
    if not allow_unsafe and not checkSafePath("local", path):
        raise Exception("Path is not a subdirectory of the sandbox path")

    try:
        with open(path, "wb") as f:
            f.write(content.getbuffer())
    except Exception as e:
         raise Exception(f"File write to path {path} failed: {e}")

def localCopyFile(path_to: str, path_from: str, allow_unsafe = False):
    """
    Copy a file on the local machine
    """
    if not allow_unsafe and not checkSafePath("local", path):
        raise Exception("Path is not a subdirectory of the sandbox path")

    if path_to == path_from:
        return

    result = subprocess.run(
        ["cp", path_from, path_to],
        text=True,
        capture_output=True,        
        check=False,
    )
    
    if result.returncode != 0:
        raise Exception(f"File copy failed: {result.stderr}")      

def getAbsoluteLocalPath(path):    
    if path == ".":
        path = "./"
    return str(Path(path).resolve())

class LocalJobStatus:
    """The primary component of the workflow manager. It provides submission, tracking and updating of workflows backed by a database. State is updated upon calls to its functions."""

    def __init__(self, filename: str | None = None):
        db_path = ":memory:" if filename is None else str(Path(filename).expanduser())
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

        with self.conn as conn:        
                    conn.execute("""
                    CREATE TABLE IF NOT EXISTS localjobs (
                    job_id TEXT PRIMARY KEY,
                    status TEXT
                    )
                    """)
    def enqueueJob(self, job_id : str):
        with self.conn as conn:        
            cur = conn.execute("INSERT INTO localjobs VALUES (?,?)",
                                (job_id, "queued"
                                ) )        

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
    status_man_db_path = path

def statusMan():
    global status_man
    if status_man == None:
        status_man = LocalJobStatusWrapper(status_man_db_path)
    return status_man

def getJobState(jobid: str) -> str:
    with statusMan() as m:
        return m.jobStatus(jobid)

def executeJobScript(script_body: str, job_run_dir : str, allow_unsafe=False) -> str:
    """
    Execute a job script on the local machine
    script_body: The content of the batch script. If you are executing an existing script, use "source /path/to/script"    
    """    

    jobid = subprocess.run(["cat", "/proc/sys/kernel/random/uuid"], capture_output=True, text=True).stdout.strip()
    with statusMan() as m:
        m.enqueueJob(jobid)

    def _run():
        wfapiLog(f"Executing local job {jobid}")
        with statusMan() as m:
            m.startJob(jobid)
        
        if not allow_unsafe and not checkSafePath("local", job_run_dir):
            raise Exception("Path is not below the privileged directory")

        with open(f"{job_run_dir}/exec_wrap.sh",'w') as f:
            f.write(script_body)

        cmd = f"cd {job_run_dir} && chmod u+x exec_wrap.sh && ./exec_wrap.sh"
        print(f"Executing: {cmd}")
        logfile = f"{job_run_dir}/run.{jobid}.log"
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