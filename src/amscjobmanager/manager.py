from .job_data import JobData
import threading
from .action_manager import ActionStatus
from .logging import wfmanLog
import time
            
class JobManager:
    """Launches a background thread that progresses the state of the underlying JobData instance"""

    def __init__(self, filename: str | None = None, poll_freq=30, max_workflows_active=10):
        """
        filename: The path to the database for persisting manager status. Use None for an in-memory database (WARNING: if you use an in-memory database and kill the manager before all workflows are complete, it will forget about those uncompleted)
        poll_freq: how often the action monitors poll the API for status updates
        max_workflows_active: if >0, the manager will attempt to maintain this many active workflows, activating more when others finish; if 0, they must be activated manually
        """
        
        self.job_data = JobData(filename, max_workflows_active=max_workflows_active)
        self._stop = threading.Event()
        self._failed = threading.Event()
        self._thread = None
        self._lock = threading.Lock()
        self.poll_freq = poll_freq

    def isAlive(self):
        """Return whether the background thread is alive without a recorded failure."""
        return self._thread is not None and self._thread.is_alive() and not self._failed.is_set()
        
    def start(self):
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._failed.clear()
        self._thread = threading.Thread(target=self._run)
        self._thread.start()

    def stop(self, wait_until_done=True, *, close_database=False):
        """
        Ask the manager thread to stop and wait until it does.
        wait_until_done : block until there are no more active or pending workflows before stopping (default True)
        close_database : close the JobData connection after the thread stops
        A stopped or failed background thread cannot drain workflows, so it
        ends the completion wait even if incomplete records remain.
        """
        def __nincomplete():
            with self._lock:
                return self.job_data.countWorkflowsWithStatus([ ActionStatus.PENDING, ActionStatus.SCHEDULING, ActionStatus.ACTIVE, ActionStatus.COMPLETED, ActionStatus.CANCELING ]) #note, complete (non-null) actions are awaiting progression
        
        if wait_until_done:
            while(__nincomplete() > 0 and self.isAlive()):
                time.sleep(2)
        
        self._stop.set()

        if self._thread is not None:
            self._thread.join()
        if close_database:
            self.job_data.conn.close()

    def _run(self):
        """Progress workflows until stopped; log failures and always release the lock."""
        try:
            while not self._stop.wait(0.5):
                # Only hold the lock while accessing manager state. The context
                # releases it on errors as well as normal completion.
                with self._lock:
                    if self._stop.is_set():
                        break
                    try:
                        self.job_data.startWorkflows()
                        self.job_data.progressActiveState(poll_freq=self.poll_freq)
                    except BaseException:
                        # Mark failure before unlocking so a waiting request
                        # cannot mistake the exiting thread for a healthy one.
                        self._failed.set()
                        raise
                if self._stop.wait(2):
                    break
        except Exception as exc:
            wfmanLog(f"JobManager background thread failed; workflow progression stopped: {type(exc).__name__}: {exc}")

    def __call__(self, op_lambda):
        """
        Perform an operation on the JobData database under lock
        """
        with self._lock:
            return op_lambda(self.job_data)
        
    def __enter__(self):
        """
        Allow the user to acquire a lock on the database for manipulation using 'with'
        """
        self._lock.acquire()
        return self.job_data
        
    def __exit__(self,exc_type, exc_val, exc_tb):
        self._lock.release() #unlock before exception!
        if exc_type:
            raise Exception("Caught exception",exc_type,exc_val,exc_tb)
            

    
