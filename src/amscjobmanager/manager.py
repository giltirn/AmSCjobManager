from .job_data import JobData
import threading
from .action_manager import ActionStatus
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
        self._thread = None
        self._lock = threading.Lock()
        self.poll_freq = poll_freq

    def isAlive(self):
        return self._thread is not None and self._thread.is_alive()
        
    def start(self):
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run)
        self._thread.start()

    def stop(self, wait_until_done=True):
        """
        Ask the manager thread to stop and wait until it does.
        wait_until_done : block until there are no more active or pending workflows before stopping (default True)
        """
        def __nincomplete():
            with self._lock:
                return self.job_data.countWorkflowsWithStatus([ ActionStatus.PENDING, ActionStatus.ACTIVE, ActionStatus.COMPLETED ]) #note, complete (non-null) actions are awaiting progression
        
        if wait_until_done:
            while(__nincomplete() > 0):
                time.sleep(2)
        
        self._stop.set()

        if self._thread is not None:
            self._thread.join()

    def _run(self):
        """Performed on the background thread"""
        self._lock.acquire()
        
        while not self._stop.is_set():
            #A safe checkpoint for allowing the user to obtain a lock and modify the state (e.g. manually activating workflows, restarting after failure, etc)
            self._lock.release()
            time.sleep(0.5)
            self._lock.acquire()
            
            if self._stop.is_set():
                break

            self.job_data.startWorkflows() #start new workflows as required
            self.job_data.progressActiveState(poll_freq=self.poll_freq) #attempt to progress active workflows
            time.sleep(2)
        self._lock.release()

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
            

    
