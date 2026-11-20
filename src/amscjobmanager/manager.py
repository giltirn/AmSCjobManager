from .workflow_data import WorkflowData
import threading
from .action_manager import ActionStatus
from .logging import wfmanLog
import time
            
class WorkflowManager:
    """Progress workflows in a single-use background manager.

    WorkflowData owns domain operations and public function definitions; this
    wrapper owns the progression thread, its lifetime, and the caller-facing lock.
    Construction recovers unfinished actions through WorkflowData. Once stopped or
    failed, this instance cannot restart; create a new manager for recovery.
    Submit through ``with manager as workflows: workflows.enqueueWorkflow(...)`` or the callable
    interface. Both accept WorkflowData's optional submission_key for retry-safe
    submission, with uniqueness and persistence owned by WorkflowData.
    """

    def __init__(self, filename: str | None = None, poll_freq=30, max_workflows_active=10):
        """Create a single-use manager and recover its unfinished action workers.

        filename (str | None) is the persistent SQLite path, or None for a
        memory-only database. poll_freq (float, default 30) is seconds between
        provider status polls/recovery attempts. max_workflows_active (int,
        default 10) limits automatic workflow starts; zero disables them.
        Return None. WorkflowData construction recovers existing unfinished actions;
        start() separately launches workflow progression. After shutdown or
        failure, create a new instance to recover persisted work.
        """
        self.workflow_data = WorkflowData(filename, max_workflows_active=max_workflows_active, poll_freq=poll_freq)
        self._stop = threading.Event()
        self._failed = threading.Event()
        self._thread = None
        self._lock = threading.Lock()
        self.poll_freq = poll_freq

    def isAlive(self):
        """Return whether the background thread is alive without a recorded failure."""
        return self._thread is not None and self._thread.is_alive() and not self._failed.is_set()
        
    def start(self):
        """Start workflow progression once for this manager instance.

        Take no arguments; return None. An already running manager is unchanged.
        After shutdown or background failure, raise RuntimeError directing the
        caller to construct a new manager against its persistent database.
        Action recovery already occurred during WorkflowData construction.
        """
        with self._lock:
            if self._stop.is_set() or self._failed.is_set():
                raise RuntimeError('WorkflowManager cannot be restarted; create a new WorkflowManager for recovery')
            if self._thread is not None:
                if self._thread.is_alive():
                    return
                raise RuntimeError('WorkflowManager cannot be restarted; create a new WorkflowManager for recovery')
            self._thread = threading.Thread(target=self._run)
            try:
                self._thread.start()
            except BaseException:
                self._failed.set()
                self._thread = None
                self.workflow_data.stopWorkers()
                raise

    def stop(self, wait_until_done=True, *, close_database=False):
        """Permanently stop this manager and its action workers.

        wait_until_done (bool, default True) waits for unfinished workflows while
        the manager remains healthy. close_database (bool, default False) closes
        storage after shutdown. Return None. Repeated calls are safe, including
        after database closure. Accepted remote jobs are not canceled by shutdown.
        This instance cannot be started again, even if it had never been started.
        """
        # A failed/stopped thread cannot drain workflows; do not query closed
        # storage or wait for unfinished records during repeated shutdown.
        while wait_until_done and self.isAlive():
            with self._lock:
                remaining = self.workflow_data.countWorkflowsWithStatus([
                    ActionStatus.PENDING, ActionStatus.ACTIVE, ActionStatus.CANCELING,
                ])
            if remaining == 0:
                break
            time.sleep(2)

        self._stop.set()
        if self._thread is not None:
            self._thread.join()
        self.workflow_data.stopWorkers()
        if close_database:
            self.workflow_data.conn.close()

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
                        self.workflow_data.startWorkflows()
                        self.workflow_data.progressActiveState()
                    except BaseException:
                        # Mark failure before unlocking so a waiting request
                        # cannot mistake the exiting thread for a healthy one.
                        self._failed.set()
                        raise
                if self._stop.wait(2):
                    break
        except Exception as exc:
            wfmanLog(f"WorkflowManager background thread failed; workflow progression stopped: {type(exc).__name__}: {exc}")

    def __call__(self, op_lambda):
        """
        Perform an operation on the WorkflowData database under lock
        """
        with self._lock:
            return op_lambda(self.workflow_data)
        
    def __enter__(self):
        """
        Allow the user to acquire a lock on the database for manipulation using 'with'
        """
        self._lock.acquire()
        return self.workflow_data
        
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Release the manager lock and propagate any original context-block exception.

        exc_type (type[BaseException] | None), exc_val (BaseException | None),
        and exc_tb (traceback | None) describe the block's error; all are None
        on success. Return None so Python preserves the original exception and
        traceback without suppressing or wrapping it.
        """
        self._lock.release()
            

    
