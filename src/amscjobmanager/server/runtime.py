"""Asynchronous manager initialization and user-query routing for an MCP server."""

from threading import RLock
from typing import Callable

from ..logging import wfmanLog
from ..manager import JobManager
from ..workers import Workers, WorkerInterrupted, checkForThreadCurrentWorkerInterruption
from .queries import UserQueryQueue


class ServerRuntime:
    """Own server startup, its manager and an in-memory question/response queue."""

    def __init__(self, initialize_manager: Callable[[], JobManager]):
        """Prepare startup without constructing a manager or starting threads.

        initialize_manager (callable) takes no arguments, configures the API and
        returns a new JobManager. Return None. Its work runs later on a startup
        worker so the listener can serve questions during configuration/recovery.
        queries (UserQueryQueue) belongs to this runtime's server session.
        """
        self.queries = UserQueryQueue()
        self._initialize_manager = initialize_manager
        self._manager = None
        self._state = 'created'
        self._lock = RLock()
        self._workers = Workers()
        self._startup_worker = None

    @property
    def manager(self) -> JobManager | None:
        """Return the running manager, or None before readiness/after shutdown.

        Take no arguments. Readiness is publication after successful startup;
        callers must also check manager.isAlive() for later background failure.
        """
        with self._lock:
            return self._manager

    @property
    def state(self) -> str:
        """Return created, starting, running, failed or stopped; take no arguments.

        A published manager whose background thread dies is reported as failed.
        """
        with self._lock:
            if self._state == 'running' and not self._manager.isAlive():
                return 'failed'
            return self._state

    def start(self) -> None:
        """Start asynchronous manager initialization for the server.

        Take no arguments; return None without waiting. Starting/running calls
        reuse startup; failed/stopped runtimes cannot restart. The caller must
        first set logging.api_query_user_func to queries.ask, then restore its
        previous hook after stop(). The server utility owns this mode selection.
        """
        def initialize() -> None:
            """Build/start the captured manager, or report failure without killing HTTP.

            Take no arguments; return None. Publish only a fully started manager.
            Shutdown during a slow initializer cleans up its late result rather
            than leaving a manager running after its listener has stopped.
            """
            manager = None
            published = False
            try:
                checkForThreadCurrentWorkerInterruption()
                manager = self._initialize_manager()
                checkForThreadCurrentWorkerInterruption()
                with self._lock:
                    if self._state != 'starting':
                        raise WorkerInterrupted('Server startup was stopped')
                    manager.start()
                    self._manager = manager
                    self._state = 'running'
                    published = True
                wfmanLog('Workflow manager ready')
            except WorkerInterrupted:
                with self._lock:
                    if self._state == 'starting':
                        self._state = 'failed'
            except Exception as exc:
                with self._lock:
                    if self._state != 'stopped':
                        self._state = 'failed'
                wfmanLog(f'Server manager initialization failed: {type(exc).__name__}: {exc}')
            finally:
                if manager is not None and not published:
                    manager.stop(wait_until_done=False, close_database=True)

        with self._lock:
            if self._state == 'starting' or (self._state == 'running' and self._manager.isAlive()):
                return
            if self._state != 'created':
                raise RuntimeError('ServerRuntime cannot be restarted; create a new server runtime')
            self._state = 'starting'
            try:
                self._startup_worker = self._workers.start('startup', initialize)
            except BaseException:
                self._state = 'failed'
                self.queries.close()
                raise

    def stop(self) -> None:
        """Shut down questions, startup and the manager without canceling remote jobs.

        Take no arguments; return None. Close the queue before releasing waiting
        workers, and close manager storage. Repeated stop is safe. A slow API
        call may finish later; authentication is explicitly stopped so late
        replies cannot prompt after shutdown. Startup cannot publish a late
        manager. The caller restores its query hook after this method returns.
        """
        from ..iri_api.auth import stopAuthenticationWorkers

        with self._lock:
            if self._state == 'stopped':
                return
            self._state = 'stopped'
            manager = self._manager
            self._manager = None
            self.queries.close()
            self._workers.stop()
            stop_authentication = self._startup_worker is not None

        # Stop callers before authentication so no active manager/startup worker
        # can register a new login after the authentication registry is stopped.
        try:
            if manager is not None:
                manager.stop(wait_until_done=False, close_database=True)
        finally:
            if stop_authentication:
                stopAuthenticationWorkers()
