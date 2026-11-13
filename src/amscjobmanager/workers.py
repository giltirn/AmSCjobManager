"""Shared worker execution for actions, authentication and blocking user queries."""

from __future__ import annotations

from concurrent.futures import Future, TimeoutError
from contextvars import ContextVar
from dataclasses import dataclass, field
from threading import Event, RLock, Thread
from typing import Callable, Hashable


# The ContextVar holds a separate current Worker for each worker thread;
# setting it in one thread does not change the value seen by another.
_thread_current_worker: ContextVar[Worker | None] = ContextVar('amsc_worker', default=None)


class WorkerInterrupted(RuntimeError):
    """Signal interruption of a worker's current call or worker shutdown."""


def getThreadCurrentWorkerInterruptionEvent() -> Event | None:
    """Expose the signal asking a worker's current call to finish running.

    Take no arguments; return this worker's interrupt Event, or None outside a
    worker. It is set for either an interrupt or shutdown. Blocking query hooks
    may wait on it alongside their response slot and raise WorkerInterrupted.
    The signal neither cancels remote work nor changes action/database state.
    """
    worker = _thread_current_worker.get()
    return None if worker is None else worker.interrupt


def checkForThreadCurrentWorkerInterruption() -> None:
    """Interrupt the current worker call if interruption or shutdown was requested.

    Take no arguments; return None if execution may continue, including outside
    a worker. Otherwise raise WorkerInterrupted without clearing either signal.
    The action manager decides whether to complete action cancellation or exit
    for shutdown; this check performs no provider calls or database updates.
    """
    worker = _thread_current_worker.get()
    if worker is not None and (worker.interrupt.is_set() or worker.stop.is_set()):
        raise WorkerInterrupted('Worker call interrupted')


@dataclass
class Worker:
    """Hold an operation's result and cooperative interruption/shutdown signals.

    future (Future) holds the operation's result or exception.
    interrupt (Event) asks the current call to finish running. It can be cleared
    once handled, allowing action cancellation API calls to proceed after normal
    execution has stopped. Setting it does not itself cancel remote work.
    stop (Event) requests permanent shutdown of this worker, without canceling
    remote work. It remains set even when interrupt is cleared, preventing
    execution from continuing or handing off to action cancellation. Workers.stop()
    sets both signals; subsequent work requires a new Worker.
    wake (Event) ends a polling sleep early, including for an explicit poll request.

    Signals apply only to this worker; callers and shared authentication have
    independent lifetimes. They are checked cooperatively and cannot forcibly
    terminate a thread or an in-progress API call. All handles disappear on
    process exit and are never serialized.
    """

    future: Future = field(default_factory=Future)
    interrupt: Event = field(default_factory=Event)
    stop: Event = field(default_factory=Event)
    wake: Event = field(default_factory=Event)


class Workers:
    """Share pending calls by context, retaining each stack until its result is read."""

    def __init__(self, *, blocking: bool = False):
        """Set default result waiting policy; return None.

        blocking (bool), default False, returns unfinished calls immediately.
        Authentication uses True to preserve synchronous Python interfaces.
        """
        self.blocking = blocking
        self._tasks: dict[Hashable, Worker] = {}
        self._lock = RLock()

    def start(self, context: Hashable, operation: Callable, *args, **kwargs) -> Worker:
        """Start/reuse context (Hashable) and return its in-memory Worker handle.

        operation (Callable) and args/kwargs are used for a new worker only.
        Does not wait for completion. Results stay available until explicitly
        consumed or discarded, allowing one supervisor per action. A stopped
        worker is replaced so a later authentication session can start fresh.
        """
        with self._lock:
            worker = self._tasks.get(context)
            if worker is None or worker.stop.is_set():
                worker = Worker()
                self._tasks[context] = worker

                def execute() -> None:
                    """Run the captured call with cooperative interruption; return None.

                    Publish its result/error in the Future. The operation owns
                    any database transitions; this registry only runs the call.
                    """
                    if not worker.future.set_running_or_notify_cancel():
                        return
                    token = _thread_current_worker.set(worker)
                    try:
                        # The entry point handles interruption, including signals
                        # arriving before its thread starts, so it can clean up.
                        worker.future.set_result(operation(*args, **kwargs))
                    except BaseException as exc:
                        worker.future.set_exception(exc)
                    finally:
                        _thread_current_worker.reset(token)

                try:
                    Thread(target=execute, name='amsc-worker', daemon=True).start()
                except BaseException:
                    del self._tasks[context]
                    raise
        return worker

    def run(self, context: Hashable, operation: Callable, *args, **kwargs) -> tuple[bool, object]:
        """Start/reuse context and return (ready, result), or propagate its failure.

        context (Hashable) identifies one operation. operation (Callable) and
        args/kwargs are used only when creating it. ready=False means unfinished
        and result=None; ready=True returns the operation's result, including
        None. Completed results/errors are consumed once. Blocking registries
        wait; an interrupted parent worker can stop waiting for shared authentication
        without interrupting authentication needed by another caller.
        """
        # Serialize the caller's interruption check and registration with stop(),
        # so server shutdown cannot miss a new authentication worker.
        with self._lock:
            checkForThreadCurrentWorkerInterruption()
            worker = self.start(context, operation, *args, **kwargs)
        if not self.blocking and not worker.future.done():
            return False, None
        try:
            # A parent action may stop waiting for shared authentication while
            # other callers still need it. Interrupt only the parent's wait.
            while True:
                checkForThreadCurrentWorkerInterruption()
                try:
                    result = worker.future.result(timeout=.05)
                    checkForThreadCurrentWorkerInterruption()
                    return True, result
                except TimeoutError:
                    if worker.future.done():
                        raise
        finally:
            if worker.future.done():
                with self._lock:
                    if self._tasks.get(context) is worker:
                        del self._tasks[context]

    def interrupt(self, context: Hashable) -> None:
        """Ask a worker to finish its current call while keeping its thread available.

        context (Hashable) identifies the worker. Return None after setting its
        interrupt and wake signals, or do nothing if absent. Does not terminate
        threads, cancel remote jobs or discard results. An action manager uses
        this after persisting CANCELING so the worker can switch from execution
        to _completeCancellation; accepted submissions must still be recorded.
        """
        with self._lock:
            worker = self._tasks.get(context)
            if worker is not None:
                worker.interrupt.set()
                worker.wake.set()

    def discard(self, context: Hashable) -> None:
        """Interrupt and forget context (Hashable); return None.

        Only discard finished supervisors or read-only operations whose late
        results have no effects to recover. Does not forcibly terminate threads.
        """
        with self._lock:
            worker = self._tasks.pop(context, None)
            if worker is not None:
                worker.interrupt.set()

    def stop(self) -> None:
        """Request shutdown of all workers without canceling their remote jobs.

        Take no arguments; return None after setting stop, interrupt and wake
        signals. Unlike interrupt(), stop remains set when execution resumes.
        Running API calls may finish; durable intents remain for restart recovery.
        Query hooks must honor their interrupt Event to wake promptly.
        """
        with self._lock:
            for worker in self._tasks.values():
                worker.stop.set()
                worker.interrupt.set()
                worker.wake.set()


def sleepCurrentThreadWorker(timeout: float | None) -> None:
    """Sleep the current thread's worker until its timeout or an early wake signal.

    timeout (float | None) is seconds to sleep; None waits until signaled.
    Return None. Stop, interrupt and explicit wake signals end the sleep early
    without affecting other workers. Requires a current worker context.
    """
    worker = _thread_current_worker.get()
    if worker is None:
        raise RuntimeError('A supervisor wait requires a worker context')
    try:
        worker.wake.wait(timeout)
    finally:
        worker.wake.clear()


def clearThreadCurrentWorkerInterruptUnlessStopping() -> None:
    """Clear a handled worker interrupt while preserving any shutdown request.

    Take no arguments; clear the current worker's handled interrupt and return
    None, including outside a worker. If its stop signal is set, leave that
    signal intact and raise WorkerInterrupted so the call exits instead.
    Action managers call this only when a recorded CANCELING intent is ready
    for _completeCancellation, after normal execution has finished running or during
    cancellation recovery. Clearing the interrupt lets cancellation API calls
    proceed; it neither resumes a thread/remote job nor changes database state.
    """
    worker = _thread_current_worker.get()
    if worker is not None:
        worker.interrupt.clear()
        if worker.stop.is_set():
            raise WorkerInterrupted('Worker shutdown requested')
