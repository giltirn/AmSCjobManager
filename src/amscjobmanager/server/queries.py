"""In-memory user questions and one-time responses for a server session."""

from dataclasses import dataclass
from threading import Condition, Event
import time
from uuid import uuid4

from ..workers import WorkerInterrupted, checkForThreadCurrentWorkerInterruption, getThreadCurrentWorkerInterruptionEvent


class QueryResponseError(ValueError):
    """A response cannot be delivered to a pending question in this session."""

    def __init__(self, code: str, message: str):
        """Create a reply error with public code and message strings; return None."""
        super().__init__(message)
        self.code = code


@dataclass
class _PendingQuery:
    """One question: title/prompt strings, Unix creation time and worker interrupt.

    response is None until answered, including when an empty string is a valid
    answer. interrupt is the asking worker's Event, or None outside a worker.
    Neither responses nor question metadata are persisted.
    """

    title: str
    prompt: str
    created_at: float
    interrupt: Event | None
    response: str | None = None


class UserQueryQueue:
    """Route blocking user questions to a session-scoped, thread-safe inbox."""

    def __init__(self):
        """Create an empty inbox with a fresh session UUID; return None.

        session_id (str) identifies this lifetime; query IDs are positive integers
        allocated sequentially within it. Questions are never stored in SQLite.
        """
        self.session_id = str(uuid4())
        self._condition = Condition()
        self._pending = {}
        self._next_id = 1
        self._closed = False

    def ask(self, title: str, query: str) -> str:
        """Publish a question and wait for its response without blocking other callers.

        title (str) describes the request; query (str) contains the prompt or
        authorization instructions. Return the supplied response string exactly.
        This matches the wfapiUserQuery hook. Interruption/shutdown raises
        WorkerInterrupted and invalidates the request. No response is logged.
        """
        checkForThreadCurrentWorkerInterruption()
        with self._condition:
            if self._closed:
                raise WorkerInterrupted('Server query queue is closed')
            query_id = self._next_id
            self._next_id += 1
            pending = _PendingQuery(title, query, time.time(), getThreadCurrentWorkerInterruptionEvent())
            self._pending[query_id] = pending
            try:
                # Replies notify immediately. Timed waits also notice worker
                # interruption, whose Event does not notify this Condition.
                while pending.response is None:
                    checkForThreadCurrentWorkerInterruption()
                    if self._closed:
                        raise WorkerInterrupted('Server query queue is closed')
                    self._condition.wait(.1)
                checkForThreadCurrentWorkerInterruption()
                if self._closed:
                    raise WorkerInterrupted('Server query queue is closed')
                return pending.response
            finally:
                self._pending.pop(query_id, None)
                pending.response = None

    def listPending(self) -> list[dict]:
        """List unanswered questions in ascending query-ID order.

        Take no arguments. Return list[dict] with session_id (UUID str), query_id
        (positive int), title/prompt (str), and created_at (Unix seconds, float).
        Interrupted, answered and closed requests are omitted. Response contents
        are never included, and listing does not consume a pending question.
        """
        with self._condition:
            if self._closed:
                return []
            return [dict(session_id=self.session_id, query_id=query_id, title=item.title,
                         prompt=item.prompt, created_at=item.created_at)
                    for query_id, item in self._pending.items()
                    if item.response is None and (item.interrupt is None or not item.interrupt.is_set())]

    def respond(self, session_id: str, query_id: int, response: str) -> None:
        """Deliver a response once to a still-pending question in this session.

        session_id (UUID str) and query_id (positive int) come from listPending;
        response (str) is the answer, including an empty string if intended.
        Return None after delivery, without validating authentication or running
        API calls on this thread. QueryResponseError reports stale sessions,
        closed queues and nonpending requests. No response is logged or retained
        after the asking worker consumes it.
        """
        with self._condition:
            if session_id != self.session_id:
                raise QueryResponseError('stale_session', 'The query belongs to a different server session')
            if self._closed:
                raise QueryResponseError('query_queue_closed', 'The server query queue is closed')
            pending = self._pending.get(query_id)
            if (pending is None or pending.response is not None or
                    (pending.interrupt is not None and pending.interrupt.is_set())):
                raise QueryResponseError('query_not_pending', 'The query is no longer pending')
            pending.response = response
            self._condition.notify_all()

    def close(self) -> None:
        """Invalidate this inbox and interrupt every waiting question.

        Take no arguments; return None. Repeated closure is harmless. Pending
        response contents are discarded, and no later request/reply is accepted.
        This does not cancel remote jobs or change durable workflow records.
        """
        with self._condition:
            self._closed = True
            for item in self._pending.values():
                item.response = None
            self._pending.clear()
            self._condition.notify_all()
