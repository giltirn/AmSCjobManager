"""Serialize access to a shared SQLite connection through explicit context blocks."""

import sqlite3
from threading import RLock


class DatabaseClosed(sqlite3.ProgrammingError):
    """The owning manager closed storage before a stopped supervisor finished running."""


class LockedConnection:
    """Lock and yield one persistent SQLite connection for each context block.

    All statements and result fetching must finish inside ``with wrapper as
    conn``. The yielded conn is an ordinary sqlite3.Connection; no SQL methods
    are exposed on the wrapper. Keep API calls and user waits outside blocks.
    """

    def __init__(self, connection: sqlite3.Connection):
        """Take ownership of a connection and initialize its shared access lock.

        connection (sqlite3.Connection) must allow cross-thread use when shared
        by workers. Return None; keep the connection open until close() is called.
        """
        self._connection = connection
        self._lock = RLock()
        self._closed = False

    def __enter__(self) -> sqlite3.Connection:
        """Acquire exclusive access and yield the underlying SQLite connection.

        Take no arguments; return sqlite3.Connection, to be used only inside
        this context block. Raise DatabaseClosed if storage was already closed.
        """
        self._lock.acquire()
        try:
            if self._closed:
                raise DatabaseClosed('Workflow database is closed')
            return self._connection.__enter__()
        except BaseException:
            self._lock.release()
            raise

    def __exit__(self, exc_type, exc_value, traceback):
        """Commit or roll back the block and release exclusive connection access.

        exc_type (type | None), exc_value (BaseException | None) and traceback
        describe a block failure; all None means success. Return SQLite's context
        result without suppressing errors. The connection remains open.
        """
        try:
            return self._connection.__exit__(exc_type, exc_value, traceback)
        finally:
            self._lock.release()

    def close(self) -> None:
        """Close storage after any current context block finishes.

        Take no arguments; return None. Repeated calls are harmless, and later
        context entries raise DatabaseClosed so stopped workers cannot write.
        """
        with self._lock:
            if not self._closed:
                self._connection.close()
                self._closed = True
