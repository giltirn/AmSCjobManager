"""Thread serialization, transaction outcomes and lifetime of shared SQLite storage."""

from concurrent.futures import ThreadPoolExecutor
import sqlite3
from threading import Event
import unittest

from amscjobmanager.database import DatabaseClosed, LockedConnection
from amscjobmanager.action_manager import ActionStatus, ActionStatusGroup, initializeActionStatusViews


class DatabaseTests(unittest.TestCase):
    def setUp(self):
        """Create a shared in-memory database and its test table; return None."""
        self.database = LockedConnection(sqlite3.connect(':memory:', check_same_thread=False))
        self.addCleanup(self.database.close)
        with self.database as conn:
            conn.execute('CREATE TABLE effects (value TEXT)')

    def test_raw_connection_persists_and_transaction_outcomes_are_preserved(self):
        """Yield a normal connection, commit success and roll back a failed block.

        Re-entering the context must retain the same in-memory database, while
        an exception must leave no partially written row behind.
        """
        with self.database as conn:
            self.assertIs(type(conn), sqlite3.Connection)
            original = conn
            conn.execute("INSERT INTO effects VALUES ('committed')")
        with self.assertRaisesRegex(RuntimeError, 'abort'):
            with self.database as conn:
                conn.execute("INSERT INTO effects VALUES ('rolled back')")
                raise RuntimeError('abort')
        with self.database as conn:
            self.assertIs(conn, original)
            self.assertEqual(conn.execute('SELECT value FROM effects').fetchall(), [('committed',)])

    def test_status_views_define_shared_groups_and_workflow_mapping(self):
        """Query both views and verify the SQL rules match the action lifecycle.

        Every enum state must have a workflow mapping. LIVE excludes submission
        and terminal states; EXECUTING adds submission. A completed head maps to
        ACTIVE until JobData explicitly advances beyond the workflow's last stage.
        """
        initializeActionStatusViews(self.database)
        with self.database as conn:
            mapping = dict(conn.execute('SELECT action_status, workflow_status FROM action_status_mapping'))
            groups = conn.execute('SELECT group_name, action_status FROM action_status_groups').fetchall()
        self.assertEqual(mapping, {
            'PENDING': 'PENDING', 'SCHEDULING': 'ACTIVE', 'ACTIVE': 'ACTIVE',
            'PROVIDER_HELD': 'ACTIVE', 'COMPLETED': 'ACTIVE', 'FAILED': 'FAILED',
            'CANCELING': 'CANCELING', 'CANCELED': 'CANCELED',
        })
        self.assertEqual(set(mapping), {status.name for status in ActionStatus})
        self.assertEqual(set(groups), {
            ('LIVE', 'ACTIVE'), ('LIVE', 'PROVIDER_HELD'),
            ('EXECUTING', 'SCHEDULING'), ('EXECUTING', 'ACTIVE'), ('EXECUTING', 'PROVIDER_HELD'),
        })
        self.assertEqual(set(groups), {(group.name, status.name)
                                       for group in ActionStatusGroup for status in group.value})

    def test_status_views_are_read_only_and_initialization_preserves_records(self):
        """Reject view mutations and safely initialize the shared rules repeatedly.

        Action managers and JobData can share a connection; none may overwrite
        existing records or turn library status rules into editable table data.
        """
        initializeActionStatusViews(self.database)
        with self.database as conn:
            conn.execute("INSERT INTO effects VALUES ('retained')")
        initializeActionStatusViews(self.database)
        for statement in (
            "INSERT INTO action_status_groups VALUES ('LIVE', 'FAILED')",
            "UPDATE action_status_mapping SET workflow_status = 'FAILED' WHERE action_status = 'ACTIVE'",
        ):
            with self.subTest(statement=statement), self.assertRaises(sqlite3.OperationalError):
                with self.database as conn:
                    conn.execute(statement)
        with self.database as conn:
            self.assertEqual(conn.execute('SELECT value FROM effects').fetchall(), [('retained',)])
            self.assertEqual(conn.execute("SELECT workflow_status FROM action_status_mapping WHERE action_status = 'ACTIVE'").fetchone(),
                             ('ACTIVE',))

    def test_concurrent_transaction_cannot_commit_another_threads_write(self):
        """Keep a failed transaction isolated from another thread's successful write.

        Pause one writer after insertion. A second writer must wait outside its
        context until the first rolls back, then commit only its own row.
        """
        first_inside, second_ready, second_inside, release = Event(), Event(), Event(), Event()

        def first_writer():
            """Insert a row, wait for the test, then roll it back; return None."""
            with self.database as conn:
                conn.execute("INSERT INTO effects VALUES ('first')")
                first_inside.set()
                if not release.wait(2):
                    raise TimeoutError('Test did not release the first writer')
                raise RuntimeError('abort first')

        def second_writer():
            """Wait for exclusive access and commit a separate row; return None."""
            second_ready.set()
            with self.database as conn:
                second_inside.set()
                conn.execute("INSERT INTO effects VALUES ('second')")

        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(first_writer)
            try:
                self.assertTrue(first_inside.wait(1))
                second = pool.submit(second_writer)
                self.assertTrue(second_ready.wait(1))
                self.assertFalse(second_inside.wait(.05), 'Transactions overlapped')
            finally:
                release.set()
            with self.assertRaisesRegex(RuntimeError, 'abort first'):
                first.result(timeout=2)
            second.result(timeout=2)
        with self.database as conn:
            self.assertEqual(conn.execute('SELECT value FROM effects').fetchall(), [('second',)])

    def test_close_waits_for_access_and_rejects_late_workers(self):
        """Serialize shutdown with an active block and reject subsequent access.

        Hold a context while another thread closes storage. Closure must wait,
        and later worker entry must raise DatabaseClosed rather than use SQLite.
        """
        attempted, closed = Event(), Event()

        def close_database():
            """Request database closure, then signal completion; return None."""
            attempted.set()
            self.database.close()
            closed.set()

        with ThreadPoolExecutor(max_workers=1) as pool:
            with self.database as conn:
                closing = pool.submit(close_database)
                self.assertTrue(attempted.wait(1))
                self.assertFalse(closed.wait(.05), 'Storage closed inside an active block')
                conn.execute("INSERT INTO effects VALUES ('before close')")
            closing.result(timeout=2)
        with self.assertRaises(DatabaseClosed):
            with self.database:
                self.fail('Closed storage was yielded')
        self.database.close()


if __name__ == '__main__':
    unittest.main()
