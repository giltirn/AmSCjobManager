import pickle
import sqlite3
import time
from enum import Enum

def _ser(obj):
    return pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
def _unser(ser):
    return pickle.loads(ser)

class ActionStatus(Enum):
    PENDING = 0 #not yet started
    SCHEDULING = 1 #submission is in progress and must be reconciled after a crash
    ACTIVE = 2 #a live action (any status not failed or completed, e.g. queued, new, etc)
    COMPLETED = 3 #action completed successfully
    FAILED = 4 #action failed


class SimulatedCrash(RuntimeError):
    """Test-only exception raised at an explicitly requested crash boundary."""

    def __init__(self, point):
        self.point = point
        super().__init__(f"Simulated crash at {point}")
    
class ActionManager:
    """A database-backed manager for initiating and querying action status"""

    def _queryStatusInternal(self, machine, api_key):
        """Return the API status"""        
        raise NotImplementedError("Derived class must implement _queryStatusInternal")

    def _findRemoteActionByName(self, machine, name):
        """Return the remote API key for an action name, or None if absent."""
        return None

    @staticmethod
    def _remoteActionName(action_id):
        """Return the deterministic name used to find an action after a crash."""
        return f"amscjm:{action_id}"
    
    def __init__(self, connection : sqlite3.Connection, table_name, api_action_status_map : dict,
                 *, _test_crash_at: str | None = None,
                 scheduling_retry_delay=30, max_submission_attempts=3):
        """
        api_action_status_map : map between return status from the API to an ActionStatus
        """
        self.conn = connection
        self.table_name = table_name
        self.api_action_status_map = api_action_status_map #
        self._test_crash_at = _test_crash_at
        self.scheduling_retry_delay = scheduling_retry_delay
        self.max_submission_attempts = max_submission_attempts
        
        with self.conn as conn:        
            conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
            action_id INTEGER PRIMARY KEY,
            machine TEXT,
            details BLOB,
            api_key TEXT,
            api_status TEXT,
            action_status TEXT,
            last_update INTEGER,
            job_id INTEGER,
            submission_attempts INTEGER NOT NULL DEFAULT 0,
            last_submission_attempt INTEGER,
            last_submission_error TEXT
            )
            """)

    def _maybeSimulateCrash(self, point):
        if point == self._test_crash_at:
            raise SimulatedCrash(point)

    def recordSchedulingIntent(self, action, job_id, action_id):
        """Persist a SCHEDULING action row before contacting a remote API."""
        with self.conn as conn:
            existing = conn.execute(
                f"SELECT action_id FROM {self.table_name} WHERE action_id = ?",
                (action_id,),
            ).fetchone()
            if existing is not None:
                return existing["action_id"]

            conn.execute(
                f"INSERT INTO {self.table_name}(action_id, machine, details, api_key, api_status, action_status, last_update, job_id) VALUES (?,?,?,?,?,?,?,?)",
                (
                    action_id,
                    getattr(action, "machine", None),
                    _ser(action),
                    None,
                    None,
                    ActionStatus.SCHEDULING.name,
                    int(time.time()),
                    job_id,
                ),
            )
        self._maybeSimulateCrash("action_intent_persisted")
        return action_id

    def submitScheduledAction(self, action_id):
        """Submit an action whose durable scheduling intent was recorded."""
        with self.conn as conn:
            entry = conn.execute(
                f"SELECT details, job_id, action_status, api_key FROM {self.table_name} WHERE action_id = ?",
                (action_id,),
            ).fetchone()
        if entry is None:
            raise Exception(f"Unknown action {action_id}")
        if entry["api_key"] is not None:
            return action_id
        if entry["action_status"] != ActionStatus.SCHEDULING.name:
            raise Exception(f"Action {action_id} is not awaiting submission")

        # Record the attempt before the remote call.  If the process dies in
        # the call, reconciliation can use this timestamp for retry backoff.
        with self.conn as conn:
            conn.execute(
                f"UPDATE {self.table_name} SET submission_attempts = submission_attempts + 1, "
                "last_submission_attempt = ?, last_submission_error = NULL WHERE action_id = ?",
                (int(time.time()), action_id),
        )

        action = _unser(entry["details"])
        self._maybeSimulateCrash("before_remote_submission")
        try:
            api_key = action.initiateAction(
                entry["job_id"], name=self._remoteActionName(action_id)
            )
        except SimulatedCrash:
            raise
        except Exception as exc:
            with self.conn as conn:
                conn.execute(
                    f"UPDATE {self.table_name} SET last_submission_error = ? WHERE action_id = ?",
                    (str(exc), action_id),
                )
                attempts = conn.execute(
                    f"SELECT submission_attempts FROM {self.table_name} WHERE action_id = ?",
                    (action_id,),
                ).fetchone()["submission_attempts"]
            if attempts >= self.max_submission_attempts:
                return self._markSchedulingFailed(action_id, str(exc))
            # The action remains SCHEDULING.  The recurring reconciliation
            # pass will retry it after the persisted backoff interval.
            return action_id
        self._maybeSimulateCrash("remote_submission_completed")
        api_status = self._queryStatusInternal(getattr(action, "machine", None), api_key)
        self._maybeSimulateCrash("remote_status_obtained")
        action_status = self.api_action_status_map[api_status]

        with self.conn as conn:        
            conn.execute(
                f"UPDATE {self.table_name} SET api_key = ?, api_status = ?, action_status = ?, last_update = ? WHERE action_id = ?",
                (api_key, api_status, action_status.name, int(time.time()), action_id),
            )
        self._maybeSimulateCrash("action_state_persisted")
        return action_id

    def startAction(self, action, job_id, action_id):
        """Record then submit an action using its JobData-issued ID."""
        action_id = self.recordSchedulingIntent(action, job_id, action_id)
        return self.submitScheduledAction(action_id)

    def hasAction(self, action_id):
        """Return whether this manager has persisted the supplied action ID."""
        with self.conn as conn:
            return conn.execute(
                f"SELECT 1 FROM {self.table_name} WHERE action_id = ?", (action_id,)
            ).fetchone() is not None

    def reconcileScheduledAction(self, action_id):
        """Reconcile a SCHEDULING row, retrying an unsubmitted action safely."""
        with self.conn as conn:
            action = conn.execute(
                f"SELECT machine, api_key, action_status, submission_attempts, "
                f"last_submission_attempt FROM {self.table_name} WHERE action_id = ?",
                (action_id,),
            ).fetchone()
        if action is None:
            raise Exception(f"Unknown action {action_id}")
        if action["action_status"] != ActionStatus.SCHEDULING.name:
            return action_id

        api_key = action["api_key"] or self._findRemoteActionByName(
            action["machine"], self._remoteActionName(action_id)
        )
        if api_key is None:
            if action["submission_attempts"] >= self.max_submission_attempts:
                return self._markSchedulingFailed(
                    action_id,
                    "Remote action was not found after the maximum number of submission attempts",
                )

            if not self._retryAllowed(action):
                return None

            try:
                return self.submitScheduledAction(action_id)
            except Exception as exc:
                with self.conn as conn:
                    attempts = conn.execute(
                        f"SELECT submission_attempts FROM {self.table_name} WHERE action_id = ?",
                        (action_id,),
                    ).fetchone()["submission_attempts"]
                if attempts >= self.max_submission_attempts:
                    return self._markSchedulingFailed(action_id, str(exc))
                return None

        api_status = self._queryStatusInternal(action["machine"], api_key)
        action_status = self.api_action_status_map[api_status]
        with self.conn as conn:
            conn.execute(
                f"UPDATE {self.table_name} SET api_key = ?, api_status = ?, action_status = ?, last_update = ? WHERE action_id = ?",
                (api_key, api_status, action_status.name, int(time.time()), action_id),
            )
        return action_id

    def _retryAllowed(self, action):
        """Apply exponential backoff while remote action indexing catches up."""
        if action["submission_attempts"] == 0:
            return True
        retry_delay = self.scheduling_retry_delay * (
            2 ** (action["submission_attempts"] - 1)
        )
        return int(time.time()) >= action["last_submission_attempt"] + retry_delay

    def _markSchedulingFailed(self, action_id, error):
        with self.conn as conn:
            conn.execute(
                f"UPDATE {self.table_name} SET api_status = ?, action_status = ?, "
                "last_submission_error = ?, last_update = ? WHERE action_id = ?",
                (error, ActionStatus.FAILED.name, error, int(time.time()), action_id),
            )
        return action_id


    def updateStatuses(self):
        """Force update of all active statuses"""
        with self.conn as conn:
            actions = conn.execute(f"SELECT action_id, api_key, machine FROM {self.table_name} WHERE action_status = ?", (ActionStatus.ACTIVE.name,) ).fetchall()
            for t in actions:
                api_status = self._queryStatusInternal(t['machine'],t['api_key'])
                action_status = self.api_action_status_map[api_status]

                conn.execute(f"UPDATE {self.table_name} SET api_status = ?, action_status = ?, last_update = ? WHERE action_id = ?", (api_status, action_status.name, int(time.time()), t['action_id']) )

    def __str__(self):
        with self.conn as conn:
            actions = conn.execute(f"SELECT api_key, action_status, api_status, last_update FROM {self.table_name}").fetchall()
            out = ""
            for t in actions:
                out = out + f"({t['api_key']},{t['action_status']}[ {t['api_status']} ],{t['last_update']})\n"
            return out
                    
            
    def queryStatus(self, action_id, update_freq=30, force_update=False):
        """
        Query the status of an action by id. This will use the last known status unless it has been more than update_freq seconds since the last poll or force_update == True
        Return: action_status, api_status
        """
        with self.conn as conn:
            action = conn.execute(f"SELECT action_status, api_status, last_update, machine, api_key FROM {self.table_name} WHERE action_id = ?", (action_id,) ).fetchone()
            action_status = getattr(ActionStatus, action['action_status'],None)
            api_status = action['api_status']
            if force_update or (int(time.time()) > action['last_update'] + update_freq):
                api_status = self._queryStatusInternal(action['machine'],action['api_key'])
                action_status = self.api_action_status_map[api_status]
                
                conn.execute(f"UPDATE {self.table_name} SET api_status = ?, action_status = ?, last_update = ? WHERE action_id = ?", (api_status, action_status.name, int(time.time()), action_id) )
            return action_status, api_status
        
    def waitForAction(self, action_id, check_freq=30):
        """Blocking wait until the action either completes or fails. Status checks are performed every check_freq seconds. Return the final status."""
        while( (action_status := self.queryStatus(action_id,force_update=True)[0] ) == ActionStatus.ACTIVE):
            time.sleep(check_freq)
        return action_status

    def getActiveActions(self):
        """
        Get all active actions and returning a list of dictionaries, each containing "api_key", "api_status" and other custom fields defined on a per-action basis
        """
        out = []
        with self.conn as conn:
            entries = conn.execute(f"SELECT job_id, api_key, api_status, details FROM {self.table_name} WHERE action_status = ?", (ActionStatus.ACTIVE.name,) ).fetchall()
            for entry in entries:
                action = _unser(entry['details'])
                dc = action.getInfo()
                dc['job_id'] = entry['job_id']
                dc['api_key'] = entry['api_key']
                dc['api_status'] = entry['api_status']
                out.append(dc)
        return out

    def getActionInfo(self, action_id)->dict:
        """
        For the given action, return a dictionary containing "api_key", "api_status" and other custom fields defined on a per-action basis
        """        
        with self.conn as conn:
            entry = conn.execute(f"SELECT job_id, details, api_status, api_key FROM {self.table_name} WHERE action_id = ?", (action_id,) ).fetchone()
            action = _unser(entry['details'])
            dc = action.getInfo()
            dc['job_id'] = entry['job_id']
            dc['api_key'] = entry['api_key']
            dc['api_status'] = entry['api_status']
            return dc
