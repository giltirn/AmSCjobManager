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
    ACTIVE = 1 #a live action (any status not failed or completed, e.g. queued, new, etc)
    COMPLETED = 2 #action completed successfully
    FAILED = 3 #action failed
    
class ActionManager:
    """A database-backed manager for initiating and querying action status"""

    def _queryStatusInternal(self, machine, api_key):
        """Return the API status"""        
        raise NotImplementedError("Derived class must implement _queryStatusInternal")
    
    def __init__(self, connection : sqlite3.Connection, table_name, api_action_status_map : dict):
        """
        api_action_status_map : map between return status from the API to an ActionStatus
        """
        self.conn = connection
        self.table_name = table_name
        self.api_action_status_map = api_action_status_map #
        
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
            job_id INTEGER
            )
            """)

    def startAction(self, action, job_id):
        """Initiate the action and insert into the database"""
        api_key = action.initiateAction(job_id)
        api_status = self._queryStatusInternal(action.machine, api_key)
        action_status = self.api_action_status_map[api_status]
        
        with self.conn as conn:        
            cur = conn.execute(f"INSERT INTO {self.table_name}(machine, details, api_key, api_status, action_status, last_update, job_id) VALUES (?,?,?,?,?,?,?)",
                               (action.machine, _ser(action), api_key, api_status, action_status.name, int(time.time()), job_id)
                               )
            action_id = cur.lastrowid
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
