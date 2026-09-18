from pathlib import Path
import sqlite3
from .actions_base import ActionClass, actionClass
from .globus_transfer_action import GlobusDataTransfers
from .compute_action_manager import ComputeActions
from .action_manager import ActionStatus, _ser, _unser
from .logging import wfmanLog
import time
import json
from typing import Tuple

class JobData:
    """The primary component of the workflow manager. It provides submission, tracking and updating of workflows backed by a database. State is updated upon calls to its functions."""

    def __init__(self, filename: str | None = None, max_workflows_active=10):
        db_path = ":memory:" if filename is None else str(Path(filename).expanduser())
        self.max_workflows_active = max_workflows_active
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        
        self.action_man = { ActionClass.TRANSFER : GlobusDataTransfers(self.conn),
                            ActionClass.COMPUTE : ComputeActions(self.conn) }

        with self.conn as conn:        
            conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
            job_id INTEGER PRIMARY KEY,
            job_group TEXT,
            workflow BLOB NOT NULL,
            workflow_stage INTEGER NOT NULL,
            head_action_type TEXT,
            head_action_class TEXT,
            head_action_status TEXT,
            head_action_id INTEGER,
            last_status_change INTEGER
            )
            """)
       
    def enqueueJob(self, workflow, job_group = None):
        """Insert a job workflow into the queued workflows but do not start it"""
        if not isinstance(workflow, list):
            workflow = [workflow]
        assert len(workflow) > 0
        with self.conn as conn:        
            cur = conn.execute("INSERT INTO jobs(job_group, workflow, workflow_stage, head_action_type, head_action_class, head_action_status, last_status_change) VALUES (?,?,?,?,?,?,?)",
                               (job_group, _ser(workflow), -1,
                                type(workflow[0]).__name__,
                                actionClass(workflow[0]).name,
                                ActionStatus.PENDING.name,
                                int(time.time())
                                ) )
            job_id = cur.lastrowid
            return job_id
   
    def jobState(self, job_id):
        """Return the complete state of the workflow"""
        with self.conn as conn:
            row = dict(conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone())
            row['head_action_status'] = ActionStatus[row['head_action_status']]
            row['head_action_class'] = ActionClass[row['head_action_class']]
            return row

    def jobStatus(self, job_id)->ActionStatus:
        """Return the status of the workflow"""
        state = self.jobState(job_id)
        if state['head_action_status'] == ActionStatus.PENDING:
            return ActionStatus.PENDING
        elif state['head_action_status'] == ActionStatus.FAILED:
            return ActionStatus.FAILED
        elif state['head_action_class'] == ActionClass.NONE and state['head_action_status'] == ActionStatus.COMPLETED:
            return ActionStatus.COMPLETED
        else:
            return ActionStatus.ACTIVE
 
    def progressWorkflows(self, condition: Tuple[str, list | None]):
        """
        Find actions matching a certain condition and initiate the next stage of the workflow
        condition:
           ("COMPLETE",None) - Progress all active workflows whose head action status is ActionStatus.COMPLETED and for which there are remaining workflow actions
           ("VALID_IN", [job_id1, job_id2, ...]) - Progress valid workflows (those whose head action status is either ActionStatus.PENDING or ActionStatus.COMPLETED, not in a failure state) based on a list of job indices.
        """

        pending_actions = []        
        completed_actions = [] #list of action ids of completed actions
        
        with self.conn as conn:
            if condition[0] == "COMPLETE" and condition[1] == None:
                progress_actions = conn.execute("SELECT job_id, workflow, workflow_stage, head_action_type, head_action_status, head_action_id, head_action_class FROM jobs WHERE head_action_class != ? AND head_action_status = ?",
                                                (ActionClass.NONE.name,ActionStatus.COMPLETED.name)).fetchall()
            elif condition[0] == "VALID_IN" and isinstance(condition[1],list):
                placeholders = ",".join("?" for _ in condition[1])
                progress_actions = conn.execute(f"SELECT job_id, workflow, workflow_stage, head_action_type, head_action_status, head_action_id, head_action_class FROM jobs WHERE head_action_class != ? AND job_id IN ({placeholders}) AND head_action_status IN (?,?)",
                                                ( ActionClass.NONE.name, *condition[1], ActionStatus.PENDING.name, ActionStatus.COMPLETED.name )
                                                )
            else:
                raise Exception("Unknown condition" + str(condition))
                
            for a in progress_actions:              
                #Get information on the next workflow task
                workflow = _unser(a['workflow'])
                workflow_stage = a['workflow_stage']

                #Record completed actions so we can update any monitors
                if a['head_action_status'] == ActionStatus.COMPLETED.name:
                    completed_actions.append(  (a['head_action_id'], getattr(ActionClass, a['head_action_class'], None) ) )
                
                job_id = a['job_id']
                next_workflow_stage = workflow_stage+1

                next_action = None if next_workflow_stage == len(workflow) else workflow[next_workflow_stage]
                next_action_class = ActionClass.NONE if next_action == None else actionClass(next_action)
                next_action_status = ActionStatus.COMPLETED if next_action == None else ActionStatus.PENDING
                
                wfmanLog(f"Progressing job {job_id} action {a['head_action_type']} status {a['head_action_status']} to action {type(next_action).__name__}")
                
                #Update the next action and put into pending status
                conn.execute("UPDATE jobs SET head_action_type = ?, head_action_class = ?, head_action_status = ?, head_action_id = ?, last_status_change = ?, workflow_stage = ? WHERE job_id = ?",
                             (type(next_action).__name__,  next_action_class.name, next_action_status.name, -1, int(time.time()), next_workflow_stage, job_id )
                              )

                #Gather information to initiate next action
                if next_action_status == ActionStatus.PENDING:
                    pending_actions.append( (next_action_class, next_action, job_id ) )


        # #Inform GUI regarding completed actions (requires database activity)
        # for action_id, action_class in completed_actions:
        #     info = self.action_man[action_class].getActionInfo(action_id)
        #     if action_class == ActionClass.TRANSFER:
        #         updateGUI('update_transfer', json.dumps(info))
        #     elif action_class == ActionClass.COMPUTE:
        #         updateGUI('update_compute', json.dumps(info))

        #Initiate the required actions
        head_action_updates = [] #(job_id, head_action_id, head_action_status, head_action_class)

        for action_class, action, job_id in pending_actions:
            wfmanLog(f"Initiating action of type {action_class.name} for {job_id}")
            aman = self.action_man[action_class]
            action_id = aman.startAction(action, job_id)
            action_status, _ = aman.queryStatus(action_id)
            head_action_updates.append( (job_id, action_id, action_status, action_class) )
            
        ####WARNING: If the manager is killed here we can think the action is pending but it is already underway. The action DBs will know about it but not the main DB. How to fix?
        #Maybe instead of PENDING we have some other marker, e.g. SCHEDULING. Then if we come across an entry with this status we will know to check the action DB to see if it was actually scheduled
        
        #Update job state DB
        with self.conn as conn:
            for job_id, action_id, status, _ in head_action_updates:
                conn.execute("UPDATE jobs SET head_action_status = ?, head_action_id = ?, last_status_change = ? WHERE job_id = ?",
                             (status.name, action_id, int(time.time()), job_id )
                             )

        #Inform GUI regarding new actions (requires database activity)
        # for _, action_id, _, action_class in head_action_updates:
        #     info = self.action_man[action_class].getActionInfo(action_id)
        #     if action_class == ActionClass.TRANSFER:
        #         updateGUI('add_transfer', json.dumps(info))
        #     elif action_class == ActionClass.COMPUTE:
        #         updateGUI('add_compute', json.dumps(info))

                


    def startWorkflows(self, job_ids : list | None = None):
        """
        Start the workflows specified by the list of job ids. If None, additional workflows will be started until the total number of active workflows reaches the maximum
        """
        if job_ids == None:        
            with self.conn as conn:
                count = int(conn.execute("SELECT COUNT(*) FROM jobs WHERE head_action_status = ?", (ActionStatus.ACTIVE.name,) ).fetchone()[0])
                rem =  self.max_workflows_active - count

                if rem > 0:
                    toschedule = conn.execute("SELECT job_id FROM jobs WHERE head_action_status = ? ORDER BY job_id ASC LIMIT ?", (ActionStatus.PENDING.name, rem)).fetchall()                   
                    job_ids = [ j[0] for j in toschedule ]
                    if len(job_ids) > 0:
                        wfmanLog("Number of active workflows",count,"want to activate",rem,"more.\nActivating",len(job_ids),"workflows with job ids", job_ids)

        if job_ids:
            self.progressWorkflows(("VALID_IN",job_ids))
                        


    def progressActiveWorkflows(self):
        """
        Find COMPLETE actions and initiate the next stage of the workflow
        """
        self.progressWorkflows(("COMPLETE",None))
        
    def progressActiveActions(self, poll_freq=30, force_poll=False):
        """
        Find active actions nd try to progress their status.
        poll_freq: control the minimum time lag between manager polls of the API for status updates. Queries within this period return only the cached status.
        force_poll: force the manager to poll the API for status updates, use wisely!

        Return: dict  job_id -> new status
        """       
        with self.conn as conn:
            active_actions = conn.execute("SELECT head_action_id, job_id, head_action_type, head_action_class FROM jobs WHERE head_action_status = ?", (ActionStatus.ACTIVE.name, )).fetchall()

        updates = {}
        for t in active_actions:
            job_id = t['job_id']
            action_class = getattr(ActionClass, t['head_action_class'], None)
            aman = self.action_man[action_class] 
            action_status, _ = aman.queryStatus(t['head_action_id'], update_freq=poll_freq, force_update = force_poll)
            if action_status != ActionStatus.ACTIVE:
                updates[job_id] = action_status
            if job_id in updates:
                wfmanLog(f"Progressed job {job_id} action {t['head_action_type']} of class {action_class.name} to {updates[job_id].name}")
                
        #Update head action state
        if len(updates) > 0:
            with self.conn as conn:
                for job_id, status in updates.items():
                    conn.execute("UPDATE jobs SET head_action_status = ? WHERE job_id = ?",(status.name, job_id))
        return updates


    def progressActiveState(self, poll_freq=30, force_poll=False):
        """
        Updates knowledge of action state and then progresses the workflow for those actions that have completed
        poll_freq: control the minimum time lag between manager polls of the API for status updates. Queries within this period return only the cached status.
        force_poll: force the manager to poll the API for status updates, use wisely!"""
        self.progressActiveActions(poll_freq=poll_freq, force_poll=force_poll)
        self.progressActiveWorkflows()
    
    def countWorkflowsWithStatus(self, statuses : ActionStatus | list[ActionStatus]):
        """
        Count the number of workflows for which the head action (if not none) has a status in the provided list
        """
        
        with self.conn as conn:
            if isinstance(statuses, ActionStatus):            
                return int(conn.execute("SELECT COUNT(*) FROM jobs WHERE head_action_class != ? AND head_action_status = ?", (ActionClass.NONE.name, statuses.name,) ).fetchone()[0])
            elif isinstance(statuses, list):
                placeholders = ",".join("?" for _ in statuses)
                names = [s.name for s in statuses]                
                return int(conn.execute(f"SELECT COUNT(*) FROM jobs WHERE head_action_class != ? AND head_action_status IN ({placeholders})", (ActionClass.NONE.name, *names) ).fetchone()[0])
            else:
                raise Exception("Unexpected type for 'statuses'", type(statuses))

    def getActiveActions(self, action_class : ActionClass)->dict:
        """
        Get all active actions and returning a list of dictionaries, each containing "api_key", "api_status" and other custom fields defined on a per-action basis
        """
        return self.action_man[action_class].getActiveActions()
