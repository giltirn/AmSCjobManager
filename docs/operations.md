# Operating the manager

[Documentation home](../README.md#documentation)

## Lifecycle and locking

Initialize API configuration before creating work. `JobManager.start()` launches
a background thread that starts eligible workflows and progresses active ones.
Calls to `start()` while it is already running return without starting another
thread.

Always access JobData through the manager's lock while that thread is running:

```python
with manager as jobs:
    state = jobs.jobState(job_id)
    status = jobs.jobStatus(job_id)

# The callable form also acquires the lock.
status = manager(lambda jobs: jobs.jobStatus(job_id))
```

Keep these blocks short. The manager holds the same lock while doing its API
work, so calls from the application may wait for an in-flight request. The
context manager releases the lock on exit; it does not start or stop execution.
An exception raised in its block is currently rewrapped as a generic exception.

A typical remote monitoring loop, after configuration and workflow construction:

```python
import time
from amscjobmanager import ActionStatus, JobManager

manager = JobManager(filename="workflow.sqlite", poll_freq=30,
                     max_workflows_active=4)
try:
    with manager as jobs:
        job_id = jobs.enqueueJob(workflow)
    manager.start()
    while True:
        if not manager.isAlive():
            raise RuntimeError("JobManager thread stopped")
        with manager as jobs:
            status = jobs.jobStatus(job_id)
            state = jobs.jobState(job_id)
        print(status.name, state["workflow_stage"], state["head_action_type"])
        if status in (ActionStatus.COMPLETED, ActionStatus.FAILED,
                      ActionStatus.CANCELED):
            break
        time.sleep(2)
finally:
    manager.stop(wait_until_done=False, close_database=True)
```

An unexpected polling exception can currently terminate the manager thread and
leave its lock held. `isAlive()` is useful for detection, but the separate check
does not remove a race with a subsequent lock acquisition. See
[troubleshooting](troubleshooting.md) for the current limitation.

## Scheduling and polling parameters

| Parameter | Meaning |
| --- | --- |
| `filename=None` | In-memory workflow DB. Use an on-disk filename for remote recovery. |
| `max_workflows_active=10` | Limit used when automatically starting pending workflows. `SCHEDULING`, `ACTIVE`, and `CANCELING` heads occupy slots. `0` disables automatic starts. |
| `poll_freq=30` | Minimum interval in seconds between successful cached-status refreshes for an active action. `0` makes it eligible each progression pass. It is not an API request timeout. |

The manager loop also sleeps between passes. Requests and blocking filesystem
operations can take longer than a polling interval. Deferred compute submissions
can occupy all active slots during maintenance and delay pending workflows for
other resources.

For manual starts, use `jobs.startWorkflows([job_id, ...])` under the lock. An
explicit list starts eligible heads without applying the automatic concurrency
selection. Applications can also use JobData without a background manager and
call `startWorkflows()` and `progressActiveState()` themselves.

## Workflow and action states

| Head-action state | Meaning |
| --- | --- |
| `PENDING` | Workflow has been enqueued but not started. |
| `SCHEDULING` | Scheduling intent is durable; submission, deferred resource checks, or submission reconciliation is in progress. |
| `ACTIVE` | Remote action is live, including jobs queued by the remote scheduler. |
| `COMPLETED` | Head action succeeded. There may be another workflow step to start. |
| `FAILED` | Action failed or submission attempts were exhausted. The workflow will not advance automatically. |
| `CANCELING` | Cancellation intent is durable and remote cancellation/reconciliation remains unresolved. |
| `CANCELED` | Workflow has been canceled, or a remotely canceled compute action has stopped its progression. |

`jobState(job_id)` returns a dictionary containing the head-action state and
class, action ID/type, workflow stage, group, timestamps, and serialized workflow.
The stage is `-1` before the first action and increases as steps are selected.

`jobStatus(job_id)` returns a coarser workflow status. It exposes `PENDING`,
`FAILED`, `CANCELING`, and `CANCELED` directly. It returns `COMPLETED` only when
the workflow has finished all steps and its head class is `NONE`; otherwise it
returns `ACTIVE`. In particular, a head in `SCHEDULING` is reported as an active
workflow. Inspect `head_action_status` for that distinction.

Remote compute status `canceled` maps to `CANCELED`. Globus status `FAILED`
includes remotely canceled transfers, so an external Globus cancellation is
seen as `FAILED`. Globus `INACTIVE` currently also maps to `FAILED`, even though
the suspended remote task may still exist. A workflow canceled through
`removeJob()` retains a `CANCELED` workflow head.

## Submission failures and maintenance

The action manager first tries to recover an earlier submission by its durable
name. A found remote ID is saved before polling it. If lookup itself errors,
reconciliation waits for another pass.

Before a new compute submission, `action.queryResourceStatus()` calls the
compute availability API. A result other than up, or a failed status query,
leaves the intent `SCHEDULING` without consuming a submission attempt. A down
resource also postpones finalizing an exhausted submission as failed. Existing
remote jobs are recovered before this pre-check. Transfers return no resource
availability signal and follow their normal submission policy.

The current action-manager defaults permit three submission attempts with
exponential backoff starting at 30 seconds. An attempt is committed before
`initiateAction()`. If the request fails ambiguously, later reconciliation
searches by name before trying again. Once the attempt limit is reached, a
successful lookup finding no remote work leads to `FAILED` when the resource
check permits it. These defaults are ActionManager settings; JobManager does
not expose constructor arguments for them.

An up pre-check cannot guarantee the subsequent request succeeds. Ordinary API,
credential, and argument errors are currently not classified separately for
retry policy. A failed **execution** after successful submission stops the
workflow; it is not automatically resubmitted.

## Cancellation

```python
with manager as jobs:
    jobs.removeJob(job_id)  # cancel_running=True by default
```

The workflow record remains in the database. For a live compute/transfer action,
the workflow and action first enter `CANCELING` before remote cancellation.
Subsequent passes reconcile unfinished requests. A direct cancellation error is
raised to the caller while leaving its persisted intent available for retry.
Cancellation has no fixed attempt limit.

An ambiguously submitted action is looked up by name without submitting more
work. If indexing may lag, reconciliation waits for the submission backoff
interval before accepting that no task was found. If a remote task is already
terminal, reconciliation avoids another cancel request.

`removeJob(job_id, cancel_running=False)` stops workflow progression by marking
its head `CANCELED` without canceling running remote work. Use that option only
when another owner will manage the remote task. Canceling a workflow does not
delete prior action records, remove files, or undo completed operations.
Local process cancellation is currently unsupported.

## Shutdown and restart

`manager.stop()` defaults to waiting for unfinished workflows to settle before
stopping its thread. Failed or canceled workflows do not need to become
successful. Resource downtime, unresolved cancellation, or a stopped thread can
prevent a waiting shutdown from finishing.

`manager.stop(wait_until_done=False)` requests loop shutdown and joins the thread;
it still waits for an in-flight request. It does not cancel remote jobs or
transfers. `close_database=True` closes the workflow DB after the thread stops.
Local worker threads have their own lifetime.

To resume remote work, initialize the same API configuration and create a new
JobManager with the same database filename. Start it without enqueuing the old
workflow again. Scheduling/cancellation intents are reconciled and active heads
are polled. Previously `FAILED` or `CANCELED` workflows stay stopped; there is no
public automatic retry/resume operation for those states.

Keep database files private and action definitions compatible: reopening
unpickles stored Python objects. Use one manager process per workflow database.
Remote names currently contain only the action ID; independent databases can
reuse those IDs, so overlapping managers against the same remote account can
produce ambiguous name/label discovery. Cross-service recovery is not an
exactly-once execution guarantee.

## Logging

The default manager/API log hooks print to the console. Redirect them before
starting the manager if your application needs durable logs:

```python
import logging
from amscjobmanager import logging as manager_logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("amscjobmanager")
manager_logging.wfman_log_func = lambda *parts: logger.info(" ".join(map(str, parts)))
manager_logging.api_log_func = lambda *parts: logger.info(" ".join(map(str, parts)))
```

Some execution/transfer paths also use `print` directly. These hooks do not
capture scheduler output files or replace the job's `run.log`/`err.log`.
