# Operating the manager

[Documentation home](../README.md#documentation)

## Lifecycle and locking

Initialize API configuration before constructing a manager: opening an existing
database immediately recovers workers for unfinished actions. `WorkflowManager.start()` launches
a background thread that starts eligible workflows and progresses active ones.
Calls to `start()` while it is already running return without starting another
thread.

Always access WorkflowData through the manager's lock while that thread is running:

```python
with manager as workflows:
    state = workflows.workflowState(workflow_id)
    status = workflows.workflowStatus(workflow_id)

# The callable form also acquires the lock.
status = manager(lambda workflows: workflows.workflowStatus(workflow_id))
```

Keep these blocks short. The manager holds the same lock while doing its API
work, so calls from the application may wait for an in-flight request. The
context manager releases the lock on exit; it does not start or stop execution.
An exception raised in its block is currently rewrapped as a generic exception.

A typical remote monitoring loop, after configuration and workflow construction:

```python
import time
from amscjobmanager import ActionStatus, WorkflowManager

manager = WorkflowManager(filename="workflow.sqlite", poll_freq=30,
                     max_workflows_active=4)
try:
    with manager as workflows:
        workflow_id = workflows.enqueueWorkflow(workflow)
    manager.start()
    while True:
        if not manager.isAlive():
            raise RuntimeError("WorkflowManager thread stopped")
        with manager as workflows:
            status = workflows.workflowStatus(workflow_id)
            state = workflows.workflowState(workflow_id)
        print(status.name, state["workflow_stage"], state["head_action_type"])
        if status in (ActionStatus.COMPLETED, ActionStatus.FAILED,
                      ActionStatus.CANCELED):
            break
        time.sleep(2)
finally:
    manager.stop(wait_until_done=False, close_database=True)
```

Failures from individual status polls are logged and deferred, allowing the
manager to keep polling other workflows. Unexpected errors outside polling can
still terminate the background thread; `isAlive()` helps detect those failures.
See [troubleshooting](troubleshooting.md).

## Scheduling and polling parameters

| Parameter | Meaning |
| --- | --- |
| `filename=None` | In-memory workflow DB. Use an on-disk filename for remote recovery. |
| `max_workflows_active=10` | Limit used when automatically starting pending workflows. ACTIVE and CANCELING workflows occupy slots, including those with provider-held heads. `0` disables automatic starts. |
| `poll_freq=30` | Worker delay in seconds between provider API status requests or recovery attempts. `0` uses a 0.01-second minimum delay. Set at manager construction; recorded-status reads do not change it. It is not an API request timeout. |

The manager loop also sleeps between workflow passes. API requests and blocking
filesystem operations can take longer than a supervisor interval. Deferred compute submissions
can occupy all active slots during maintenance and delay pending workflows for
other resources.

A failed status poll leaves the last known action/workflow state and successful
update timestamp unchanged. It does not consume a submission attempt or add a
failure state. The supervisor retries after its normal delay;
no extra poll-failure counter or backoff is maintained. Only a successful status
read can authorize the next workflow step. Direct status API calls can still
raise errors to their callers.

For manual starts, use `workflows.startWorkflows([workflow_id, ...])` under the lock. An
explicit list starts eligible heads without applying the automatic concurrency
selection. Applications can also use WorkflowData without a background manager and
call `startWorkflows()` and `progressActiveState()` themselves.

## Workflow and action states

| Head-action state | Meaning |
| --- | --- |
| `PENDING` | Action is queued, including dependency waits before the first stage or between stages. Its workflow consumes no active slot. |
| `SCHEDULING` | Scheduling intent is durable; submission, deferred resource checks, or submission reconciliation is in progress. |
| `ACTIVE` | Remote action is live, including jobs queued by the remote scheduler. |
| `PROVIDER_HELD` | Provider accepted the action but suspended execution. It remains live, polled, and cancellable. |
| `COMPLETED` | Head action succeeded. There may be another workflow step to start. |
| `FAILED` | Action failed or submission attempts were exhausted. The workflow will not advance automatically. |
| `CANCELING` | Cancellation intent is durable and remote cancellation/reconciliation remains unresolved. |
| `CANCELED` | Workflow has been canceled, or a remotely canceled compute action has stopped its progression. |

`workflowState(workflow_id)` returns a dictionary containing the head-action state and
class, action ID/type, workflow stage, group, timestamps, and overall workflow
status. Serialized action definitions are not loaded or returned.
The stage is `-1` before the first action and increases as steps are selected.

`workflowStatus(workflow_id)` reads the persisted overall workflow status: PENDING,
ACTIVE, COMPLETED, FAILED, CANCELING, or CANCELED. Workflows remain ACTIVE while
their head is SCHEDULING, ACTIVE, PROVIDER_HELD, or an intermediate COMPLETED
stage. COMPLETED is recorded after progression advances past the final stage.
Inspect `head_action_status` for the separate execution state.

Use `headActionStatus(workflow_id)` for the recorded head action status, including
`SCHEDULING` and intermediate `COMPLETED`. With `return_api_status=True` it
returns `(action_status, api_status)`, where the latter is the provider status
string or `None` when unavailable. Set `force_poll=True` to wake supervisors
for all active or provider-held heads and copy their currently recorded status without advancing
workflow steps. The requested polls may finish after this call returns; read
again to observe their results. Failed polls retain the last known state and
are logged.
Call under the `WorkflowManager` lock while its background thread is running.
Compute `ACTIVE` includes provider states `new`, `queued`, and `active`;
only the provider status distinguishes waiting from execution.

Remote compute status `canceled` maps to `CANCELED`. Globus status `FAILED`
includes remotely canceled transfers, so an external Globus cancellation is
seen as `FAILED`. Globus `INACTIVE` and IRI compute `held` map to
`PROVIDER_HELD`. Held actions continue polling and may return to ACTIVE or
reach a terminal status without resubmission. Their workflows remain ACTIVE
and cannot advance to later stages until the held action completes successfully.
Normal cancellation stops held actions; terminal failed transfers require no
remote cancellation. Canceling a workflow preserves previously completed action
history while recording the overall workflow as CANCELING or CANCELED.

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
check permits it. These defaults are AsyncActionManager settings; WorkflowManager does
not expose constructor arguments for them.

An up pre-check cannot guarantee the subsequent request succeeds. Ordinary API,
credential, and argument errors are currently not classified separately for
retry policy. A failed **execution** after successful submission stops the
workflow; it is not automatically resubmitted.

## Cancellation

```python
with manager as workflows:
    workflows.cancelWorkflow(workflow_id)
```

The workflow record remains in the database. Every affected workflow first enters
`CANCELING`, with its action rows unchanged. `WorkflowData.performPendingCancellations()` cancels pending stages
locally, preserves completed/failed history, and delegates executing heads to their
action managers. Those heads enter `CANCELING` before remote cancellation. Once
resolved, the workflow becomes `CANCELED`. Subsequent passes retry unfinished
requests. A direct cancellation error is
raised to the caller while leaving its persisted intent available for retry.
Cancellation has no fixed attempt limit.

An ambiguously submitted action is looked up by name without submitting more
work. If indexing may lag, reconciliation waits for the submission backoff
interval before accepting that no task was found. If a remote task is already
terminal, reconciliation avoids another cancel request.

Cancellation always stops executing work in the affected workflows. It does not
delete prior action records, remove files, or undo completed operations.
On reopening the database, workflow cancellation is
processed by `performPendingCancellations()` before ordinary execution workers are
restored. An externally canceled head follows the same path to cancel its remaining
queued stages.

## Shutdown and restart

`manager.stop()` defaults to waiting for unfinished workflows to settle before
stopping its thread. Failed or canceled workflows do not need to become
successful. Resource downtime, unresolved cancellation, or a stopped thread can
prevent a waiting shutdown from finishing.

`manager.stop(wait_until_done=False)` requests loop shutdown, joins the workflow
thread, and signals action workers to stop. API requests already in progress may
finish later. Shutdown does not cancel remote jobs or transfers.
`close_database=True` also closes workflow storage; repeated `stop()` calls are
safe even after closure. Local compute processes have their own lifetime.

A `WorkflowManager` is single-use. Calling `start()` on a running instance is harmless,
but calling it after shutdown, failure, or `stop()` before the first start raises
an error. Workers belonging to the stopped instance are not revived.

To resume remote work, initialize the same API configuration and create a new
WorkflowManager with the same database filename. Its WorkflowData constructor restores
supervision of unfinished actions; call `start()` for workflow progression without
enqueuing the old workflow again. Scheduling/cancellation intents are reconciled and active heads
are polled. Previously `FAILED` or `CANCELED` workflows stay stopped; there is no
public automatic retry/resume operation for those states.

Keep database files private and action definitions compatible: reopening
unpickles stored Python objects. Use one live WorkflowData instance per workflow
database, whether used directly or owned by a WorkflowManager. Locks coordinate only
threads sharing that instance; multiple instances/processes are not supported.
Close the previous instance before reopening the database for recovery.
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

These hooks do not capture scheduler output files or replace the job's
`run.log`/`err.log`.

## Workers and user queries

`wfapiUserQuery(title, query)` retains its two-argument hook interface. The
terminal implementation blocks for input and returns the stripped answer.
Authentication and action API operations use the shared `Workers` class, so
questions block their worker rather than the workflow manager. Each worker
retains the ordinary call stack; there are no completion-callback chains or
condition exceptions.

Normal Python authentication calls block for their shared worker result.
Each action has one worker thread. For compute and transfer actions,
`AsyncActionManager` runs submission, status polling and cancellation sequentially
through separate execution and cancellation-completion hooks. API actions run
their complete remaining sequence directly on their worker, without periodic
polling or a retry delay. `startOrResumeAction` persists
intent and starts or reuses execution without waiting; `WorkflowData` observes recorded
action states and advances workflow stages.
Nested calls can wait for shared authentication without occupying the workflow
manager. Independent implementations initialize under separate client locks.

Authentication workers do not inherit their callers' shutdown state. Canceling
an action or stopping a WorkflowManager stops its caller's wait, while shared
authentication can continue. There is no waiter counting; unused questions may
remain pending until answered or the server shuts down. Server shutdown closes
the query queue and explicitly stops outstanding authentication workers. Late
answers/provider responses are ignored at interruption checks, and later calls
can start fresh authentication. Each component retains its own worker registry.

Action workers write their own records through the same `LockedConnection`
context manager as `WorkflowData`. It holds a reentrant lock and yields the persistent,
ordinary SQLite connection for each `with self.conn as conn:` block. All SQL
statements and result fetching must finish inside that block; the wrapper exposes
no SQL methods. Leaving the block commits or rolls back and releases the lock.
The connection remains open until manager shutdown. No database lock is held
during API calls, questions or retry delays. Compute
and transfer recovery uses the saved remote ID, action status, submission
attempts and retry timing, without a scheduling step counter. A saved remote ID
bypasses submission. API sequences retain their operation cursor to skip completed
operations. Existing prototype counter columns are ignored.

Resumption refers to recovery of missing worker supervision when constructing
a new manager after shutdown or process failure, not a pause/resume feature for workflows. Normal
workers progress independently; repeated start/resume calls reuse live workers.

After a process crash, workers and questions are gone. WorkflowData construction
calls `resumeActions` once to restore supervisors for current unfinished workflow
heads, including already ACTIVE jobs; historical abandoned records are not restarted. API sequences replay the
unfinished operation. Submission recovery discovers accepted work by its stable
name before retrying. Cancellation retains its intent and remote ID and checks
remote status before another cancellation request. No query UUIDs, user responses
or condition records are persisted.

`queryRecordedStatus(action_id)` reads cached records without changing the
provider polling interval configured at manager construction. Its optional
`force_update=True` requests an early worker poll and returns the current record
without waiting. Likewise, `force_poll=True` wakes an existing supervisor
without waiting; its eventual successful poll updates the action record without
another query or workflow pass. Poll failures retain existing state and are logged.
`WorkflowData` copies the latest recorded state into the workflow on its next pass.

Worker interruption and action cancellation are separate. `Workers.interrupt`
asks the current Python call to finish running by raising `WorkerInterrupted`.
After execution has finished running, a separate cancellation worker starts
`_completeCancellation`. Only that action-manager hook cancels remote work. `Workers.stop` instead requests worker shutdown, leaving
remote jobs and persisted action state recoverable. Neither signal kills a thread.

`checkForThreadCurrentWorkerInterruption()` raises for either signal during normal
execution. Only when handling a recorded CANCELING intent,
`clearThreadCurrentWorkerInterruptUnlessStopping()` clears the handled interrupt
so cancellation API calls can proceed. It still raises if shutdown was requested;
it never resumes a thread or resumes/cancels a remote job. Query hooks can
inspect `getThreadCurrentWorkerInterruptionEvent()` to wake a waiting worker. The current terminal
`input()` cannot be forcibly interrupted: cancellation is observed after input
returns. A running submission's accepted ID is saved before its execution
worker hands off to the cancellation worker. Stopping a manager signals supervisors and retains durable
state; accepted remote jobs are not automatically canceled merely by stopping.
Workers are daemon threads, so a noninterruptible API call cannot prevent process
exit. Closing storage fences late database updates; restart recovery uses the
persisted intent and stable remote name.

The optional MCP server routes questions through an in-memory `UserQueryQueue`.
`list_user_queries` exposes pending prompts; `respond_to_user_query` delivers one
answer using the server-session UUID and query number. The waiting worker then
continues its original API/authentication call. Answers are not workflow records,
are not logged by the queue, and do not persist across restart. Restarted workers
ask fresh questions; old-session replies are rejected. See [server interactions](server.md#answer-user-questions).

The server utility selects queue routing by setting `api_query_user_func` to the
queue's `ask` function. It restores the previous hook on shutdown. Outside server
mode, direct Python calls use the default terminal hook.
The listener starts before asynchronous configuration/manager construction, so
bootstrap authentication questions can be answered through MCP too.
