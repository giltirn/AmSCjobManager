from .workflow_data import WorkflowData
import threading
from .action_manager import ActionStatus
from .logging import wfmanLog
from .globals import addSandboxDirs, getSandboxDirs, removeSandboxDirs
from .manager_functions import EmptyFunctionResult, FunctionDefinition, FunctionError
from .manager_models import (
    ListWorkflowArguments, MachineFilter, SandboxArguments, SandboxDirectoryListing,
    SubmitWorkflowArguments, WorkflowIdArguments, WorkflowListing, WorkflowReceipt, WorkflowSummary,
)
import time
            
class WorkflowManager:
    """Progress workflows in a single-use background manager.

    Construction recovers unfinished actions through WorkflowData. Once stopped or
    failed, this instance cannot restart; create a new manager for recovery.
    Submit through ``with manager as workflows: workflows.enqueueWorkflow(...)`` or the callable
    interface. Both accept WorkflowData's optional submission_key for retry-safe
    submission, with uniqueness and persistence owned by WorkflowData.
    """

    def __init__(self, filename: str | None = None, poll_freq=30, max_workflows_active=10):
        """Create a single-use manager and recover its unfinished action workers.

        filename (str | None) is the persistent SQLite path, or None for a
        memory-only database. poll_freq (float, default 30) is seconds between
        provider status polls/recovery attempts. max_workflows_active (int,
        default 10) limits automatic workflow starts; zero disables them.
        Return None. WorkflowData construction recovers existing unfinished actions;
        start() separately launches workflow progression. After shutdown or
        failure, create a new instance to recover persisted work.
        """
        self.workflow_data = WorkflowData(filename, max_workflows_active=max_workflows_active, poll_freq=poll_freq)
        self._stop = threading.Event()
        self._failed = threading.Event()
        self._thread = None
        self._lock = threading.Lock()
        self.poll_freq = poll_freq

    def getMCPFunctions(self) -> tuple[FunctionDefinition, ...]:
        """Describe workflow and sandbox inspection operations independently of transport.

        Take no arguments; return definitions with local annotated callables.
        Callables receive a WorkflowManager and validated input, acquire its lock,
        and return output models. Discovery does not execute operations.
        """
        def _submit_workflow(manager: WorkflowManager, args: SubmitWorkflowArguments) -> WorkflowReceipt:
            """Enqueue args.stages through manager; return its durable workflow ID and status."""
            with manager as workflows:
                if not manager.isAlive():
                    raise FunctionError('manager_unavailable', 'Workflow manager is not running.')
                workflow_id = workflows.enqueueWorkflow(args.stages, args.workflow_group, submission_key=args.submission_key)
                return WorkflowReceipt(workflow_id=workflow_id, workflow_status=workflows.workflowStatus(workflow_id).name)

        def _cancel_workflow(manager: WorkflowManager, args: WorkflowIdArguments) -> WorkflowReceipt:
            """Request cancellation of args.workflow_id; return its recorded workflow status."""
            with manager as workflows:
                if not manager.isAlive():
                    raise FunctionError('manager_unavailable', 'Workflow manager is not running.')
                try:
                    workflows.cancelWorkflow(args.workflow_id)
                except ValueError as exc:
                    raise FunctionError('workflow_not_found', str(exc)) from exc
                return WorkflowReceipt(workflow_id=args.workflow_id, workflow_status=workflows.workflowStatus(args.workflow_id).name)

        def _list_workflows(manager: WorkflowManager, args: ListWorkflowArguments) -> WorkflowListing:
            """Read manager's workflow/head summaries using args filters and optional poll wakeup."""
            statuses = None if args.statuses is None else [ActionStatus[status] for status in args.statuses]
            with manager as workflows:
                if not manager.isAlive():
                    raise FunctionError('manager_unavailable', 'Workflow manager is not running.')
                # Validate an explicit ID even if filters exclude it, and apply
                # filtering again after a requested asynchronous status refresh.
                try:
                    states = workflows.listWorkflows(args.workflow_id, statuses)
                    if args.refresh:
                        workflows.progressActiveActions(force_poll=True)
                        states = workflows.listWorkflows(args.workflow_id, statuses)
                except ValueError as exc:
                    raise FunctionError('workflow_not_found', str(exc)) from exc

                summaries = []
                for state in states:
                    workflow_id = state['workflow_id']
                    head_status, api_status = workflows.headActionStatus(workflow_id, return_api_status=True)
                    summaries.append(WorkflowSummary(
                        workflow_id=workflow_id, workflow_group=state['workflow_group'],
                        workflow_status=workflows.workflowStatus(workflow_id).name, workflow_stage=state['workflow_stage'],
                        head_action_id=state['head_action_id'], head_action_type=state['head_action_type'],
                        head_action_class=state['head_action_class'].name, head_action_status=head_status.name,
                        api_status=api_status, last_status_change=state['last_status_change'],
                    ))
                return WorkflowListing(result=summaries)

        def _list_sandboxes(manager: WorkflowManager, args: MachineFilter) -> SandboxDirectoryListing:
            """Read registered roots selected by args.machine without directory probes or authentication."""
            with manager:
                if not manager.isAlive():
                    raise FunctionError('manager_unavailable', 'Workflow manager is not running.')
                return SandboxDirectoryListing(result=getSandboxDirs(args.machine))

        return (
            FunctionDefinition('submit_workflow', 'Persist an ordered workflow for background execution, or retrieve a keyed submission.', _submit_workflow),
            FunctionDefinition('cancel_workflow', 'Request workflow cancellation, including its current compute or transfer job.', _cancel_workflow, idempotent=True),
            FunctionDefinition('list_workflows', 'List all workflows, or select one by its positive workflow_id.', _list_workflows, read_only=True, idempotent=True),
            FunctionDefinition('list_sandbox_directories', 'List registered sandbox directories for all machines or one selected machine.', _list_sandboxes, read_only=True, idempotent=True),
        )

    def getAdminFunctions(self) -> tuple[FunctionDefinition, ...]:
        """Describe privileged sandbox administration through annotated local callables.

        Take no arguments; return definitions receiving the current WorkflowManager
        and validated SandboxArguments. Changes last for this process session.
        """
        def _add_sandbox(manager: WorkflowManager, args: SandboxArguments) -> EmptyFunctionResult:
            """Register args.path on args.machine under manager's lock; return an empty result."""
            with manager:
                if not manager.isAlive():
                    raise FunctionError('manager_unavailable', 'Workflow manager is not running.')
                addSandboxDirs({args.machine: args.path})
            return EmptyFunctionResult()

        def _remove_sandbox(manager: WorkflowManager, args: SandboxArguments) -> EmptyFunctionResult:
            """Remove args.machine/path registration without deleting files; return an empty result."""
            with manager:
                if not manager.isAlive():
                    raise FunctionError('manager_unavailable', 'Workflow manager is not running.')
                removeSandboxDirs({args.machine: args.path})
            return EmptyFunctionResult()

        return (
            FunctionDefinition('add_sandbox', 'Add a sandbox directory for this server session; validation occurs on first use.', _add_sandbox,
                               idempotent=True, success_message='Sandbox directory added.'),
            FunctionDefinition('remove_sandbox', 'Remove an exact sandbox registration without deleting files.', _remove_sandbox,
                               idempotent=True, success_message='Sandbox directory removed.'),
        )

    def isAlive(self):
        """Return whether the background thread is alive without a recorded failure."""
        return self._thread is not None and self._thread.is_alive() and not self._failed.is_set()
        
    def start(self):
        """Start workflow progression once for this manager instance.

        Take no arguments; return None. An already running manager is unchanged.
        After shutdown or background failure, raise RuntimeError directing the
        caller to construct a new manager against its persistent database.
        Action recovery already occurred during WorkflowData construction.
        """
        with self._lock:
            if self._stop.is_set() or self._failed.is_set():
                raise RuntimeError('WorkflowManager cannot be restarted; create a new WorkflowManager for recovery')
            if self._thread is not None:
                if self._thread.is_alive():
                    return
                raise RuntimeError('WorkflowManager cannot be restarted; create a new WorkflowManager for recovery')
            self._thread = threading.Thread(target=self._run)
            try:
                self._thread.start()
            except BaseException:
                self._failed.set()
                self._thread = None
                self.workflow_data.stopWorkers()
                raise

    def stop(self, wait_until_done=True, *, close_database=False):
        """Permanently stop this manager and its action workers.

        wait_until_done (bool, default True) waits for unfinished workflows while
        the manager remains healthy. close_database (bool, default False) closes
        storage after shutdown. Return None. Repeated calls are safe, including
        after database closure. Accepted remote jobs are not canceled by shutdown.
        This instance cannot be started again, even if it had never been started.
        """
        # A failed/stopped thread cannot drain workflows; do not query closed
        # storage or wait for unfinished records during repeated shutdown.
        while wait_until_done and self.isAlive():
            with self._lock:
                remaining = self.workflow_data.countWorkflowsWithStatus([
                    ActionStatus.PENDING, ActionStatus.ACTIVE, ActionStatus.CANCELING,
                ])
            if remaining == 0:
                break
            time.sleep(2)

        self._stop.set()
        if self._thread is not None:
            self._thread.join()
        self.workflow_data.stopWorkers()
        if close_database:
            self.workflow_data.conn.close()

    def _run(self):
        """Progress workflows until stopped; log failures and always release the lock."""
        try:
            while not self._stop.wait(0.5):
                # Only hold the lock while accessing manager state. The context
                # releases it on errors as well as normal completion.
                with self._lock:
                    if self._stop.is_set():
                        break
                    try:
                        self.workflow_data.startWorkflows()
                        self.workflow_data.progressActiveState()
                    except BaseException:
                        # Mark failure before unlocking so a waiting request
                        # cannot mistake the exiting thread for a healthy one.
                        self._failed.set()
                        raise
                if self._stop.wait(2):
                    break
        except Exception as exc:
            wfmanLog(f"WorkflowManager background thread failed; workflow progression stopped: {type(exc).__name__}: {exc}")

    def __call__(self, op_lambda):
        """
        Perform an operation on the WorkflowData database under lock
        """
        with self._lock:
            return op_lambda(self.workflow_data)
        
    def __enter__(self):
        """
        Allow the user to acquire a lock on the database for manipulation using 'with'
        """
        self._lock.acquire()
        return self.workflow_data
        
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Release the manager lock and propagate any original context-block exception.

        exc_type (type[BaseException] | None), exc_val (BaseException | None),
        and exc_tb (traceback | None) describe the block's error; all are None
        on success. Return None so Python preserves the original exception and
        traceback without suppressing or wrapping it.
        """
        self._lock.release()
            

    
