"""MCP workflow submission, inspection, cancellation and user-interaction tools."""

import json
from typing import Annotated, Literal

from mcp.server import MCPServer
from mcp.types import CallToolResult, TextContent, ToolAnnotations
from pydantic import BaseModel, Field, StrictBool, StrictStr

from ..action_manager import ActionStatus
from ..workflow import Workflow
from .queries import QueryResponseError
from .runtime import ServerRuntime


class WorkflowSummary(BaseModel):
    """Public JSON workflow record, constructed from manager API results.

    Fields:
        workflow_id (int): Positive database ID, not the response-list index.
        job_group (str | None): Optional group supplied at submission.
        workflow_status (str): Overall ActionStatus name. ACTIVE includes
            scheduling and completed intermediate steps; COMPLETED means done.
        workflow_stage (int): Zero-based action position, -1 before starting,
            or the action count after full completion.
        head_action_id (int | None): Positive action ID, assigned at enqueue
            independently of the workflow ID; None after full completion.
        head_action_type (str | None): Action class name, or 'NoneType' after
            normal completion. Nullable to represent unset stored fields.
        head_action_class (str): ActionClass name: COMPUTE, TRANSFER, API, or NONE.
        head_action_status (str): Exact head ActionStatus name, including
            SCHEDULING, PROVIDER_HELD, and intermediate COMPLETED states.
        api_status (str | None): Provider status such as 'queued' or 'ACTIVE',
            preserving provider casing; None if no head status is recorded.
            This is not the complete API response body.
        last_status_change (int | None): Recorded Unix timestamp in whole
            seconds, not necessarily the most recent poll time.
    """

    workflow_id: int = Field(description='Positive workflow database ID, not a response-list index.')
    job_group: str | None = Field(description='Optional group supplied when the workflow was enqueued.')
    workflow_status: str = Field(description='Overall status. COMPLETED means every step finished; ACTIVE includes provider-queued work and intermediate completion; dependency waits are PENDING.')
    workflow_stage: int = Field(description='Zero-based action position; -1 before starting, or the action count after full completion.')
    head_action_id: int | None = Field(description='Current positive action ID, assigned at enqueue and independent of workflow ID; null after full workflow completion.')
    head_action_type: str | None = Field(description='Head action class name; NoneType after normal workflow completion.')
    head_action_class: str = Field(description='Action category: COMPUTE, TRANSFER, API, or NONE.')
    head_action_status: str = Field(description='Exact head status, including SCHEDULING, PROVIDER_HELD, and intermediate COMPLETED.')
    api_status: str | None = Field(description='Recorded provider status with original casing, such as queued or ACTIVE; null if unknown. Not the full API response.')
    last_status_change: int | None = Field(description='Recorded Unix timestamp in whole seconds; not necessarily the last API poll time.')


class UserQuerySummary(BaseModel):
    """Pending server question; responses are deliberately excluded.

    session_id (str) is the server-session UUID, query_id (int) is positive and
    session-local, title/prompt (str) are instructions, and created_at (float)
    is the Unix creation time in seconds. IDs are not workflow IDs/list indices.
    """

    session_id: str = Field(description='Server-session UUID required when replying; changes on restart.')
    query_id: int = Field(description='Positive query number within this server session; not a workflow ID or list index.')
    title: str = Field(description='Short description of the requested user interaction.')
    prompt: str = Field(description='Question or authentication URL/instructions for the user.')
    created_at: float = Field(description='Question creation time as Unix seconds.')


class QueryResponseReceipt(BaseModel):
    """Delivery acknowledgment: accepted=True means queued, not authenticated."""

    accepted: Literal[True] = Field(description='Response delivered once. Authentication/API execution continues separately and may ask another question.')


class WorkflowReceipt(BaseModel):
    """Recorded workflow state after submission or a cancellation request.

    workflow_id (positive int) is the database ID, not a stage/list index.
    workflow_status (str) is the overall ActionStatus name. Submission confirms
    persistence, not remote execution; cancellation may remain CANCELING while
    background workers contact the provider. Reused keys can return terminal work.
    """

    workflow_id: int = Field(gt=0, description='Positive workflow database ID, independent of stage/action IDs.')
    workflow_status: str = Field(description='Recorded overall workflow status; cancellation may remain CANCELING until confirmed.')


def registerTools(mcp_server: MCPServer, runtime: ServerRuntime) -> None:
    """Register workflow lifecycle and user-interaction tools on an MCP server.

    mcp_server (MCPServer) receives the tool handlers and response schemas;
    runtime (ServerRuntime) supplies manager readiness, workflow state and its
    query queue. Return None. Registration starts no workers or listeners.
    Workflow tools require a live manager; query tools remain available during
    initialization or failure without acquiring the manager/database lock.
    """
    @mcp_server.tool(annotations=ToolAnnotations(read_only_hint=False, destructive_hint=True))
    def submit_workflow(
        stages: Annotated[Workflow, Field(description='Ordered nonempty list of execute_job, batch_script, globus_transfer or api stages. Each stage requires its type tag; API operations have their own type tags.')],
        job_group: Annotated[StrictStr | None, Field(description='Optional nonunique campaign/group label.')] = None,
        submission_key: Annotated[StrictStr | None, Field(description='Optional exact-string idempotency key. Reuse after an uncertain response to retrieve the original workflow; changed work requires a new key.')] = None,
    ) -> Annotated[CallToolResult, WorkflowReceipt]:
        """Persist an ordered workflow for background execution, or retrieve a keyed submission.

        stages (nonempty list of Pydantic action dataclasses) contains tagged
        compute, Globus transfer and API stages in execution order. API stages
        contain ordered, replay-safe operations. job_group (str | None) is a
        nonunique label; submission_key (str | None) identifies one submission
        within this database. None always creates new work. Reusing a key returns
        the original workflow without replacing its stages, group or status.

        Return CallToolResult with WorkflowReceipt fields at structured_content's
        root: workflow_id (positive int) and workflow_status (ActionStatus name,
        str). Persistence is acknowledged without waiting for provider submission
        or execution. Stage/path/provider failures are recorded asynchronously.
        All API writes use existing sandbox checks; no unsafe override is exposed.
        Programs and shell snippets retain the job user's ordinary permissions.
        File paths refer to the manager host or configured target, not the CLI host.
        Authentication questions appear through list_user_queries.

        An unavailable manager returns is_error=True and an error object with
        code manager_unavailable and a message string. Invalid stage schemas are
        rejected by MCP before any workflow is enqueued. There is no automatic
        client retry: retain submission_key to retry an uncertain outcome safely.
        Unknown dependency keys or required canceled producers return an error
        with code invalid_input and JobData's validation message; no partial
        workflow is retained.
        """
        # Enqueue only while the manager is healthy, checking again after the
        # lock is acquired. JobData owns persistence and key deduplication.
        error = {'code': 'manager_unavailable',
                 'message': f'The workflow manager is {runtime.state}; check pending user queries and the server log'}
        manager = runtime.manager
        if manager is not None and manager.isAlive():
            with manager as jobs:
                if manager.isAlive():
                    try:
                        workflow_id = jobs.enqueueJob(stages, job_group, submission_key=submission_key)
                    except ValueError as exc:
                        # JobData owns reference validation. Preserve its useful
                        # input error instead of MCP's generic internal-error text.
                        error = {'code': 'invalid_input', 'message': str(exc)}
                    else:
                        receipt = WorkflowReceipt(workflow_id=workflow_id,
                                                  workflow_status=jobs.jobStatus(workflow_id).name).model_dump(mode='json')
                        error = None
        if error is not None:
            return CallToolResult(is_error=True, structured_content={'error': error},
                                  content=[TextContent(type='text', text=error['message'])])
        return CallToolResult(structured_content=receipt,
                              content=[TextContent(type='text', text=json.dumps(receipt))])

    @mcp_server.tool(annotations=ToolAnnotations(read_only_hint=False, destructive_hint=True, idempotent_hint=True))
    def cancel_workflow(
        workflow_id: Annotated[int, Field(gt=0, strict=True, description='Positive workflow database ID, not a stage index or provider job/task ID.')],
    ) -> Annotated[CallToolResult, WorkflowReceipt]:
        """Request workflow cancellation, including its current compute or transfer job.

        workflow_id (positive int) identifies the persisted workflow. Call the
        public JobData cancellation path with remote cancellation enabled;
        queued workflows are canceled directly, and workers reconcile active
        remote cancellation. Completed filesystem effects are retained.

        Return CallToolResult with WorkflowReceipt fields workflow_id (int) and
        workflow_status (str) at structured_content's root. CANCELING means the
        request is recorded and background cancellation is still pending; inspect
        list_workflows for its outcome. Canceled workflows remain in the database
        and repeated requests are safe. This neither deletes files nor removes
        submission keys. There is no option to abandon running remote work.
        Unavailable/unknown workflows return is_error=True with an error object
        containing code manager_unavailable/workflow_not_found and message (str).
        """
        # Persist cancellation under the normal manager lock. Remote API work
        # remains on action workers so client calls and other workflows continue.
        error = {'code': 'manager_unavailable',
                 'message': f'The workflow manager is {runtime.state}; check pending user queries and the server log'}
        manager = runtime.manager
        if manager is not None and manager.isAlive():
            with manager as jobs:
                if manager.isAlive():
                    try:
                        jobs.cancelJob(workflow_id)
                    except ValueError as exc:
                        error = {'code': 'workflow_not_found', 'message': str(exc)}
                    else:
                        receipt = WorkflowReceipt(workflow_id=workflow_id,
                                                  workflow_status=jobs.jobStatus(workflow_id).name).model_dump(mode='json')
                        error = None
        if error is not None:
            return CallToolResult(is_error=True, structured_content={'error': error},
                                  content=[TextContent(type='text', text=error['message'])])
        return CallToolResult(structured_content=receipt,
                              content=[TextContent(type='text', text=json.dumps(receipt))])

    @mcp_server.tool(annotations=ToolAnnotations(read_only_hint=True, destructive_hint=False))
    def list_workflows(
        workflow_id: Annotated[int, Field(gt=0, strict=True, description='Select a positive workflow database ID, not a list index. Omit to list all workflows.')] | None = None,
        refresh: Annotated[StrictBool, Field(description='Wake supervisors to poll active workflow heads before filtering. Returns recorded state without waiting; no steps are submitted or advanced.')] = False,
        statuses: Annotated[list[Literal['PENDING', 'ACTIVE', 'COMPLETED', 'FAILED', 'CANCELING', 'CANCELED']] | None,
                            Field(description='Filter overall workflow statuses using OR. COMPLETED selects fully finished workflows, not intermediate steps. Omit for all statuses.')] = None,
    ) -> Annotated[CallToolResult, list[WorkflowSummary]]:
        """List all workflows, or select one by its positive workflow_id.

        Args:
            workflow_id (int | None): Positive workflow database ID, not a
                list position. None selects all recorded workflows.
            refresh (bool): If True, request polling for all ACTIVE heads before filtering,
                even when workflow_id selects one record. Pending workers
                return cached state; supervisors record results when ready. Defaults to False.
            statuses (list[str] | None): Overall workflow status names from
                PENDING, ACTIVE, COMPLETED, FAILED, CANCELING, and CANCELED.
                Entries use uppercase names and OR semantics. None selects
                every status; [] selects none. Combine with ID using AND.

        Returns:
            CallToolResult: On success, is_error=False and structured_content
            is {'result': list[dict]}, ordered by ascending workflow ID; each
            entry has the fields/types documented by WorkflowSummary. An empty
            match produces {'result': []}; a matching explicit ID produces a
            one-element list. Response-list indices start at zero and are not
            workflow IDs. content also contains a JSON text copy of the list.
            On an anticipated error, is_error=True and structured_content is
            {'error': {'code': str, 'message': str}}, with code
            workflow_not_found or manager_unavailable; content holds the error
            message. Invalid arguments are rejected separately by the SDK.

        Include completed/canceled records. An unknown explicit ID is an error.
        refresh requests polling of all active workflow heads without waiting
        for workers; responses contain currently recorded state.
        it does not submit or advance steps. Failed polls log their errors and
        retain cached statuses. ACTIVE includes queued compute jobs: api_status
        distinguishes provider states such as new, queued, and active. A null
        api_status means no provider status is recorded for the current head.
        statuses optionally filters overall workflow statuses; COMPLETED means
        the entire workflow finished, not an intermediate completed action.
        None includes all statuses, and an empty list matches nothing.
        If the manager is stopped or its worker has failed, return a structured
        manager_unavailable error rather than silently serving stale records.
        """
        # Check before waiting for the lock, then again after acquiring it:
        # the background thread may have failed while this request waited.
        error = {"code": "manager_unavailable",
                 "message": f'The workflow manager is {runtime.state}; check pending user queries and the server log'}
        summaries = []
        selected_statuses = None if statuses is None else [ActionStatus[status] for status in statuses]
        current_manager = runtime.manager
        if current_manager is not None and current_manager.isAlive():
            with current_manager as jobs:
                if current_manager.isAlive():
                    error = None
                    try:
                        states = jobs.listWorkflows(workflow_id, selected_statuses)
                    except ValueError as exc:
                        error = {"code": "workflow_not_found", "message": str(exc)}
                        states = []
                    if refresh and error is None:
                        jobs.progressActiveActions(force_poll=True)
                        states = jobs.listWorkflows(workflow_id, selected_statuses)

                    for state in states:
                        job_id = state['job_id']
                        head_status, api_status = jobs.headActionStatus(job_id, return_api_status=True)
                        summaries.append(WorkflowSummary(
                            workflow_id=job_id,
                            job_group=state['job_group'],
                            workflow_status=jobs.jobStatus(job_id).name,
                            workflow_stage=state['workflow_stage'],
                            head_action_id=state['head_action_id'],
                            head_action_type=state['head_action_type'],
                            head_action_class=state['head_action_class'].name,
                            head_action_status=head_status.name,
                            api_status=api_status,
                            last_status_change=state['last_status_change'],
                        ))
        if error is not None:
            return CallToolResult(
                is_error=True, structured_content={"error": error},
                content=[TextContent(type="text", text=error['message'])],
            )
        records = [summary.model_dump(mode='json') for summary in summaries]
        return CallToolResult(
            structured_content={"result": records},
            content=[TextContent(type="text", text=json.dumps(records))],
        )

    @mcp_server.tool(annotations=ToolAnnotations(read_only_hint=True, destructive_hint=False))
    def list_user_queries() -> Annotated[CallToolResult, list[UserQuerySummary]]:
        """List unanswered user questions, including during manager initialization.

        Take no arguments. Return CallToolResult with is_error=False and
        {'result': list[dict]}, ordered by query_id. Fields are documented
        by UserQuerySummary. An empty list means no pending questions.
        Listing does not consume requests or expose previous responses.
        Questions may be shared authentication needs rather than belong to
        a single workflow. No manager or database lock is acquired.
        """
        records = [UserQuerySummary(**item).model_dump(mode='json') for item in runtime.queries.listPending()]
        return CallToolResult(structured_content={'result': records},
                              content=[TextContent(type='text', text=json.dumps(records))])

    @mcp_server.tool(annotations=ToolAnnotations(read_only_hint=False, destructive_hint=False))
    def respond_to_user_query(
        session_id: Annotated[StrictStr, Field(description='Server-session UUID from list_user_queries.')],
        query_id: Annotated[int, Field(gt=0, strict=True, description='Positive pending query number from list_user_queries; not a workflow ID.')],
        response: Annotated[StrictStr, Field(description='Response text. Delivery does not confirm successful authentication. Use JSON file/stdin input to avoid putting codes in command arguments.')],
    ) -> Annotated[CallToolResult, QueryResponseReceipt]:
        """Deliver one response and allow the waiting worker to continue.

        session_id (UUID str) and query_id (positive int) identify a pending
        question; response (str) is the answer, including an empty string.
        Return CallToolResult with {'accepted': True} on delivery.
        This confirms delivery only; rejected authorization may produce a
        new question. API calls execute on the asking worker, not this tool.
        Stale, closed, answered or interrupted requests return is_error=True
        with {'error': {'code': str, 'message': str}}. No answer is echoed,
        logged or persisted, and no sandbox privileges are granted.
        """
        try:
            runtime.queries.respond(session_id, query_id, response)
        except QueryResponseError as exc:
            return CallToolResult(is_error=True,
                                  structured_content={'error': {'code': exc.code, 'message': str(exc)}},
                                  content=[TextContent(type='text', text=str(exc))])
        receipt = QueryResponseReceipt(accepted=True).model_dump(mode='json')
        return CallToolResult(structured_content=receipt,
                              content=[TextContent(type='text', text=json.dumps(receipt))])
