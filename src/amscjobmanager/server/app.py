"""Read-only MCP tools attached to an existing JobManager."""

import json
from typing import Annotated, Literal

from mcp.server import MCPServer
from mcp.types import CallToolResult, TextContent, ToolAnnotations
from pydantic import BaseModel, Field, StrictBool

from ..manager import JobManager
from ..action_manager import ActionStatus


class WorkflowSummary(BaseModel):
    """Public JSON workflow record, constructed from manager API results.

    Fields:
        workflow_id (int): Positive database ID, not the response-list index.
        job_group (str | None): Optional group supplied at submission.
        workflow_status (str): Overall ActionStatus name. ACTIVE includes
            scheduling and completed intermediate steps; COMPLETED means done.
        workflow_stage (int): Zero-based action position, -1 before starting,
            or the action count after full completion.
        head_action_id (int | None): Positive action ID, independent of the
            workflow ID; None if no action is currently assigned.
        head_action_type (str | None): Action class name, or 'NoneType' after
            normal completion. Nullable to represent unset stored fields.
        head_action_class (str): ActionClass name: COMPUTE, TRANSFER, API, or NONE.
        head_action_status (str): Exact head ActionStatus name, including
            SCHEDULING and intermediate COMPLETED states.
        api_status (str | None): Provider status such as 'queued' or 'ACTIVE',
            preserving provider casing; None if no head status is recorded.
            This is not the complete API response body.
        last_status_change (int | None): Recorded Unix timestamp in whole
            seconds, not necessarily the most recent poll time.
    """

    workflow_id: int = Field(description='Positive workflow database ID, not a response-list index.')
    job_group: str | None = Field(description='Optional group supplied when the workflow was enqueued.')
    workflow_status: str = Field(description='Overall status. COMPLETED means every step finished; ACTIVE includes queued work and intermediate completion.')
    workflow_stage: int = Field(description='Zero-based action position; -1 before starting, or the action count after full completion.')
    head_action_id: int | None = Field(description='Current positive action ID, independent of workflow ID; null when none is assigned.')
    head_action_type: str | None = Field(description='Head action class name; NoneType after normal workflow completion.')
    head_action_class: str = Field(description='Action category: COMPUTE, TRANSFER, API, or NONE.')
    head_action_status: str = Field(description='Exact head status, including SCHEDULING and intermediate COMPLETED.')
    api_status: str | None = Field(description='Recorded provider status with original casing, such as queued or ACTIVE; null if unknown. Not the full API response.')
    last_status_change: int | None = Field(description='Recorded Unix timestamp in whole seconds; not necessarily the last API poll time.')


def createServer(manager: JobManager) -> MCPServer:
    """Return an MCP server exposing read-only tools for manager.

    Args:
        manager (JobManager): Existing manager whose public API supplies tool
            results. Its background thread must be started for workflow calls.

    Returns:
        MCPServer: Configured SDK server with the list_workflows tool. This
            creates neither a listener nor a separate manager/database.

    The caller owns manager startup/shutdown and the HTTP listener. SDK worker
    threads execute synchronous tools under the manager lock. No database
    connection is opened by this interface and no workflow is submitted.
    The manager must be started before workflow tools are called; stopped or
    failed managers return manager_unavailable while the listener stays usable.
    """
    # The SDK passes this level to Uvicorn, whose INFO access logger writes
    # to stdout. Suppress routine framework/access chatter so a background
    # server sharing the client's terminal cannot pollute JSON output.
    server = MCPServer("AmSC Job Manager", log_level="WARNING")

    @server.tool(annotations=ToolAnnotations(read_only_hint=True, destructive_hint=False))
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
        error = {"code": "manager_unavailable", "message": "The workflow manager is not running; check the server log"}
        summaries = []
        selected_statuses = None if statuses is None else [ActionStatus[status] for status in statuses]
        if manager.isAlive():
            with manager as jobs:
                if manager.isAlive():
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

    return server
