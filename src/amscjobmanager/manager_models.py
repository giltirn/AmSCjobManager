"""Transport-independent input/output models for WorkflowManager operations."""

from typing import Literal
from pydantic import BaseModel, Field, StrictBool, StrictStr

from .manager_functions import ConfiguredMachineArguments, FunctionArguments
from .workflow import Workflow


class SubmitWorkflowArguments(FunctionArguments):
    """Ordered stages (Workflow), optional group label and exact submission retry key."""

    stages: Workflow = Field(description='Ordered nonempty list of execute_job, batch_script, globus_transfer or api stages, with type tags.')
    workflow_group: StrictStr | None = Field(default=None, description='Optional nonunique campaign/group label.')
    submission_key: StrictStr | None = Field(default=None, description='Optional exact-string idempotency key; reuse after an uncertain response to retrieve the original workflow.')


class WorkflowIdArguments(FunctionArguments):
    """workflow_id (positive strict int) is a workflow database ID, not an action/stage/provider ID."""

    workflow_id: int = Field(gt=0, strict=True, description='Positive workflow database ID, not a stage index or provider ID.')


class ListWorkflowArguments(FunctionArguments):
    """Optional workflow ID, recorded-state refresh request, and overall-status filter.

    workflow_id (int | None) selects one row; refresh (bool) requests earlier
    polling without waiting. statuses (list[str] | None) uses OR for its uppercase
    workflow states, combined with ID using AND; None means all and [] means none.
    """

    workflow_id: int | None = Field(default=None, gt=0, strict=True, description='Optional positive workflow database ID; omit for all workflows.')
    refresh: StrictBool = Field(default=False, description='Wake supervisors to poll active workflow heads before filtering; return recorded state without waiting or advancing stages.')
    statuses: list[Literal['PENDING', 'ACTIVE', 'COMPLETED', 'FAILED', 'CANCELING', 'CANCELED']] | None = Field(default=None, description='Filter overall workflow statuses using OR. COMPLETED selects fully finished workflows. Omit for all; [] matches none.')


class MachineFilter(FunctionArguments):
    """machine (str | None) filters case-insensitively; None includes all recorded entries."""

    machine: StrictStr | None = Field(default=None, description='Optional case-insensitive machine filter; omit for all machines.')


class SandboxArguments(ConfiguredMachineArguments):
    """Configured machine (str) and exact path (nonempty str) identify a sandbox registration."""

    path: StrictStr = Field(min_length=1, description='Sandbox root; removal matches the registered path text exactly.')


class WorkflowSummary(BaseModel):
    """Public JSON workflow record, constructed from manager API results.

    Fields:
        workflow_id (int): Positive database ID, not the response-list index.
        workflow_group (str | None): Optional group supplied at submission.
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
        api_status (str | None): Provider status such as 'queued' or 'ACTIVE', or a manager-side
            submission reason such as 'INSUFFICIENT_WALLET_BALANCE',
            preserving provider casing; None if no head status is recorded.
            This is not the complete API response body.
        last_status_change (int | None): Recorded Unix timestamp in whole
            seconds, not necessarily the most recent poll time.
    """

    workflow_id: int = Field(description='Positive workflow database ID, not a response-list index.')
    workflow_group: str | None = Field(description='Optional group supplied when the workflow was enqueued.')
    workflow_status: str = Field(description='Overall status. COMPLETED means every step finished; ACTIVE includes provider-queued work and intermediate completion; dependency waits are PENDING.')
    workflow_stage: int = Field(description='Zero-based action position; -1 before starting, or the action count after full completion.')
    head_action_id: int | None = Field(description='Current positive action ID, assigned at enqueue and independent of workflow ID; null after full workflow completion.')
    head_action_type: str | None = Field(description='Head action class name; NoneType after normal workflow completion.')
    head_action_class: str = Field(description='Action category: COMPUTE, TRANSFER, API, or NONE.')
    head_action_status: str = Field(description='Exact head status, including SCHEDULING, PROVIDER_HELD, and intermediate COMPLETED.')
    api_status: str | None = Field(description='Recorded provider status with original casing, or submission reason INSUFFICIENT_WALLET_BALANCE while SCHEDULING; null if unknown. Not the full API response.')
    last_status_change: int | None = Field(description='Recorded Unix timestamp in whole seconds; not necessarily the last API poll time.')


class WorkflowReceipt(BaseModel):
    """Recorded workflow state after submission or a cancellation request.

    workflow_id (positive int) is the database ID, not a stage/list index.
    workflow_status (str) is the overall ActionStatus name. Submission confirms
    persistence, not remote execution; cancellation may remain CANCELING while
    background workers contact the provider. Reused keys can return terminal work.
    """

    workflow_id: int = Field(gt=0, description='Positive workflow database ID, independent of stage/action IDs.')
    workflow_status: str = Field(description='Recorded overall workflow status; cancellation may remain CANCELING until confirmed.')


class SandboxDirectoryListing(BaseModel):
    """Registered root paths, without internal directory-validation metadata.

    result (dict[str, list[str]]) maps lowercase machines to exact registered
    filesystem root strings in registration order. Empty lists mean no roots
    for that entry; {} means no matching entries. Implicit /tmp and staging
    allowances are excluded. Listing performs no authentication or path probes.
    """

    result: dict[str, list[str]] = Field(description='Registered filesystem roots grouped by lowercase machine name. Lists contain path strings, including roots awaiting validation. Empty lists mean no registered roots; an empty object means no matching machines. Implicit /tmp and staging allowances are excluded.')


class WorkflowListing(BaseModel):
    """result (list[WorkflowSummary]) lists workflows in ascending database-ID order."""

    result: list[WorkflowSummary]
