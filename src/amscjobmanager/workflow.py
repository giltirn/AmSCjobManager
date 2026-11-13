"""Native Pydantic schema and JSON adapter for supported workflow stage lists."""

from typing import Annotated

from pydantic import Field, TypeAdapter

from .api_actions import APIAction
from .compute_actions import ExecuteBatchScriptComputeAction, ExecuteJobComputeAction
from .globus_transfer_action import GlobusTransfer


# A type tag selects one of the built-in action classes; arbitrary Python
# classes and pickle data are never accepted by this JSON representation.
WorkflowStage = Annotated[
    ExecuteBatchScriptComputeAction | ExecuteJobComputeAction | GlobusTransfer | APIAction,
    Field(discriminator='type', json_schema_extra={'required': ['type']}),
]
Workflow = Annotated[list[WorkflowStage], Field(min_length=1)]
# Validate/dump Python action lists and JSON with the same schemas used by MCP.
workflow_adapter = TypeAdapter(Workflow)
