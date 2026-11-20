"""Generic discovery and dispatch for backend-provided annotated operations."""

import json

from pydantic import ValidationError

from ..logging import wfmanLog
from ..manager_functions import FunctionError


class FunctionRegistry:
    """Map channel/name pairs to annotated callables and their current backend owner."""

    def __init__(self, workflows=None, *, query_provider=None):
        """Collect explicitly exposed operations without retaining backend instances.

        workflows (locked WorkflowData | None) supplies workflow, sandbox, and
        action-manager definitions; query_provider
        (UserQueryQueue | None) supplies always-available interaction definitions.
        Return None after rejecting conflicting names or invalid annotations.
        Stored owner keys identify a category/provider, never a bound instance.
        """
        self._functions = {'mcp': {}, 'admin': {}}
        providers = []
        if workflows is not None:
            providers.append(('workflows', workflows))
        if query_provider is not None:
            providers.append(('queries', query_provider))
        if workflows is not None:
            providers.extend((action_class, workflows.getManager(action_class)) for action_class in workflows.getManagerClasses())

        # Validate all registrations before this registry is published.
        for owner, provider in providers:
            for channel, definitions in (('mcp', provider.getMCPFunctions()), ('admin', provider.getAdminFunctions())):
                for definition in definitions:
                    if not definition.name or definition.name in self._functions[channel]:
                        raise ValueError(f'Duplicate or empty {channel} function name: {definition.name}')
                    definition.describe()
                    self._functions[channel][definition.name] = (owner, definition)

    def hasFunction(self, channel: str, name: str) -> bool:
        """Return whether name (str) is explicitly registered in channel ('mcp' or 'admin')."""
        return name in self._functions[channel]

    def describe(self, channel: str) -> list:
        """Return fresh FunctionDescription objects for channel ('mcp' or 'admin'), in registration order."""
        return [definition.describe() for _, definition in self._functions[channel].values()]

    def invoke(self, workflows, channel: str, name: str, arguments: dict, *,
               manager=None, query_provider=None) -> tuple[dict, str]:
        """Validate inputs, resolve the current owner, invoke once, and validate output.

        workflows (locked WorkflowData | None) supports direct workflow/action invocation.
        channel (str) selects mcp/admin; name (str) is an advertised command;
        arguments (dict) supplies its model fields. manager (WorkflowManager | None)
        acquires WorkflowData under its lock when workflows is not supplied;
        query_provider (UserQueryQueue | None) supplies the query service.
        Return (serialized output-model dict, acknowledgment str). Workflow and
        action callbacks share lock/health handling here. Direct WorkflowData
        callers supply any required lock; query callbacks need no manager.
        No operation is retried and no backend instance is retained.
        """
        try:
            owner, definition = self._functions[channel][name]
        except KeyError:
            raise FunctionError('invalid_input', 'Unknown function for this interface.') from None
        input_model, result_model = definition.models()
        validated = input_model.model_validate(arguments)

        # Resolve owner instances at invocation, never from discovery closures.
        try:
            if owner == 'queries':
                if query_provider is None:
                    raise FunctionError('manager_unavailable', 'User-query service is unavailable.')
                value = definition.invoke(query_provider, validated)
            elif workflows is not None:
                value = definition.invoke(workflows if owner == 'workflows' else workflows.getManager(owner), validated)
            else:
                if manager is None:
                    raise FunctionError('manager_unavailable', 'Workflow manager is not running.')
                with manager as locked_workflows:
                    if not manager.isAlive():
                        raise FunctionError('manager_unavailable', 'Workflow manager is not running.')
                    value = definition.invoke(locked_workflows if owner == 'workflows'
                                              else locked_workflows.getManager(owner), validated)
        except ValidationError as exc:
            wfmanLog(f'Invalid model constructed by {name}: {type(exc).__name__}')
            raise FunctionError('operation_failed', 'Function returned an invalid result; check the server log.') from exc

        # The callable returns its output model; a faulty result must not retry effects.
        try:
            if not isinstance(value, result_model):
                raise TypeError('Operation did not return its annotated output model')
            result = result_model.model_validate(value.model_dump()).model_dump(mode='json')
            json.dumps(result, allow_nan=False)
        except (ValueError, TypeError) as exc:
            wfmanLog(f'Invalid result from {name}: {type(exc).__name__}')
            raise FunctionError('operation_failed', 'Function returned an invalid result; check the server log.') from exc
        return result, definition.success_message
