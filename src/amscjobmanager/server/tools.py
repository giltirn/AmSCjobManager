"""MCP transport adapter for backend-provided function schemas and operations."""

import asyncio
import json

from mcp.server import MCPServer
from mcp.types import CallToolResult, ListToolsResult, TextContent, Tool, ToolAnnotations

from ..logging import wfmanLog
from ..manager_functions import FunctionError
from .runtime import ServerRuntime


class ManagerMCPServer(MCPServer):
    """Adapt runtime function schemas to dynamically discovered MCP tools."""

    def __init__(self, runtime: ServerRuntime):
        """Retain runtime (ServerRuntime) for discovery/dispatch; return None without starting it."""
        self.runtime = runtime
        super().__init__('AmSC Job Manager', log_level='WARNING')

    async def list_tools(self) -> list[Tool]:
        """Take no arguments; return baseline tools plus published manager tools when awaited.

        Initialization leaves authentication tools discoverable. The protocol
        handler includes the initialization message as discovery metadata.

        Implements MCPServer.list_tools
        """
        descriptions = self.runtime.describeFunctions('mcp', include_unready=True)
        return [Tool(name=item.name, description=item.description,
                             input_schema=item.input_schema, output_schema=item.output_schema,
                             annotations=ToolAnnotations(read_only_hint=item.read_only,
                                                         destructive_hint=not item.read_only if item.destructive is None else item.destructive,
                                                         idempotent_hint=item.idempotent))
                        for item in descriptions]

    async def _handle_list_tools(self, ctx, params) -> ListToolsResult:
        """Serve MCP discovery with readiness metadata; ctx/params are SDK request objects.

        Return ListToolsResult when awaited. Baseline user-query tools remain
        listed even if manager discovery reports server_initializing/unavailable.
        No client notifications or background discovery loops are started.

        Implements MCPServer._handle_list_tools
        """
        tools = await self.list_tools()
        metadata = {}
        try:
            self.runtime.describeFunctions('mcp')
        except FunctionError as exc:
            metadata['initialization_error'] = {'code': exc.code, 'message': str(exc)}
        return ListToolsResult(tools=tools, _meta=metadata, ttl_ms=0)

    async def call_tool(self, name: str, arguments: dict, context=None) -> CallToolResult:
        """Call a built-in or runtime-registered MCP operation.

        name (str) and arguments (dict) are SDK-validated request fields;
        context is the SDK invocation context or None. Return CallToolResult
        when awaited. Manager methods run off the event loop, behind readiness,
        input/output validation and the WorkflowManager lock. Admin names are rejected.
        """
        try:
            result, _ = await asyncio.to_thread(self.runtime.invokeFunction, 'mcp', name, arguments)
            return CallToolResult(structured_content=result,
                                  content=[TextContent(type='text', text=json.dumps(result['result'] if set(result) == {'result'} else result))])
        except FunctionError as exc:
            code, message = exc.code, str(exc)
        except ValueError as exc:
            code, message = 'invalid_input', str(exc)
        except Exception as exc:
            wfmanLog(f'Manager function {name} failed: {type(exc).__name__}: {exc}')
            code, message = 'operation_failed', 'Operation failed; check the server log.'
        return CallToolResult(is_error=True, structured_content={'error': {'code': code, 'message': message}},
                              content=[TextContent(type='text', text=message)])
