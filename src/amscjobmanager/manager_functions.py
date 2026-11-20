"""Typed, transport-independent descriptions of explicitly exposed operations."""

from dataclasses import dataclass
import inspect
from typing import Callable, get_type_hints

from pydantic import BaseModel, ConfigDict, Field, StrictStr, field_validator


class FunctionArguments(BaseModel):
    """Base input contract: reject extra fields and omit values from validation errors."""

    model_config = ConfigDict(extra='forbid', strict=True, hide_input_in_errors=True)


class ConfiguredMachineArguments(FunctionArguments):
    """Required machine (nonempty str) selecting a configured target, normalized lowercase."""

    machine: StrictStr = Field(min_length=1, description='Configured machine name, or local; case-insensitive.')

    @field_validator('machine')
    @classmethod
    def validateMachine(cls, value: str) -> str:
        """Validate value (str) against configured targets and return its lowercase name."""
        from .api_general import getKnownMachines

        value = value.lower()
        if value not in getKnownMachines():
            raise ValueError('Machine is not configured')
        return value


class EmptyFunctionResult(BaseModel):
    """Successful operation without a return value; result is always None."""

    result: None = Field(default=None, description='No value is returned by this operation.')


class FunctionDescription(BaseModel):
    """Public command metadata shared by MCP and local administration.

    name/description (str) identify and explain an external operation.
    input_schema/output_schema (dict) are JSON schemas for the operation models.
    parameter_order (list[str]) orders input names for positional CLI arguments.
    read_only/idempotent (bool) describe effects and repeatability, not permission.
    destructive (bool | None) overrides the mutation hint; None uses not read_only.
    No method names, manager references, passwords or credential state are exposed.
    """

    model_config = ConfigDict(extra='forbid')
    name: str
    description: str
    input_schema: dict
    output_schema: dict
    parameter_order: list[str] = Field(default_factory=list)
    read_only: bool = False
    idempotent: bool = False
    destructive: bool | None = None


class FunctionError(ValueError):
    """A backend-facing operation error carrying a stable public code and message."""

    def __init__(self, code: str, message: str):
        """Store code/message (str); return None without logging request data."""
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class FunctionDefinition:
    """Expose an annotated callable and its public metadata.

    name/description (str) identify the operation. invoke (callable) accepts the
    resolved owner and a validated Pydantic input instance, returning a Pydantic
    output instance. Models are inferred from the second parameter and return
    annotations; define model classes at module scope for postponed annotations.
    read_only/idempotent (bool) describe effects/repeatability; destructive
    (bool | None) optionally overrides the inferred mutation hint. success_message
    (str) supplies an admin acknowledgment. Callables must accept their owner
    explicitly instead of retaining a bound manager instance.
    """

    name: str
    description: str
    invoke: Callable
    read_only: bool = False
    idempotent: bool = False
    destructive: bool | None = None
    success_message: str = 'Operation completed.'

    def models(self) -> tuple[type[BaseModel], type[BaseModel]]:
        """Infer the input/output Pydantic classes from the operation annotations.

        Take no arguments. Return (input model class, output model class).
        Reject bound methods, unresolved/missing model annotations, and callables
        that cannot accept exactly two positional arguments (owner and input).
        No operation is executed during introspection.
        """
        if not callable(self.invoke) or inspect.ismethod(self.invoke):
            raise ValueError(f'Function must be an unbound callable: {self.name}')
        parameters = list(inspect.signature(self.invoke).parameters.values())
        if len(parameters) != 2 or any(parameter.kind not in (
                inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
                for parameter in parameters):
            raise ValueError(f'Function must accept owner and input positionally: {self.name}')
        try:
            hints = get_type_hints(self.invoke)
            input_model = hints[parameters[1].name]
            result_model = hints['return']
        except (KeyError, NameError, TypeError) as exc:
            raise ValueError(f'Function requires resolvable input/output annotations: {self.name}') from exc
        if any(not isinstance(model, type) or not issubclass(model, BaseModel)
               or getattr(model, '__pydantic_root_model__', False)
               for model in (input_model, result_model)):
            raise ValueError(f'Function input/output must be Pydantic object models: {self.name}')
        return input_model, result_model

    def describe(self) -> FunctionDescription:
        """Generate public schemas from the annotated callable without executing it.

        Take no arguments. Return FunctionDescription; parameter_order follows
        the input model's declaration order for positional CLI arguments.
        """
        input_model, result_model = self.models()
        return FunctionDescription(
            name=self.name, description=self.description,
            input_schema=input_model.model_json_schema(),
            output_schema=result_model.model_json_schema(),
            parameter_order=list(input_model.model_fields),
            read_only=self.read_only, idempotent=self.idempotent, destructive=self.destructive,
        )
