import re
from dataclasses import field
from typing import Annotated, Callable, Union
from typing_extensions import TypeAliasType
from pydantic import AfterValidator, ConfigDict, Field, StrictStr, TypeAdapter
from pydantic.dataclasses import dataclass
from enum import Enum

"""Actions are classes of operations that can be performed via the AmSC infrastructure"""

def replaceJobIdSubstring(in_str, job_id):
    """Replace instances of <JOBID> with the job index in path strings"""
    return re.sub(r'<JOBID>', str(job_id), in_str)

def _validateDependencyCondition(condition: str | list) -> str | list:
    """Validate alternating operands and Boolean operators in one expression list.

    condition (str | list) is a Pydantic-validated nonempty key or recursive
    list of such keys/lists. Return the unchanged value. Lists use zero-based
    even positions for operands and odd positions for uppercase AND/OR strings.
    Reject missing operands and operators used as operands. Pydantic applies
    this validator recursively; this does not check producer existence.
    """
    if isinstance(condition, list):
        if len(condition) % 2 == 0:
            raise ValueError('Dependency expressions must alternate operands and operators, ending with an operand')
        for index, item in enumerate(condition):
            if index % 2:
                if not isinstance(item, str) or item not in ('AND', 'OR'):
                    raise ValueError('Dependency operators must be AND or OR')
            elif isinstance(item, str) and item in ('AND', 'OR'):
                raise ValueError('AND and OR must occur between dependency operands')
    return condition


# A named recursive alias gives Python, JSON, and MCP the same nested-array
# schema. The list validator handles alternation; no expression text is parsed.
DependencyCondition = TypeAliasType(
    'DependencyCondition',
    Annotated[
        Union[Annotated[StrictStr, Field(min_length=1)],
              Annotated[list['DependencyCondition'], Field(min_length=1, strict=True)]],
        AfterValidator(_validateDependencyCondition),
    ],
)
dependency_condition_adapter = TypeAdapter(DependencyCondition)


def evaluateDependencies(condition: DependencyCondition | None, completed: Callable[[str], bool]) -> bool:
    """Evaluate a nested-list dependency condition using producer completion results.

    condition (str | list | None) is a producer key or an infix expression such
    as ['prepared', 'AND', ['solver_a', 'OR', 'solver_b']]. Nested lists group
    operands; AND takes precedence over OR. None means no condition and returns
    True. completed (Callable[[str], bool]) reports whether each referenced key
    has a successful producer. It is called for each key occurrence; errors
    propagate. Return bool after structural validation. This utility does not
    access storage or enforce scheduler dependencies.
    """
    if condition is None:
        return True
    condition = dependency_condition_adapter.validate_python(condition)

    # Resolve every key before evaluation. Only generated Boolean literals,
    # validated operator translations, and grouping enter the Python expression.
    def booleanExpression(operand: str | list) -> str:
        """Translate operand (key str | nested list) to controlled Boolean text.

        Return str containing only True/False, and/or, parentheses, and spaces.
        Resolve keys through the enclosing completed callback, never copying key
        text or callback representations into the generated expression.
        """
        if isinstance(operand, str):
            return 'True' if completed(operand) else 'False'
        return '(' + ' '.join(item.lower() if index % 2 else booleanExpression(item)
                              for index, item in enumerate(operand)) + ')'

    return eval(booleanExpression(condition), {'__builtins__': {}}, {})


@dataclass(config=ConfigDict(extra="forbid"))
class ActionDefinition:
    """Common optional metadata for queued workflow action definitions.

    depends_on (str | list | None) is a key or nested infix expression using
    AND/OR, with nested lists acting as parentheses and normal AND precedence.
    None means no explicit prerequisites. producer_key (str | None) labels
    successful completion, with repeated labels permitted.
    Both fields are keyword-only. JobData checks producer existence at submission,
    waits for successful completion before scheduling, and propagates cancellation
    when required unfinished producers are canceled. Keys are database-scoped.
    """

    depends_on: DependencyCondition | None = field(default=None, kw_only=True)
    producer_key: StrictStr | None = field(default=None, kw_only=True)


@dataclass(config=ConfigDict(extra="forbid"))
class TransferActionBase(ActionDefinition):
    """Base class of all transfer actions"""

    def queryResourceStatus(self) -> bool | None:
        """Return whether the transfer resource is up, if this can be queried.

        ``None`` means no availability query is available for this action.
        Transfer submission then relies on the normal API error handling.
        """
        return None

    def getInfo(self)->dict:
        """
        Return the transfer information in a common dictionary format with entries {"origin", "destination"}
        """
        raise NotImplementedError("Derived class must implement getInfo")

    def initiateAction(self, job_id, name: str | None = None)->str:
        """Initiate the action and return the API key; name identifies the action."""
        raise NotImplementedError("Derived class must implement initiateAction")

@dataclass(config=ConfigDict(extra="forbid"))
class ComputeActionBase(ActionDefinition):
    """Common target and scheduling attributes for compute action definitions.

    machine (str) selects a configured target or local; account/queue (str)
    select the project and scheduler queue. time (str) is walltime in seconds.
    Subclasses define execution fields and may implement provider operations.
    """

    machine : StrictStr
    account : StrictStr
    queue : StrictStr
    time : StrictStr

    def queryResourceStatus(self) -> bool:
        """Return whether this action's compute resource reports being up.

        API failures propagate so scheduling can defer without recording a
        remote submission attempt.
        """
        from .api_general import queryMachineStatus

        return queryMachineStatus(self.machine)

    def getInfo(self)->dict:
        """
        Return the transfer information in a common dictionary format with entries {"machine", "queue", "time"}

        NOTE: Derived classes can override this to provide any custom information they want
        """
        return {"machine" : self.machine, "queue" : self.queue, "time" : self.time}    

    def initiateAction(self, job_id, name: str | None = None)->str:
        """Initiate the action and return the API key; name identifies the action."""
        raise NotImplementedError("Derived class must implement initiateAction")

class ActionClass(Enum):
    NONE = 0
    TRANSFER = 1
    COMPUTE = 2
    API = 3
    
def actionClass(action):
    from .api_actions import APIAction

    if issubclass(type(action), TransferActionBase):
        return ActionClass.TRANSFER
    elif issubclass(type(action), ComputeActionBase):
        return ActionClass.COMPUTE
    elif isinstance(action, APIAction):
        return ActionClass.API
    else:
        raise Exception("Unknown action type",type(action))
