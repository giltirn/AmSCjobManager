from .actions_base import ComputeActionBase
from dataclasses import field
from pydantic import ConfigDict, Field, StrictBool, StrictStr
from pydantic.dataclasses import dataclass
from typing import Annotated, List, Literal
from .api_general import executeBatchJobCompat, executeJob

@dataclass(config=ConfigDict(extra="forbid"))
class ExecuteBatchScriptComputeAction(ComputeActionBase):
    """Run a batch-script stage with checked working-directory API access.

    script_body (str) is shell code; rundir (str) is the target working directory.
    nodes/ranks_per_node (positive int) and gpus_per_rank (nonnegative int) set
    resource counts; exclusive (bool) defaults to True. Inherited machine,
    account, queue and time select the target, project, queue and walltime in
    seconds (str). type is the JSON discriminator, always 'batch_script'.
    """

    script_body: StrictStr
    rundir: Annotated[StrictStr, Field(description="Target working directory inside a configured sandbox.")]
    ranks_per_node: Annotated[int, Field(strict=True, ge=1, description="MPI ranks per node.")]
    gpus_per_rank: Annotated[int, Field(strict=True, ge=0, description="GPUs per MPI rank.")]
    exclusive: StrictBool = True
    type: Literal["batch_script"] = field(default="batch_script", kw_only=True)

    def initiateAction(self, job_id, name: str | None = None)->str:
        return executeBatchJobCompat(self.machine, self.script_body, self.nodes, self.ranks_per_node, self.gpus_per_rank,  time=self.time, queue=self.queue, account=self.account, job_run_dir=self.rundir, exclusive=self.exclusive, allow_unsafe=False, name=name)


@dataclass(config=ConfigDict(extra="forbid"))
class ExecuteJobComputeAction(ComputeActionBase):
    """Run an executable with optional pre-launch and post-launch shell stages.

    executable (str), arguments (list[str]) and rundir (str) describe execution
    on machine (str). nodes/ranks_per_node (positive int), gpus_per_rank
    (nonnegative int), account/queue/time (str; time in seconds), and exclusive
    (bool, default True) specify resources. pre_launch/post_launch (str | None)
    are optional shell code. type is the JSON discriminator, 'execute_job'.
    """

    executable: Annotated[StrictStr, Field(description="Executable path or command on the target machine.")]
    arguments: List[StrictStr]
    rundir: Annotated[StrictStr, Field(description="Target working directory inside a configured sandbox.")]
    ranks_per_node: Annotated[int, Field(strict=True, ge=1, description="MPI ranks per node.")]
    gpus_per_rank: Annotated[int, Field(strict=True, ge=0, description="GPUs per MPI rank.")]
    pre_launch: StrictStr | None = None
    post_launch: StrictStr | None = None
    exclusive: StrictBool = True
    type: Literal["execute_job"] = field(default="execute_job", kw_only=True)

    def initiateAction(self, job_id, name: str | None = None) -> str:
        return executeJob(
            self.machine, self.executable, self.arguments, self.nodes, self.ranks_per_node,
            self.gpus_per_rank,
            time=self.time, queue=self.queue, account=self.account,
            job_run_dir=self.rundir, exclusive=self.exclusive,
            allow_unsafe=False, name=name, pre_launch=self.pre_launch,
            post_launch=self.post_launch,
        )
