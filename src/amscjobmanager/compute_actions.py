from .actions_base import ComputeActionBase
from dataclasses import dataclass
from typing import List
from .api_general import executeBatchJobCompat, executeJob

@dataclass
class ExecuteBatchScriptComputeAction(ComputeActionBase):
    script_body: str
    rundir: str
    nodes: int
    ranks_per_node: int
    gpus_per_rank: int
    exclusive : bool = True

    def initiateAction(self, job_id, name: str | None = None)->str:
        return executeBatchJobCompat(self.machine, self.script_body, self.nodes, self.ranks_per_node, self.gpus_per_rank,  time=self.time, queue=self.queue, account=self.account, job_run_dir=self.rundir, exclusive=self.exclusive, allow_unsafe=False, name=name)


@dataclass
class ExecuteJobComputeAction(ComputeActionBase):
    """Submit a job through IRI's pre-launch/executable/post-launch interface."""

    executable: str
    arguments: List[str]
    rundir: str
    nodes: int
    ranks_per_node: int
    gpus_per_rank: int
    pre_launch: str | None = None
    post_launch: str | None = None
    exclusive: bool = True

    def initiateAction(self, job_id, name: str | None = None) -> str:
        return executeJob(
            self.machine, self.executable, self.arguments, self.nodes, self.ranks_per_node,
            self.gpus_per_rank,
            time=self.time, queue=self.queue, account=self.account,
            job_run_dir=self.rundir, exclusive=self.exclusive,
            allow_unsafe=False, name=name, pre_launch=self.pre_launch,
            post_launch=self.post_launch,
        )
