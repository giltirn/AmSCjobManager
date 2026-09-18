from .actions_base import ComputeActionBase
from .api_general import executeBatchJobCompat
from dataclasses import dataclass

@dataclass
class ExecuteBatchScriptComputeAction(ComputeActionBase):
    script_body: str
    rundir: str
    nodes: int
    ranks_per_node: int
    gpus_per_rank: int
    exclusive : bool = True

    def initiateAction(self, job_id)->str:
        return executeBatchJobCompat(self.machine, self.script_body, self.nodes, self.ranks_per_node, self.gpus_per_rank,  time=self.time, queue=self.queue, account=self.account, job_run_dir=self.rundir, exclusive=self.exclusive, allow_unsafe=False)