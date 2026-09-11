from amscjobmanager.manager_config import readManagerConfigFile, setupManager
from amscjobmanager.actions_base import ComputeActionBase
from amscjobmanager.api_general import executeBatchJobCompat
from amscjobmanager.manager import JobManager
from amscjobmanager.action_manager import ActionStatus
import sys
from dataclasses import dataclass
import time

@dataclass
class TestComputeAction(ComputeActionBase):
    toprint: str
    rundir: str

    def initiateAction(self, job_id)->str:
        script_body = f"""
echo "{self.toprint}"
sleep 20
""" 
        return executeBatchJobCompat(self.machine, script_body, nodes=1, ranks_per_node=1, gpus_per_rank=0,  time=self.time, queue=self.queue, account=self.account, job_run_dir=self.rundir, exclusive=True, allow_unsafe=False)



if len(sys.argv) == 1:
    raise Exception("Must provide the manager configuration JSON")

config = readManagerConfigFile(sys.argv[1])
print(type(config))
setupManager(config)

man = JobManager(poll_freq=2)
man.start()

with man as jd:
    jobid = jd.enqueueJob([ TestComputeAction(machine="local", account="", queue="", time=600, rundir=config.sandbox_directories["local"], toprint="Hello world!") ])

status = None
while status != ActionStatus.COMPLETED:
    with man as jd:
        status = jd.jobStatus(jobid)
    print("MAIN THREAD POLL", status)
    time.sleep(2)

man.stop()
