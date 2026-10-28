"""Account-free reference workflow using JobManager and the SPOOF backend.

Run from the repository root, with the package dependencies installed:

    PYTHONPATH=src python examples/spoof_workflow_apiv1.py

Initial setup uses filesystem operations to stage a calculation and settings
on Perlmutter and fake input data on Polaris. The managed workflow then runs:

    Polaris --data transfer--> Perlmutter --jobspec compute--> result
                                                            |
                                                     data transfer
                                                            |
                                                            v
                                                  personal endpoint

SPOOF logs filesystem requests and simulates transfer/compute completion; it
does NOT store remote files, execute the calculation, or move data. Only the
temporary local source files, placeholder download and SQLite workflow database
are real. They are removed when the example finishes. No tokens, accounts or
endpoints are used. Directory listings, types, metadata and downloaded contents
are illustrative fixed responses, not evidence that the calculation ran.

This illustrates the available v1-style API operations, not every operation
in the v1 specification. Cancellation is not demonstrated. getUsername is omitted because
remote username lookup is a v2 feature. No batch-script compute action is used.
"""

import os

# Backend selection happens at import time: this MUST precede package imports.
# Deliberately override an inherited IRI setting so this example cannot submit
# real jobs or transfers when run as a standalone script.
os.environ["AMSC_JOB_MANAGER_API_IMPL"] = "SPOOF"

import io
import json
from pathlib import Path, PurePosixPath
import tempfile
import time
import uuid

from amscjobmanager import ActionStatus, JobManager, api_general as api
from amscjobmanager import globals as manager_globals
from amscjobmanager import spoof_api
from amscjobmanager.compute_actions import ExecuteJobComputeAction
from amscjobmanager.globus_transfer_action import GlobusTransfer
from amscjobmanager.manager_config_models import ManagerConfig


CALCULATION = '''\
import json
import os
from pathlib import Path

rank = int(os.environ.get("SLURM_PROCID", "0"))
settings = json.loads(Path("settings.json").read_text())
values = [float(value) for value in Path("input/dataset.txt").read_text().split()]
result = sum(values) * settings["scale"]
print(f"Hello from rank {rank}: scaled sum = {result}")
if rank == 0:
    Path("output/result.txt").write_text(f"{result}\\n")
'''


def main() -> None:
    """Stage fake inputs, enqueue three actions, and watch JobManager run them."""
    if manager_globals.api_impl != "SPOOF":
        raise RuntimeError("Run this example in a fresh Python process with the SPOOF backend")

    # Use SPOOF's default randomized 1–3 second compute and transfer durations.
    spoof_api.resetSpoofActionState()
    run_id = uuid.uuid4().hex[:12]
    perlmutter_sandbox = PurePosixPath("/global/cfs/cdirs/my_proj1/example-user/agent_safe_dir")
    polaris_sandbox = PurePosixPath("/lus/eagle/projects/my_proj1/example-user/agent_safe_dir")
    run_dir = perlmutter_sandbox / f"spoof-reference-{run_id}"
    polaris_dir = polaris_sandbox / f"spoof-reference-{run_id}"
    # Eagle's Globus namespace omits the filesystem's /lus/eagle/projects prefix.
    polaris_transfer_dir = PurePosixPath("/") / polaris_dir.relative_to("/lus/eagle/projects")
    personal_result_dir = PurePosixPath("/staging/results") / run_id

    with tempfile.TemporaryDirectory(prefix="amsc-spoof-example-") as temporary_directory:
        local_dir = Path(temporary_directory)
        config = ManagerConfig(
            # These token paths are illustrative only: SPOOF never reads them.
            iri_implementations={
                "nersc": {"api_version": 1, "token_path": str(local_dir / "unused-nersc-token.json")},
                "alcf": {"api_version": 1, "token_path": str(local_dir / "unused-alcf-token.json")},
            },
            transferapi_key_path=str(local_dir / "unused-transfer-token.json"),
            sandbox_directories={
                "perlmutter": [str(perlmutter_sandbox)],
                "polaris": [str(polaris_sandbox)],
                "local": [str(local_dir)],
            },
            globus_personal_endpoint={
                "endpoint_uuid": "00000000-0000-0000-0000-000000000003",
                "staging_directory_globus": "/staging",
                "staging_directory_local": str(local_dir),
            },
        )
        api.setupWorkflowAgentFromConfig(config)

        # 1. Initial metadata and filesystem setup, before managed execution.
        print("\nSPOOF example: all remote filesystem and execution activity is simulated.")
        print("Configured machines:", sorted(set(api.getKnownMachines())))
        print("Endpoint tags advertised by SPOOF:", api.listSpecialGlobusEndpoints())
        for resource_type in ("compute", "storage"):
            if not api.queryMachineStatus("perlmutter", rtype=resource_type):
                raise RuntimeError(f"Perlmutter {resource_type} is unavailable")
        projects = api.getUserAccountProjects("perlmutter")
        account = projects[0]
        print("Example account:", account)
        print("Allocation:", api.getProjectAllocation("perlmutter", account))
        print("Queues:", api.getMachineQueues("perlmutter"))

        for directory in (run_dir, run_dir / "input", run_dir / "output"):
            api.remoteMkdir("perlmutter", str(directory))
        api.remoteChmod("perlmutter", str(run_dir), "700")
        api.remoteMkdir("polaris", str(polaris_dir))

        # Real local sources illustrate uploading small files through the
        # filesystem API, rather than using a cross-machine data transfer.
        calculation_file = local_dir / "calculate.py"
        calculation_file.write_text(CALCULATION, encoding="utf-8")
        settings_file = local_dir / "settings.json"
        settings_file.write_text(json.dumps({"scale": 2}), encoding="utf-8")
        for source_file in (calculation_file, settings_file):
            api.uploadSmallFile(
                "perlmutter", str(run_dir / source_file.name), str(source_file),
            )
        api.remoteChmod("perlmutter", str(run_dir / "calculate.py"), "755")
        api.uploadBytes("polaris", str(polaris_dir / "dataset.txt"), io.BytesIO(b"1 2 3 4\n"))
        print("Illustrative directory listing:", api.remoteLs("perlmutter", str(run_dir)))
        print("Illustrative file metadata:", api.pathStat("perlmutter", str(run_dir / "settings.json")))
        print("Illustrative file type:", api.pathType("perlmutter", str(run_dir / "calculate.py")))
        print("Simulated directory check:", api.isDirectory("perlmutter", str(run_dir)))
        print("Simulated existence check:", api.pathExists("perlmutter", str(run_dir / "settings.json")))

        # Endpoint arguments can be UUIDs or configured tags. This SPOOF example
        # illustrates the "perlmutter" tag alongside dummy UUIDs for Polaris and
        # the personal collection. Real deployments must map tags to collection
        # UUIDs; NERSC transfers should use its DTN collection.
        polaris_collection = "00000000-0000-0000-0000-000000000001"
        perlmutter_endpoint = "perlmutter"
        personal_collection = config.globus_personal_endpoint.endpoint_uuid

        # 2. Build the sequential workflow. JobManager owns submission,
        # polling, action ordering and durable state; no direct executeJob or
        # globusCopy calls are needed here. Transfer destinations are directories.
        workflow = [
            GlobusTransfer(
                source_endpoint=polaris_collection,
                source_path=str(polaris_transfer_dir / "dataset.txt"),
                dest_endpoint=perlmutter_endpoint,
                dest_path=f"{run_dir}/input/",
            ),
            ExecuteJobComputeAction(
                machine="perlmutter", account=account, queue="debug", time="300",
                executable="/usr/bin/python3", arguments=["calculate.py"],
                rundir=str(run_dir), nodes=1, ranks_per_node=4, gpus_per_rank=1,
                pre_launch="echo 'Pre-launch setup'; mkdir -p output",
                post_launch="echo 'Post-launch inspection'; test -s output/result.txt",
            ),
            GlobusTransfer(
                source_endpoint=perlmutter_endpoint,
                source_path=str(run_dir / "output/result.txt"),
                dest_endpoint=personal_collection,
                dest_path=f"{personal_result_dir}/",
            ),
        ]

        # 3. Run the actual background JobManager. Its database is real and
        # on disk here; use a persistent filename to retain state in an application.
        manager = JobManager(filename=str(local_dir / "workflow.sqlite"), poll_freq=0,
                             max_workflows_active=1)
        try:
            with manager as jobs:
                job_id = jobs.enqueueJob(workflow)
            print(f"\nEnqueued workflow {job_id}: transfer -> jobspec compute -> transfer")
            manager.start()
            previous_state = None
            while True:
                # Always access the manager's database under its lock.
                with manager as jobs:
                    status = jobs.jobStatus(job_id)
                    state = jobs.jobState(job_id)
                summary = (status.name, state["workflow_stage"], state["head_action_type"])
                if summary != previous_state:
                    print(f"Workflow {job_id}: {summary}")
                    previous_state = summary
                if status == ActionStatus.COMPLETED:
                    break
                if status == ActionStatus.FAILED:
                    raise RuntimeError(f"Workflow {job_id} failed: {state}")
                if not manager.isAlive():
                    raise RuntimeError("JobManager stopped before the workflow completed")
                time.sleep(0.2)
        finally:
            manager.stop(wait_until_done=False)
            manager.job_data.conn.close()

        # SPOOF records action events, allowing inspection without pretending
        # that the calculation ran or that result.txt was really generated.
        submissions = [event for event in spoof_api.getSpoofActionHistory()
                       if event["event"] == "submitted"]
        if [event["action_type"] for event in submissions] != ["transfer", "compute", "transfer"]:
            raise RuntimeError("Unexpected simulated action order")
        for event in submissions:
            print(f"Simulated {event['action_type']}: {event['api_key']} (label {event['name']})")
        # Small-file reads/downloads use the filesystem API, independently of
        # the managed bulk transfer to the personal endpoint above.
        result_path = str(run_dir / "output/result.txt")
        print("Illustrative result listing:", api.remoteLs("perlmutter", str(run_dir / "output")))
        print("Placeholder result contents:", api.downloadFileContents("perlmutter", result_path).strip())
        local_result = api.downloadFile("perlmutter", str(local_dir / "fake-result.txt"), result_path)
        print("Locally downloaded placeholder:", Path(local_result).read_text(encoding="utf-8").strip())
        print(f"\nSimulated result destination: {personal_collection}:{personal_result_dir}/result.txt")
        print("Workflow completed. No real remote files, transfers or calculations were performed.")


if __name__ == "__main__":
    main()
