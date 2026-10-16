"""Integration-test native and compatibility IRI compute submissions.

Run manually with a configured IRI machine:

    python3 test/test_iri_compute_execution.py path/to/manager-config.json perlmutter <account>
    python3 test/test_iri_compute_execution.py path/to/manager-config.json aurora <account>

Each invocation submits two short jobs to one node: ``executeJob`` and
``executeBatchJobCompat``.  The test leaves its separate, uniquely named
remote directories behind so that their ``run.log`` and ``err.log`` files can
be inspected if a submission fails.
"""

from pathlib import PurePosixPath
import sys
import time
import uuid

from amscjobmanager import iri_api
from amscjobmanager.manager_config import readManagerConfigFile
from amscjobmanager.manager_config_models import ManagerConfig


MACHINE_TESTS = {
    "aurora": {
        "rank_environment": "PALS_RANKID",
        "ranks_per_node": 8,
        "gpus_per_rank": 1,
        "compatibility_launch": "mpiexec -np 8 ./script.sh",
    },
    "perlmutter": {
        "rank_environment": "SLURM_PROCID",
        "ranks_per_node": 4,
        "gpus_per_rank": 1,
        "compatibility_launch": "srun -n 4 ./script.sh",
    },
}
ACTIVE_JOB_STATES = {"new", "queued", "active"}
POLL_SECONDS = 5


if len(sys.argv) != 4:
    template_path = ManagerConfig.write_template_json()
    raise SystemExit(
        "Usage: python3 test/test_iri_compute_execution.py "
        "<manager-config.json> <perlmutter|aurora> <account>\n"
        f"Configuration template available at {template_path}"
    )

config = readManagerConfigFile(sys.argv[1])
if config.iri_config_cache_directory is not None:
    iri_api.loadCachedImplementations(config.iri_config_cache_directory)
machine = sys.argv[2].lower()
account = sys.argv[3]
try:
    machine_test = MACHINE_TESTS[machine]
except KeyError as exc:
    raise SystemExit(
        f"This test currently supports: {', '.join(MACHINE_TESTS)}"
    ) from exc

try:
    implementation = iri_api.MACHINE_IMPLEMENTATIONS[machine]
    implementation_config = config.iri_implementations[implementation]
except KeyError as exc:
    raise SystemExit(
        f"Configuration must provide IRI credentials for {machine!r} "
        f"({iri_api.MACHINE_IMPLEMENTATIONS.get(machine, 'unknown implementation')!r})"
    ) from exc

sandbox_directories = iri_api.setupWorkflowAgent(
    {implementation: implementation_config},
    config.transferapi_key_path,
    config.sandbox_directories,
    config.globus_personal_endpoint,
    iri_config_cache_directory=config.iri_config_cache_directory,
)
if not sandbox_directories.get(machine):
    raise SystemExit(f"Configuration must provide a sandbox directory for {machine!r}")


def wait_for_completion(job_id: str, label: str) -> None:
    """Wait for a job and fail the test if it does not complete successfully."""
    while True:
        state = iri_api.getJobState(machine, job_id)
        print(f"{label}: {job_id} is {state}")
        if state not in ACTIVE_JOB_STATES:
            if state != "completed":
                raise RuntimeError(f"{label} failed with terminal state {state!r}")
            return
        time.sleep(POLL_SECONDS)


def verify_logs(
    label: str, job_directory: PurePosixPath, expected_files: set[str]
) -> None:
    """Check expected output files, rank output, and an empty stderr log."""
    directory_contents = {
        PurePosixPath(path).name
        for path in iri_api.remoteLs(machine, str(job_directory))
    }
    missing_files = expected_files - directory_contents
    if missing_files:
        raise RuntimeError(
            f"{label} directory is missing expected files {sorted(missing_files)}; "
            f"contents: {sorted(directory_contents)}"
        )
    run_log = iri_api.downloadFileContents(machine, str(job_directory / "run.log"))
    error_log = iri_api.downloadFileContents(machine, str(job_directory / "err.log"))
    missing_lines = []
    for rank in range(machine_test["ranks_per_node"]):
        for line in (
            f"Hello from rank {rank}",
            f"Rank {rank} is done",
        ):
            if line not in run_log:
                missing_lines.append(line)
    if missing_lines:
        raise RuntimeError(
            f"{label} run.log is missing expected output: {missing_lines}; "
            f"contents:\n{run_log}"
        )
    if error_log.strip():
        raise RuntimeError(f"{label} err.log is not empty:\n{error_log}")
    print(f"verified logs: {label}")


def select_queue() -> str:
    queues = iri_api.getMachineQueues(machine)
    for queue, _ in queues:
        if queue == "debug":
            return queue
    if not queues:
        raise RuntimeError(f"No queues are available for {machine!r}")
    return queues[0][0]


test_id = uuid.uuid4().hex
sandbox = PurePosixPath(sandbox_directories[machine][0])
test_root = sandbox / "amsc-job-manager-iri-compute" / test_id
native_dir = test_root / "execute-job"
compatibility_dir = test_root / "execute-batch-job-compat"
for directory in (native_dir, compatibility_dir):
    iri_api.remoteMkdir(machine, str(directory))

rank_environment = machine_test["rank_environment"]
rank_program = (
    f'echo "Hello from rank ${rank_environment}"\n'
    "sleep 10\n"
    f'echo "Rank ${rank_environment} is done"'
)
pre_launch = f'echo "pre-launch completed" > pre-launch.txt'
post_launch = f'echo "post-launch completed" > post-launch.txt'
queue = select_queue()
print(f"Using account {account} and queue {queue}")
common_job_options = {
    "nodes": 1,
    "ranks_per_node": machine_test["ranks_per_node"],
    "gpus_per_rank": machine_test["gpus_per_rank"],
    "time": "300",
    "queue": queue,
    "account": account,
}
print("Submitting test for executeJob")
native_job_id = iri_api.executeJob(
    machine,
    executable="/bin/bash",
    arguments=["-lc", rank_program],
    job_run_dir=str(native_dir),
    pre_launch=pre_launch,
    post_launch=post_launch,
    name=f"amsc-job-manager-execute-job-{test_id}",
    **common_job_options,
)
wait_for_completion(native_job_id, "executeJob")
verify_logs(
    "executeJob",
    native_dir,
    {"run.log", "err.log", "pre-launch.txt", "post-launch.txt"},
)

compatibility_script = f"""\
cat <<'EOF' > script.sh
#!/bin/bash
echo "Hello from rank ${rank_environment}"
sleep 10
echo "Rank ${rank_environment} is done"
EOF

chmod u+x script.sh
{machine_test["compatibility_launch"]}
"""
print("Submitting test for executeBatchJobCompat")
compatibility_job_id = iri_api.executeBatchJobCompat(
    machine,
    compatibility_script,
    job_run_dir=str(compatibility_dir),
    name=f"amsc-job-manager-execute-batch-job-compat-{test_id}",
    **common_job_options,
)
wait_for_completion(compatibility_job_id, "executeBatchJobCompat")
verify_logs("executeBatchJobCompat", compatibility_dir, {"run.log", "err.log"})

print(f"passed: {machine}; remote artifacts remain under {test_root}")
