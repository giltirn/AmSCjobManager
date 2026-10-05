"""Smoke-test the existing IRI API wrappers against a configured NERSC sandbox.

Run manually with an IRI manager configuration:

    python3 test/test_iri_api_smoke.py path/to/manager-config.json

The test intentionally checks only that each wrapper command is accepted by
the remote API.  It submits a short-lived, harmless compute job and cancels it
after querying its state.  It leaves a uniquely named directory and tiny file
inside the configured sandbox for inspection if a remote command fails.
"""

from io import BytesIO
from pathlib import PurePosixPath
import sys
import uuid

from amscjobmanager import iri_api
from amscjobmanager.manager_config import readManagerConfigFile


if len(sys.argv) != 2:
    raise SystemExit("Usage: python3 test/test_iri_api_smoke.py <manager-config.json>")


def run(name, operation):
    """Run one wrapper and report the command accepted by the IRI API."""
    operation()
    print(f"accepted: {name}")


config = readManagerConfigFile(sys.argv[1])
sandbox_directories = iri_api.setupWorkflowAgent(
    config.iriapi_key_path,
    config.transferapi_key_path,
    config.sandbox_directories,
)

machine = "perlmutter"
if machine not in sandbox_directories:
    raise SystemExit(
        f"Configuration must provide a sandbox directory for {machine!r}"
    )

sandbox = PurePosixPath(sandbox_directories[machine])
test_id = uuid.uuid4().hex
test_dir = sandbox / "amsc-job-manager-iri-api-smoke" / test_id
test_file = test_dir / "smoke.txt"

# Account/resource/status endpoints.
run("getKnownMachines", iri_api.getKnownMachines)
run("listSpecialGlobusEndpoints", iri_api.listSpecialGlobusEndpoints)
run("getMachineQueues", lambda: iri_api.getMachineQueues(machine))
run("getUserAccountProjects", lambda: iri_api.getUserAccountProjects(machine))
run("getResourceID(login)", lambda: iri_api.getResourceID(machine, "login"))
run("getResourceID(compute)", lambda: iri_api.getResourceID(machine, "compute"))
run("queryMachineStatus(login)", lambda: iri_api.queryMachineStatus(machine, "login"))
run("queryMachineStatus(compute)", lambda: iri_api.queryMachineStatus(machine, "compute"))

# Filesystem task endpoints.  All remote writes are confined to the configured
# sandbox, and subsequent calls use the artifacts created by the earlier ones.
run("remoteMkdir", lambda: iri_api.remoteMkdir(machine, str(test_dir)))
run(
    "uploadBytes",
    lambda: iri_api.uploadBytes(
        machine,
        str(test_file),
        BytesIO(b"AmSC job manager IRI API smoke test\n"),
    ),
)
run("remoteChmod", lambda: iri_api.remoteChmod(machine, str(test_file), "600"))
run("remoteLs", lambda: iri_api.remoteLs(machine, str(test_dir)))
run("pathStat", lambda: iri_api.pathStat(machine, str(test_file)))
run("pathType", lambda: iri_api.pathType(machine, str(test_file)))
run("downloadFileContents", lambda: iri_api.downloadFileContents(machine, str(test_file)))

# Compute job endpoints.  Use values advertised by the IRI API rather than
# embedding a project or queue that only works for one account.  The job is
# deliberately inert and is canceled after its state has been queried, even if
# the query itself raises an exception.
queues = iri_api.getMachineQueues(machine)
accounts = iri_api.getUserAccountProjects(machine)
if not queues:
    raise RuntimeError(f"No queues are available for {machine!r}")
if not accounts:
    raise RuntimeError(f"No accounts are available for {machine!r}")

job_id = None
try:
    job_id = iri_api.executeBatchJobCompat(
        machine,
        "sleep 120",
        nodes=1,
        ranks_per_node=1,
        gpus_per_rank=1,
        time="300",
        queue=queues[0][0],
        account=accounts[0],
        job_run_dir=str(test_dir),
        name=f"amsc-job-manager-iri-api-smoke-{test_id}",
    )
    print("accepted: executeBatchJobCompat")
    run("getJobState", lambda: iri_api.getJobState(machine, job_id))
finally:
    if job_id is not None:
        run("cancelJob", lambda: iri_api.cancelJob(machine, job_id))

print(f"IRI API smoke test completed; remote artifacts remain at {test_dir}")
