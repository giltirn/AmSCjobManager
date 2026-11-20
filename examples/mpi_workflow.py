r"""Upload, compile, run and retrieve a real MPI reduction through WorkflowManager.

    python examples/mpi_workflow.py manager-config.json local --ranks-per-node 4
    python examples/mpi_workflow.py manager-config.json frontier \
        --account YOUR_PROJECT --queue batch --nodes 2 --ranks-per-node 4

A staged API action creates the run/result directories and uploads the source.
The next stage compiles in pre-launch and runs MPI on the selected machine.
Results are downloaded after successful completion. Every run retains its
source, executable, logs, result and SQLite databases for inspection.

The example follows eight steps, marked in main():
    1. Read the requested machine and resources.
    2. Select the machine's compiler and module setup.
    3. Initialize the API from the user configuration.
    4. Select sandboxed artifact locations and initialize manager state.
    5. Describe directory creation and source upload as an API action.
    6. Describe compilation and execution as a compute action.
    7. Enqueue the API and compute stages, then monitor the workflow.
    8. Download and validate the completed calculation's result.

See examples/README.md for configuration and build-profile overrides.
"""

import argparse
import json
import math
from pathlib import Path, PurePosixPath
import shlex
import time
import uuid

from amscjobmanager import ActionStatus, WorkflowManager, api_general as api
from amscjobmanager import globals as manager_globals
from amscjobmanager.api_actions import APIAction, MakeDirectory, UploadFile
from amscjobmanager.compute_actions import ExecuteJobComputeAction
from amscjobmanager.local_api import setStatusManagerDatabasePath
from amscjobmanager.manager_config import readManagerConfigFile


def create_parser() -> argparse.ArgumentParser:
    """Take no arguments; return the example's configured CLI argument parser.

    The parser describes the target, resource request, build overrides, artifact
    locations and polling interval. It does not parse arguments or contact APIs.
    """
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("config", help="Manager configuration JSON file")
    parser.add_argument("machine", type=str.lower, help="Configured machine, or local")
    parser.add_argument("--account", help="Required for remote execution")
    parser.add_argument("--queue", help="Required for remote execution")
    parser.add_argument("--nodes", type=int, default=1)
    parser.add_argument("--ranks-per-node", type=int, default=1)
    parser.add_argument("--gpus-per-rank", type=int, default=0, help="Requested GPU count; the calculation itself uses CPUs")
    parser.add_argument("--time", type=int, default=300, help="Remote job walltime in seconds")
    parser.add_argument("--build-config", help="JSON mapping machine names to replacement build profiles")
    parser.add_argument("--sandbox-directory", help="Register and use this machine sandbox instead of the first configured sandbox")
    parser.add_argument("--output-directory", help="Empty local output directory inside a local sandbox or /tmp")
    parser.add_argument("--poll-interval", type=float, default=20, help="Manager status polling interval in seconds (default: 20)")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the staged MPI example and return zero after validating its result.

    argv (list[str] | None) supplies CLI arguments without the program name;
    None reads sys.argv. Enqueue API setup followed by compute, monitor without
    a queue timeout, then download the result. Exceptions retain artifacts and
    request cancellation of unfinished work before stopping the manager.
    """
    # 1. Read the request. The library validates compute resources and paths;
    # retain only checks specific to this example and its monitoring loop.
    parser = create_parser()
    args = parser.parse_args(argv)
    if not math.isfinite(args.poll_interval) or args.poll_interval <= 0:
        parser.error("Polling interval must be a finite positive number")
    if args.machine != "local" and (not args.account or not args.queue):
        parser.error("Remote execution requires --account and --queue")

    # 2. Select how to build on the target machine. Bundled profiles provide
    # defaults; a user profile can replace the compiler, flags, and module setup.
    example_directory = Path(__file__).resolve().parent
    profiles = json.loads((example_directory / "mpi_build_profiles.json").read_text())
    if args.build_config:
        overrides = json.loads(Path(args.build_config).read_text())
        profiles.update({name.lower(): profile for name, profile in overrides.items()})
    if args.machine not in profiles:
        parser.error(f"No build profile for {args.machine!r}; supply --build-config")
    profile = profiles[args.machine]
    compiler = profile["compiler"]
    flags = profile.get("compiler_flags", [])
    setup = profile.get("setup", "")

    # 3. Initialize the API using the user's credentials and machine settings.
    # Honor environment overrides, but require IRI so this example runs real work.
    if manager_globals.api_impl != "IRI":
        raise RuntimeError("The live MPI example requires the IRI backend; check AMSC_JOB_MANAGER_API_IMPL")
    config = readManagerConfigFile(args.config)
    expected_ranks = args.nodes * args.ranks_per_node
    sandboxes = api.setupWorkflowAgentFromConfig(config)
    if args.sandbox_directory is None and not sandboxes.get(args.machine):
        parser.error(f"Configure a sandbox for {args.machine!r} or supply --sandbox-directory")

    # 4. Select separate locations for execution files, downloaded results and
    # manager state. Only the state directory must exist before enqueueing: the
    # manager needs a database before it can execute the staged API operations.
    run_id = uuid.uuid4().hex[:12]
    sandbox_path = args.sandbox_directory if args.sandbox_directory is not None else sandboxes[args.machine][0]
    if args.sandbox_directory is not None:
        # Pass the original path so the library validates it before normalization.
        manager_globals.addSandboxDirs({args.machine: [sandbox_path]})
    sandbox = Path(sandbox_path).resolve() if args.machine == "local" else PurePosixPath(sandbox_path)
    run_directory = PurePosixPath(str(sandbox)) / f"mpi-example-{run_id}"
    local_base = Path(sandbox) if args.machine == "local" else (
        Path(sandboxes["local"][0]).resolve() if sandboxes.get("local") else Path("/tmp")
    )
    output_directory = Path(args.output_directory) if args.output_directory else local_base / f"mpi-results-{run_id}"
    if output_directory.exists() and (not output_directory.is_dir() or any(output_directory.iterdir())):
        parser.error("Output directory must be absent or empty; choose a new directory for this run")
    state_directory = local_base / f"mpi-state-{run_id}"
    api.remoteMkdir("local", str(state_directory))
    if args.machine == "local":
        # Local asynchronous compute uses a separate status database, configured
        # before the manager starts submitting work.
        setStatusManagerDatabasePath(str(state_directory / "local-jobs.sqlite"))

    print(f"Run directory: {args.machine}:{run_directory}", flush=True)
    print(f"Downloaded results: {output_directory}", flush=True)
    print(f"Workflow state and databases: {state_directory}", flush=True)

    # 5. Demonstrate staging API actions as a workflow stage. Constructing these
    # operations does not create directories or upload files: WorkflowManager executes
    # them when this stage is reached, before advancing to the compute stage.
    # The API manager checkpoints each operation for recovery after interruption.
    remote_source = run_directory / "mpi_reduction.cpp"
    remote_executable = run_directory / "mpi_reduction"
    remote_result = run_directory / "result.json"
    preparation = APIAction([
        MakeDirectory("local", str(output_directory)),
        MakeDirectory(args.machine, str(run_directory)),
        UploadFile(args.machine, str(remote_source), str(example_directory / remote_source.name)),
    ])

    # 6. Describe the compute action without executing it yet. Pre-launch sets
    # up the compiler and builds the uploaded source; the job then launches the
    # binary across the requested ranks. Pass the expected count for MPI to check.
    compile_command = shlex.join([*compiler, *flags, str(remote_source), "-o", str(remote_executable)])
    pre_launch = "\n".join(["set -e", f"cd {shlex.quote(str(run_directory))}", setup or ":", compile_command])
    compute = ExecuteJobComputeAction(
        machine=args.machine, account=args.account or "", queue=args.queue or "", time=str(args.time),
        executable=str(remote_executable), arguments=[str(expected_ranks), str(remote_result)],
        rundir=str(run_directory), nodes=args.nodes, ranks_per_node=args.ranks_per_node,
        gpus_per_rank=args.gpus_per_rank, pre_launch=pre_launch,
    )

    # 7. Persist and enqueue the ordered stages. Successful API preparation is
    # required before compute can start. WorkflowManager owns execution/progression;
    # access its workflow database under the manager lock.
    manifest = {"machine": args.machine, "run_directory": str(run_directory),
                "output_directory": str(output_directory), "expected_ranks": expected_ranks,
                "build_profile": profile, "nodes": args.nodes, "ranks_per_node": args.ranks_per_node}
    manifest_path = state_directory / "run.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    manager = WorkflowManager(filename=str(state_directory / "workflow.sqlite"), poll_freq=args.poll_interval, max_workflows_active=1)
    workflow_id = None
    status = None
    try:
        with manager as workflows:
            workflow_id = workflows.enqueueWorkflow([preparation, compute], workflow_group=f"mpi-example-{run_id}")
        manifest["workflow_id"] = workflow_id
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
        print(f"Workflow {workflow_id}: expecting {expected_ranks} MPI ranks", flush=True)
        # Keep this program alive while the background manager handles queueing,
        # execution, and polling. Force a refresh on our own timer so queued
        # jobs can be distinguished from running jobs, even without a change.
        manager.start()
        previous = None
        next_poll = time.monotonic()
        while True:
            if not manager.isAlive():
                raise RuntimeError("WorkflowManager stopped before the workflow completed")
            with manager as workflows:
                force_poll = time.monotonic() >= next_poll
                action_status, api_status = workflows.headActionStatus(
                    workflow_id, force_poll=force_poll, return_api_status=True
                )
                if force_poll:
                    next_poll = time.monotonic() + args.poll_interval
                status = workflows.workflowStatus(workflow_id)
                state = workflows.workflowState(workflow_id)
            summary = (status.name, action_status.name, api_status, state["workflow_stage"], state["head_action_type"])
            if force_poll or summary != previous:
                print(f"Workflow {workflow_id}: workflow_status={status.name}, "
                      f"action_status={action_status.name}, api_status={api_status!r}, "
                      f"stage={state['workflow_stage']}, action={state['head_action_type']}", flush=True)
                previous = summary
            if status == ActionStatus.COMPLETED:
                break
            if status in (ActionStatus.FAILED, ActionStatus.CANCELED):
                raise RuntimeError(f"Workflow {workflow_id} ended as {status.name}; inspect logs in {args.machine}:{run_directory}")
            time.sleep(min(1, args.poll_interval))
    except BaseException:
        # Request cancellation of unfinished work on interruption or an error.
        # Preserve terminal failure records and report unconfirmed cancellation.
        if workflow_id is not None and status not in (ActionStatus.COMPLETED, ActionStatus.FAILED, ActionStatus.CANCELED):
            try:
                with manager as workflows:
                    workflows.cancelWorkflow(workflow_id)
            except Exception as error:
                print(f"Cancellation could not be confirmed: {error}; inspect {state_directory / 'workflow.sqlite'} before resubmitting", flush=True)
        raise
    finally:
        # Release the manager thread and database while retaining run artifacts.
        manager.stop(wait_until_done=False, close_database=True)

    # 8. Retrieve results only after successful workflow completion. Validate the
    # MPI communicator size and reduction output as evidence the calculation ran.
    downloaded = api.downloadFile(args.machine, str(output_directory / "result.json"), str(remote_result))
    result = json.loads(Path(downloaded).read_text())
    if (type(result.get("sum")) is not int or type(result.get("ranks")) is not int
            or result["sum"] != expected_ranks or result["ranks"] != expected_ranks
            or result.get("valid") is not True):
        raise RuntimeError(f"Incorrect MPI result: expected {expected_ranks} ranks, received {result!r}")
    print(f"PASS: sum={result['sum']}, ranks={result['ranks']}, valid=true", flush=True)
    print(f"Downloaded result: {downloaded}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
