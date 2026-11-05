r"""Upload, compile, run and retrieve a real MPI reduction through JobManager.

    python examples/mpi_workflow.py manager-config.json local --ranks-per-node 4
    python examples/mpi_workflow.py manager-config.json frontier \
        --account YOUR_PROJECT --queue batch --nodes 2 --ranks-per-node 4

Source staging and result retrieval use the filesystem API. The managed compute
action compiles in pre-launch and runs MPI on the selected machine. Every run
retains its source, executable, logs, result and SQLite databases for inspection.

The example follows eight steps, marked in main():
    1. Read the requested machine and resources.
    2. Select the machine's compiler and module setup.
    3. Initialize the API from the user configuration.
    4. Select sandboxed run and output directories.
    5. Upload the MPI source before starting the workflow.
    6. Describe compilation and execution as a compute action.
    7. Persist, enqueue, and monitor the workflow.
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


def main(argv=None) -> int:
    """Run a real MPI workflow from config/machine CLI arguments; return zero.

    argv optionally supplies CLI arguments. Validate resources and build settings,
    stage the bundled C++ source in the selected machine sandbox, then enqueue and
    monitor a compute action. Download only after completion and check its JSON.
    Exceptions retain artifacts; interrupted workflows request
    cancellation before stopping the manager. Importing this file submits no work.
    """
    # 1. Read the target and resource request. Reject invalid requests before
    # initializing the API or creating any workflow artifacts.
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
    args = parser.parse_args(argv)
    if args.nodes < 1 or args.ranks_per_node < 1 or args.gpus_per_rank < 0 or args.time < 1:
        parser.error("Nodes, ranks-per-node and time must be positive; GPUs-per-rank must be nonnegative")
    if not math.isfinite(args.poll_interval) or args.poll_interval <= 0:
        parser.error("Polling interval must be a finite positive number")
    if args.machine == "local" and args.nodes != 1:
        parser.error("Local execution requires --nodes 1")
    if args.machine != "local" and (not args.account or not args.queue):
        parser.error("Remote execution requires --account and --queue")
    if any("~" in value for value in (args.config, args.build_config or "", args.output_directory or "", args.sandbox_directory or "")):
        parser.error("Paths containing '~' are not supported; use explicit paths")
    if args.sandbox_directory is not None and not args.sandbox_directory.strip():
        parser.error("Sandbox directory must not be empty")

    # 2. Select how to build on the target machine. Bundled profiles provide
    # defaults; a user profile can replace the compiler, flags, and module setup.
    example_directory = Path(__file__).resolve().parent
    profiles = json.loads((example_directory / "mpi_build_profiles.json").read_text())
    if args.build_config:
        overrides = json.loads(Path(args.build_config).read_text())
        if not isinstance(overrides, dict):
            parser.error("Build configuration must map machine names to profiles")
        profiles.update({name.lower(): profile for name, profile in overrides.items()})
    if args.machine not in profiles:
        parser.error(f"No build profile for {args.machine!r}; supply --build-config")
    profile = profiles[args.machine]
    if not isinstance(profile, dict):
        parser.error("A build profile must be a JSON object")
    compiler = profile.get("compiler")
    flags = profile.get("compiler_flags", [])
    setup = profile.get("setup", "")
    if not isinstance(compiler, list) or not compiler or not all(isinstance(arg, str) and arg for arg in compiler):
        parser.error("Profile compiler must be a nonempty list of executable/argument strings")
    if not isinstance(flags, list) or not all(isinstance(arg, str) for arg in flags) or not isinstance(setup, str):
        parser.error("Profile compiler_flags must be a string list and setup must be a shell-command string")
    if "~" in compiler[0]:
        parser.error("Compiler executable paths containing '~' are not supported")

    # 3. Initialize the API using the user's credentials and machine settings.
    # Honor environment overrides, but require IRI so this example runs real work.
    from amscjobmanager import ActionStatus, JobManager, api_general as api
    from amscjobmanager import globals as manager_globals
    from amscjobmanager.compute_actions import ExecuteJobComputeAction
    from amscjobmanager.local_api import setStatusManagerDatabasePath
    from amscjobmanager.manager_config import readManagerConfigFile

    if manager_globals.api_impl != "IRI":
        raise RuntimeError("The live MPI example requires the IRI backend; check AMSC_JOB_MANAGER_API_IMPL")
    config = readManagerConfigFile(args.config)
    expected_ranks = args.nodes * args.ranks_per_node
    if args.machine == "local" and expected_ranks > 1 and config.local_mpi_launcher is None:
        parser.error("Configure local_mpi_launcher to run multiple local ranks")
    sandboxes = api.setupWorkflowAgentFromConfig(config)
    if args.machine not in api.getKnownMachines():
        parser.error(f"Configure a provider for machine {args.machine!r}")
    if args.sandbox_directory is None and not sandboxes.get(args.machine):
        parser.error(f"Configure a sandbox for {args.machine!r} or supply --sandbox-directory")

    # 4. Choose authorized locations for this run. Keep execution files on the
    # target machine and results/databases locally, in separate unique folders.
    run_id = uuid.uuid4().hex[:12]
    sandbox_path = args.sandbox_directory if args.sandbox_directory is not None else sandboxes[args.machine][0]
    sandbox = Path(sandbox_path).resolve() if args.machine == "local" else PurePosixPath(sandbox_path)
    if not sandbox.is_absolute() or ".." in sandbox.parts:
        parser.error("The selected machine sandbox must be absolute and contain no '..'")
    if args.sandbox_directory is not None:
        # The explicit CLI path grants an additional sandbox for this run.
        # Use the existing validation/creation behavior without rewriting config.
        manager_globals.addSandboxDirs({args.machine: [str(sandbox)]})
    run_directory = PurePosixPath(str(sandbox)) / f"mpi-example-{run_id}"
    local_base = Path(sandbox) if args.machine == "local" else (
        Path(sandboxes["local"][0]).resolve() if sandboxes.get("local") else Path("/tmp")
    )
    output_directory = Path(args.output_directory).resolve() if args.output_directory else local_base / f"mpi-results-{run_id}"
    if output_directory.exists() and (not output_directory.is_dir() or any(output_directory.iterdir())):
        parser.error("Output directory must be absent or empty; choose a new directory for this run")
    api.remoteMkdir("local", str(output_directory))
    if args.machine == "local":
        setStatusManagerDatabasePath(str(output_directory / "local-jobs.sqlite"))
    print(f"Run directory: {args.machine}:{run_directory}", flush=True)
    print(f"Results and workflow database: {output_directory}", flush=True)

    # 5. Upload the source during initial setup, before any job is submitted.
    # The filesystem API selects local copying, native IRI, or a Globus fallback.
    remote_source = run_directory / "mpi_reduction.cpp"
    remote_executable = run_directory / "mpi_reduction"
    remote_result = run_directory / "result.json"
    api.remoteMkdir(args.machine, str(run_directory))
    api.uploadSmallFile(args.machine, str(remote_source), str(example_directory / remote_source.name), definitely_is_file=True)

    # 6. Describe the compute action without executing it yet. Pre-launch sets
    # up the compiler and builds the uploaded source; the job then launches the
    # binary across the requested ranks. Pass the expected count for MPI to check.
    compile_command = shlex.join([*compiler, *flags, str(remote_source), "-o", str(remote_executable)])
    pre_launch = "\n".join(["set -e", f"cd {shlex.quote(str(run_directory))}", setup or ":", compile_command])
    action = ExecuteJobComputeAction(
        machine=args.machine, account=args.account or "", queue=args.queue or "", time=str(args.time),
        executable=str(remote_executable), arguments=[str(expected_ranks), str(remote_result)],
        rundir=str(run_directory), nodes=args.nodes, ranks_per_node=args.ranks_per_node,
        gpus_per_rank=args.gpus_per_rank, pre_launch=pre_launch,
    )

    # 7. Save the run description and enqueue the action in a persistent workflow.
    # JobManager owns submission and progression; access its database under lock.
    manifest = {"machine": args.machine, "run_directory": str(run_directory), "expected_ranks": expected_ranks,
                "build_profile": profile, "nodes": args.nodes, "ranks_per_node": args.ranks_per_node}
    manifest_path = output_directory / "run.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    manager = JobManager(filename=str(output_directory / "workflow.sqlite"), poll_freq=args.poll_interval, max_workflows_active=1)
    job_id = None
    status = None
    try:
        with manager as jobs:
            job_id = jobs.enqueueJob([action], job_group=f"mpi-example-{run_id}")
        manifest["job_id"] = job_id
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
        print(f"Workflow {job_id}: expecting {expected_ranks} MPI ranks", flush=True)
        # Keep this program alive while the background manager handles queueing,
        # execution, and polling. Force a refresh on our own timer so queued
        # jobs can be distinguished from running jobs, even without a change.
        manager.start()
        previous = None
        next_poll = time.monotonic()
        while True:
            if not manager.isAlive():
                raise RuntimeError("JobManager stopped before the workflow completed")
            with manager as jobs:
                force_poll = time.monotonic() >= next_poll
                action_status, api_status = jobs.headActionStatus(
                    job_id, force_poll=force_poll, return_api_status=True
                )
                if force_poll:
                    next_poll = time.monotonic() + args.poll_interval
                status = jobs.jobStatus(job_id)
                state = jobs.jobState(job_id)
            summary = (status.name, action_status.name, api_status, state["workflow_stage"], state["head_action_type"])
            if force_poll or summary != previous:
                print(f"Workflow {job_id}: workflow_status={status.name}, "
                      f"action_status={action_status.name}, api_status={api_status!r}, "
                      f"stage={state['workflow_stage']}, action={state['head_action_type']}", flush=True)
                previous = summary
            if status == ActionStatus.COMPLETED:
                break
            if status in (ActionStatus.FAILED, ActionStatus.CANCELED):
                raise RuntimeError(f"Workflow {job_id} ended as {status.name}; inspect logs in {args.machine}:{run_directory}")
            time.sleep(min(1, args.poll_interval))
    except BaseException:
        # Request cancellation of unfinished work on interruption or an error.
        # Preserve terminal failure records and report unconfirmed cancellation.
        if job_id is not None and status not in (ActionStatus.COMPLETED, ActionStatus.FAILED, ActionStatus.CANCELED):
            try:
                with manager as jobs:
                    jobs.cancelJob(job_id)
            except Exception as error:
                print(f"Cancellation could not be confirmed: {error}; inspect workflow.sqlite before resubmitting", flush=True)
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
