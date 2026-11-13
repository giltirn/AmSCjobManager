# Workflow examples

[`spoof_workflow_apiv1.py`](spoof_workflow_apiv1.py) demonstrates simulated
multi-machine execution. [`mpi_workflow.py`](mpi_workflow.py) submits a real
MPI calculation using the IRI backend, including its local implementation.

## Run a local workflow through the MCP server and CLI

Install the server extra into your active Python environment, then run:

```bash
python -m pip install -e '.[server]'
bash examples/mcp_workflow.sh
```

The script starts its own server on loopback port 8765; pass a different port as
an argument, for example `bash examples/mcp_workflow.sh 8766`. Its configuration,
logs and SQLite databases live in a unique `/tmp/amsc-mcp-example.*` directory.
Bash heredocs create the configuration and workflow JSON directly, and standard
`sed` commands extract the workflow ID and status from the formatted CLI
responses. It submits a two-stage workflow through the CLI: an API stage creates
a working directory and writes `input.txt` containing `1`, then a local compute
stage runs one Bash process (one node, one rank) to add one and write `output.txt`.
No MPI installation, remote credentials, or existing configuration is required.

After completion, it prints the output contents and the expected value `2`.
The server is stopped automatically on exit; interruption requests cancellation
of unfinished work. Files and databases remain at the printed `/tmp` location
for inspection. The library's default IRI backend is required; the script checks
this without changing `AMSC_JOB_MANAGER_API_IMPL`.

## Run MPI locally

Install an MPI C++ compiler and launcher. The bundled local profile uses
`mpic++` without module commands. Create an existing sandbox and a manager
configuration such as:

```json
{
  "iri_implementations": {},
  "local_mpi_launcher": "/usr/bin/mpiexec",
  "sandbox_directories": {
    "local": ["/tmp/amsc-mpi-sandbox"]
  }
}
```

From the repository root, run:

```bash
python examples/mpi_workflow.py manager-config.json local \
    --nodes 1 --ranks-per-node 4
```

Local execution requires `--nodes 1`; `--ranks-per-node` sets the number of MPI
processes. No remote credentials, account, queue, or Globus endpoint are needed
for this target. The local output directory defaults to a unique directory in
the first local sandbox. Use `--output-directory` to select an absent or empty
directory within a local sandbox or `/tmp`.

Use `--sandbox-directory /path/to/sandbox` to register and select a different
sandbox through `addSandboxDirs()`. It need not be listed in the configuration,
and the configuration file is not modified. Existing sandbox validation and
the prompt to create a missing directory still apply. Without this option, the
first configured machine sandbox is used. On the local target, the selected
sandbox also becomes the default base for the local output directory.

## Run on an IRI machine

Use your normal manager configuration with a sandbox for the selected machine:

```bash
python examples/mpi_workflow.py manager-config.json frontier \
    --account YOUR_PROJECT --queue batch \
    --nodes 2 --ranks-per-node 4 --time 300
```

Alternatively, supply `--sandbox-directory /absolute/remote/path`. The provider
must still be configured; remote sandbox paths must be absolute and contain no
`~` characters. Path and sandbox validation are performed by the library.

Remote execution requires explicit `--account` and `--queue` values. `--time`
sets the remote walltime in seconds. The calculation uses CPU MPI; the optional
`--gpus-per-rank` defaults to zero and can be supplied if required by your
site's allocation. Queues with additional QoS requirements need suitable site
configuration; this example does not select QoS independently.

The workflow begins with an `APIAction` stage that creates the local result
directory and target run directory, then uploads the bundled C++ source. These
operations are deferred until the manager reaches that stage; constructing the
workflow does not perform them. Each operation is checkpointed so recovery can
skip finished operations and safely replay an interrupted one.

The next stage compiles the uploaded source in the compute action's pre-launch
and executes it through `ExecuteJobComputeAction`. The manager enqueues and
monitors both stages, then the example calls
`api.downloadFile()` only after successful workflow completion. These filesystem
calls dispatch to local copying, native IRI, or configured Globus fallbacks.
The latter need transfer credentials and a personal endpoint for uploads and
downloads. Source staging is managed as part of the workflow; result retrieval
and validation occur afterward, following the same sequence on all targets.

The local state directory is created before enqueueing because the manager needs
a database before it can execute the API stage. The example separates CLI parser
construction from `main()`, whose numbered sections describe configuration,
artifact locations, stage construction, monitoring and result validation.

## Calculation and results

[`mpi_reduction.cpp`](mpi_reduction.cpp) constructs one zero-filled integer
array per rank, with one element per process. Rank `r` sets element `r` to one.
An `MPI_Allreduce` sums the arrays; each rank verifies that all resulting
elements equal one. Another collective confirms that every rank passed. The
program also checks the actual communicator size against the requested count.

Rank zero writes `result.json`, for example:

```json
{"sum": 4, "ranks": 4, "valid": true}
```

The example downloads this file and checks that both integers equal
`nodes * ranks_per_node` and `valid` is true. Compilation, reduction, rank-count,
and output errors fail the job; results are not downloaded from failed jobs.

Each run prints its target run directory, local output directory and local state
directory. Artifacts
are retained:

- Run directory: uploaded source, compiled executable, wrapper, and job logs.
- Local output directory (`mpi-results-...` by default): downloaded `result.json`
  on success; this directory is created by the API stage.
- Local state directory (`mpi-state-...`): `workflow.sqlite` and `run.json`.
  Local execution also records `local-jobs.sqlite` here. This directory uses the
  same local base as the default output directory and exists before enqueueing.

Rerunning creates a new workflow. The saved database is available for inspection
and manual recovery. On interruption, the example requests
cancellation of unfinished work and reports when cancellation cannot be
confirmed. There is no workflow timeout, so waiting in a remote queue does not
cancel the run. `--poll-interval` defaults to 20 seconds. The example forces an
API status refresh on this timer and prints both the head action status and
the provider's status, even when unchanged. `ACTIVE` includes queued jobs;
the provider status distinguishes queueing from execution. Failed polls log
the error and retain the last known statuses. `--time` still sets
the requested remote job walltime after allocation.

## Build profiles and new machines

[`mpi_build_profiles.json`](mpi_build_profiles.json) contains default module
setup, compiler arguments, and compiler flags for `local`, `frontier`,
`perlmutter`, `polaris`, and `aurora`. Compilation uses the MPI C++ wrappers
documented by [OLCF](https://docs.olcf.ornl.gov/systems/frontier_user_guide.html#compiling),
[NERSC](https://docs.nersc.gov/development/compilers/wrappers/),
[Polaris](https://docs.alcf.anl.gov/polaris/compiling-and-linking/), and
[Aurora](https://docs.alcf.anl.gov/aurora/compiling-and-linking/).

To override a profile or configure a new machine, supply a JSON file:

```json
{
  "my_machine": {
    "setup": "module load my-mpi-environment",
    "compiler": ["mpic++"],
    "compiler_flags": ["-O2", "-std=c++17"]
  }
}
```

```bash
python examples/mpi_workflow.py manager-config.json my_machine \
    --account YOUR_PROJECT --queue YOUR_QUEUE \
    --build-config my-build-profiles.json --ranks-per-node 4
```

Each supplied machine profile replaces that machine's default profile in full.
`compiler` is a nonempty argument list, `compiler_flags` is an optional argument
list, and `setup` is an optional shell-command string. Compiler arguments and
paths are quoted literally; setup runs as supplied before compilation. Module
names can change, so adjust these example profiles to the installed environment.
The new machine must also be registered through the normal IRI configuration
or discovery mechanism. Profiles configure compilation; execution continues
through the library's existing IRI launcher configuration and ALCF handling.
