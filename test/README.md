# Tests

Run these commands from the repository root after installing the package in the
active environment (`python -m pip install -e .`). Server transport tests also
use the optional extra: `python -m pip install -e '.[server]'`.

## Organization

| Directory | Coverage | Requirements |
| --- | --- | --- |
| `unit/` | Validation, action schemas, mocked provider requests, status/query interpretation, and path mapping | No credentials or external services |
| `integration/api/` | Authentication workers, lazy initialization, and JSON configuration persistence | Temporary files and threads; provider calls are mocked |
| `integration/workflows/` | SQLite records, scheduling/cancellation, dependencies, wallets, workers, crash recovery, and SPOOF workflows | Temporary databases/files, threads, and local subprocesses |
| `integration/local/` | Real filesystem operations, sandbox checks, local compute, and cancellation | Local executables including Bash and process-signaling support |
| `integration/server/` | Backend function registry, MCP tools, CLI subprocesses, user queries, and admin protocol | Loopback/Unix sockets; optional server dependencies for transport checks |
| `live/iri/` | Actual IRI authentication, discovery, filesystem, and compute checks; configured local smoke test | Explicit user configuration; credentials and allocation/account access as applicable |
| `live/globus/` | Actual file operations, transfers, and transfer-label lookup | Explicit configuration, collection authentication, and personal staging as applicable |
| `support/` | Shared deterministic action adapters, remote-transfer fixtures, MCP diagnostics, and repository paths | Imported by tests; contains no test cases |

Unit versus integration describes the scope. Local versus remote describes the
environment. A test using real SQLite, worker threads, or processes belongs in
integration even if its remote provider is mocked. A script requiring a user's
configuration belongs in live checks even when it supports the local target.
Mixed modules are placed with their integration coverage rather than split just
to classify each individual assertion.

## Automated offline runs

```bash
python test/run_tests.py                  # unit tests by default
python test/run_tests.py unit
python test/run_tests.py integration
python test/run_tests.py all

# Filename globs select modules within the requested offline group.
python test/run_tests.py integration --pattern 'test_local_*.py' -v
python test/run_tests.py integration --pattern test_mcp_server.py -v
python test/run_tests.py all --list        # list paths without importing tests
```

The runner uses the current Python interpreter and a fresh subprocess for each
module. It runs children from the repository root, adds `test/` to their
`PYTHONPATH`, and selects IRI initially; SPOOF modules explicitly switch backend
before importing the library. This prevents backend selection, globals, and
fixtures from leaking between modules. Independent modules continue after a
failure, and the runner returns a nonzero exit status if any module fails.

No runner command selects live scripts. `live/` deliberately has no `__init__.py`,
so standard unittest discovery from `test/` also excludes it. Do not add package
initializers there. Use the runner for combined offline runs: collecting SPOOF
and IRI modules in one process can change their behavior through import order.

For standard unittest discovery or selecting a class/method:

```bash
python -m unittest discover -s test/unit -t test -v
PYTHONPATH=test python -m unittest integration.workflows.test_submission_keys -v
PYTHONPATH=test python -m unittest \
    integration.server.test_mcp_server.MCPSubprocessTests.test_local_workflow_status_through_json_cli -v
```

Using `unit` and `integration` as the import roots avoids collision with Python's
own `test` package. Shared support imports and spawned recovery processes need
`test/` on `PYTHONPATH`; the runner supplies it automatically. Ordinary tests do
not require a local MPI installation or C++ compiler; local launcher behavior
uses test executables. The separate MPI workflow example requires those tools.

Missing server dependencies produce diagnostics/skips in unittest; install the
server extra for complete transport coverage. No separately running server or
Uvicorn command is needed. Restricted environments must permit local listeners
and subprocess creation for the applicable integration tests.

## Explicit live checks

These scripts are not automated offline tests. They may prompt for credentials,
write remote files, submit compute/transfer work, and retain artifacts. Read the
script and select a suitable sandbox/account before invoking it. Run one script
directly with the active Python interpreter:

| Script | Invocation after `python` | Effects/artifacts |
| --- | --- | --- |
| IRI authorization | `test/live/iri/test_iri_authorization.py CONFIG` | Initializes configured credentials; PAT validation alone does not verify server acceptance |
| IRI discovery | `test/live/iri/test_iri_discovery.py CONFIG IMPLEMENTATION --refresh` | Authenticates the selected provider, prompts for mappings, and writes its configuration cache |
| IRI smoke | `test/live/iri/test_iri_api_smoke.py CONFIG MACHINE` | Queries resources/filesystems, submits a short compute job, then requests cancellation |
| IRI file round trip | `test/live/iri/test_iri_file_roundtrip.py CONFIG` | Uploads, lists, and downloads through configured mounted filesystems; inspect remote artifacts |
| IRI compute | `test/live/iri/test_iri_compute_execution.py CONFIG perlmutter ACCOUNT` | Submits native and compatibility compute jobs; also supports Aurora and retains run files |
| Aurora job names | `test/live/iri/test_aurora_job_names_live.py CONFIG --account PROJECT` | Tries rejected/accepted job names using real submissions and requests cancellation |
| Configured local smoke | `test/live/iri/test_local.py CONFIG` | Runs local commands using the supplied sandbox configuration |
| Globus file round trip | `test/live/globus/test_globus_file_roundtrip.py CONFIG` | Performs real blocking uploads/downloads through personal staging |
| Globus transfer lookup | `test/live/globus/test_globus_transfer_lookup.py CONFIG` | Submits a labeled IRI transfer and compares its ID with direct Globus lookup; uses the Perlmutter/DTN route |
| Globus operations | `test/live/globus/test_globus_operations_live.py CONFIG MACHINE` | Exercises file operations and transfers; retains remote, `/tmp`, and staging artifacts and cancels outstanding known tasks on failure |

`CONFIG` is the user's manager configuration JSON; `MACHINE`, `IMPLEMENTATION`,
`ACCOUNT`, and `PROJECT` select configured targets and allocation names.
`live/iri/test_api.py`, when present, is a developer scratch script with
site-specific calls, not a maintained regression entry point.

For a complete staged MPI calculation supporting both local and remote targets,
see the [MPI workflow example](../examples/README.md). For further coverage and
crash-boundary details, see [development](../docs/development.md).
