# MCP server prototype

The optional server exposes workflow submission, cancellation, listing and
status through MCP Streamable HTTP. The command-line client supports traditional commands and JSON requests,
calling the same tools as an MCP agent. Pending user questions, including
configuration and authentication requests, can be listed and answered through
the same listener. Workflows can combine both compute action forms, Globus
transfers and API-operation stages. Privileged sandbox changes and unsafe
API overrides remain unavailable.

## Start the server

Install the optional dependencies from the repository root:

```bash
python -m pip install -e '.[server]'
python utilities/manager_server.py manager-config.json \
    --database workflows.sqlite --port 8765 --log-file manager.log
```

The listener binds to `127.0.0.1` and serves `/mcp`. Its `ServerRuntime` owns
asynchronous initialization, a user-query queue, and the eventual background
`JobManager`. Initialization starts after HTTP is listening, so configuration or
authentication questions can be answered even before a manager exists. Once ready,
the manager recovers/progresses workflows in its database. Pending questions keep
the asking worker waiting while unrelated workflows continue. Run only one
manager/server against that database. Stopping closes the queue and manager
without canceling accepted remote jobs. Use `cancel_workflow` to request remote
cancellation explicitly; replying to a question delivers text to its waiting operation.

Successful HTTP startup prints an administrator notice to stdout:

```text
Server started: http://127.0.0.1:8765/mcp (PID 12345, log: /path/to/manager.log)
```

The notice is flushed immediately and appears only after the listener has bound
its socket. This confirms listener readiness, not completion of manager
initialization. Bind failures do not produce a success notice; initialization
failures are logged while the listener remains available. `Workflow manager ready`
in the log indicates successful initialization.
Graceful shutdown, including Ctrl-C and SIGTERM, prints
`Server terminating: http://127.0.0.1:8765/mcp`. Ctrl-C completes manager cleanup
and exits successfully without a KeyboardInterrupt/CancelledError traceback.
SIGKILL cannot emit a termination
notice because it does not allow Python code to run. These lifecycle notices
also use `wfmanLog` to record them in the server log. Routine requests produce
no stdout notices, and client stdout remains JSON (except explicit help).

The server assigns the existing `logging.py` hooks to an append-only log file:
`wfmanLog` carries manager/server messages, and `wfapiLog` carries API messages.
`--log-file` selects the file; without it, the default is the database filename
with `.log` appended (for example `workflows.sqlite.log`). Hooks are installed
before configuration and restored after the manager stops. This does not
redirect stdout. MCP/Uvicorn use WARNING level: routine INFO/access messages
are suppressed, while warnings/errors retain standard logging to stderr. This
prevents per-request access logs from appearing alongside a client's JSON in
the same terminal; the administrator lifecycle notices above are deliberate.
User questions always pass from `wfapiUserQuery` to `api_query_user_func`.
The server utility replaces that hook with its queue's `ask` function before
initialization and restores the previous hook on shutdown. Outside server mode,
the default hook prompts in the terminal. Worker shutdown signals prevent late
server calls from prompting after the terminal hook has been restored.

`server.runner.ManagerHTTPServer(initialize_manager, port=8765, log_path=...)`
constructs its own runtime and MCP server, then registers the tools through
`server.tools.registerTools`. `initialize_manager` is a no-argument
factory that configures the API and returns a new `JobManager`; it runs after
listener startup. The server owns runtime startup/shutdown and emits readiness
and termination announcements.

Embedding callers install `logging.api_query_user_func = server.runtime.queries.ask`
before running and restore the previous hook after shutdown, as the utility does.
In-process SDK tests connect to `server.mcp_server` and explicitly start/stop
`server.runtime` without opening an HTTP listener. Tests cover existing Python
submissions as well as complete MCP/CLI submission and cancellation pathways.

Workflow tools require a running manager. If its background thread fails, the
manager logs the underlying exception through `wfmanLog` and releases its lock.
The listener stays available, but workflow requests return an MCP tool error
with code `manager_unavailable` instead of presenting cached records as current.
The same error applies before manager startup and after shutdown. Health is
checked before acquiring the lock and again after acquiring it, to cover a
failure while the request was waiting. Tool discovery remains available.
The server does not automatically restart a failed manager. Manager instances
are single-use; recovery requires a new manager/server using the same database.

## Submit and cancel workflows

`submit_workflow(stages, job_group=None, submission_key=None)` persists an ordered,
nonempty list of built-in actions. It returns `workflow_id` (the positive database
ID) and `workflow_status` (the recorded overall status). This confirms persistence;
provider submission and execution happen on background workers. Authentication
questions can be answered through the existing query tools. Stage failures appear
in workflow status and the server log.

`submission_key` is optional. Reusing an exact string returns the original
workflow without replacing its stages or group, including after completion or
cancellation. Omit it to create new work every time. The client never retries
requests automatically; reuse the key after a timeout or lost response when the
outcome is uncertain. `job_group` remains an optional, nonunique label.

For example, save the following as `submission.json` and submit it with
`python utilities/manager_client.py --file submission.json`:

```json
{
  "tool": "submit_workflow",
  "arguments": {
    "submission_key": "example-run-1",
    "job_group": "examples",
    "stages": [
      {
        "type": "api",
        "operations": [
          {"type": "make_directory", "machine": "local", "path": "/tmp/amsc-example"},
          {"type": "write_file", "machine": "local", "path": "/tmp/amsc-example/input", "content": "hello"}
        ]
      },
      {
        "type": "execute_job",
        "machine": "local", "account": "", "queue": "", "time": "60",
        "rundir": "/tmp/amsc-example", "nodes": 1, "ranks_per_node": 1, "gpus_per_rank": 0,
        "executable": "/bin/cat", "arguments": ["input"],
        "pre_launch": "printf starting", "post_launch": "printf finished"
      }
    ]
  }
}
```

Every stage requires a `type` tag:

| Stage type | Fields in addition to `type` |
| --- | --- |
| `execute_job` | `machine`, `account`, `queue`, `time` (seconds as a string), `rundir`, `nodes`, `ranks_per_node`, `gpus_per_rank`, `executable`, `arguments`; optional `pre_launch`, `post_launch`, `exclusive` (default true). |
| `batch_script` | The same target/resource fields as `execute_job`, with `script_body` instead of executable/arguments/hooks; optional `exclusive` (default true). Batch scheduler headers are handled as described in the compute API documentation. |
| `globus_transfer` | `source_endpoint`, `source_path`, `dest_endpoint`, `dest_path`; optional `source_path_type` and `dest_path_type`, both defaulting to `globus`. Destination is a directory; `filesystem` paths require a configured endpoint mapping. |
| `api` | `operations`, an ordered list (default empty) of the operations below. |

API operations use their own `type` tags. `make_directory` takes `machine`, `path`
and optional `create_parents` (default true). `write_file` takes `machine`, `path`
and `content`. `upload_file` and `download_file` take `machine`, `dest_path` and
`source_path`; their local paths refer to the manager host. `copy_file` takes
`dest_path` and `source_path` on the manager host. `run_local` takes `executable`,
`arguments`, `cwd` and optional positive `timeout` in seconds (default 60).
Long-running local programs should be local compute actions. Operations must
be safe to replay after partial execution or a crash before their checkpoint.

`write_file.content` accepts UTF-8 text or an object such as `{"base64": "AP8="}`
for binary data. Unknown fields, unsupported stage/operation types and malformed
binary content are rejected before enqueueing the workflow. Target availability,
filesystem checks and provider-specific validation occur during stage execution.
All filesystem API operations enforce their normal sandbox checks. Shell scripts
and executables, including `run_local`, retain the operating-system permissions
of the executing user; their child-process writes are not kernel-sandboxed.

Traditional options use the same discovered schema; pass each stage as a JSON
argument after `--stages`. For larger workflows, use `--file` or stdin:

```bash
python utilities/manager_client.py submit_workflow --submission-key setup-1 \
    --stages '{"type":"api","operations":[{"type":"make_directory","machine":"local","path":"/tmp/example"}]}'
python utilities/manager_client.py submit_workflow --help
python utilities/manager_client.py cancel_workflow --workflow-id 1
```

`cancel_workflow` retains the workflow and requests cancellation of its current
compute or transfer task. Its receipt may report `CANCELING`; query
`list_workflows` until the final state is recorded. Pending workflows can become
`CANCELED` immediately. Repeated cancellation is safe, previous file effects are
retained, and there is no option to abandon a running remote task.

Python code uses the same Pydantic dataclasses and JSON schema:

```python
from amscjobmanager.workflow import workflow_adapter

# stages is an ordinary list of the existing built-in action objects.
encoded = workflow_adapter.dump_json(stages)  # bytes containing JSON
restored = workflow_adapter.validate_json(encoded)  # native action objects
schema = workflow_adapter.json_schema()
```

Existing positional Python constructors remain supported. JSON import/export
supports the built-in stage union; custom Python-derived actions remain usable
through the Python workflow API. Database persistence continues using trusted
pickle records. No arbitrary class loading or pickle input is exposed over MCP.

## Answer user questions

The queue belongs to the server runtime, not an MCP connection. A CLI process can
list questions, disconnect, and a later CLI process can answer them:

```bash
python utilities/manager_client.py list_user_queries
python utilities/manager_client.py respond_to_user_query --help
python utilities/manager_client.py respond_to_user_query \
    --session-id SERVER_SESSION_UUID --query-id 1 --response ANSWER
```

For authorization codes, prefer a request document through stdin or a private
file so the answer is not placed in process arguments or shell history:

```bash
python utilities/manager_client.py --file - < response.json
```

The document uses the session and query identifiers returned by the listing:

```json
{
  "tool": "respond_to_user_query",
  "arguments": {
    "session_id": "SERVER_SESSION_UUID",
    "query_id": 1,
    "response": "ANSWER"
  }
}
```

`list_user_queries` returns pending records containing `session_id` (UUID string),
`query_id` (positive session-local integer), `title`, `prompt`, and `created_at`
(Unix seconds). Listing does not consume questions or include previous answers.
Shared authentication can serve several workflows, so a question is not
necessarily associated with one workflow ID.

A response returns `accepted: true` in the CLI operation result. This acknowledges
one-time delivery, not successful authentication or completion of the waiting
operation. The asking worker performs token exchange and persists credentials
through the existing API code. A rejected code can produce a new query number.
Responses are not echoed, logged by the query service, or persisted in SQLite.

The session UUID changes on restart. Replies to previous sessions return
`stale_session`; answered, interrupted or unknown questions return
`query_not_pending`; a closed runtime returns `query_queue_closed`. No question or
response is recovered from the database. Instead, action recovery starts fresh
workers that ask again if needed. Disconnecting a client does not invalidate its
question. Action cancellation invalidates questions asked directly by that action's
worker; shared authentication questions can remain needed by other callers.

Query tools remain available during initialization and after manager failure.
Workflow tools return `manager_unavailable` until the manager is ready and healthy.
Shutdown closes the queue, wakes waiters and rejects late questions/replies.
Answering questions does not unlock privileged sandbox operations; those controls
remain future work.

## Query workflows

```bash
python utilities/manager_client.py list_workflows

python utilities/manager_client.py list_workflows --workflow-id 1 --refresh

python utilities/manager_client.py list_workflows --statuses FAILED COMPLETED

python utilities/manager_client.py list_workflows --no-refresh

python utilities/manager_client.py -h

python utilities/manager_client.py -h list_workflows

python utilities/manager_client.py list_workflows --help

python utilities/manager_client.py --list-tools
```

The client discovers the selected tool's input schema and generates its options;
parameter names such as `workflow_id` become `--workflow-id`. Discovery and
execution use a single MCP connection. The tool name must match the server's
advertised name, including underscores. Global options precede that name:

```bash
python utilities/manager_client.py --url http://127.0.0.1:9000/mcp --timeout 10 \
    list_workflows --statuses FAILED
```

`-h`/`--help` discovers the server's commands and prints their names with brief
human-readable descriptions, along with the frontend's connection/input options.
`-h TOOL`, `--help TOOL`, and `TOOL --help` print that command's description,
input options/types/defaults/choices, and an output field/type tree. The output
tree resolves local schema references and marks array positions as `[0..]`;
field descriptions explain database IDs, status meanings, and indexing.
Both general and command-specific help require the optional server dependencies
and a reachable server, because tool names, descriptions, and schemas come from
its live response. Help discovers metadata without executing tools. Connection
failures return the usual error envelope and nonzero exit code rather than an
incomplete offline command list. Unknown tools or malformed tool options produce an
`invalid_input` JSON envelope and a nonzero exit code. Valid command responses
use the same JSON envelope as the JSON input mode below.

Option conversion follows the advertised schema:

| Parameter | Command-line input |
| --- | --- |
| Integer, number, string | `--workflow-id 1`; one value per option |
| Boolean | `--refresh` or `--no-refresh`; no value follows the flag |
| Array | `--statuses FAILED COMPLETED`; one or more values |
| Object or other complex schema | One JSON value, for example `--metadata '{"mode":"test"}'` if a future tool exposes that parameter |

Enum choices, required fields, numeric bounds, and object structure are validated
before the tool executes. Omitted options are not sent, so server defaults apply;
explicit false/zero values are preserved. Use JSON input for explicit `null`,
empty arrays, or complex argument structures that are inconvenient in a shell.

JSON input remains available for single calls and batches:

```bash
python utilities/manager_client.py --request \
    '{"tool":"list_workflows","arguments":{}}'

python utilities/manager_client.py --request \
    '{"tool":"list_workflows","arguments":{"workflow_id":1,"refresh":true}}'

python utilities/manager_client.py --request \
    '{"tool":"list_workflows","arguments":{"statuses":["FAILED","COMPLETED"]}}'

python utilities/manager_client.py --list-tools
```

Use `--url` to select an endpoint other than `http://127.0.0.1:8765/mcp`.
The client does not load manager configuration, site credentials, or databases.
Traditional commands cannot be combined with --request, --file, or --list-tools.

`list_workflows(workflow_id=None, refresh=False, statuses=None)` returns all recorded workflows
in ID order, including completed and canceled ones, or a one-element list for
an explicit ID. An empty database returns an empty list; an unknown explicit
ID is an error. Each record contains:

- `workflow_id`, `job_group`, and `workflow_status` from the public manager API.
- `workflow_stage`, `head_action_id`, `head_action_type`, `head_action_class`,
  and `head_action_status`.
- `api_status`: the provider's recorded status string, or `null` when unavailable.
- `last_status_change`: the recorded timestamp in Unix seconds.

`statuses` optionally selects overall workflow statuses: `PENDING`, `ACTIVE`,
`COMPLETED`, `FAILED`, `CANCELING`, or `CANCELED`. Multiple values are combined
with OR; the workflow ID and status filters are combined with AND. An existing
workflow excluded by its status returns an empty list. `null` includes every
status, and `[]` matches none. Filtering by `COMPLETED` selects finished workflows,
not workflows whose head completed an intermediate step. Refresh happens before
filtering, so newly observed failures/completions can appear immediately.

Enums are returned as their string names. Serialized action definitions are
excluded. The MCP structured result wraps the list in `result`, following the
SDK's schema convention; the CLI unwraps this field into each operation's result.

`refresh=true` wakes supervisors to poll all active workflow heads without
waiting for results; responses contain currently recorded status. This applies even
when one workflow was selected. It propagates statuses to workflow records without submitting or
advancing steps. Failed polls log errors and retain cached values. Compute head
status `ACTIVE` includes provider states `new`, `queued`, and `active`, so inspect
`api_status` to distinguish queueing from execution. `workflow_status` remains
`ACTIVE` between steps; `head_action_status` can be `SCHEDULING`, `PROVIDER_HELD`,
or `COMPLETED`. A held head remains monitored and can be canceled normally.

## JSON sequences and results

Pass a request object or a nonempty array using `--request`, `--file requests.json`,
or `--file -` for standard input:

```json
[
  {"tool": "list_workflows", "arguments": {}},
  {"tool": "list_workflows", "arguments": {"workflow_id": 999}},
  {"tool": "list_workflows", "arguments": {}}
]
```

The full document's request structure is validated before connecting. Tool
arguments are validated by the server when each call executes. Calls run
sequentially over one connection, stopping at the first failure by default.
`--continue-on-error` continues after tool errors; connection/protocol failures
always stop further calls. There are no automatic retries or rollback.

The CLI parses JSON into Pydantic `ToolRequest` models before invoking the
library's `execute_requests(url, requests, ...)`. Python callers supply one
`ToolRequest` or a nonempty list of them; the executor does not accept raw
dictionaries or parse JSON. A malformed later batch entry prevents the CLI
from executing any of the batch.

For a traditional command, the frontend calls
`execute_command(url, tool_name, arguments, timeout=...)`. The library discovers
the tool, converts options into the same `ToolRequest` model, then uses the
shared execution code on the already-open connection.

For the sequence above, the response could be:

```json
{
  "success": false,
  "results": [
    {"index": 0, "tool": "list_workflows", "status": "succeeded", "result": []},
    {"index": 1, "tool": "list_workflows", "status": "failed",
     "error": {"code": "workflow_not_found", "message": "Workflow 999 does not exist"}},
    {"index": 2, "tool": "list_workflows", "status": "skipped",
     "reason": "Stopped after operation 1 failed"}
  ]
}
```

The CLI prints one JSON envelope to stdout, routes library diagnostics to stderr
using the same logging hooks without redirecting stdout, and
exits zero only if every operation succeeds. Invalid input or an initial
connection failure produces a top-level `error` and an empty `results` array.
A connection failure during a call marks its outcome unknown and skips later
operations; earlier results remain available. `--timeout` defaults to 30 seconds
for HTTP connection/read operations and each tool call.

## Test locally

From the repository root, activate your environment and install the optional
dependencies into that same interpreter:

```bash
source ~/miniconda3/bin/activate
conda activate ai
python -m pip install -e '.[server]'
python -m unittest discover -s test -p test_mcp_server.py -v
```

You can also run `python test/test_mcp_server.py -v`. To select only the local
workflow/HTTP test, append `-k local_job_status` to the unittest discovery command.

The test prints each MCP request and response to stderr. Subprocess checks also
show the full CLI command, the JSON payload (including file/stdin contents),
exit code, and any child stderr. Mocked exchanges and inputs rejected locally
are explicitly labeled, so they are not mistaken for requests sent to a server.
This diagnostic output is enabled even without `-v`; `-v` additionally displays
individual unittest names. To save it, append `2> test_mcp_server.log`.

Do not launch Uvicorn separately for this test. The test constructs
`uvicorn.Server` and invokes its `run()` method internally, then starts separate
CLI processes against its temporary endpoint. Neither a `uvicorn ...` command
nor a separately running manager server is required. For normal server use,
run `python utilities/manager_server.py ...` as shown above; that frontend starts
the SDK's HTTP server itself. Using `python -m pip` ensures dependencies are
installed for the interpreter used by the tests, rather than a potentially
different interpreter associated with a standalone `pip` or `uvicorn` executable.

Missing or incompatible MCP/Uvicorn dependencies produce a startup diagnostic
with the original import error, active Python interpreter, and an installation
command for that interpreter. Direct execution exits with status 1 instead of
reporting an all-skipped success. Under unittest discovery, optional-dependency
skips remain, accompanied by that diagnostic. Install `.[server]` in the active
environment. Tests require permission to bind/connect to loopback
sockets and run local executables; restricted execution sandboxes may need
those permissions enabled.

The tests use the actual SDK, a temporary loopback listener in a child process,
and separate CLI subprocesses. A local `/bin/sleep 30` job is submitted through
the Python manager API alongside short successful and failing jobs; the CLI
then lists, filters, and refreshes them using both traditional and JSON commands.
Tests also cover server-discovered command summaries and detailed input/output
help through both `-h TOOL` and `TOOL -h`, schema-derived types/defaults, one-connection discovery
and execution, invalid arguments rejected before execution,
tool discovery, error/skip/continuation responses, JSON files/stdin, and input
validation. Cleanup cancels the local job and stops the test server.

`test_mcp_workflows.py` exercises submission/cancellation through the real SDK and
HTTP CLI, including both compute forms, API operations, a simulated asynchronous
Globus provider, pending/active cancellation, generated nested-schema help and
submission-key reuse after server restart. `test_workflow_schema.py` verifies
native JSON/pickle round trips and rejects invalid schemas before execution.
