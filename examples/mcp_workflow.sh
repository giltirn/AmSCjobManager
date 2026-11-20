#!/usr/bin/env bash
# Run a two-stage local workflow entirely through the MCP command-line client.
# Requires an active Python environment with amsc-job-manager[server].
# Usage: bash examples/mcp_workflow.sh [port]  (default: 8765)
set -euo pipefail

repository=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
if ! python_executable=$(command -v python); then
    echo 'Error: python was not found on PATH.' >&2
    echo 'Activate your conda environment with amsc-job-manager[server] installed, then rerun this script.' >&2
    exit 1
fi
example_dir=$(mktemp -d /tmp/amsc-mcp-example.XXXXXX)
port=${1:-8765}
server_pid=
workflow_id=
workflow_status=PENDING

# Stop this example's server on exit and request cancellation if interrupted.
# Takes no arguments, preserves exit status, and retains /tmp artifacts/logs.
cleanup() {
    local status=$?
    if [[ -n "$server_pid" ]]; then
        if [[ -n "$workflow_id" && "$workflow_status" != COMPLETED &&
              "$workflow_status" != FAILED && "$workflow_status" != CANCELED ]]; then
            "${client[@]}" cancel_workflow --workflow-id "$workflow_id" >/dev/null 2>&1 || true
        fi
        kill "$server_pid" 2>/dev/null || true
        wait "$server_pid" 2>/dev/null || true
    fi
    if (( status != 0 )) && [[ -f "$example_dir/server-console.log" ]]; then
        tail -n 20 "$example_dir/server-console.log" >&2
    fi
    echo "Example files and database: $example_dir"
    return "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# 1. Write the server configuration and CLI request directly as JSON. The
#    workflow directory is created later by the first API action, not here.
if [[ ${AMSC_JOB_MANAGER_API_IMPL-IRI} != IRI ]]; then
    echo 'This example requires the IRI backend, including its local implementation.' >&2
    exit 1
fi

cat >"$example_dir/config.json" <<JSON
{
  "iri_implementations": {},
  "sandbox_directories": {"local": ["$example_dir"]}
}
JSON

# Escape the arithmetic dollar sign so Bash evaluates it inside the compute job.
cat >"$example_dir/submission.json" <<JSON
{
  "tool": "submit_workflow",
  "arguments": {
    "workflow_group": "local-mcp-example",
    "submission_key": "increment-workflow",
    "stages": [
      {
        "type": "api",
        "operations": [
          {"type": "make_directory", "machine": "local", "path": "$example_dir/work"},
          {"type": "write_file", "machine": "local", "path": "$example_dir/work/input.txt", "content": "1\n"}
        ]
      },
      {
        "type": "execute_job",
        "machine": "local", "account": "", "queue": "", "time": "60",
        "rundir": "$example_dir/work", "nodes": 1, "ranks_per_node": 1, "gpus_per_rank": 0,
        "executable": "/bin/bash",
        "arguments": [
          "-c",
          "set -euo pipefail; read -r value < input.txt; echo \"\$((value + 1))\" > output.txt"
        ]
      }
    ]
  }
}
JSON

# 2. Start the server in /tmp so both workflow and local-job databases stay there.
#    The local target uses one node and one rank; no MPI installation is needed.
endpoint="http://127.0.0.1:$port/mcp"

(
    cd "$example_dir"
    exec "$python_executable" "$repository/utilities/manager_server.py" config.json \
        --database workflows.sqlite --port "$port" --poll-interval 1 --log-file server.log
) >"$example_dir/server-console.log" 2>&1 &
server_pid=$!
echo "Starting server: $endpoint"

# Format client command
client=("$python_executable" "$repository/utilities/manager_client.py" --url "$endpoint" --timeout 5)

# Wait for our own server to bind, then for its manager to answer a CLI call.
# Checking its PID notice avoids connecting to an unrelated server on a busy port.
startup_deadline=$((SECONDS + 30))
until grep -Fq "Server started: $endpoint (PID $server_pid," "$example_dir/server-console.log" 2>/dev/null &&
      "${client[@]}" list_workflows >"$example_dir/status.json" 2>"$example_dir/client.log"; do
    if ! kill -0 "$server_pid" 2>/dev/null || (( SECONDS >= startup_deadline )); then
        echo 'Server did not become ready; see the logs below.' >&2
        exit 1
    fi
    sleep 0.2
done

# 3. Submit the whole workflow once. The manager executes the API setup before
#    launching Bash, which reads input.txt and writes the incremented output.txt.
"${client[@]}" --file "$example_dir/submission.json" >"$example_dir/submitted.json"
# Extract the known field from the CLI's indented JSON response.
workflow_id=$(sed -n 's/^[[:space:]]*"workflow_id":[[:space:]]*\([0-9][0-9]*\).*/\1/p' "$example_dir/submitted.json")
: "${workflow_id:?Missing workflow_id in CLI response}"
echo "Submitted workflow $workflow_id"

# 4. Poll recorded workflow status through the client; print the result only
#    after both stages have completed. Failed/canceled workflows exit with error.
while :; do
    "${client[@]}" list_workflows --workflow-id "$workflow_id" >"$example_dir/status.json"
    workflow_status=$(sed -n 's/^[[:space:]]*"workflow_status":[[:space:]]*"\([A-Z_]*\)".*/\1/p' "$example_dir/status.json")
    : "${workflow_status:?Missing workflow_status in CLI response}"
    echo "Workflow $workflow_id: $workflow_status"
    case "$workflow_status" in
        COMPLETED) break ;;
        FAILED|CANCELED)
            echo "Workflow did not complete successfully; see $example_dir/server.log" >&2
            exit 1 ;;
    esac
    sleep 1
done

result=$(<"$example_dir/work/output.txt")
echo "Output file contents: $result"
echo 'Expected value: 2'
[[ "$result" == 2 ]]
