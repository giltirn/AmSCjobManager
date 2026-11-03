"""Exercise all implemented Globus filesystem and transfer operations live.

    python test/test_globus_operations_live.py manager-config.json frontier

Requires transfer credentials and a reachable personal endpoint/staging directory.
The first sandbox for the selected machine must exist and map to a collection.
All remote writes use a unique child of that sandbox, with checks enabled.
Remote files and local test artifacts are retained for inspection. No compute
jobs, chmod, rename or delete operations are submitted. Outstanding known copy
tasks are canceled if a check fails. Upload/download wrappers use their existing
blocking waits; --timeout bounds explicit polling and label lookup only.
"""

import argparse
from datetime import datetime
from hashlib import sha256
from pathlib import Path, PurePosixPath
import os
import tempfile
import time
import uuid


def wait_transfer(api, task_id, timeout, poll_interval, *, canceled=False):
    """Poll a submitted task to a terminal state and return that state.

    api supplies the Globus wrappers; task_id identifies this test's transfer.
    timeout/poll_interval bound polling in seconds. Normal transfers must
    succeed. After a cancellation request, FAILED is expected but SUCCEEDED is
    accepted because a small transfer can finish before cancellation arrives.
    Query errors, suspended tasks, unexpected states and timeouts fail the test.
    """
    deadline = time.monotonic() + timeout
    while True:
        status = api.globusTransferStatus(task_id)
        if status == "SUCCEEDED" or (canceled and status == "FAILED"):
            return status
        if status != "ACTIVE":
            raise RuntimeError(f"Transfer {task_id} ended or stalled with status {status!r}")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"Transfer {task_id} remained ACTIVE for {timeout} seconds")
        time.sleep(min(poll_interval, remaining))


def check_metadata(api, machine, path, kind, size=None):
    """Check and return shared Globus stat metadata for a known test artifact.

    path is a machine filesystem path; kind is its expected type and size is
    optionally its expected byte count. Validate the common metadata fields,
    octal permissions when available, and the timestamp's UTC timezone.
    """
    metadata = api.pathStatGlobus(machine, str(path))
    required = {"type", "size", "permissions", "user", "group", "last_modified"}
    if set(metadata) != required or metadata["type"] != kind:
        raise AssertionError(f"Unexpected metadata for {path}: {metadata!r}")
    if not isinstance(metadata["size"], int) or metadata["size"] < 0:
        raise AssertionError(f"Invalid size for {path}: {metadata!r}")
    if size is not None and metadata["size"] != size:
        raise AssertionError(f"Incorrect size for {path}: expected {size}, got {metadata['size']}")
    if metadata["permissions"] is not None:
        int(metadata["permissions"], 8)
    if metadata["last_modified"] is not None:
        modified = datetime.fromisoformat(metadata["last_modified"])
        if modified.utcoffset() is None or modified.utcoffset().total_seconds() != 0:
            raise AssertionError(f"Modification time is not UTC: {metadata!r}")
    return metadata


def main(argv=None) -> int:
    """Parse config/machine arguments and run the live checks; return zero on success.

    argv optionally supplies command-line arguments. Validate the first sandbox
    mapping and transfer prerequisites before creating remote artifacts. Fail
    with an exception at the first incorrect result, retaining artifacts and
    reporting their locations. Importing this module performs no live work.
    """
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("config", help="Manager configuration JSON file")
    parser.add_argument("machine", type=str.lower, help="Configured IRI machine, e.g. frontier")
    parser.add_argument("--timeout", type=float, default=300, help="Seconds allowed per explicit task poll or label lookup")
    parser.add_argument("--poll-interval", type=float, default=5, help="Seconds between explicit polling requests")
    args = parser.parse_args(argv)
    if not (0 < args.timeout < float("inf")) or not (0 < args.poll_interval < float("inf")):
        parser.error("--timeout and --poll-interval must be finite positive numbers")

    from amscjobmanager import iri_api as api
    from amscjobmanager.manager_config import readManagerConfigFile

    config = readManagerConfigFile(args.config)
    sandboxes = api.setupWorkflowAgentFromConfig(config)
    machine = args.machine
    if machine not in api.MACHINE_IMPLEMENTATIONS or not sandboxes.get(machine):
        parser.error(f"Machine {machine!r} needs a configured IRI implementation and sandbox")
    sandbox = PurePosixPath(sandboxes[machine][0])
    # Do not silently choose a different sandbox if the first has no route.
    endpoint, sandbox_globus = api.getRemoteGlobusLocation(machine, str(sandbox))
    endpoint = api.replaceSpecialGlobusEndpoint(endpoint)
    if not config.transferapi_key_path:
        parser.error("Configuration must provide transferapi_key_path")
    if config.globus_personal_endpoint is None:
        parser.error("Configuration must provide globus_personal_endpoint for upload/download checks")
    print(f"Target: {machine}:{sandbox} -> {endpoint}:{sandbox_globus}", flush=True)
    check_metadata(api, machine, sandbox, "dir")

    test_id = uuid.uuid4().hex
    remote_root = sandbox / f"amsc-globus-operations-{test_id}"
    local_root = Path(tempfile.mkdtemp(prefix="amsc-globus-operations-", dir="/tmp"))
    pending = set()
    staging_root = None
    print(f"Remote artifacts: {machine}:{remote_root}", flush=True)
    print(f"Local artifacts: {local_root}", flush=True)
    try:
        # Exercise staging construction and both the staged and unstaged paths
        # through the blocking upload/download wrappers.
        staging_root, staging_globus = api.createGlobusStagingDirectory()
        print(f"Personal staging artifacts: {staging_root} (Globus {staging_globus})", flush=True)
        inputs = remote_root / "inputs"
        api.remoteMkdirGlobus(machine, str(inputs))
        api.remoteMkdirGlobus(machine, str(inputs))  # Existing directory: mkdir -p.
        direct_dir = remote_root / "direct"
        api.remoteMkdirGlobus(machine, str(direct_dir), create_parents=False)
        check_metadata(api, machine, inputs, "dir")
        if api.pathTypeGlobus(machine, str(inputs)) != "directory":
            raise AssertionError("Directory path-type check failed")
        if api.remoteLsGlobus(machine, str(inputs)):
            raise AssertionError("New input directory is not empty")

        # Missing-path checks must not be confused with authorization failures.
        missing = inputs / "not-created"
        try:
            api.pathStatGlobus(machine, str(missing))
        except FileNotFoundError:
            pass
        else:
            raise AssertionError("Stat did not report the missing path")
        if "cannot open" not in api.pathTypeGlobus(machine, str(missing)):
            raise AssertionError("Path-type query did not report the missing path")
        print("PASS: mkdir, existing-directory handling, stat, type, empty listing, missing paths", flush=True)

        payloads = {"payload.bin": os.urandom(64 * 1024),
                    "text with spaces.txt": b"Globus live test: UTF-8 text \xc2\xb5\n",
                    "empty.bin": b"", ".hidden": b"hidden test file\n"}
        for index, (name, payload) in enumerate(payloads.items()):
            # A differing basename tests automatic staging/renaming; .hidden
            # is uploaded directly from the configured personal staging area.
            source = (staging_root / name) if name == ".hidden" else (local_root / f"source-{name}")
            source.write_bytes(payload)
            remote_file = inputs / name
            options = {}
            target = str(remote_file)
            if index % 2:
                selected_endpoint, target = api.getRemoteGlobusLocation(machine, target)
                options = {"remote_path_type": "globus", "remote_endpoint": api.replaceSpecialGlobusEndpoint(selected_endpoint)}
            task_id = api.uploadFileGlobus(machine, target, str(source), **options)
            if api.globusTransferStatus(task_id) != "SUCCEEDED":
                raise AssertionError(f"Blocking upload did not succeed: {task_id}")
            check_metadata(api, machine, remote_file, "file", len(payload))
            if api.pathTypeGlobus(machine, str(remote_file)) != "regular file":
                raise AssertionError(f"Incorrect file type for {remote_file}")

            # Download to an explicit renamed output or an existing directory,
            # using the same namespace variant as the upload.
            output = local_root / f"download-{name}" if index % 2 == 0 else local_root
            downloaded = Path(api.downloadFileGlobus(machine, str(output), target, **options))
            expected_output = output if index % 2 == 0 else output / name
            if downloaded != expected_output or sha256(downloaded.read_bytes()).digest() != sha256(payload).digest():
                raise AssertionError(f"Download path or checksum mismatch for {remote_file}")
            print(f"PASS: upload/download, metadata and content: {name!r}", flush=True)
        if set(api.remoteLsGlobus(machine, str(inputs))) != set(payloads):
            raise AssertionError("Listing does not match uploaded files, including hidden entries")
        # Exercise download directly into staging without copying back to /tmp.
        direct_output = staging_root / "payload.bin"
        downloaded = api.downloadFileGlobus(machine, str(direct_output), str(inputs / "payload.bin"))
        if Path(downloaded).read_bytes() != payloads["payload.bin"]:
            raise AssertionError("Direct staging download changed file contents")

        # Copy within the remote collection using both supported namespaces.
        # Explicit labels verify that IRI movement IDs are direct Globus IDs.
        for namespace in ("filesystem", "globus"):
            destination = remote_root / f"copy-{namespace}"
            api.remoteMkdirGlobus(machine, str(destination))
            label = f"amsc-globus-live-{test_id}-{namespace}"
            source_endpoint = destination_endpoint = machine
            source_path, destination_path = str(inputs / "payload.bin"), str(destination)
            if namespace == "globus":
                source_endpoint, source_path = api.getRemoteGlobusLocation(machine, source_path)
                destination_endpoint, destination_path = api.getRemoteGlobusLocation(machine, destination_path)
                source_endpoint = api.replaceSpecialGlobusEndpoint(source_endpoint)
                destination_endpoint = api.replaceSpecialGlobusEndpoint(destination_endpoint)
            task_id = api.globusCopy(destination_endpoint, destination_path, source_endpoint, source_path,
                                     source_path_type=namespace, dest_path_type=namespace, label=label)
            pending.add(task_id)
            wait_transfer(api, task_id, args.timeout, args.poll_interval)
            pending.remove(task_id)
            deadline = time.monotonic() + args.timeout
            while (found := api.findGlobusTransferByLabel(label)) is None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f"Transfer label was not indexed: {label}")
                time.sleep(min(args.poll_interval, remaining))
            if found != task_id or api.findGlobusTransfersByLabel(label) != task_id:
                raise AssertionError(f"Label lookup disagrees with IRI transfer ID {task_id}")
            copied = destination / "payload.bin"
            check_metadata(api, machine, copied, "file", len(payloads["payload.bin"]))
            result = api.downloadFileGlobus(machine, str(local_root / f"verified-{namespace}.bin"), str(copied))
            if Path(result).read_bytes() != payloads["payload.bin"]:
                raise AssertionError(f"{namespace} copy changed file contents")
            print(f"PASS: {namespace} copy, status, label lookup and checksum", flush=True)

        # A directory source copies its contents directly into the destination,
        # including hidden and zero-byte files, without nesting the source name.
        # The destination is outside that source subtree.
        recursive = remote_root / "recursive"
        api.remoteMkdirGlobus(machine, str(recursive))
        task_id = api.globusCopy(machine, str(recursive), machine, str(inputs),
                                 source_path_type="filesystem", dest_path_type="filesystem",
                                 label=f"amsc-globus-live-{test_id}-recursive")
        pending.add(task_id)
        wait_transfer(api, task_id, args.timeout, args.poll_interval)
        pending.remove(task_id)
        if set(api.remoteLsGlobus(machine, str(recursive))) != set(payloads):
            raise AssertionError("Recursive copy lost files")
        for name, payload in payloads.items():
            result = api.downloadFileGlobus(machine, str(local_root / f"recursive-{name}"), str(recursive / name))
            if Path(result).read_bytes() != payload:
                raise AssertionError(f"Recursive copy changed {name!r}")
        print("PASS: recursive directory copy and content verification", flush=True)

        # A tiny task can complete before cancellation reaches Globus. Report
        # that race explicitly; do not claim that a completed task was canceled.
        task_id = api.globusCopy(machine, str(direct_dir), machine, str(inputs / "payload.bin"),
                                 source_path_type="filesystem", dest_path_type="filesystem",
                                 label=f"amsc-globus-live-{test_id}-cancel")
        pending.add(task_id)
        api.cancelGlobusTransfer(task_id)
        status = wait_transfer(api, task_id, args.timeout, args.poll_interval, canceled=True)
        pending.remove(task_id)
        print(f"PASS: cancellation request; terminal status {status}" +
              (" (transfer completed before cancellation took effect)" if status == "SUCCEEDED" else ""), flush=True)
        print(f"ALL GLOBUS CHECKS PASSED: {machine}:{remote_root}", flush=True)
        return 0
    finally:
        # Do not leave known nonblocking transfers running after a failed check.
        for task_id in pending:
            try:
                api.cancelGlobusTransfer(task_id)
                wait_transfer(api, task_id, args.timeout, args.poll_interval, canceled=True)
            except Exception as error:
                print(f"Could not confirm termination of {task_id}: {error}; inspect this task in Globus", flush=True)
        print(f"Artifacts retained: {machine}:{remote_root}; local {local_root}; staging {staging_root}", flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
