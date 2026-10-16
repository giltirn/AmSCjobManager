"""Exercise IRI filesystem upload/list/download for every mounted filesystem.

Run manually with a manager configuration:

    python3 test/test_iri_file_roundtrip.py path/to/manager-config.json

Every configured IRI machine must provide at least one sandbox directory below
each filesystem listed in its ``mounted_filesystems`` configuration.  The test
uses one such sandbox per filesystem and leaves remote test artifacts behind
for inspection.
"""

from hashlib import sha256
from io import BytesIO
from pathlib import Path, PurePosixPath
import os
import sys
import tempfile
import uuid

from amscjobmanager import iri_api
from amscjobmanager.manager_config import readManagerConfigFile
from amscjobmanager.manager_config_models import ManagerConfig


if len(sys.argv) != 2:
    template_path = ManagerConfig.write_template_json()
    raise SystemExit(
        "Usage: python3 test/test_iri_file_roundtrip.py <manager-config.json>\n"
        f"Configuration template available at {template_path}"
    )


def sandbox_for_filesystem(
    directories: list[str], filesystem: dict
) -> PurePosixPath | None:
    """Return a configured sandbox below any IRI root of filesystem."""
    roots = [
        PurePosixPath(root)
        for mapping in filesystem["storage_resource_paths"]
        for root in mapping["filesystem_roots"]
    ]
    for directory in directories:
        sandbox = PurePosixPath(directory)
        if any(sandbox.is_relative_to(root) for root in roots):
            return sandbox
    return None


config = readManagerConfigFile(sys.argv[1])
sandbox_directories = iri_api.setupWorkflowAgent(
    config.iri_implementations,
    config.transferapi_key_path,
    config.sandbox_directories,
    config.globus_personal_endpoint,
    iri_config_cache_directory=config.iri_config_cache_directory,
)

targets: list[tuple[str, str, PurePosixPath, dict]] = []
missing_sandboxes = []
for machine, directories in sandbox_directories.items():
    machine = machine.lower()
    if machine not in iri_api.MACHINE_IMPLEMENTATIONS:
        continue
    for filesystem_name, filesystem in iri_api.getMountedFilesystems(machine).items():
        sandbox = sandbox_for_filesystem(directories, filesystem)
        if sandbox is None:
            roots = [
                root
                for mapping in filesystem["storage_resource_paths"]
                for root in mapping["filesystem_roots"]
            ]
            missing_sandboxes.append(
                f"{machine}:{filesystem_name} ({', '.join(roots)})"
            )
            continue
        targets.append((machine, filesystem_name, sandbox, filesystem))

if not targets:
    raise SystemExit(
        "No configured IRI sandbox filesystem targets found. Skipped "
        "filesystems without a sandbox:\n  " + "\n  ".join(missing_sandboxes)
    )

test_id = uuid.uuid4().hex
payload = os.urandom(32 * 1024)
payload_digest = sha256(payload).hexdigest()
with tempfile.TemporaryDirectory(prefix="amsc-iri-file-roundtrip-") as temporary_directory:
    temporary_directory = Path(temporary_directory)
    for machine, filesystem_name, sandbox, filesystem in targets:
        # Check every IRI root alias before using the configured sandbox.  This
        # catches resource-name and storage-path configuration mistakes even
        # when aliases share the same physical filesystem.
        for mapping in filesystem["storage_resource_paths"]:
            for filesystem_root in mapping["filesystem_roots"]:
                resource_id = iri_api.getResourceID(
                    machine, "storage", filesystem_root
                )
                print(
                    f"resolved: {machine}:{filesystem_root} "
                    f"to storage resource {resource_id}"
                )

        remote_directory = (
            sandbox / "amsc-job-manager-iri-file-roundtrip" / test_id
        )
        remote_file = remote_directory / "roundtrip.bin"
        local_file = temporary_directory / f"{machine}-{filesystem_name}.bin"

        iri_api.remoteMkdir(machine, str(remote_directory))
        iri_api.uploadBytes(machine, str(remote_file), BytesIO(payload))

        directory_contents = {
            PurePosixPath(path).name
            for path in iri_api.remoteLs(machine, str(remote_directory))
        }
        if remote_file.name not in directory_contents:
            raise RuntimeError(
                f"Upload was not listed in {machine}:{remote_directory}; "
                f"contents: {sorted(directory_contents)}"
            )

        iri_api.downloadFile(machine, str(local_file), str(remote_file))
        downloaded_digest = sha256(local_file.read_bytes()).hexdigest()
        if downloaded_digest != payload_digest:
            raise RuntimeError(
                f"Round-trip checksum mismatch for {machine}:{filesystem_name}; "
                f"expected {payload_digest}, got {downloaded_digest}"
            )
        print(f"passed: {machine}:{filesystem_name} via {remote_directory}")

print("IRI filesystem file round-trip test completed")
if missing_sandboxes:
    print(
        "Skipped mounted filesystems without a configured sandbox:\n  "
        + "\n  ".join(missing_sandboxes)
    )
