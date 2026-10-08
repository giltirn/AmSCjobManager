"""Exercise blocking Globus file upload/download for configured sandboxes.

Run manually with a manager configuration containing ``globus_personal_endpoint``:

    python3 test/test_globus_file_roundtrip.py path/to/manager-config.json

Each configured IRI sandbox is mapped to its Globus collection. Sandboxes that
share a collection are tested only once. The script leaves its remote test
files in the configured sandboxes for inspection.
"""

from pathlib import Path, PurePosixPath
import hashlib
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
        "Usage: python3 test/test_globus_file_roundtrip.py <manager-config.json>\n"
        f"Configuration template available at {template_path}"
    )


config = readManagerConfigFile(sys.argv[1])
if config.globus_personal_endpoint is None:
    raise SystemExit(
        "Configuration must provide globus_personal_endpoint to run this test"
    )

sandbox_directories = iri_api.setupWorkflowAgent(
    config.iri_implementations,
    config.transferapi_key_path,
    config.sandbox_directories,
    config.globus_personal_endpoint,
)

test_id = uuid.uuid4().hex
selected_targets: dict[str, tuple[str, PurePosixPath]] = {}
for machine, directories in sandbox_directories.items():
    machine = machine.lower()
    if machine not in iri_api.MACHINE_IMPLEMENTATIONS:
        continue

    for directory in directories:
        sandbox = PurePosixPath(directory)
        remote_directory = sandbox / "amsc-job-manager-globus-roundtrip" / test_id
        endpoint, _ = iri_api._remote_globus_location(machine, str(remote_directory))
        endpoint_uuid = iri_api.replaceSpecialGlobusEndpoint(endpoint)
        selected_targets.setdefault(endpoint_uuid, (machine, remote_directory))

if not selected_targets:
    raise SystemExit("No configured IRI sandbox maps to a Globus endpoint")

payload = os.urandom(32 * 1024)
payload_digest = hashlib.sha256(payload).hexdigest()
with tempfile.TemporaryDirectory(prefix="amsc-globus-roundtrip-") as temporary_directory:
    temporary_directory = Path(temporary_directory)
    source_file = temporary_directory / "upload-source.bin"
    source_file.write_bytes(payload)

    for endpoint_uuid, (machine, remote_directory) in selected_targets.items():
        remote_file = remote_directory / "roundtrip.bin"
        downloaded_file = temporary_directory / f"download-{endpoint_uuid}.bin"

        iri_api.remoteMkdir(machine, str(remote_directory), allow_unsafe=False)
        upload_transfer = iri_api.uploadFileGlobus(
            machine, str(remote_file), str(source_file)
        )
        download_transfer = iri_api.downloadFileGlobus(
            machine, str(remote_file), str(downloaded_file)
        )

        downloaded_digest = hashlib.sha256(downloaded_file.read_bytes()).hexdigest()
        if downloaded_digest != payload_digest:
            raise RuntimeError(
                f"Round-trip checksum mismatch for {machine} via {endpoint_uuid}: "
                f"expected {payload_digest}, got {downloaded_digest}"
            )
        print(
            f"passed: {machine} via {endpoint_uuid} "
            f"(upload {upload_transfer}, download {download_transfer})"
        )

print("Globus file round-trip test completed")
