"""Integration test for labeled Globus transfer discovery.

Run with an IRI manager configuration containing a Perlmutter sandbox:

    python3 test/test_globus_transfer_lookup.py path/to/manager-config.json

The test leaves its uniquely named source and destination directories in the
sandbox so a failed transfer can be inspected afterwards.
"""

from io import BytesIO
import json
from pathlib import Path, PurePosixPath
import sys
import time
import uuid

from amscjobmanager.api_general import (
    findGlobusTransfersByLabel,
    globusCopy,
    remoteMkdir,
    setupWorkflowAgent,
    uploadBytes,
)


if len(sys.argv) != 2:
    raise SystemExit("Usage: python3 test/test_globus_transfer_lookup.py <manager-config.json>")

config = json.loads(Path(sys.argv[1]).read_text())
sandbox_directories = setupWorkflowAgent(
    config["iriapi_key_path"],
    config["transferapi_key_path"],
    config["sandbox_directories"],
)

perlmutter_sandbox = PurePosixPath(sandbox_directories["perlmutter"])
test_id = uuid.uuid4().hex
test_dir = perlmutter_sandbox / "amsc-job-manager-transfer-tests" / test_id
source_file = test_dir / "source.txt"
destination_dir = test_dir / "dtn-destination"
label = f"amscjm:transfer-wrapper-test:{test_id}"

# Perlmutter and the NERSC DTN share this filesystem, so make both directories
# through Perlmutter. The configured sandbox applies to the shared path.
remoteMkdir("perlmutter", str(test_dir), allow_unsafe=False)
remoteMkdir("perlmutter", str(destination_dir), allow_unsafe=False)
uploadBytes(
    "perlmutter",
    str(source_file),
    BytesIO(b"AmSC job manager Globus wrapper integration test\n"),
    allow_unsafe=False,
)

transfer_id = globusCopy(
    "dtn",
    str(destination_dir),
    "perlmutter",
    str(source_file),
    allow_unsafe=False,
    block_until_complete=False,
    label=label,
)
assert transfer_id

# Task-list indexing can lag submission.  Completed tasks remain discoverable,
# so this works whether the tiny transfer is active or already historical.
deadline = time.monotonic() + 60
found_transfer_id = None
while time.monotonic() < deadline:
    found_transfer_id = findGlobusTransfersByLabel(label)
    if found_transfer_id is not None:
        break
    time.sleep(2)

assert found_transfer_id == transfer_id, (
    f"Transfer {transfer_id} was not found by label {label}; got {found_transfer_id}"
)
print(f"Found Globus transfer {transfer_id} by label {label}")
