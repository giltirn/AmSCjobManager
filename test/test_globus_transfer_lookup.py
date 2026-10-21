"""Check that an IRI transfer ID matches its Globus label lookup ID.

Run with an IRI manager configuration containing a Perlmutter sandbox:

    python3 test/test_globus_transfer_lookup.py path/to/manager-config.json

The test leaves its uniquely named source and destination directories in the
sandbox so a failed transfer can be inspected afterwards.
"""

from io import BytesIO
from pathlib import PurePosixPath
import sys
import time
import uuid

from amscjobmanager import iri_api
from amscjobmanager.manager_config import readManagerConfigFile
from amscjobmanager.manager_config_models import ManagerConfig


if len(sys.argv) != 2:
    template_path = ManagerConfig.write_template_json()
    raise SystemExit(
        "Usage: python3 test/test_globus_transfer_lookup.py <manager-config.json>\n"
        f"Configuration template available at {template_path}"
    )

config = readManagerConfigFile(sys.argv[1])
sandbox_directories = iri_api.setupWorkflowAgentFromConfig(config)

perlmutter_sandbox = PurePosixPath(sandbox_directories["perlmutter"][0])
test_id = uuid.uuid4().hex
test_dir = perlmutter_sandbox / "amsc-job-manager-transfer-tests" / test_id
source_file = test_dir / "source.txt"
destination_dir = test_dir / "dtn-destination"
label = f"amscjm:transfer-wrapper-test:{test_id}"

# NERSC recommends using its DTN collection for all transfers. The configured
# Perlmutter sandbox path is shared with that collection.
iri_api.remoteMkdir("perlmutter", str(test_dir), allow_unsafe=False)
iri_api.remoteMkdir("perlmutter", str(destination_dir), allow_unsafe=False)
iri_api.uploadBytes(
    "perlmutter",
    str(source_file),
    BytesIO(b"AmSC job manager Globus wrapper integration test\n"),
    allow_unsafe=False,
)

# Submit through IRI's movement API; discovery below uses Globus Transfer.
transfer_id = iri_api.globusCopy(
    "dtn",
    f"{destination_dir}/",
    "dtn",
    str(source_file),
    allow_unsafe=False,
    block_until_complete=False,
    label=label,
)
if not transfer_id:
    raise AssertionError("IRI did not return a transfer ID")

# Task-list indexing can lag submission.  Completed tasks remain discoverable,
# so this works whether the tiny transfer is active or already historical.
deadline = time.monotonic() + 60
found_transfer_id = None
while time.monotonic() < deadline:
    found_transfer_id = iri_api.findGlobusTransferByLabel(label)
    if found_transfer_id is not None:
        break
    time.sleep(2)

if found_transfer_id != transfer_id:
    raise AssertionError(
        f"IRI returned transfer ID {transfer_id!r} for label {label!r}, "
        f"but Globus returned {found_transfer_id!r}"
    )
print(f"Found Globus transfer {transfer_id} by label {label}")
