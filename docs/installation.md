# Installation and first run

[Documentation home](../README.md#documentation)

## Requirements

Use Python 3.12 or newer: the source uses Python 3.12 f-string syntax. The package
metadata does not currently declare a `requires-python` constraint or a tested
version matrix.

The [package metadata](../pyproject.toml) declares its dependencies, including
`globus_sdk`, `authlib`, `httpx`, and Pydantic. Install from this repository:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e .
```

An editable installation makes source changes available without reinstalling.
Use `python -m pip install .` for a non-editable installation.

Local execution uses Linux/Unix utilities including Bash, `whoami`, `file`,
`mkdir`, `cp`, `chmod`, `stat`, and `/proc/sys/kernel/random/uuid`. The current local runner is
not portable to Windows. Remote jobs require the requested executable, runtime,
and launcher to exist on the destination machine.

## Run without remote accounts

From the repository root:

```bash
python examples/spoof_workflow_apiv1.py
```

The script selects SPOOF itself. It creates temporary local inputs and a real
SQLite database, simulates remote actions, prints their progression, and removes
its temporary directory when finished. It does not run the calculation or
transfer its data. See the [example walkthrough](workflows.md).

## Select an implementation

`AMSC_JOB_MANAGER_API_IMPL` accepts `IRI` (the default) or `SPOOF`. Set it before
**any** `amscjobmanager` import, including imports from submodules:

```python
import os
os.environ["AMSC_JOB_MANAGER_API_IMPL"] = "SPOOF"

from amscjobmanager import JobManager
```

The choice is process-wide and happens at import time. Use a fresh Python
process or notebook kernel to change it. Local execution uses the IRI selection
with `machine="local"`; there is no `LOCAL` environment setting.

## Prepare a real configuration

Generate a starter file:

```bash
python utilities/generate_config_template.py
```

This writes `manager-config-template.json` in the working directory if it does
not already exist. Edit its paths and account-specific settings before use;
the generated values are placeholders.

Follow [configuration and authentication](configuration.md), then initialize
the library before making API calls or starting a manager. Use a persistent
workflow database outside the checkout's temporary directories for real work.
