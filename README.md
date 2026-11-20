# AmSCjobManager

AmSCjobManager is a Python library built to exploit the new capabilities of the
[DOE Integrated Research Infrastructure (IRI) API](https://iri.science/). Its
primary focus is turning IRI's programmatic access to DOE resources into robust,
long-running compute campaigns spanning multiple machines and facilities across
the DOE complex.

IRI opens up new ways to connect scientific facilities, data, and advanced
computing resources. AmSCjobManager puts those capabilities to work by combining
IRI resource queries, filesystem operations, and compute submission with Globus
data movement, durable workflow state, and automatic progression between steps.

Build a workflow as a list of compute, transfer and API actions. The manager submits
each action, monitors it, and starts the next after successful completion.
Multiple workflows can run concurrently, with durable state that supports
recovery when the application restarts.

## Why use AmSCjobManager?

- **Put DOE IRI capabilities to work.** Build campaigns around IRI's facility
  APIs for resource discovery, data access, and job control. The library handles
  supported provider differences so applications can compose these operations
  into workflows spanning DOE facilities.
- **Robust management of long-running campaigns.** SQLite records workflow and
  action state, including the intention to submit or cancel remote work. After
  a restart, reconciliation recovers remote identifiers and finishes interrupted
  operations. Compute submissions wait during reported resource downtime
  without exhausting their retry allowance.
- **Concurrent workflows across facilities.** Asynchronous compute and transfer
  actions let the manager coordinate independent workflows without waiting for
  each remote task to finish before managing others. A configurable concurrency
  limit controls how many workflows are active.
- **Complex, interdependent workflows.** Connect workflow stages using producer
  keys and nested AND/OR dependency conditions evaluated against successful
  action completion. Build parallel branches, joins, and conditional dependencies
  across workflows and facilities. Inspect connected groups through Python or
  MCP/CLI. See [workflow dependencies](docs/workflows.md#connect-workflows-with-dependencies).
- **Connect agents through a private MCP server.** Run the manager as a persistent
  server listening on localhost and connect MCP-compatible agents to submit,
  monitor, and cancel workflows. A command-line client supports the same
  operations. Sandbox changes use a separate password-protected admin utility.
  See the [server and client guide](docs/server.md).
- **Sandboxed API filesystem activity.** Configure permitted directories for
  each machine to constrain filesystem mutations and compute run directories.
  Operations with sandbox enforcement reject destinations outside those roots
  unless explicitly authorized through the API. These checks help restrict the
  blast radius of agent actions when exposing the checked operations. The
  [sandboxing guide](docs/sandboxing.md) describes enforcement coverage,
  explicit overrides, staging permissions, and current exceptions.
- **Machine-specific compute budgets.** Optional persistent node-hour wallets
  reserve a job's requested cost before submission and refund estimated unused
  time when it ends. Agents can inspect balances; funding and disabling limits
  use the password-protected local admin utility. See [compute wallets](docs/compute_wallets.md).
- **Compute and data movement in one workflow.** Express staging, computation,
  and result transfers as ordered actions. Configured path mappings translate
  filesystem paths into Globus paths, reducing the mappings callers need to
  remember.
- **Develop without remote resources.** Use local execution for real commands
  on the manager host, or SPOOF to exercise workflow orchestration without
  credentials, scheduler allocations, or real transfers.

## Install and try it

Use Python 3.12 or newer. From a checkout of this repository:

```bash
python -m pip install -e .
python examples/spoof_workflow_apiv1.py
```

The [reference example](examples/spoof_workflow_apiv1.py) selects SPOOF before
importing the library. It demonstrates staging, a transfer → compute → transfer
workflow, monitoring, and shutdown without credentials or remote execution.
Its remote filesystem responses and downloaded results are simulated.

For workflow submission, monitoring, and cancellation through agents or the
traditional/JSON command-line client, see the optional
[private MCP server](docs/server.md).

For a real calculation, see the [MPI workflow example](examples/README.md).
It uploads C++ source, compiles in pre-launch, runs MPI under `WorkflowManager`,
then downloads and verifies the result on local or configured IRI machines.

For real operations, select IRI before importing the package and initialize it
from a configuration file:

```python
import os
os.environ["AMSC_JOB_MANAGER_API_IMPL"] = "IRI"

from amscjobmanager.manager_config import readManagerConfigFile, setupManager

config = readManagerConfigFile("manager-config.json")
setupManager(config)
```

See [installation](docs/installation.md) and
[configuration and authentication](docs/configuration.md) before connecting to a
facility. Creating a `WorkflowManager` does not initialize API configuration.

## Documentation

| Guide | Contents |
| --- | --- |
| [Architecture](docs/architecture.md) | Workflows, actions, API implementations, and persistence |
| [Installation](docs/installation.md) | Runtime requirements, installation, and first run |
| [Configuration and authentication](docs/configuration.md) | JSON fields, token files, discovery, and setup |
| [Implementations](docs/implementations.md) | IRI, local execution, SPOOF, and capability differences |
| [Sandboxing](docs/sandboxing.md) | Path checks, staging permissions, and protection limits |
| [Paths and transfers](docs/transfers.md) | Filesystem/Globus namespaces, endpoints, and file staging |
| [Building workflows](docs/workflows.md) | Action parameters and a walkthrough of the example |
| [API action stages](docs/api_actions.md) | Deferred filesystem preparation, short local processing, and checkpoint recovery |
| [Operating the manager](docs/operations.md) | Locks, status, cancellation, retries, and restart recovery |
| [Private MCP server and clients](docs/server.md) | Agent connections, CLI commands, user queries, and protected local administration |
| [API reference](docs/api.md) | Public entry points, arguments, and return values |
| [Troubleshooting](docs/troubleshooting.md) | Common errors and current limitations |
| [Test guide](test/README.md) | Unit, local integration, and explicitly invoked live tests |
| [Development](docs/development.md) | Source layout, tests, and adding action types |

Start with the runnable example, then read the workflow and transfer guides.
Use the sandboxing guide when deciding which operations to expose to callers:
path checks are application safeguards, and do not confine commands inside a
compute job.
