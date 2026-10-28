# Configuration and authentication

[Documentation home](../README.md#documentation)

## Load a configuration

Configuration is JSON validated by the Pydantic
[`ManagerConfig`](../src/amscjobmanager/manager_config_models.py) model.

```python
from amscjobmanager.manager_config import readManagerConfigFile, setupManager

config = readManagerConfigFile("manager-config.json")
setupManager(config)
```

`readManagerConfigFile()` returns a `ManagerConfig`. `setupManager()` configures
the selected implementation and returns `None`. The lower-level
`api_general.setupWorkflowAgentFromConfig(config)` returns the configured sandbox
mapping instead. Use either setup entry point; calling both is unnecessary.

Use absolute paths in configuration. Relative token, cache, or database paths
otherwise depend on the application's working directory. Paths containing `~`
are rejected, including sandbox/staging roots, credentials, cache directories,
and database/configuration filenames. There is no username or home-directory
inference on either local or remote machines.

## Example JSON

These are illustrative paths. Choose directories and endpoint IDs you control:

```json
{
  "iri_config_cache_directory": "/home/example/.config/amscjobmanager/iri-cache",
  "iri_implementations": {
    "nersc": {
      "token_type": "globus",
      "token_path": "/home/example/.config/amscjobmanager/nersc-token.json"
    }
  },
  "transferapi_key_path": "/home/example/.config/amscjobmanager/transfer-token.json",
  "sandbox_directories": {
    "perlmutter": ["/global/cfs/cdirs/MY_PROJECT/example/workflows"],
    "local": ["/home/example/workflows"]
  },
  "globus_personal_endpoint": {
    "endpoint_uuid": "00000000-0000-0000-0000-000000000001",
    "staging_directory_globus": "/staging",
    "staging_directory_local": "/home/example/globus-staging"
  }
}
```

| Field | Required/default | Meaning |
| --- | --- | --- |
| `iri_implementations` | Required mapping | Providers to configure. An empty object permits local-only use. Provider tags are normalized to lowercase. |
| `sandbox_directories` | Required mapping | Machine names to lists of permitted filesystem roots. Include `local` for local writes and Globus downloads outside staging. |
| `transferapi_key_path` | Optional, `null` | Token file for IRI movement and Globus Transfer authentication. `null` disables transfer authentication; the first transfer request then fails with a configuration error. |
| `globus_personal_endpoint` | Optional, `null` | Personal collection and its two staging paths; needed by the blocking local file transfer helpers and certain provider fallbacks. |
| `iri_config_cache_directory` | Optional, `null` | Persistent discovery-cache directory. Required when discovering a provider from an API URL. |

Each `iri_implementations` value has:

| Field | Required/default | Meaning |
| --- | --- | --- |
| `token_path` | Required | Globus token cache or an existing AMSC PAT JSON file. |
| `token_type` | `"globus"` | `"globus"` or `"amsc_pat"`. |
| `api_url` | Optional | API root or Swagger URL used for discovery. Omit for built-in configuration unless deliberately using discovery. |
| `api_version` | Optional | Integer `1` or `2`; must agree with a registered provider definition. Specify it for unattended setup of an unknown provider. |

`globus_personal_endpoint` requires all three fields shown above. The staging
paths must refer to **the same physical directory**, one in the collection's
namespace and one accessible to the manager's OS. The configured staging root
grants permission to write there; it need not be a sandbox directory.

The workflow database filename, polling frequency, and workflow concurrency limit
are arguments to `JobManager`, not fields in this JSON file.

## Authentication

With `token_type="globus"`, the library loads a stored refresh token or performs
an interactive Globus login, then saves updated token information. Use writable,
private token-file locations with existing parent directories. The account must
also have facility access, an eligible allocation, and permission to use the
requested filesystem or collection. Authentication alone does not grant those
permissions.

With `token_type="amsc_pat"`, supply the JSON credential produced by the AMSC PAT
generator. It contains string fields `project`, `description`, `token_id`, and
`token`. The library validates and loads this file; it does not modify or refresh
it. Configure the same PAT for multiple providers only when that credential is
valid for each provider.

Compute/filesystem authentication and transfer authentication are separate. The
transfer token file stores credentials for both the IRI movement service and
the Globus Transfer service. Direct Globus credentials support transfer lookup
by label and cancellation. Collection data access may require additional Globus
consent; the transfer login requests scopes for configured collection routes.

Protect credential files and exclude them from version control. They contain
usable secrets.

## When setup performs requests

For built-in or cached providers, setup registers credentials and paths without
immediately authenticating every remote client. The first operation using a
provider authenticates and validates its pending sandbox directories. Local
sandboxes are checked during setup. A missing sandbox can prompt for permission
to create it; a failed validation remains pending.

Fresh discovery must authenticate to query a provider and may ask for machine
names and mount roots. Complete that setup interactively before starting an
unattended manager. A cached definition can be loaded without discovery prompts.

Sandbox entries for machines outside the configured providers are ignored. Check
the configured machine names rather than assuming that every JSON entry was
registered for use.

## Provider discovery

The built-in provider definitions are in
[`iri_api/config.py`](../src/amscjobmanager/iri_api/config.py). To configure another
provider, supply its tag, `api_url`, `api_version`, credentials, and a cache
directory. Fresh compute/filesystem discovery currently requires IRI API v2.
It discovers topology; queues and Globus filesystem routes are not discovered.
Transfers therefore require suitable route configuration in addition to an
IRI-discovered machine.

Discovery writes `<provider>.json` in the cache directory and registers the
result for the process. All configured cache entries are loaded before lazy
client setup. Invalid or mismatched caches raise errors rather than silently
being replaced.

To refresh a provider after setup:

```python
from amscjobmanager import iri_api

definition = iri_api.discoverImplementation("my_provider", refresh=True)
```

Review changes to machine names, filesystem mounts, and configured sandboxes
before continuing existing workflows.

## Local-only configuration

With the IRI selection, local execution needs no remote credentials:

```json
{
  "iri_implementations": {},
  "sandbox_directories": {"local": ["/home/example/workflows"]}
}
```

Pre-create the directory. See [implementations](implementations.md) for the
current limits of local execution and cancellation.
