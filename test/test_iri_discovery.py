"""Discover configuration for one selected IRI implementation.

Run manually with a manager configuration and implementation tag:

    python3 test/test_iri_discovery.py path/to/manager-config.json esnet
    python3 test/test_iri_discovery.py path/to/manager-config.json esnet --refresh

The configuration must specify iri_config_cache_directory. The implementation's
api_url overrides its cached or built-in API root. Unknown providers prompt
for the URL and version unless these are specified in the configuration.
Discovery results are printed and
checked for registration and a successful cache reload. Other implementations,
sandboxes, transfers, and compute jobs are not initialized or exercised.

On a cache miss or --refresh, authenticate only the selected implementation
and enumerate its compute systems and filesystem mounts. Queue and Globus
discovery are pending. Fresh discovery asks the user to confirm inferred
filesystem roots or provide replacements; cached roots are reused without prompts.
Compute machine names default to implementation-prefixed lowercase names with
underscores replacing spaces, and also require confirmation or a replacement.
"""

import argparse
import json
from pathlib import Path
import sys

from amscjobmanager.iri_api.config import IRI_IMPLEMENTATIONS, MACHINE_IMPLEMENTATIONS
from amscjobmanager.iri_api.discovery import discoverImplementation, loadCachedImplementation, loadCachedImplementations
from amscjobmanager.manager_config import readManagerConfigFile


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", help="Manager configuration JSON file")
    parser.add_argument("implementation", type=str.lower, help="Configured implementation tag, e.g. esnet")
    parser.add_argument("--refresh", action="store_true", help="Run fresh discovery even if cached configuration exists")
    args = parser.parse_args()
    config = readManagerConfigFile(args.config)
    settings = {name.lower(): entry for name, entry in config.iri_implementations.items()}
    if args.implementation not in settings:
        parser.error(f"Implementation {args.implementation!r} is not configured")
    if not config.iri_config_cache_directory:
        parser.error("Configuration must specify iri_config_cache_directory")
    loadCachedImplementations(config.iri_config_cache_directory)
    api_url = settings[args.implementation].api_url or IRI_IMPLEMENTATIONS.get(
        args.implementation, {}
    ).get("api_base")
    print(f"Discovering {args.implementation}")
    print(f"Refresh requested: {args.refresh}")
    try:
        definition = discoverImplementation(
            args.implementation, api_url, config.iri_config_cache_directory,
            refresh=args.refresh,
            token_path=settings[args.implementation].token_path,
            token_type=settings[args.implementation].token_type,
            api_version=settings[args.implementation].api_version,
        )
    except NotImplementedError as exc:
        print(f"Discovery not completed: {exc}", file=sys.stderr)
        return 1

    if IRI_IMPLEMENTATIONS.get(args.implementation) != definition:
        raise RuntimeError("Discovered configuration was not registered")
    for machine in definition["machines"]:
        if MACHINE_IMPLEMENTATIONS.get(machine) != args.implementation:
            raise RuntimeError(f"Discovered machine {machine!r} was not registered")
    cached = loadCachedImplementation(
        args.implementation, definition["api_base"], config.iri_config_cache_directory
    )
    if cached != definition:
        raise RuntimeError("Cached configuration does not match the discovery result")

    print(json.dumps(definition, indent=2))
    print(f"Discovered {len(definition['machines'])} compute system(s) and {len(definition['filesystems'])} filesystem mount(s); queue and Globus discovery are pending.")
    cache_path = Path(config.iri_config_cache_directory).expanduser() / f"{args.implementation}.json"
    print(f"passed: {args.implementation}: registration and cache reload verified ({cache_path})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
