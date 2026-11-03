r"""Discover one IRI provider using credentials from a manager configuration.

    python utilities/discover_iri.py manager-config.json olcf \
        --api-url https://amsc-moderate.s3m.olcf.ornl.gov/api/v2 \
        --api-version 2 --refresh

The selected iri_implementations entry supplies token_path and token_type.
API URL/version and cache directory default to configuration values, with the
discovery API's existing registered-provider defaults or interactive prompts
when omitted. Cached discovery is reused unless --refresh is supplied. Fresh
discovery may prompt for machine names and filesystem roots; no jobs are run.
"""

import argparse
import json
from pathlib import Path


def main(argv=None) -> int:
    """Discover the selected configured provider and print its definition.

    argv optionally supplies CLI arguments. Read credentials from the selected
    provider only, apply CLI connection/cache overrides without rewriting the
    configuration, and return zero after discovery or cache loading succeeds.
    Argument errors exit through argparse; API/authentication failures propagate.
    """
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("config", help="Manager configuration JSON file")
    parser.add_argument("implementation", type=str.lower, help="Configured provider tag, e.g. olcf")
    parser.add_argument("--api-url", help="Override the configured IRI API root or Swagger URL")
    parser.add_argument("--api-version", type=int, choices=(1, 2), help="Override the configured API version; fresh discovery requires v2")
    parser.add_argument("--cache-directory", help="Override iri_config_cache_directory")
    parser.add_argument("--refresh", action="store_true", help="Query the provider even when a cached definition exists")
    args = parser.parse_args(argv)

    # Keep --help and module import free of backend initialization and requests.
    from amscjobmanager.iri_api.discovery import discoverImplementation
    from amscjobmanager.manager_config import readManagerConfigFile

    config = readManagerConfigFile(args.config)
    providers = {name.lower(): settings for name, settings in config.iri_implementations.items()}
    if args.implementation not in providers:
        parser.error(f"Implementation {args.implementation!r} is not configured in {args.config}")
    cache_directory = args.cache_directory or config.iri_config_cache_directory
    if not cache_directory:
        parser.error("Set iri_config_cache_directory in the configuration or supply --cache-directory")
    settings = providers[args.implementation]
    definition = discoverImplementation(
        args.implementation,
        api_url=args.api_url if args.api_url is not None else settings.api_url,
        api_version=args.api_version if args.api_version is not None else settings.api_version,
        cache_directory=cache_directory,
        token_path=settings.token_path,
        token_type=settings.token_type,
        refresh=args.refresh,
    )
    print(json.dumps(definition, indent=2))
    print(f"Configuration cache: {Path(cache_directory).resolve() / (args.implementation + '.json')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
