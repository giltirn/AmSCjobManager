"""Initialize credentials for every configured IRI implementation.

Run manually:

    python3 test/test_iri_authorization.py path/to/manager-config.json

Globus credentials are refreshed or obtained interactively as usual. AMSC PAT
files must contain string fields project, description, token_id, and token.
PAT files are validated and loaded without modification or Globus login.

No IRI API calls, sandbox checks, or transfer-service login are performed.
For PATs this verifies local credential setup, not server acceptance or expiry;
that requires a protected API request such as the existing smoke test.
"""

import argparse

from amscjobmanager.iri_api.auth import setupIRIapiCompute
from amscjobmanager.iri_api.config import IRI_IMPLEMENTATIONS
from amscjobmanager.iri_api.discovery import configureImplementation, loadCachedImplementations
from amscjobmanager.manager_config import readManagerConfigFile


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", help="Manager configuration JSON file")
    args = parser.parse_args()
    config = readManagerConfigFile(args.config)
    if not config.iri_implementations:
        parser.error("No IRI implementations are configured")

    failures = []
    if config.iri_config_cache_directory is not None:
        loadCachedImplementations(config.iri_config_cache_directory)
    for implementation, settings in config.iri_implementations.items():
        implementation = implementation.lower()
        try:
            if implementation not in IRI_IMPLEMENTATIONS:
                configureImplementation(
                    implementation, settings, config.iri_config_cache_directory,
                    cache_only=True,
                )
            client = setupIRIapiCompute(
                implementation, settings.token_path, token_type=settings.token_type
            )
            client.close()
        except Exception as exc:
            # Avoid printing provider exceptions that could include credentials.
            print(f"FAILED: {implementation} ({settings.token_type}): {type(exc).__name__}")
            failures.append(implementation)
        else:
            result = (
                "PAT file validated and loaded"
                if settings.token_type == "amsc_pat"
                else "Globus authorization completed"
            )
            print(f"passed: {implementation}: {result}")

    if failures:
        print("Authorization setup failed for: " + ", ".join(failures))
        return 1
    print("Credential setup passed for all implementations; no IRI API calls made.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
