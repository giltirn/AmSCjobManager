"""Discover and cache IRI implementation configuration.

Compute systems, filesystems and mounts are discovered through the public
IRI resource-list query. Queue and Globus discovery are not implemented yet. Cache files
contain configuration and resource metadata, not authentication credentials.

Example:

    discoverImplementation(
        "esnet", "https://iri-dev.ppg.es.net/api/v2#/", "/path/to/iri-cache",
        api_version=2, token_path="/path/to/project-pat.json", token_type="amsc_pat",
    )

Use refresh=True to replace cached configuration after a fresh discovery.
A cache miss requires an authenticated implementation client, or token_path
and token_type arguments to create one. Cache hits do not authenticate.

Manager configuration can select this through an implementation entry with
api_url and the top-level iri_config_cache_directory setting. Unknown providers
prompt for their API URL and version if omitted. Cached definitions use the existing
IRI_IMPLEMENTATIONS layout, wrapped with schema_version, implementation,
api_base, configuration, and metadata fields. Only configuration is registered;
metadata preserves raw API responses and records which sections were discovered.
"""

import os
import re
import tempfile
from pathlib import Path, PurePosixPath
from typing import Any, Literal
from urllib.parse import quote, urljoin, urlsplit, urlunsplit

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .config import IRI_IMPLEMENTATIONS, registerImplementation


class DiscoveredMachine(BaseModel):
    model_config = ConfigDict(extra="allow")

    iriapi_group: str | None = None
    iriapi_resource_name: str | None = None
    iriapi_resource_id: str | None = None
    mounted_filesystems: list[str] = Field(default_factory=list)
    queues: list[tuple[str, str]] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_resource_reference(self):
        if not self.iriapi_resource_id and not self.iriapi_resource_name:
            raise ValueError("A discovered machine needs a resource ID or name")
        return self


class DiscoveredImplementation(BaseModel):
    """Configuration consumed by the existing resource and path routers."""

    model_config = ConfigDict(extra="allow")

    api_base: str
    api_version: int = Field(strict=True, ge=1, le=2)
    status_resources_supports_filters: bool
    machines: dict[str, DiscoveredMachine]
    filesystems: dict[str, dict[str, Any]] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_machine_mounts(self):
        for name, machine in self.machines.items():
            if name != name.lower() or name == "local":
                raise ValueError(f"Discovered machine name must be lowercase and cannot be 'local': {name!r}")
            unknown = set(machine.mounted_filesystems) - self.filesystems.keys()
            if unknown:
                raise ValueError(f"Discovered machine {name!r} references unknown filesystems: {sorted(unknown)}")
        return self


class ImplementationCache(BaseModel):
    """Keep operational configuration separate from discovery observations."""

    schema_version: Literal[1] = 1
    implementation: str
    api_base: str
    configuration: DiscoveredImplementation
    metadata: dict[str, Any] = Field(default_factory=dict)


def _discovery_location(name: str, api_url: str, cache_directory: str | Path):
    name = name.lower()
    if not re.fullmatch(r"[a-z][a-z0-9_-]*", name):
        raise ValueError("Implementation tags must start with a letter and contain only letters, digits, '-' or '_'")
    url = urlsplit(api_url)
    if url.scheme not in {"https", "http"} or not url.netloc or url.username or url.password or url.query:
        raise ValueError("IRI API URL must be an HTTP(S) URL without credentials or query parameters")
    # Swagger fragments such as '#/' do not form part of the REST API root.
    api_base = urlunsplit((url.scheme, url.netloc, url.path.rstrip("/"), "", ""))
    if not str(cache_directory).strip():
        raise ValueError("An IRI configuration cache directory is required")
    if "~" in str(cache_directory):
        raise ValueError("Cache paths containing '~' are not supported")
    return name, api_base, Path(cache_directory) / f"{name}.json"


def loadCachedImplementation(
    name: str, api_url: str | None = None, cache_directory: str | Path | None = None
) -> dict | None:
    """Load and register a cache entry, or return None if none exists.

    Invalid, mismatched, and unsupported-version entries fail explicitly;
    they do not silently replace working configuration or trigger network I/O.
    Defaults to the configured cache directory and registered API URL, or the
    cached API URL when loading a previously unknown provider.
    """
    if cache_directory is None:
        from .state import getConfigurationCacheDirectory

        cache_directory = getConfigurationCacheDirectory()
    name = name.lower()
    if not re.fullmatch(r"[a-z][a-z0-9_-]*", name):
        raise ValueError("Implementation tags must start with a letter and contain only letters, digits, '-' or '_'")
    if "~" in str(cache_directory):
        raise ValueError("Cache paths containing '~' are not supported")
    cache_path = Path(cache_directory) / f"{name}.json"
    if not cache_path.exists():
        return None
    cache = ImplementationCache.model_validate_json(cache_path.read_text(encoding="utf-8"))
    if api_url is None:
        api_url = IRI_IMPLEMENTATIONS.get(name, {}).get("api_base", cache.api_base)
    name, api_base, cache_path = _discovery_location(name, api_url, cache_directory)
    if cache.implementation != name or cache.api_base != api_base:
        raise ValueError(f"IRI configuration cache {str(cache_path)!r} belongs to a different implementation or API URL; use refresh=True to replace it")
    if cache.configuration.api_base.rstrip("/") != api_base:
        raise ValueError("Cached configuration API root does not match the requested API URL")
    definition = cache.configuration.model_dump()
    definition["api_base"] = api_base
    registerImplementation(name, definition)
    return definition


def loadCachedImplementations(cache_directory: str | Path | None = None) -> None:
    """Validate and register every provider JSON cache in the given directory.

    Defaults to the directory configured during workflow setup. A missing
    directory is an empty cache. Invalid entries fail explicitly
    with their filename; loading performs no authentication or API requests.
    """
    if cache_directory is None:
        from .state import getConfigurationCacheDirectory

        cache_directory = getConfigurationCacheDirectory()
    if "~" in str(cache_directory):
        raise ValueError("Cache paths containing '~' are not supported")
    for path in sorted(Path(cache_directory).glob("*.json")):
        try:
            cache = ImplementationCache.model_validate_json(path.read_text(encoding="utf-8"))
            name, api_base, expected_path = _discovery_location(
                cache.implementation, cache.api_base, cache_directory
            )
            if path != expected_path:
                raise ValueError("Cache filename must match its implementation tag")
            loadCachedImplementation(name, api_base, cache_directory)
        except (ValueError, OSError) as exc:
            raise ValueError(f"Cannot load IRI configuration cache {str(path)!r}: {exc}") from exc


def _linked_resources(resource: dict, relation: str, inventory: dict) -> list[dict]:
    """Resolve advertised topology links against the queried resource inventory."""
    links = resource.get("_links", {}).get(relation, [])
    if isinstance(links, dict):
        links = [links]
    linked = []
    for link in links:
        href = urljoin(resource.get("self_uri", ""), link["href"])
        if href not in inventory:
            raise ValueError(f"Resource {resource['id']!r} links to an unknown resource through {relation}: {href}")
        linked.append(inventory[href])
    return linked


def _mount_path_root(mount_path: str) -> str:
    """Ask the user to confirm an inferred routing root or supply one.

    Whole-component {user_id} and {project_id} placeholders delimit the fixed
    POSIX prefix. Literal absolute paths can be used in full. Unrecognized
    syntax requires a user-supplied root instead of a guessed mapping.
    The selected root is persisted with the discovered configuration.
    """
    path = PurePosixPath(mount_path)
    root = []
    templated = False
    understood = path.is_absolute() and ".." not in path.parts and "~" not in mount_path
    for part in path.parts:
        if "{" in part or "}" in part:
            if part not in {"{user_id}", "{project_id}"}:
                understood = False
            templated = True
        if not templated:
            root.append(part)
    if templated and len(root) <= 1:
        understood = False
    try:
        if understood:
            inferred = str(PurePosixPath(*root))
            while True:
                answer = input(f"Mount path {mount_path!r}: use filesystem root {inferred!r}? [y/n]: ").strip().lower()
                if answer in {"y", "yes"}:
                    return inferred
                if answer in {"n", "no"}:
                    break
                print("Please answer yes or no.")
        else:
            print(f"Could not infer a filesystem root from mount path {mount_path!r}.")
        while True:
            supplied = input(f"Filesystem root for mount path {mount_path!r}: ").strip()
            path = PurePosixPath(supplied)
            if path.is_absolute() and ".." not in path.parts and "{" not in supplied and "}" not in supplied and "~" not in supplied:
                return str(path)
            print("Please provide an absolute POSIX root without placeholders, '~', or '..'.")
    except EOFError as exc:
        raise ValueError(
            f"Discovering mount path {mount_path!r} requires interactive root confirmation; "
            "run discovery interactively first to cache the selected root"
        ) from exc


def _query_implementation_configuration(name: str, api_base: str) -> tuple[dict, dict]:
    """Return operational configuration and separate discovery metadata."""
    # Runtime import avoids a cycle while state.py initializes the package.
    from .resources import getResources

    resources = getResources(implementation=name)
    machines = {}
    for resource in resources:
        resource_type = resource["resource_type"]
        if not (
            resource_type in {"urn:doe-iri:resource:compute", "urn:doe-iri:resource:compute:system"}
            or resource_type.startswith("urn:doe-iri:resource:compute:system:")
        ):
            continue
        machine = _machine_name(name, resource)
        if machine in machines:
            raise ValueError(
                f"Compute resources {machines[machine]['iriapi_resource_id']!r} "
                f"and {resource['id']!r} both map to machine {machine!r}"
            )
        machines[machine] = {
            "iriapi_group": resource.get("group"),
            "iriapi_resource_name": resource.get("name"),
            "iriapi_resource_id": resource["id"],
            "mounted_filesystems": [],
        }
    # Keep each mount presentation separate: one filesystem may appear at
    # different roots on different machines, without making those roots aliases.
    inventory = {}
    for resource in resources:
        uri = f"{api_base}/status/resources/{quote(resource['id'], safe='')}"
        inventory[uri] = resource
        if resource.get("self_uri"):
            inventory[resource["self_uri"]] = resource
    filesystems = {}
    for storage in resources:
        if not storage["resource_type"].startswith("urn:doe-iri:resource:storage:system"):
            continue
        for filesystem in _linked_resources(storage, "iri:provides-filesystem", inventory):
            for mount in _linked_resources(filesystem, "iri:has-mount", inventory):
                root = _mount_path_root(mount["attributes"]["mount_path"])
                mount_name = mount["id"]
                definition = {
                    "description": mount.get("description") or filesystem.get("description") or mount.get("name") or mount_name,
                    "storage_resource_paths": [{
                        "filesystem_roots": [root],
                        "resource_name": storage.get("name"),
                        "resource_id": storage["id"],
                    }],
                    "globus_filesystems": [],
                }
                if mount_name in filesystems and filesystems[mount_name] != definition:
                    raise ValueError(f"Mount {mount_name!r} has multiple backing storage resources")
                filesystems[mount_name] = definition
                for compute in _linked_resources(mount, "iri:mounted-on", inventory):
                    for machine in machines.values():
                        if machine["iriapi_resource_id"] == compute["id"] and mount_name not in machine["mounted_filesystems"]:
                            machine["mounted_filesystems"].append(mount_name)
    configuration = {
        **IRI_IMPLEMENTATIONS[name],
        "api_base": api_base,
        "api_version": 2,
        "status_resources_supports_filters": IRI_IMPLEMENTATIONS[name].get("status_resources_supports_filters", True),
        "machines": machines,
        "filesystems": filesystems,
    }
    configuration.pop("discovered_sections", None)
    return configuration, {"discovered_sections": ["compute", "filesystems"], "resources": resources}


def _machine_name(implementation: str, resource: dict) -> str:
    """Confirm a provider-prefixed machine name, or accept a user replacement.

    Suggested names use the resource name (falling back to its ID), lowercase
    letters, and underscores instead of whitespace. Custom names need not keep
    the suggested prefix, but must be lowercase, space-free and not 'local'.
    """
    label = resource.get("name") or resource["id"]
    suggested = f"{implementation}_{'_'.join(label.lower().split())}"
    try:
        if re.fullmatch(r"[a-z0-9][a-z0-9_-]*", suggested):
            while True:
                answer = input(f"Compute resource {label!r} ({resource['id']}): use machine name {suggested!r}? [y/n]: ").strip().lower()
                if answer in {"y", "yes"}:
                    return suggested
                if answer in {"n", "no"}:
                    break
                print("Please answer yes or no.")
        else:
            print(f"Could not derive a valid machine name from compute resource {label!r}.")
        while True:
            supplied = input(f"Machine name for compute resource {label!r} ({resource['id']}): ").strip()
            if supplied != "local" and re.fullmatch(r"[a-z0-9][a-z0-9_-]*", supplied):
                return supplied
            print("Use lowercase letters, digits, underscores or hyphens, starting with a letter or digit; 'local' is reserved.")
    except EOFError as exc:
        raise ValueError(
            f"Discovering compute resource {label!r} requires interactive machine-name confirmation; "
            "run discovery interactively first to cache the selected name"
        ) from exc


def discoverImplementation(
    name: str, api_url: str | None = None, cache_directory: str | Path | None = None, *, refresh: bool = False,
    token_path: str | None = None, token_type: str = "globus",
    api_version: int | None = None,
) -> dict:
    """Load cached configuration or discover and register v2 compute and filesystems.

    After setup, only the implementation name is needed: use its registered
    API definition, configured cache directory and lazily acquired client.
    Explicit connection settings support bootstrapping a provider before setup.
    Cached configuration can be loaded without credentials. This
    discovers compute and filesystem topology, but not queues or Globus routes.
    Mount roots use the provisional {user_id}/{project_id} convention.
    Fresh discovery asks the user to confirm or supply each mount's root;
    machine names are also confirmed, with defaults prefixed by the provider
    tag and whitespace replaced by underscores. Loading cached configuration
    does not prompt again.
    Pass api_url=None to use a registered API root or prompt for an unknown
    provider. Unknown providers also prompt for api_version when omitted.
    """
    if cache_directory is None:
        from .state import getConfigurationCacheDirectory

        cache_directory = getConfigurationCacheDirectory()
    name = name.lower()
    previous = IRI_IMPLEMENTATIONS.get(name)
    try:
        if api_url is None:
            api_url = previous["api_base"] if previous else input(f"IRI API URL for {name!r}: ").strip()
        if api_version is None:
            api_version = previous["api_version"] if previous else int(input(f"IRI API version for {name!r} (1 or 2): ").strip())
    except EOFError as exc:
        raise ValueError(
            f"Unknown IRI provider {name!r}: specify api_url and api_version "
            "in its configuration when interactive input is unavailable"
        ) from exc
    if type(api_version) is not int or api_version not in {1, 2}:
        raise ValueError("IRI API version must be 1 or 2")
    name, api_base, cache_path = _discovery_location(name, api_url, cache_directory)
    if previous is not None and api_version != previous["api_version"]:
        raise ValueError(f"API version for {name!r} conflicts with its registered configuration")
    if not refresh:
        cached = loadCachedImplementation(name, api_base, cache_directory)
        if cached is not None:
            return cached

    if api_version != 2:
        raise NotImplementedError(f"Compute discovery requires IRI API v2, not {name!r}'s v1 API")
    # Register only the API root for authentication and implementation-level
    # queries. No machine mapping is needed to bootstrap discovery.
    bootstrap_required = previous is None or previous["api_base"] != api_base
    if bootstrap_required:
        registerImplementation(name, {
            **(previous or {}), "api_base": api_base, "api_version": api_version,
            "machines": (previous or {}).get("machines", {}),
        })
    try:
        if token_path is not None:
            from .state import setupImplementationClient

            setupImplementationClient(name, token_path, token_type)
        definition, metadata = _query_implementation_configuration(name, api_base)
        configuration = DiscoveredImplementation.model_validate(definition)
    except Exception:
        if bootstrap_required and previous is not None:
            registerImplementation(name, previous)
        raise
    if configuration.api_base.rstrip("/") != api_base:
        raise ValueError("Discovered configuration API root does not match the requested API URL")
    definition = configuration.model_dump()
    definition["api_base"] = api_base
    cache = ImplementationCache(
        implementation=name, api_base=api_base, configuration=configuration,
        metadata=metadata,
    )
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=cache_path.parent,
            prefix=f".{name}-", suffix=".tmp", delete=False,
        ) as handle:
            temporary_path = Path(handle.name)
            handle.write(cache.model_dump_json(indent=2) + "\n")
        os.replace(temporary_path, cache_path)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
    registerImplementation(name, definition)
    return definition


def configureImplementation(
    name: str, settings, cache_directory: str | Path | None, *, cache_only: bool = False
) -> None:
    """Prepare built-in or user-defined implementations before client setup."""
    name = name.lower()
    api_url = getattr(settings, "api_url", None) if not isinstance(settings, dict) else settings.get("api_url")
    api_version = getattr(settings, "api_version", None) if not isinstance(settings, dict) else settings.get("api_version")
    existing = IRI_IMPLEMENTATIONS.get(name, {})
    if existing and api_version is not None and api_version != existing["api_version"]:
        raise ValueError(f"API version for {name!r} conflicts with its registered configuration")
    if api_url is not None or not existing:
        if cache_directory is None:
            raise ValueError(f"Implementation {name!r} requires iri_config_cache_directory")
        if cache_only:
            if api_url is None:
                raise ValueError(f"No cached configuration loaded for {name!r}; run discovery first")
            if loadCachedImplementation(name, api_url, cache_directory) is None:
                raise ValueError(f"No cached configuration exists for {name!r}; run discovery first")
        else:
            token_path = settings.get("token_path") if isinstance(settings, dict) else settings.token_path
            token_type = settings.get("token_type", "globus") if isinstance(settings, dict) else settings.token_type
            discoverImplementation(
                name, api_url, cache_directory,
                token_path=token_path, token_type=token_type,
                api_version=api_version,
            )
