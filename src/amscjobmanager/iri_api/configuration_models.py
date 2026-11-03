"""Shared schema for packaged and user-cached IRI implementation definitions."""

import re
from typing import Any, Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, model_validator


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

    @model_validator(mode="after")
    def _validate_identity(self):
        """Require a valid provider tag and matching HTTP API roots in both sections."""
        if not re.fullmatch(r"[a-z][a-z0-9_-]*", self.implementation):
            raise ValueError("Implementation tags must start with a letter and contain only lowercase letters, digits, '-' or '_'")
        url = urlsplit(self.api_base)
        if url.scheme not in {"http", "https"} or not url.netloc or url.username or url.password or url.query or url.fragment:
            raise ValueError("IRI API root must be an HTTP(S) URL without credentials, query parameters, or fragments")
        if self.configuration.api_base.rstrip("/") != self.api_base.rstrip("/"):
            raise ValueError("Configuration API root does not match the envelope API URL")
        return self

    @classmethod
    def fromFile(cls, path):
        """Read and validate a Path or package resource, returning its definition.

        The JSON filename must match its implementation tag. Invalid JSON,
        schema violations, and read errors propagate to the calling loader.
        """
        configuration = cls.model_validate_json(path.read_text(encoding="utf-8"))
        if path.name != f"{configuration.implementation}.json":
            raise ValueError("Configuration filename must match its implementation tag")
        return configuration


