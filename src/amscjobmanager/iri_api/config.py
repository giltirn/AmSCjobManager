"""Packaged IRI defaults and the process-wide implementation registry.

User cache entries loaded during setup replace same-name packaged definitions.
"""

from importlib.resources import files

from .configuration_models import ImplementationCache

IRI_IMPLEMENTATIONS = {}
MACHINE_IMPLEMENTATIONS = {}


TRANSFER_API_BASE = "https://amsc-data-api.nersc.gov"


def registerImplementation(name: str, definition: dict) -> None:
    """Register configuration and update the machine index in place.

    Discovery supplies a validated definition. Existing imported references
    to these dictionaries continue to see the updated configuration.
    """
    version = definition["api_version"]
    if type(version) is not int or version not in {1, 2}:
        raise ValueError(f"Implementation {name!r} must have integer api_version 1 or 2")
    machines = definition["machines"]
    for machine in machines:
        owner = MACHINE_IMPLEMENTATIONS.get(machine)
        if owner is not None and owner != name:
            raise ValueError(
                f"Machine {machine!r} already belongs to implementation {owner!r}"
            )
    for machine in list(MACHINE_IMPLEMENTATIONS):
        if MACHINE_IMPLEMENTATIONS[machine] == name:
            del MACHINE_IMPLEMENTATIONS[machine]
    IRI_IMPLEMENTATIONS[name] = definition
    MACHINE_IMPLEMENTATIONS.update({machine: name for machine in machines})


def getImplementationAPIVersion(name: str) -> int:
    """Read the explicitly configured integer IRI version."""
    return IRI_IMPLEMENTATIONS[name]["api_version"]
# Package resources remain accessible when installed without a source checkout.
for _path in sorted(files(__package__).joinpath("implementations").iterdir(), key=lambda path: path.name):
    if _path.name.endswith(".json"):
        try:
            _configuration = ImplementationCache.fromFile(_path)
            registerImplementation(_configuration.implementation, _configuration.configuration.model_dump())
        except (ValueError, OSError) as exc:
            raise ValueError(f"Cannot load packaged IRI configuration {_path.name!r}: {exc}") from exc
