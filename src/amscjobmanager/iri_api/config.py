"""Built-in IRI implementation, machine, and endpoint configuration."""

IRI_IMPLEMENTATIONS = {
    "nersc": {
        "api_version": 1,
        "resource_server": "ed3e577d-f7f3-4639-b96e-ff5a8445d699",
        "api_base": "https://api.iri.nersc.gov/api/v1",
        "status_resources_supports_filters": True,
        "storage_iriapi_group": "perlmutter",
        "storage_resource_type": "compute",
        "filesystems": {
            "perlmutter_login_filesystem": {
                "description": "Perlmutter filesystem exposed through its login resource",
                "storage_resource_paths": [
                    {"filesystem_roots": ["/"], "resource_name": "login"},
                ],
                "globus_filesystems": [
                    {
                        "filesystem_roots": ["/"],
                        "endpoint": "dtn",
                        "globus_root": "/",
                    },
                ],
            },
        },
        "required_scopes": {
            "https://auth.globus.org/scopes/ed3e577d-f7f3-4639-b96e-ff5a8445d699/iri_api",
        },
        "machines": {
            "perlmutter": {
                "iriapi_group": "perlmutter",
                "iriapi_resource_name": "compute",
                "launcher": "srun",
                "mounted_filesystems": ["perlmutter_login_filesystem"],
                "queues": [
                    ("debug", "max time 0.5 hours, max nodes 8"),
                    ("regular", "use for standard, production jobs or those too large for debug"),
                ],
            },
            "perlmutter_login": {
                "iriapi_group": "perlmutter",
                "iriapi_resource_name": "login",
                "mounted_filesystems": ["perlmutter_login_filesystem"],
                "queues": [],
            },
        },
    },
    "alcf": {
        "api_version": 1,
        "resource_server": "6be511f6-a071-471f-9bc0-02a0d0836723",
        "api_base": "https://api.alcf.anl.gov/api/v1",
        "status_resources_supports_filters": False,
        "storage_iriapi_group": "storages",
        "storage_resource_type": "storage",
        "unsupported_filesystem_operations": {
            #not implemented by API but can circumvent using Globus
            "uploadBytes": "globus",
            "downloadFile": "globus",
            "downloadFileContents": "globus",            
            "pathStat": "skip", #no clear workaround
        },
        "filesystems": {
            "alcf_home": {
                "description": (
                    "ALCF home filesystem mounted by Polaris, Sophia, and Crux; "
                    "Aurora has a separate /home that IRI and Globus do not expose"
                ),
                "storage_resource_paths": [
                    {"filesystem_roots": ["/home"], "resource_name": "Home"},
                ],
                "globus_filesystems": [
                    {
                        "filesystem_roots": ["/home"],
                        "endpoint": "9032dd3a-e841-4687-a163-2720da731b5b",
                        "globus_root": "/",
                        "requires_data_access": True,
                    },
                ],
            },
            "alcf_flare": {
                "description": "ALCF Flare project filesystem",
                "storage_resource_paths": [
                    {
                        "filesystem_roots": ["/flare", "/lus/flare"],
                        "resource_name": "Flare",
                    },
                ],
                "globus_filesystems": [
                    {
                        "filesystem_roots": ["/lus/flare/projects", "/flare"],
                        "endpoint": "f39a7a0f-5bfc-46ce-9615-ba9f8592814f",
                        "globus_root": "/",
                        "requires_data_access": True,
                    },
                ],
            },
            "alcf_eagle": {
                "description": "ALCF Eagle project filesystem",
                "storage_resource_paths": [
                    {
                        "filesystem_roots": ["/eagle", "/lus/eagle"],
                        "resource_name": "Eagle",
                    },
                ],
                "globus_filesystems": [
                    {
                        "filesystem_roots": ["/lus/eagle/projects", "/eagle"],
                        "endpoint": "05d2c76a-e867-4f67-aa57-76edeb0beda0",
                        "globus_root": "/",
                        "requires_data_access": True,
                    },
                ],
            },
        },
        "required_scopes": {
            "https://auth.globus.org/scopes/6be511f6-a071-471f-9bc0-02a0d0836723/filesystem",
        },
        # ALCF requires the session policy used by its IRI authorization flow.
        "session_policy": "a128e981-c9a5-417a-97ab-8571c9831bff",
        "machines": {
            "polaris": {
                "iriapi_group": "computes",
                "iriapi_resource_name": "Polaris",
                "custom_attributes": {"filesystems": "home:eagle"},
                "mounted_filesystems": ["alcf_home", "alcf_eagle"],
                "queues": [
                    ("debug", "1-2 nodes, 5 minutes-1 hour; max 24 nodes in use"),
                    ("debug-scaling", "1-10 nodes, 5 minutes-1 hour; max one job per user"),
                    ("prod", "10-496 nodes, 5 minutes-24 hours; routing queue"),
                    ("preemptable", "1-10 nodes, 5 minutes-72 hours; may be killed for demand jobs; max 20 jobs per project"),
                    ("demand", "1-56 nodes, 5 minutes-1 hour; by request only; max 100 jobs per project"),
                    ("capacity", "1-4 nodes, 5 minutes-7 days; max 32 nodes and one running job per user"),
                ],
            },
            "aurora": {
                "iriapi_group": "computes",
                "iriapi_resource_name": "Aurora",
                "custom_attributes": {"filesystems": "flare"},
                "mounted_filesystems": ["alcf_flare"],
                "queues": [
                    ("debug", "1-2 nodes, 5 minutes-1 hour; max one job per user"),
                    ("debug-scaling", "2-256 nodes, 5 minutes-1 hour; max one job per user"),
                    ("prod", "256-10,624 nodes, 5 minutes-24 hours; routing queue"),
                    ("capacity", "1-16 nodes, 5 minutes-7 days; max 512 nodes and two running jobs per user"),
                ],
            },
        },
    },
}

MACHINE_IMPLEMENTATIONS = {
    machine: implementation
    for implementation, specification in IRI_IMPLEMENTATIONS.items()
    for machine in specification["machines"]
}


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


