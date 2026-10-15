from . import globals
from pydantic import BaseModel, Field
from pathlib import Path
from typing import Literal

if globals.api_impl in ("SPOOF", "IRI"):
    class IRIImplementationConfig(BaseModel):
        token_type: Literal["globus", "amsc_pat"] = Field(
            "globus",
            description="Authentication method for this implementation's token file",
        )
        token_path: str = Field(
            ...,
            description="Path to a Globus token file or an AMSC PAT JSON file",
        )

    class GlobusPersonalEndpointConfig(BaseModel):
        endpoint_uuid: str = Field(
            ...,
            description="UUID of the personal Globus endpoint",
        )
        staging_directory_globus: str = Field(
            ...,
            description="Staging directory path as seen by the Globus endpoint",
        )
        staging_directory_local: str = Field(
            ...,
            description="Staging directory path as seen by the manager host OS",
        )


    class ManagerConfig(BaseModel):
        iri_implementations: dict[str, IRIImplementationConfig] = Field(
            ...,
            description="IRI compute/filesystem implementations to initialize",
        )
        transferapi_key_path: str | None = Field(
            None,
            description="Optional Data Transfer API token path; omit to disable Globus transfer authentication",
        )
        sandbox_directories: dict[str, list[str]] = Field(
            ...,
            description="A map of machine names to base sandbox directories",
        )
        globus_personal_endpoint: GlobusPersonalEndpointConfig | None = Field(
            None,
            description="Optional personal Globus endpoint and its manager-accessible staging directory",
        )

        @classmethod
        def write_template_json(
            cls, output_path: str | Path = "manager-config-template.json"
        ) -> Path:
            """Write a non-destructive NERSC manager configuration template."""
            output_path = Path(output_path)
            if output_path.exists():
                return output_path

            template = cls(
                iri_implementations={
                    "nersc": IRIImplementationConfig(
                        token_type="globus",
                        token_path="/path/to/nersc-iri-token.json"
                    ),
                },
                transferapi_key_path="/path/to/iri-transfer-token.json",
                sandbox_directories={
                    "perlmutter": ["/path/to/perlmutter-sandbox"],
                    "local": ["/path/to/local-sandbox"],
                },
                globus_personal_endpoint=GlobusPersonalEndpointConfig(
                    endpoint_uuid="your-personal-globus-endpoint-uuid",
                    staging_directory_globus="/path/on/personal/endpoint",
                    staging_directory_local="/path/as/seen/by/manager",
                ),
            )
            output_path.write_text(template.model_dump_json(indent=2) + "\n")
            return output_path
else:
    raise Exception("Unknown API implementation")
