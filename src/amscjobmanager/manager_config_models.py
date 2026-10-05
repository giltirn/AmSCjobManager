from . import globals
from pydantic import BaseModel, Field
from pathlib import Path

if globals.api_impl in ("SPOOF", "IRI"):
    class IRIImplementationConfig(BaseModel):
        token_path: str = Field(
            ...,
            description="Path to this implementation's IRI compute/filesystem token file",
        )


    class ManagerConfig(BaseModel):
        iri_implementations: dict[str, IRIImplementationConfig] = Field(
            ...,
            description="IRI compute/filesystem implementations to initialize",
        )
        transferapi_key_path: str = Field(..., description="The path to the Data Transfer API key (will be created if doesn't yet exist)")
        sandbox_directories: dict[str, str] = Field(..., description="A map of machine names to base sandbox directories")

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
                        token_path="/path/to/nersc-iri-token.json"
                    ),
                },
                transferapi_key_path="/path/to/iri-transfer-token.json",
                sandbox_directories={
                    "perlmutter": "/path/to/perlmutter-sandbox",
                    "local": "/path/to/local-sandbox",
                },
            )
            output_path.write_text(template.model_dump_json(indent=2) + "\n")
            return output_path
else:
    raise Exception("Unknown API implementation")
