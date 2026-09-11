from . import globals
from pydantic import BaseModel, Field

if globals.api_impl in ("SPOOF", "IRI"):
    #For convenience we use separate files for the IRI and data transfer tokens, as the latter will eventually no longer be needed
    class ManagerConfig(BaseModel):
        iriapi_key_path: str = Field(..., description="The path to the IRI API key (will be created if doesn't yet exist)")
        transferapi_key_path: str = Field(..., description="The path to the Data Transfer API key (will be created if doesn't yet exist)")
        sandbox_directories: dict[str, str] = Field(..., description="A map of machine names to base sandbox directories")
else:
    raise Exception("Unknown API implementation")

