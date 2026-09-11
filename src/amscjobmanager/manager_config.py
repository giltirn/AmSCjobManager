from pathlib import Path
from pydantic import BaseModel, Field, ValidationError
from .api_general import setupWorkflowAgent
from . import globals
from .manager_config_models import ManagerConfig

if globals.api_impl in ("SPOOF", "IRI"):
    def setupManager(config : ManagerConfig):
        setupWorkflowAgent(config.iriapi_key_path, config.transferapi_key_path, config.sandbox_directories)
else:
    raise Exception("Unknown API implementation")
   
def readManagerConfigFile(filename):
    try:
        config = ManagerConfig.model_validate_json(Path(filename).read_text())
    except ValidationError as e:
        raise Exception(f"Could not parse manager config {filename}: {e}")
    return config
