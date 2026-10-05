"""Create a starter manager configuration in the current directory."""

from amscjobmanager.manager_config_models import ManagerConfig


template_path = ManagerConfig.write_template_json()
print(template_path)
