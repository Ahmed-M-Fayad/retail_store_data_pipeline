from pathlib import Path
import yaml
import os

def get_project_root() -> Path:
    """Returns the project root directory."""
    return Path(__file__).parent.parent.parent

def get_config_path() -> Path:
    return get_project_root() / "config" / "pipeline_config.yaml"

def get_raw_data_dir() -> Path:
    return get_project_root() / "data" / "raw"

def get_processed_data_dir() -> Path:
    return get_project_root() / "data" / "processed"

def load_config():
    config_path = get_config_path()
    if not config_path.exists():
        return None
    with open(config_path, "r") as f:
        return yaml.safe_load(f)
