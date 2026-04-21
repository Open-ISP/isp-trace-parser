from importlib.resources import files

import yaml


def load(name: str) -> dict:
    """Load a mapping YAML bundled as package data.

    Args:
        name: Mapping file stem (e.g. ``"solar_project_mapping"``).

    Returns:
        Parsed YAML contents.
    """
    resource = files(__package__).joinpath(f"{name}.yaml")
    with resource.open("r") as f:
        return yaml.safe_load(f)
