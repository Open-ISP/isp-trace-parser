from importlib.resources import files

import yaml


def load(name: str, version: str = "2024") -> dict:
    """Load a mapping YAML bundled as package data.

    Args:
        name: Mapping file stem (e.g. ``"resources"``).
        version: ISP version subdirectory (e.g. ``"2024"``).

    Returns:
        Parsed YAML contents.
    """
    resource = files(__package__).joinpath(version, f"{name}.yaml")
    with resource.open("r") as f:
        return yaml.safe_load(f)
