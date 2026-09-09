# Copyright (C) 2026 University of New South Wales
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.

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
