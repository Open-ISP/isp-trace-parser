# Copyright (C) 2026 University of New South Wales
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.

from pathlib import Path


def input_directory(path: Path | str) -> Path:
    path = is_valid_path(path)
    if not path.is_dir():
        raise FileNotFoundError(path)
    return path


def parsed_directory(path: str | Path) -> Path:
    return is_valid_path(path)


def is_valid_path(path: str | Path) -> Path:
    try:
        return Path(path)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid parsed directory path: {path}")


def start_year_before_end_year(start_year: int, end_year: int) -> None:
    if end_year < start_year:
        raise ValueError(f"Start year {end_year} < end year {start_year}")
