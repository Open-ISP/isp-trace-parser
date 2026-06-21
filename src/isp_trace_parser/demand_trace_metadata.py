from pathlib import Path

from isp_trace_parser import mappings


def build(
    files: list[Path],
    version: str,
) -> dict[Path, dict[str, str | int]]:
    """Build metadata for demand files by lookup in the demand mapping.

    The demand YAML is option-keyed, so `_expand_lookup` first expands the
    dimensions into a `(location_prefix, dimensions_suffix)`-keyed dict;
    each filename then decomposes into those two literal slices (either
    side of `_RefYear_<year>_`) for a single lookup.
    """
    lookup = _expand_lookup(version)

    file_metadata: dict[Path, dict[str, str | int]] = {}
    for path in files:
        location_prefix, _, after = path.stem.partition("_RefYear_")
        year, _, dimensions_suffix = after.partition("_")
        key = (location_prefix, dimensions_suffix)
        if not year.isdigit() or key not in lookup:
            raise ValueError(f"Unexpected trace filename: {path.name}")
        file_metadata[path] = {**lookup[key], "reference_year": int(year)}
    return file_metadata


def _expand_lookup(version: str) -> dict[tuple[str, str], dict[str, str]]:
    """Expand the demand dimensions into a year-agnostic lookup.

    Keyed by `(location_prefix, dimensions_suffix)` — the two literal
    slices of the filename either side of `_RefYear_<year>_`. For 2024,
    `location_prefix` is the subregion and `dimensions_suffix` is
    `<scenario>_<poe>_<demand_type>`. `reference_year` is added by `build`.
    """
    demand = mappings.load("demand", version=version)
    topography = mappings.load("topography", version=version)

    lookup: dict[tuple[str, str], dict[str, str]] = {}
    for subregion in topography["subregions"]:
        for scenario in demand["scenarios"]:
            for poe in demand["poe_levels"]:
                for demand_type in demand["demand_types"]:
                    key = (subregion, f"{scenario}_{poe}_{demand_type}")
                    lookup[key] = {
                        "subregion": subregion,
                        "scenario": scenario,
                        "poe": poe,
                        "demand_type": demand_type,
                    }
    return lookup
