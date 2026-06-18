from pathlib import Path

from isp_trace_parser import mappings


def build(
    files: list[Path],
    version: str,
) -> dict[Path, dict[str, str | int]]:
    """Build metadata for demand files by lookup in the demand mapping.

    The demand YAML is option-keyed, so `_expand_lookup` first expands the
    dimensions into a stem-keyed dict; each filename then decomposes
    into (subregion, year, scenario_poe_demand_type) for a single lookup.
    """
    lookup = _expand_lookup(version)

    file_metadata: dict[Path, dict[str, str | int]] = {}
    for path in files:
        subregion, sep, after = path.stem.partition("_RefYear_")
        if not sep:
            raise ValueError(f"Unexpected trace filename: {path.name}")
        year_str, _, rest = after.partition("_")
        key = f"{subregion}_{rest}"
        if not year_str.isdigit() or not rest or key not in lookup:
            raise ValueError(f"Unexpected trace filename: {path.name}")
        file_metadata[path] = {**lookup[key], "reference_year": int(year_str)}
    return file_metadata


def _expand_lookup(version: str) -> dict[str, dict[str, str]]:
    """Expand the demand dimensions into a year-agnostic stem-keyed dict.

    Keyed by `<subregion>_<scenario>_<poe>_<demand_type>` (i.e. the stem
    with `_RefYear_<year>_` removed). `reference_year` is added by `build`.
    """
    demand = mappings.load("demand", version=version)
    topography = mappings.load("topography", version=version)

    lookup: dict[str, dict[str, str]] = {}
    for subregion in topography["subregions"]:
        for scenario in demand["scenarios"].keys():
            for poe in demand["poe_levels"]:
                for demand_type in demand["demand_types"]:
                    key = f"{subregion}_{scenario}_{poe}_{demand_type}"
                    lookup[key] = {
                        "subregion": subregion,
                        "scenario": scenario,
                        "poe": poe,
                        "demand_type": demand_type,
                    }
    return lookup
