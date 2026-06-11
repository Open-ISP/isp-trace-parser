from pathlib import Path

from isp_trace_parser import mappings

# Temporrary mapping that translates the YAML's resource_type vocabulary to the legacy
# short codes still used downstream  for now (filters, parquet columns, output filenames).

_RESOURCE_TYPE_CODES: dict[str, str] = {
    "solar_sat": "SAT",
    "solar_ffp": "FFP",
    "solar_cst": "CST",
    "wind": "wind",  # lowercase to match WindMetadataFilter Literal
    "wind_high": "WH",
    "wind_medium": "WM",
    "wind_offshore_fixed": "WFX",
    "wind_offshore_floating": "WFL",
}


def resource_file_metadata(
    files: list[Path],
    version: str,
) -> dict[Path, dict[str, str]]:
    """Build metadata for resource files by lookup in the resource mapping.

    The mapping key is the trace stem — the filename with `_RefYear<year>.csv`
    stripped — so `<stem>_RefYear<year>.csv` decomposes back to (stem, year).
    """

    resource_mapping = mappings.load("resources", version=version)

    file_metadata: dict[Path, dict[str, str]] = {}
    for path in files:
        stem, sep, ref = path.stem.rpartition("_RefYear")
        entry = resource_mapping[stem]
        file_metadata[path] = {
            "name": entry["location"],
            "reference_year": int(ref),
            "resource_type": _RESOURCE_TYPE_CODES[entry["resource_type"]],
            "file_type": entry["location_type"],
        }
    return file_metadata
