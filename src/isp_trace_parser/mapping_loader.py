from pathlib import Path

from isp_trace_parser import mappings


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
            "resource_type": entry["resource_type"].removeprefix("solar_").upper(),
            "file_type": entry["location_type"],
        }
    return file_metadata
