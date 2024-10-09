import re


def extract_solar_trace_metadata(filename):
    # Case 1: Match filenames that have a name, a tech, followed by RefYear
    pattern1 = re.compile(
        r"^(?P<name>[A-Za-z0-9_\-]+)_(?P<technology>[A-Z]+)_RefYear(?P<reference_year>\d{4})\.csv$"
    )

    # Case 2: Match filenames that have a rez, a name, and tech, followed by RefYear
    pattern2 = re.compile(
        r"^[A-Z]+_(?P<name>[A-Z0-9]+)_[A-Za-z0-9_\-]+_(?P<technology>[A-Z]+)_RefYear(?P<reference_year>\d{4})\.csv$"
    )

    # Try to match with pattern 2 first
    match2 = pattern2.match(filename)
    if match2:
        match_data = match2.groupdict()
        match_data["file_type"] = "area"
        match_data["reference_year"] = int(match_data["reference_year"])
        return match_data

    # Otherwise, try to match with pattern 1 (just name and year)
    match1 = pattern1.match(filename)
    if match1:
        match_data = match1.groupdict()
        match_data["file_type"] = "project"
        match_data["reference_year"] = int(match_data["reference_year"])
        return match_data

    raise ValueError(f"Filename '{filename}' does not match the expected pattern")


def extract_wind_trace_metadata(filename):
    # Case 1: Match filenames that have a simple name followed by RefYear
    pattern1 = re.compile(r"^(?P<name>.*)_RefYear(?P<reference_year>\d{4})\.csv$")

    # Case 2: Match filenames that have a resource type and a name followed by RefYear
    pattern2 = re.compile(
        r"^(?P<name>[A-Z0-9]+)_(?P<resource_quality>W[A-Z]+)_[A-Za-z_\-]+_RefYear(?P<reference_year>\d{4})\.csv$"
    )

    # Try to match with pattern 2 first
    match2 = pattern2.match(filename)
    if match2:
        match_data = match2.groupdict()
        match_data["file_type"] = "area"
        match_data["reference_year"] = int(match_data["reference_year"])
        return match_data

    # Otherwise, try to match with pattern 1 (just name and year)
    match1 = pattern1.match(filename)
    if match1:
        match_data = match1.groupdict()
        match_data["file_type"] = "project"
        match_data["reference_year"] = int(match_data["reference_year"])
        return match_data

    raise ValueError(f"Filename '{filename}' does not match the expected pattern")


def extract_demand_trace_metadata(filename):
    # Regex pattern to match the structure of the filename
    pattern = re.compile(
        r"^(?P<subregion>[A-Z]+)_RefYear_(?P<reference_year>\d{4})_(?P<scenario>[A-Z_]+)_(?P<poe>POE\d{2})_(?P<demand_type>["
        r"A-Z_]+)\.csv$"
    )

    # Match the pattern against the filename
    match = pattern.match(filename)

    if match:
        # If the filename matches the pattern, return a dictionary of captured groups
        match_data = match.groupdict()
        match_data["reference_year"] = int(match_data["reference_year"])
        return match_data
    else:
        # If the pattern does not match, raise an error or return None
        raise ValueError(f"Filename '{filename}' does not match the expected pattern")
