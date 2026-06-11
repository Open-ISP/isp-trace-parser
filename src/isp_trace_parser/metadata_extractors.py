import re


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
