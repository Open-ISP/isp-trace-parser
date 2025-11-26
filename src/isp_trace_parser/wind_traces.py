import functools
import os
from pathlib import Path
from typing import Literal, Optional

import yaml
from joblib import Parallel, delayed
from pydantic import BaseModel, validate_call

from isp_trace_parser import input_validation
from isp_trace_parser.metadata_extractors import extract_wind_trace_metadata
from isp_trace_parser.trace_restructure_helper_functions import (
    check_filter_by_metadata,
    filter_mapping_by_names_in_input_files,
    get_all_filepaths,
    get_metadata_for_writing_save_name,
    get_metadata_that_matches_reference_year,
    get_metadata_that_matches_trace_names,
    get_unique_project_and_zone_names_in_input_files,
    get_unique_reference_years_in_metadata,
    overwrite_metadata_trace_name_with_output_name,
    process_and_save_files,
)


class WindMetadataFilter(BaseModel):
    """A Pydantic class for defining a metadata based filter that specifies which wind trace files to parser.

    All attributes of the filter are optional, any atribute not included will not be filtered on. If an attribute is
    included then only traces with metadata matching the values in the corresponding list will be parsed.

    Examples:

    Filter for only projects or zones that are in a list of names.

    >>> metadata_filters = WindMetadataFilter(
    ... name=['A', 'B', 'x'],
    ... )

    Filter for zones with a high wind resource.

    >>> metadata_filters = WindMetadataFilter(
    ... resource_type=['WH'],
    ... file_type=['zone'],
    ... )

    Attributes:
        name: list of names for projects and/or IDs for zones.
        file_type: list of 'project' and/or 'zone' (zone typically refers to REZs)
        resource_type: list of resource types, only including 'WH', 'WM', 'WL', 'WX', or 'wind'.
        reference_year: list of ints specifying reference_years
    """

    name: Optional[list[str]] = None
    file_type: Optional[list[Literal["zone", "project"]]] = None
    resource_type: Optional[list[Literal["WH", "WM", "WL", "WX", "wind"]]] = None
    reference_year: Optional[list[int]] = None


@validate_call
def parse_wind_traces(
    input_directory: str | Path,
    parsed_directory: str | Path,
    use_concurrency: bool = True,
    filters: WindMetadataFilter | None = None,
):
    """Takes a directory with AEMO wind trace data and reformats the data, saving it to a new directory.

    AEMO wind trace data comes in CSVs with columns specifying the year, day, and month, and data columns
    (labeled 01, 02, ... 48) storing the wind generation values for each half hour of the day. The file name of the CSV
    contains metadata in the following format "<project or zone name>_RefYear<reference year>.csv" for projects, or
    "<zone id>_<resource quality>_<zone name>_RefYear<reference year>.csv" for zones.
    For example, "SNOWSTH1_RefYear2011.csv" for a project or "N8_WH_Cooma-Monaro_RefYear2023.csv" for a zone.

    The trace parser reformats the data, modifies the file naming convention, and stores
    the data files with a directory structure that mirrors the new file naming convention. Firstly, the data format is
    changed to a two column format with a column "datetime" specifying the end of the half hour period the measurement
    is for in the format %Y-%m-%d %HH:%MM%:%SS, and a column "value" specifying the measurement value. The data is saved
    in parquet format in half-yearly chunks to improved read speeds. The files are saved with the following
    directory structure and naming convention:

    For projects:
         "RefYear<reference year>/Project/<project name>/"
         "RefYear<reference year>_<project name>_HalfYear<year>-<half of year>.parquet"

    For zones:
         "RefYear<reference year>/Zone/<zone name>/<resource type>/"
         "RefYear<reference year>_<zone id>_<resource quality>_HalfYear<year>-<half of year>.parquet"

    With the project and zone names mapped from the names used in the raw AEMO trace data to the names used in the IASR workbook.
    For one half-yearly chunk of the CSV example above, the parsed filepath for a project would be:

        "RefYear2011/Project/Snowtown_South_Wind_Farm/"
        "RefYear2011_Snowtown_South_Wind_Farm_HalfYear2030-1.parquet"

    By default, all trace data in the input directory is parsed. However, a filters dictionary can be provided to
    filter the traces to pass based on metadata. If a metadata type is present in the filters then only traces with a
    metadata value present in the corresponding filter list will be passed, see examples below.

    Examples:

    Parse whole directory of trace data.

    >>> parse_wind_traces(
    ... input_directory='example_input_data/wind',
    ... parsed_directory='example_parsed_data/wind',
    ... use_concurrency=False
    ... )

    Parse only a subset of the input traces.

    Excluding a type of metadata from the
    filter will result in no filtering on
    that component of the metadata and elements can
    be added to each list in the filter to selectively
    expand which traces are parsed.

    >>> metadata_filters = WindMetadataFilter(
    ... file_type=['zone'],
    ... reference_year=[2011, 2012],
    ... )

    >>> parse_wind_traces(
    ... input_directory='example_input_data/wind',
    ... parsed_directory='example_parsed_data/wind',
    ... filters=metadata_filters,
    ... use_concurrency=False
    ... )


    Args:
        input_directory: str or pathlib.Path, path to data to parse.
        parsed_directory: str or pathlib.Path, path to directory where parsed traces will be saved.
        use_concurrency: boolean, default True, specifies whether to use parallel processing
        filters: dict{str: list[str]}, dict that specifies which traces to parse, if a component
            of the metadata is missing from the dict no filtering on that component occurs. See example.

    Returns: None
    """
    input_directory = input_validation.input_directory(input_directory)
    parsed_directory = input_validation.parsed_directory(parsed_directory)

    files = get_all_filepaths(input_directory)
    file_metadata = extract_metadata_for_all_wind_files(files)

    with open(
        Path(__file__).parent.parent
        / Path("isp_trace_name_mapping_configs/wind_project_mapping.yaml"),
        "r",
    ) as f:
        project_name_mappings = yaml.safe_load(f)

    project_name_mappings = restructure_wind_project_mapping(project_name_mappings)

    with open(
        Path(__file__).parent.parent
        / Path("isp_trace_name_mapping_configs/wind_zone_mapping.yaml"),
        "r",
    ) as f:
        zone_name_mappings = yaml.safe_load(f)

    project_and_zone_input_names = get_unique_project_and_zone_names_in_input_files(
        file_metadata
    )

    zone_name_mappings = filter_mapping_by_names_in_input_files(
        zone_name_mappings, project_and_zone_input_names
    )
    zone_output_names, zone_input_names = zip(*zone_name_mappings.items())

    project_name_mappings = filter_mapping_by_names_in_input_files(
        project_name_mappings, project_and_zone_input_names
    )
    project_output_names, project_input_names = zip(*project_name_mappings.items())

    zone_partial_func = functools.partial(
        restructure_wind_zone_files,
        all_input_file_metadata=file_metadata,
        output_directory=parsed_directory,
        filters=filters,
    )

    project_partial_func = functools.partial(
        restructure_wind_project_files,
        all_input_file_metadata=file_metadata,
        output_directory=parsed_directory,
        filters=filters,
    )

    if use_concurrency:
        max_workers = os.cpu_count() - 2

        Parallel(n_jobs=max_workers)(
            delayed(zone_partial_func)(save_name, old_trace_name)
            for save_name, old_trace_name in zip(zone_output_names, zone_input_names)
        )

        Parallel(n_jobs=max_workers)(
            delayed(project_partial_func)(save_name, old_trace_name)
            for save_name, old_trace_name in zip(
                project_output_names, project_input_names
            )
        )

    else:
        for save_name, old_trace_name in zip(zone_output_names, zone_input_names):
            zone_partial_func(save_name, old_trace_name)

        for save_name, old_trace_name in zip(project_output_names, project_input_names):
            project_partial_func(save_name, old_trace_name)


def restructure_wind_zone_files(
    output_zone_name: str,
    input_trace_names: list[str] | str,
    all_input_file_metadata: dict,
    output_directory: str | Path,
    filters: dict[str, list[str]] | None = None,
) -> None:
    """
    Restructures wind zone trace files and saves them in a new format.

    Examples:

        >>> all_metadata = {
        ...     'file1.csv': {'name': 'Zone1', 'year': '2020', 'resource_quality': 'WH'},
        ...     'file2.csv': {'name': 'Zone1', 'year': '2021', 'resource_quality': 'WM'},
        ...     'file3.csv': {'name': 'Zone2', 'year': '2020', 'resource_qulaity': 'WH'}
        ... } # doctest: +SKIP

        >>> restructure_wind_zone_files(
        ...     output_zone_name='NewZone1',
        ...     input_trace_names=['Zone1'],
        ...     all_input_file_metadata=all_metadata,
        ...     output_directory='output/wind'
        ... ) # doctest: +SKIP

        # This will process only 'file1.csv' and save it in the new structure

    Args:
        output_zone_name (str): The name of the zone in the output files.
        input_trace_names (list): List of input trace names to process.
        all_input_file_metadata (dict): Metadata for all input files.
        output_directory (str | Path): Directory where restructured files will be saved.
        filters (dict[str, list[str]] | None, optional): Filters to apply to the metadata.
                                                         Keys are metadata fields, values are lists of allowed values.

    Returns:
        None: Files are saved to disk, but the function doesn't return any value.
    """
    metadata_for_trace_files = get_metadata_that_matches_trace_names(
        input_trace_names, all_input_file_metadata
    )
    reference_years = get_unique_reference_years_in_metadata(metadata_for_trace_files)
    for year in reference_years:
        files_for_year = get_metadata_that_matches_reference_year(
            year, metadata_for_trace_files
        )
        resource_qualities = get_unique_resource_quality_in_metadata(files_for_year)
        for resource_quality in resource_qualities:
            files_for_resource_quality = get_metadata_that_matches_resource_quality(
                resource_quality, files_for_year
            )
            metadata = get_metadata_for_writing_save_name(files_for_resource_quality)
            metadata = overwrite_metadata_trace_name_with_output_name(
                metadata, output_zone_name
            )
            parse_file = check_filter_by_metadata(metadata, filters)
            if parse_file:
                process_and_save_files(
                    files_for_resource_quality,
                    metadata,
                    write_output_wind_zone_filepath,
                    output_directory,
                )


def restructure_wind_project_files(
    output_project_name: str,
    input_trace_names: list,
    all_input_file_metadata: dict,
    output_directory: str | Path,
    filters: dict[str, list[str]] | None = None,
) -> None:
    """
    Restructures wind project trace files and saves them in a new format.
    """
    metadata_for_trace_files = get_metadata_that_matches_trace_names(
        input_trace_names, all_input_file_metadata
    )
    reference_years = get_unique_reference_years_in_metadata(metadata_for_trace_files)
    for year in reference_years:
        files_for_year = get_metadata_that_matches_reference_year(
            year, metadata_for_trace_files
        )
        metadata = get_metadata_for_writing_save_name(files_for_year)
        metadata = overwrite_metadata_trace_name_with_output_name(
            metadata, output_project_name
        )
        parse_file = check_filter_by_metadata(metadata, filters)
        if parse_file:
            process_and_save_files(
                files_for_year,
                metadata,
                write_output_wind_project_filepath,
                output_directory,
            )


def write_output_wind_project_filepath(metadata: dict) -> str:
    """
    Generates the output filepath for a wind project trace file.

    Returns a string representing the filepath.
    """
    m = metadata
    name = m["name"].replace(" ", "_")
    return f"RefYear{m['reference_year']}_{name}.parquet"


def write_output_wind_zone_filepath(metadata: dict) -> str:
    """
    Generates the output filepath for a wind zone trace file.

    Returns a string representing the filepath.
    """
    m = metadata
    name = m["name"].replace(" ", "_")
    return f"RefYear{m['reference_year']}_{name}_{m['resource_quality']}.parquet"


def restructure_wind_project_mapping(project_name_mapping: dict) -> dict:
    """
    Simplifies the wind project name mapping.

    Returns a dict with the workbook project names as keys and CSV file project names as values.
    """
    return {
        name: mapping_data["CSVFile"]
        for name, mapping_data in project_name_mapping.items()
    }


def extract_metadata_for_all_wind_files(filepaths: list) -> dict:
    """
    Extracts metadata for all wind trace files.

    Returns a dict with filepaths as keys and metadata dicts as values.
    """
    file_metadata = [extract_wind_trace_metadata(str(f.name)) for f in filepaths]
    return dict(zip(filepaths, file_metadata))


def get_unique_resource_quality_in_metadata(
    metadata_for_trace_files: dict[str:str],
) -> list:
    return list(
        set(
            metadata["resource_quality"]
            for metadata in metadata_for_trace_files.values()
        )
    )


def get_metadata_that_matches_resource_quality(
    resource_quality: str, metadata_for_trace_files: dict[str:str]
) -> dict[str:str]:
    return {
        f: metadata
        for f, metadata in metadata_for_trace_files.items()
        if metadata["resource_quality"] == resource_quality
    }
