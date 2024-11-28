import functools
import os
from pathlib import Path
from typing import Literal, Optional

import yaml
from joblib import Parallel, delayed
from pydantic import BaseModel, validate_call

from isp_trace_parser import input_validation
from isp_trace_parser.metadata_extractors import extract_solar_trace_metadata
from isp_trace_parser.trace_restructure_helper_functions import (
    check_filter_by_metadata,
    get_all_filepaths,
    get_just_filepaths,
    get_metadata_for_writing_save_name,
    get_metadata_that_matches_reference_year,
    get_metadata_that_matches_trace_names,
    get_unique_project_and_area_names_in_input_files,
    get_unique_reference_years_in_metadata,
    overwrite_metadata_trace_name_with_output_name,
    process_and_save_files,
)


class SolarMetadataFilter(BaseModel):
    """A Pydantic class for defining a metadata based filter that specifies which solar trace files to parser.

    All attributes of the filter are optional, any atribute not included will not be filtered on. If an attribute is
    included then only traces with metadata matching the values in the corresponding list will be parsed.

    Examples:

    Filter for only projects or areas that are in a list of names.

    >>> metadata_filters = SolarMetadataFilter(
    ... name=['A', 'B', 'x'],
    ... )

    Filter for projects that use single axis tracking.

    >>> metadata_filters = SolarMetadataFilter(
    ... technology=['SAT'],
    ... file_type=['project'],
    ... )

    Attributes:
        name: list of names for projects and/or IDs for areas.
        file_type: list of 'project' and/or 'area' (area typically refers to REZs)
        technology: list of technology types of traces, only including 'SAT', 'FFP', or 'CST'.
        reference_year: list of ints specifying reference_years
    """

    name: Optional[list[str]] = None
    file_type: Optional[list[Literal["area", "project"]]] = None
    technology: Optional[list[Literal["SAT", "FFP", "CST"]]] = None
    reference_year: Optional[list[int]] = None


@validate_call
def parse_solar_traces(
    input_directory: str | Path,
    parsed_directory: str | Path,
    use_concurrency: bool = True,
    filters: SolarMetadataFilter | None = None,
):
    """Takes a directory with AEMO solar trace data and reformats the data, saving it to a new directory.

    AEMO solar trace data comes in CSVs with columns specifying the year, day, and month, and data columns
    (labeled 01, 02, ... 48) storing the solar generation values for each half hour of the day. The file name of the CSV
    contains metadata in the following format "<project or area name>_<technology>_RefYear<reference year>.csv".
    For example, "Adelaide_Desal_FFP_RefYear2011.csv" for a project or "REZ_N0_NSW_Non-REZ_CST_RefYear2023.csv" for an area.

    The trace parser reformats the data, modifies the file naming convention, and stores
    the data files with a directory structure that mirrors the new file naming convention. Firstly, the data format is
    changed to a two column format with a column "Datetime" specifying the end of the half hour period the measurement
    is for in the format %Y-%m-%d %HH:%MM%:%SS, and a column "Value" specifying the measurement value. The data is saved
    in parquet format in half-yearly chunks to improved read speeds. The files are saved with the following
    directory structure and naming convention:

    For projects:
         "RefYear<reference year>/Project/<project name>/"
         "RefYear<reference year>_<project name>_<technology>_HalfYear<year>-<half of year>.parquet"

    For areas:
         "RefYear<reference year>/Area/<area name>/<technology>/"
         "RefYear<reference year>_<area name>_<technology>_HalfYear<year>-<half of year>.parquet"

    With the project and area names mapped from the names used in the raw AEMO trace data to the names used in the IASR workbook.
    For one half-yearly chunk of the CSV example above, the parsed filepath for a project would be:

        "RefYear2011/Project/Adelaide_Desalination_Plant_Solar_Farm/"
        "RefYear2011_Adelaide_Desalination_Plant_Solar_Farm_FFP_HalfYear2030-1.parquet"

    By default, all trace data in the input directory is parsed. However, a filters dictionary can be provided to
    filter the traces to pass based on metadata. If a metadata type is present in the filters then only traces with a
    metadata value present in the corresponding filter list will be passed, see examples below.

    Examples:

    Parse whole directory of trace data.

    >>> parse_solar_traces(
    ... input_directory='example_input_data/solar',
    ... parsed_directory='example_parsed_data/solar',
    ... use_concurrency=False
    ... )

    Parse only a subset of the input traces.

    Excluding a type of metadata from the
    filter will result in no filtering on
    that component of the metadata and elements can
    be added to each list in the filter to selectively
    expand which traces are parsed.

    >>> metadata_filters = SolarMetadataFilter(
    ... technology=['FFP', 'SAT'],
    ... file_type=['project'],
    ... )

    >>> parse_solar_traces(
    ... input_directory='example_input_data/solar',
    ... parsed_directory='example_parsed_data/solar',
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
    file_metadata = extract_metadata_for_all_solar_files(files)
    with open(
        Path(__file__).parent.parent
        / Path("isp_trace_name_mapping_configs/solar_project_mapping.yaml"),
        "r",
    ) as f:
        project_name_mapping = yaml.safe_load(f)
    with open(
        Path(__file__).parent.parent
        / Path("isp_trace_name_mapping_configs/solar_area_mapping.yaml"),
        "r",
    ) as f:
        area_name_mapping = yaml.safe_load(f)
    name_mappings = {**project_name_mapping, **area_name_mapping}

    project_and_area_input_names = get_unique_project_and_area_names_in_input_files(
        file_metadata
    )
    name_mappings = {
        k: v for k, v in name_mappings.items() if v in project_and_area_input_names
    }

    project_and_area_output_names, project_and_area_input_names = zip(
        *name_mappings.items()
    )

    partial_func = functools.partial(
        restructure_solar_files,
        all_input_file_metadata=file_metadata,
        output_directory=parsed_directory,
        filters=filters,
    )

    if use_concurrency:
        max_workers = os.cpu_count() - 2
        Parallel(n_jobs=max_workers)(
            delayed(partial_func)(save_name, old_trace_name)
            for save_name, old_trace_name in zip(
                project_and_area_output_names, project_and_area_input_names
            )
        )
    else:
        for save_name, old_trace_name in zip(
            project_and_area_output_names, project_and_area_input_names
        ):
            partial_func(save_name, old_trace_name)


def restructure_solar_files(
    output_project_or_area_name: str,
    input_trace_names: list[str],
    all_input_file_metadata: dict[Path, dict[str, str]],
    output_directory: str | Path,
    filters: SolarMetadataFilter = None,
) -> None:
    """
    Restructures solar trace files and saves them in a new format.

    This function processes solar trace files, restructures them based on the provided metadata,
    and saves them in a new format. It handles both project and area solar trace files.

    Args:
        output_project_or_area_name: The name of the project or area in the output files.
        input_trace_names: List of input trace names to process.
        all_input_file_metadata: Metadata for all input files.
        output_directory: Directory where restructured files will be saved.
        filters: Filters to apply to the metadata (SolarMetadataFilter).

    Returns:
        None: Files are saved to disk, but the function doesn't return any value.

    Example:
        >>> input_metadata = {
        ...     Path('file1.csv'): {'name': 'Project1', 'year': '2020', 'technology': 'FFP', 'file_type': 'project'},
        ...     Path('file2.csv'): {'name': 'Area1', 'year': '2020', 'technology': 'SAT', 'file_type': 'area'}
        ... }  # doctest: +SKIP

        >>> restructure_solar_files(
        ...     output_project_or_area_name='NewProject1',
        ...     input_trace_names=['Project1'],
        ...     all_input_file_metadata=input_metadata,
        ...     output_directory='/path/to/output'
        ... )  # doctest: +SKIP

        # This will process 'file1.csv' and save it with the new name 'NewProject1' in the specified output directory
    """

    metadata_for_trace_files = get_metadata_that_matches_trace_names(
        input_trace_names, all_input_file_metadata
    )
    reference_years = get_unique_reference_years_in_metadata(metadata_for_trace_files)
    for year in reference_years:
        files_for_year = get_metadata_that_matches_reference_year(
            year, metadata_for_trace_files
        )
        techs = get_unique_techs_in_metadata(files_for_year)
        for tech in techs:
            files_for_tech = get_metadata_that_matches_tech(tech, files_for_year)
            metadata = get_metadata_for_writing_save_name(files_for_tech)
            metadata = overwrite_metadata_trace_name_with_output_name(
                metadata, output_project_or_area_name
            )
            parse_file = check_filter_by_metadata(metadata, filters)
            if parse_file:
                process_and_save_files(
                    get_just_filepaths(files_for_tech),
                    metadata,
                    write_output_solar_filepath,
                    output_directory,
                )


def write_output_solar_filepath(metadata: dict[str, str]) -> str:
    """
    Generates the output filepath for a solar trace file.

    Args:
        metadata: Dictionary containing metadata for the solar trace file.

    Returns:
        A string representing the filepath.
    """
    m = metadata
    name = m["name"].replace(" ", "_")

    if m["file_type"] == "project":
        return (
            f"RefYear{m['reference_year']}/{m['file_type'].capitalize()}/{name}/"
            f"RefYear{m['reference_year']}_{name}_{m['technology']}_HalfYear{m['hy']}.parquet"
        )
    else:
        return (
            f"RefYear{m['reference_year']}/{m['file_type'].capitalize()}/{name}/{m['technology']}/"
            f"RefYear{m['reference_year']}_{name}_{m['technology']}_HalfYear{m['hy']}.parquet"
        )


def extract_metadata_for_all_solar_files(
    filepaths: list[Path],
) -> dict[Path, dict[str, str]]:
    """
    Extracts metadata for all solar trace files.

    Args:
        filepaths: List of Path objects representing the solar trace files.

    Returns:
        A dictionary with filepaths as keys and metadata dicts as values.
    """
    file_metadata = [extract_solar_trace_metadata(str(f.name)) for f in filepaths]
    return dict(zip(filepaths, file_metadata))


def get_unique_techs_in_metadata(
    metadata_for_trace_files: dict[Path, dict[str, str]],
) -> list[str]:
    """
    Gets unique technologies from the metadata of trace files.

    Args:
        metadata_for_trace_files: Dictionary containing metadata for trace files.

    Returns:
        A list of unique technologies.
    """
    return list(
        set(metadata["technology"] for metadata in metadata_for_trace_files.values())
    )


def get_metadata_that_matches_tech(
    tech: str, metadata_for_trace_files: dict[Path, dict[str, str]]
) -> dict[Path, dict[str, str]]:
    """
    Filters metadata to only include files matching a specific technology.

    Args:
        tech: The technology to filter by.
        metadata_for_trace_files: Dictionary containing metadata for trace files.

    Returns:
        A dictionary of metadata for files matching the specified technology.
    """
    return {
        f: metadata
        for f, metadata in metadata_for_trace_files.items()
        if metadata["technology"] == tech
    }
