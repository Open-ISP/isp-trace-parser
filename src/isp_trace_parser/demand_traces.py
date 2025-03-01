import functools
import os
from pathlib import Path
from typing import Literal, Optional

import yaml
from joblib import Parallel, delayed
from pydantic import BaseModel, validate_call

from isp_trace_parser import input_validation
from isp_trace_parser.metadata_extractors import extract_demand_trace_metadata
from isp_trace_parser.trace_restructure_helper_functions import (
    add_half_year_as_column,
    check_filter_by_metadata,
    get_all_filepaths,
    read_trace_csv,
    save_half_year_chunk_of_trace,
    trace_formatter,
)


class DemandMetadataFilter(BaseModel):
    """A Pydantic class for defining a metadata based filter that specifies which wind trace files to parser.

    All attributes of the filter are optional, any atribute not included will not be filtered on. If an attribute is
    included then only traces with metadata matching the values in the corresponding list will be parsed.

    Examples:

    Filter for only subregions that are in a list of names.

    >>> metadata_filters = DemandMetadataFilter(
    ... subregion=['A', 'B', 'x'],
    ... )

    Filter for only POE50 and one type of demand data.

    >>> metadata_filters = DemandMetadataFilter(
    ... poe=['POE50'],
    ... demand_type=['OPSO_MODELLING'],
    ... )

    Attributes:
        subregion: list of names of subregions.
        scenario: list of scenarios, only including "Step Change", "Progressive Change", and "Green Energy Exports"
        poe: list of POE levels, only including "POE10" and "POE50"
        demand_type, list of demand types, only including "OPSO_MODELLING", "OPSO_MODELLING_PVLITE", and "PV_TOT"
        reference_year: list of ints specifying reference_years
    """

    subregion: Optional[list[str]] = None
    scenario: Optional[
        list[Literal["Step Change", "Progressive Change", "Green Energy Exports"]]
    ] = None
    poe: Optional[list[Literal["POE50", "POE10"]]] = None
    demand_type: Optional[
        list[Literal["OPSO_MODELLING", "OPSO_MODELLING_PVLITE", "PV_TOT"]]
    ] = None
    reference_year: Optional[list[int]] = None


@validate_call
def parse_demand_traces(
    input_directory: str | Path,
    parsed_directory: str | Path,
    use_concurrency: bool = True,
    filters: DemandMetadataFilter | None = None,
):
    """Takes a directory with AEMO demand trace data and reformats the data, saving it to a new directory.

    AEMO demand trace data comes in CSVs with columns specifying the year, day, and month, and data columns
    (labeled 01, 02, ... 48) storing the demand values for each half hour of the day. The file name of the CSV
    contains metadata in the following format "<subregionID>_RefYear_<reference year>_<scenario>_<poe>_<data type>.csv".
    For example, "CNSW_RefYear_2011_HYDROGEN_EXPORT_POE10_OPSO_MODELLING.csv".

    The trace parser reformats the data, modifies the file naming convention to match the IASR workbook, and stores
    the data files with a directory structure that mirrors the new file naming convention. Firstly, the data format is
    changed to a two column format with a column "Datetime" specifying the end of the half hour period the measurement
    is for in the format %Y-%m-%d %HH:%MM%:%SS, and a column "Value" specifying the measurement value. The data is saved
    in parquet format in half-yearly chunks to improved read speeds. The files are saved in with following
    directory structure and naming convention:

         "<scenario>/RefYear<reference year>/<subregion ID>/<poe>/<data type>/"
         "<scenario>_RefYear<reference year>_<subregion ID>_<poe>_<data type>_HalfYear<year>-<half of year>.parquet"

    With the scenario name mapped from the name used in the raw AEMO trace data to the name used in the IASR workbook.
    For one half-yearly chunk of the CSV example above, the parsed filepath would be:

        "Green_Energy_Exports/RefYear2011/CNSW/POE10/OPSO_MODELLING/"
        "Green_Energy_Exports_RefYear2011_CNSW_POE10_OPSO_MODELLING_HalfYear2020-1.parquet"

    By default, all trace data in the input directory is parsed. However, a filters dictionary can be provided to
    filter the traces to pass based on metadata. If a metadata type is present in the filters then only traces a
    metadata value present in the corresponding filter list will be passed, see examples below.

    Examples:

    Parse whole directory of trace data.

    >>> parse_demand_traces(
    ... input_directory='example_input_data/demand',
    ... parsed_directory='example_parsed_data/demand',
    ... use_concurrency=False
    ... )

    Parse only a subset of the input traces.

    Excluding a type of metadata from the
    filter will result in no filtering on
    that component of the metadata and elements can
    be added to each list in the filter to selectively
    expand which traces are parsed.

    >>> metadata_filters = DemandMetadataFilter(
    ... scenario=['Green Energy Exports'],
    ... subregion=['CNSW'],
    ... poe=['POE10'],
    ... demand_type=['OPSO_MODELLING']
    ... )

    >>> parse_demand_traces(
    ... input_directory='example_input_data/demand',
    ... parsed_directory='example_parsed_data/demand',
    ... filters=metadata_filters,
    ... use_concurrency=False
    ... )


    Args:
        input_directory: str or pathlib.Path, path to data to parse.
        parsed_directory: str or pathlib.Path, path to directory where parsed tarces will be saved.
        use_concurrency: boolean, default True, specifies whether to use parallel processing
        filters: dict{str: list[str]}, dict that specifies which traces to parse, if a component
            of the metadata is missing from the dict no filtering on that component occurs. See example.

    Returns: None
    """
    input_directory = input_validation.input_directory(input_directory)
    parsed_directory = input_validation.parsed_directory(parsed_directory)

    files = get_all_filepaths(input_directory)

    with open(
        Path(__file__).parent.parent
        / Path("isp_trace_name_mapping_configs/demand_scenario_mapping.yaml"),
        "r",
    ) as f:
        demand_scenario_mapping = yaml.safe_load(f)

    partial_func = functools.partial(
        restructure_demand_file,
        demand_scenario_mapping=demand_scenario_mapping,
        output_directory=parsed_directory,
        filters=filters,
    )

    if use_concurrency:
        max_workers = os.cpu_count() - 2

        Parallel(n_jobs=max_workers)(delayed(partial_func)(file) for file in files)

    else:
        for file in files:
            partial_func(file)


def restructure_demand_file(
    input_filepath: Path,
    demand_scenario_mapping: dict[str, str],
    output_directory: str | Path,
    filters: dict[str, list[str]] = None,
) -> None:
    """
    Restructures a single demand trace file and saves it in a new format.

    This function processes a demand trace file, restructures and saves it in a new format. It handles the mapping of
    scenario names and applies filters if provided.

    Args:
        input_filepath: Path object representing the input demand trace file.
        demand_scenario_mapping: Dictionary mapping raw scenario names to IASR workbook scenario names.
        output_directory: Directory where restructured files will be saved.
        filters: Filters to apply to the metadata. Keys are metadata fields, values are lists of allowed values.

    Returns:
        None: Files are saved to disk, but the function doesn't return any value.

    Example:
        >>> input_filepath = Path('CNSW_RefYear_2011_HYDROGEN_EXPORT_POE10_OPSO_MODELLING.csv')

        >>> demand_scenario_mapping = {'HYDROGEN_EXPORT': 'Green Energy Exports'}

        >>> restructure_demand_file(
        ...     input_filepath=input_filepath,
        ...     demand_scenario_mapping=demand_scenario_mapping,
        ...     output_directory='/path/to/output'
        ... )  # doctest: +SKIP

        # This will process the input file and save it with the new scenario name in the specified output directory
    """
    file_metadata = extract_demand_trace_metadata(input_filepath.name)
    file_metadata["scenario"] = get_save_scenario_for_demand_trace(
        file_metadata, demand_scenario_mapping
    )
    parse_file = check_filter_by_metadata(file_metadata, filters)
    if parse_file:
        trace = read_trace_csv(input_filepath)
        trace = trace_formatter(trace)
        trace = add_half_year_as_column(trace)
        for half_year, chunk in trace.group_by("HY"):
            save_half_year_chunk_of_trace(
                chunk,
                file_metadata,
                half_year,
                output_directory,
                write_new_demand_filepath,
            )


def get_save_scenario_for_demand_trace(
    file_metadata: dict[str, str], demand_scenario_mapping: dict[str, str]
) -> str:
    """
    Maps the raw scenario name to the IASR workbook scenario name.

    Args:
        file_metadata: Dictionary containing metadata for the demand trace file.
        demand_scenario_mapping: Dictionary mapping raw scenario names to IASR workbook scenario names.

    Returns:
        The mapped scenario name as a string.
    """
    return demand_scenario_mapping[file_metadata["scenario"]]


def write_new_demand_filepath(metadata: dict[str, str]) -> str:
    """
    Generates the output filepath for a demand trace file.

    Args:
        metadata: Dictionary containing metadata for the demand trace file.

    Returns:
        A string representing the filepath.
    """
    m = metadata
    subregion = m["subregion"].replace(" ", "_")
    scenario = m["scenario"].replace(" ", "_")

    return (
        f"{scenario}/RefYear{m['reference_year']}/{subregion}/{m['poe']}/{m['demand_type']}/"
        f"{scenario}_RefYear{m['reference_year']}_{subregion}_{m['poe']}_{m['demand_type']}_HalfYear{m['hy']}.parquet"
    )


def extract_metadata_for_all_demand_files(
    filenames: list[Path],
) -> dict[Path, dict[str, str]]:
    """
    Extracts metadata for all demand trace files.

    Args:
        filenames: List of Path objects representing the demand trace files.

    Returns:
        A dictionary with filepaths as keys and metadata dicts as values.
    """
    file_metadata = [extract_demand_trace_metadata(str(f.name)) for f in filenames]
    return dict(zip(filenames, file_metadata))
