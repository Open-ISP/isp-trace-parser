import functools
import os
from pathlib import Path
from typing import Literal, Optional

import polars as pl
import yaml
from joblib import Parallel, delayed
from pydantic import BaseModel, validate_call

from isp_trace_parser import input_validation
from isp_trace_parser.metadata_extractors import extract_demand_trace_metadata
from isp_trace_parser.trace_restructure_helper_functions import (
    check_filter_by_metadata,
    get_all_filepaths,
    read_trace_csv,
    trace_formatter,
)


class DemandMetadataFilter(BaseModel):
    """A Pydantic class for defining a metadata based filter that specifies which demand trace files to parse.

    All attributes of the filter are optional, any attribute not included will not be filtered on. If an attribute is
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

    The trace parser reformats the data and stores the data files in parquet format with metadata columns,
    which match the IASR workbook conventions.
    The data format is changed to include a column "datetime" specifying the end of the half hour period
    the measurement is for in the format %Y-%m-%d %H:%M:%S, a column "value" specifying the measurement
    value, and metadata columns (subregion, reference_year, scenario, poe, demand_type). The scenario
    column contains the mapped scenario name from the IASR workbook. Files are saved with a new naming
    convention: "<mapped_scenario>_RefYear<reference_year>_<subregion>_<poe>_<demand_type>.parquet".

    For the CSV example above, the parsed filename would be:

        "Green_Energy_Exports_RefYear2011_CNSW_POE10_OPSO_MODELLING.parquet"

    By default, all trace data in the input directory is parsed. However, a DemandMetadataFilter can be provided
    to filter the traces based on metadata. If a metadata type is present in the filter then only traces with a
    metadata value present in the corresponding filter list will be passed, see examples below.

    Examples:

    Parse whole directory of trace data.

    >>> parse_demand_traces(
    ... input_directory='example_input_data/demand',
    ... parsed_directory='example_parsed_data/demand',
    ... use_concurrency=False
    ... ) # doctest: +SKIP

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
    ... ) # doctest: +SKIP


    Args:
        input_directory: str or pathlib.Path, path to data to parse.
        parsed_directory: str or pathlib.Path, path to directory where parsed traces will be saved.
        use_concurrency: boolean, default True, specifies whether to use parallel processing
        filters: DemandMetadataFilter or None, specifies which traces to parse. If a metadata
            attribute is not set, no filtering on that attribute occurs. See example.

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
    output_directory: Path,
    filters: DemandMetadataFilter | None = None,
) -> None:
    """
    Restructures a single demand trace file and saves it in a parquet format.

    This function processes a demand trace file, restructures and saves it in a new format, with the original
    input filename stem and a .parquet extension. It handles the mapping of scenario names and applies filters
    if provided.

    Args:
        input_filepath: Path object representing the input demand trace file.
        demand_scenario_mapping: Dictionary mapping raw scenario names to IASR workbook scenario names.
        output_directory: Directory where restructured files will be saved.
        filters: DemandMetadataFilter or None, specifies which traces to parse based on metadata.

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

        # This will process the input file and save it in parquet format in the specified output directory
    """
    file_metadata = extract_demand_trace_metadata(input_filepath.name)

    file_metadata["scenario"] = get_save_scenario_for_demand_trace(
        file_metadata, demand_scenario_mapping
    )

    parse_file = check_filter_by_metadata(file_metadata, filters)
    if parse_file:
        trace = read_trace_csv(input_filepath)
        trace = trace_formatter(trace)
        trace = _frame_with_metadata(trace, file_metadata)

        save_filepath = output_directory / write_new_demand_filename(
            metadata=file_metadata
        )
        save_filepath.parent.mkdir(parents=True, exist_ok=True)

        trace.write_parquet(save_filepath)


def _frame_with_metadata(trace: pl.DataFrame, file_metadata: dict) -> pl.DataFrame:
    """Adds metadata fields as columns to a trace DataFrame.

    Args:
        trace: The trace data Polars Dataframe to add data to.
        file_metadata: Dict containing metadata (subregion, reference_year, scenario, poe, and demand_type)
    """
    return trace.with_columns(
        subregion=pl.lit(file_metadata["subregion"]),
        reference_year=pl.lit(file_metadata["reference_year"]),
        scenario=pl.lit(file_metadata["scenario"]),
        poe=pl.lit(file_metadata["poe"]),
        demand_type=pl.lit(file_metadata["demand_type"]),
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


def write_new_demand_filename(metadata: dict[str, str]) -> str:
    """
    Generates the output filename for a demand trace file.

    Args:
        metadata: Dictionary containing metadata for the demand trace file.

    Returns:
        A string representing the filename.
    """
    m = metadata
    subregion = m["subregion"].replace(" ", "_")
    scenario = m["scenario"].replace(" ", "_")

    return f"{scenario}_RefYear{m['reference_year']}_{subregion}_{m['poe']}_{m['demand_type']}.parquet"


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
