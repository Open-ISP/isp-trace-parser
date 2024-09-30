import functools
import os
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import yaml

from isp_trace_parser import trace_formatter
from isp_trace_parser.trace_restructure_helper_functions import (
    get_all_filepaths,
    check_filter_by_metadata,
    read_trace_csv,
    add_half_year_as_column,
    save_half_year_chunk_of_trace,
)
from isp_trace_parser.metadata_extractors import extract_demand_trace_metadata


def parse_demand_traces(
    input_directory: str | Path,
    parsed_directory: str | Path,
    use_concurrency: bool = True,
    filters: dict[str : list[str]] = None,
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
    For one half-yearly chunk of the CSV example above the parsed filepath would be:

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

    Excluding one type of metadata (key) from the
    filter will result in no filtering on
    that component of the metadata and more elements can
    be added to each list in the filter to selectively
    expand which traces are parsed.

    >>> metadata_filters={
    ... 'scenario': ['Green Energy Exports'],
    ... 'subregion': ['CNSW'],
    ... 'poe': ['POE10'],
    ... 'type': ['OPSO_MODELLING']
    ... }

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
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            results = executor.map(partial_func, files)
            # Iterate through results to raise any errors that occurred.
            for result in results:
                result
    else:
        for file in files:
            partial_func(file)


def restructure_demand_file(
    input_filepath, demand_scenario_mapping, output_directory, filters
):
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


def get_save_scenario_for_demand_trace(file_metadata, demand_scenario_mapping):
    return demand_scenario_mapping[file_metadata["scenario"]]


def write_new_demand_filepath(metadata):
    m = metadata
    subregion = m["subregion"].replace(" ", "_")
    scenario = m["scenario"].replace(" ", "_")

    return (
        f"{scenario}/RefYear{m['year']}/{subregion}/{m['poe']}/{m['type']}/"
        f"{scenario}_RefYear{m['year']}_{subregion}_{m['poe']}_{m['type']}_HalfYear{m['hy']}.parquet"
    )


def extract_metadata_for_all_demand_files(filenames):
    file_metadata = [extract_demand_trace_metadata(str(f.name)) for f in filenames]
    return dict(zip(filenames, file_metadata))
