import functools
import os
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import yaml

from isp_trace_parser.metadata_extractors import extract_solar_trace_metadata
from isp_trace_parser.trace_restructure_helper_functions import (
    get_all_filepaths,
    get_metadata_that_matches_trace_names,
    get_unique_reference_years_in_metadata,
    get_metadata_that_matches_reference_year,
    get_metadata_for_writing_save_name,
    overwrite_metadata_trace_name_with_output_name,
    check_filter_by_metadata,
    process_and_save_files,
    get_unique_project_and_area_names_in_input_files,
)


def parse_solar_traces(
    input_directory, parsed_directory, use_concurrency=True, filters=None
):
    """
    Examples:

    Parse whole directory of trace data.

    >>> parse_solar_traces(
    ... input_directory='example_input_data/solar',
    ... parsed_directory='example_parsed_data/solar',
    ... use_concurrency=False
    ... )

    """
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
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            results = executor.map(
                partial_func,
                project_and_area_output_names,
                project_and_area_input_names,
            )
            # Iterate through results to raise any errors that occurred.
            for result in results:
                result
    else:
        for save_name, old_trace_name in zip(
            project_and_area_output_names, project_and_area_input_names
        ):
            partial_func(save_name, old_trace_name)


def restructure_solar_files(
    output_project_or_area_name,
    input_trace_names,
    all_input_file_metadata,
    output_directory,
    filters=None,
):
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
                    files_for_tech,
                    metadata,
                    write_output_solar_filepath,
                    output_directory,
                )


def write_output_solar_filepath(metadata):
    m = metadata
    name = m["name"].replace(" ", "_")

    if m["file_type"] == "project":
        return (
            f"RefYear{m['year']}/{m['file_type'].capitalize()}/{name}/"
            f"RefYear{m['year']}_{name}_{m['technology']}_HalfYear{m['hy']}.parquet"
        )
    else:
        return (
            f"RefYear{m['year']}/{m['file_type'].capitalize()}/{name}/{m['technology']}/"
            f"RefYear{m['year']}_{name}_{m['technology']}_HalfYear{m['hy']}.parquet"
        )


def extract_metadata_for_all_solar_files(filenames):
    file_metadata = [extract_solar_trace_metadata(str(f.name)) for f in filenames]
    return dict(zip(filenames, file_metadata))


def get_unique_techs_in_metadata(metadata_for_trace_files):
    return list(
        set(metadata["technology"] for metadata in metadata_for_trace_files.values())
    )


def get_metadata_that_matches_tech(tech, metadata_for_trace_files):
    return {
        f: metadata
        for f, metadata in metadata_for_trace_files.items()
        if metadata["technology"] == tech
    }
