import functools
import os
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import yaml

from isp_trace_parser.metadata_extractors import extract_wind_trace_metadata
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
    filter_mapping_by_names_in_input_files,
)


def parse_wind_traces(
    input_directory, parsed_directory, use_concurrency=True, filters=None
):
    """
    Examples:

    Parse whole directory of trace data.

    >>> parse_wind_traces(
    ... input_directory='example_input_data/wind',
    ... parsed_directory='example_parsed_data/wind',
    ... use_concurrency=False
    ... )

    """
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
        / Path("isp_trace_name_mapping_configs/wind_area_mapping.yaml"),
        "r",
    ) as f:
        area_name_mappings = yaml.safe_load(f)

    project_and_area_input_names = get_unique_project_and_area_names_in_input_files(
        file_metadata
    )

    area_name_mappings = filter_mapping_by_names_in_input_files(
        area_name_mappings, project_and_area_input_names
    )
    area_output_names, area_input_names = zip(*area_name_mappings.items())

    project_name_mappings = filter_mapping_by_names_in_input_files(
        project_name_mappings, project_and_area_input_names
    )
    project_output_names, project_input_names = zip(*project_name_mappings.items())

    area_partial_func = functools.partial(
        restructure_wind_area_files,
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

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            results = executor.map(
                area_partial_func, area_output_names, area_input_names
            )
            # Iterate through results to raise any errors that occurred.
            for result in results:
                result

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            results = executor.map(
                project_partial_func, project_output_names, project_input_names
            )
            # Iterate through results to raise any errors that occurred.
            for result in results:
                result

    else:
        for save_name, old_trace_name in zip(area_output_names, area_input_names):
            area_partial_func(save_name, old_trace_name)

        for save_name, old_trace_name in zip(project_output_names, project_input_names):
            project_partial_func(save_name, old_trace_name)


def restructure_wind_area_files(
    output_area_name,
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
        resource_types = get_unique_resource_types_in_metadata(files_for_year)
        for resource_type in resource_types:
            files_for_resource_type = get_metadata_that_matches_resource_type(
                resource_type, files_for_year
            )
            metadata = get_metadata_for_writing_save_name(files_for_resource_type)
            metadata = overwrite_metadata_trace_name_with_output_name(
                metadata, output_area_name
            )
            parse_file = check_filter_by_metadata(metadata, filters)
            if parse_file:
                process_and_save_files(
                    files_for_resource_type,
                    metadata,
                    write_output_wind_area_filepath,
                    output_directory,
                )


def restructure_wind_project_files(
    output_project_name,
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


def write_output_wind_project_filepath(metadata):
    m = metadata
    name = m["name"].replace(" ", "_")
    return (
        f"RefYear{m['year']}/{m['file_type'].capitalize()}/{name}/"
        f"RefYear{m['year']}_{name}_HalfYear{m['hy']}.parquet"
    )


def write_output_wind_area_filepath(metadata):
    m = metadata
    name = m["name"].replace(" ", "_")
    return (
        f"RefYear{m['year']}/{m['file_type'].capitalize()}/{name}/{m['resource_type']}/"
        f"RefYear{m['year']}_{name}_{m['resource_type']}_HalfYear{m['hy']}.parquet"
    )


def restructure_wind_project_mapping(project_name_mapping):
    return {
        name: mapping_data["CSVFile"]
        for name, mapping_data in project_name_mapping.items()
    }


def extract_metadata_for_all_wind_files(filenames):
    file_metadata = [extract_wind_trace_metadata(str(f.name)) for f in filenames]
    return dict(zip(filenames, file_metadata))


def get_unique_resource_types_in_metadata(metadata_for_trace_files):
    return list(
        set(metadata["resource_type"] for metadata in metadata_for_trace_files.values())
    )


def get_metadata_that_matches_resource_type(resource_type, metadata_for_trace_files):
    return {
        f: metadata
        for f, metadata in metadata_for_trace_files.items()
        if metadata["resource_type"] == resource_type
    }
