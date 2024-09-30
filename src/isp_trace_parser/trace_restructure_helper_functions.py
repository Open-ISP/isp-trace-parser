from pathlib import Path
from datetime import timedelta

import polars as pl

from isp_trace_parser.trace_formatter import trace_formatter


def get_all_filepaths(dir):
    dir = Path(dir)
    if dir.is_dir():
        return [path for path in Path(dir).rglob("*.csv") if path.is_file()]
    else:
        raise ValueError(f"{dir} not found.")


def read_trace_csv(file):
    pl_types = [pl.Int64] * 3 + [pl.Float64] * 48
    return pl.read_csv(file, schema_overrides=pl_types)


def read_and_format_traces(files):
    traces = []
    for f in files:
        trace_data = read_trace_csv(f)
        trace_data = trace_formatter(trace_data)
        traces.append(trace_data)
    return traces


def calculate_average_trace(traces):
    combined_traces = pl.concat(traces)
    average_trace = combined_traces.group_by("Datetime").agg(
        [pl.col("Value").mean().alias("Value")]
    )
    return average_trace


def add_half_year_as_column(trace):
    def calculate_half_year(dt):
        dt -= timedelta(seconds=1)
        if dt.month < 7:
            half_year = f"{dt.year}-1"
        else:
            half_year = f"{dt.year}-2"
        return half_year

    trace = trace.sort("Datetime")

    trace = trace.with_columns(
        (pl.col("Datetime").map_elements(calculate_half_year, pl.String).alias("HY"))
    )

    return trace


def save_half_year_chunk_of_trace(
    chunk, file_metadata, half_year, output_directory, write_output_filepath
):
    file_metadata["hy"] = half_year[0]
    data = chunk.drop("HY")
    path_in_output_directory = write_output_filepath(file_metadata)
    save_filepath = Path(output_directory) / path_in_output_directory
    save_filepath.parent.mkdir(parents=True, exist_ok=True)
    data.write_parquet(save_filepath)


def process_and_save_files(
    files, file_metadata, write_output_filepath, output_directory
):
    traces = read_and_format_traces(files)

    if len(traces) > 1:
        trace = calculate_average_trace(traces)
    else:
        trace = traces[0]

    trace = add_half_year_as_column(trace)

    for half_year, chunk in trace.group_by("HY"):
        save_half_year_chunk_of_trace(
            chunk, file_metadata, half_year, output_directory, write_output_filepath
        )


def get_metadata_that_matches_trace_names(trace_names, all_input_file_metadata):
    return {
        f: metadata
        for f, metadata in all_input_file_metadata.items()
        if metadata["name"] in trace_names
    }


def get_unique_reference_years_in_metadata(metadata_for_trace_files):
    return list(set(metadata["year"] for metadata in metadata_for_trace_files.values()))


def get_metadata_that_matches_reference_year(year, metadata_for_trace_files):
    return {
        f: metadata
        for f, metadata in metadata_for_trace_files.items()
        if metadata["year"] == year
    }


def get_metadata_for_writing_save_name(metadata_for_trace_files):
    return next(iter(metadata_for_trace_files.values()))


def overwrite_metadata_trace_name_with_output_name(metadata, save_name):
    metadata["name"] = save_name
    return metadata


def check_filter_by_metadata(metadata, filters):
    parse_file = True
    for metadata_type, metadata_value in metadata.items():
        # If element of the metadata is present in filter but not one the values in the
        # list under that key then we dont parse this file.
        if (
            filters is not None
            and metadata_type in filters
            and metadata_value not in filters[metadata_type]
        ):
            parse_file = False
    return parse_file


def get_unique_project_and_area_names_in_input_files(metadata_for_trace_files):
    names = []
    for filepath, meta_data in metadata_for_trace_files.items():
        names.append(meta_data["name"])
    return list(set(names))


def filter_mapping_by_names_in_input_files(name_mapping, names_in_input_files):
    filtered_mapping = {}
    for output_name, input_name in name_mapping.items():
        if isinstance(input_name, list):
            if input_name[0] in names_in_input_files:
                filtered_mapping[output_name] = input_name
        else:
            if input_name in names_in_input_files:
                filtered_mapping[output_name] = input_name
    return filtered_mapping
