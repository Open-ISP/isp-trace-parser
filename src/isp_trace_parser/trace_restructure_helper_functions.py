from datetime import timedelta
from pathlib import Path

import polars as pl
from pydantic import BaseModel

from isp_trace_parser.trace_formatter import trace_formatter


def get_all_filepaths(directory: Path) -> list[Path]:
    if directory.is_dir():
        return [path for path in Path(directory).rglob("*.csv") if path.is_file()]
    else:
        raise ValueError(f"{directory} not found.")


def read_trace_csv(file: Path) -> pl.DataFrame:
    pl_types = [pl.Int64] * 3 + [pl.Float64] * 48
    data = pl.read_csv(file, schema_overrides=pl_types)
    return data


def read_and_format_traces(files: list[Path]) -> list[pl.DataFrame]:
    traces = []
    for f in files:
        trace_data = read_trace_csv(f)
        trace_data = trace_formatter(trace_data)
        traces.append(trace_data)
    return traces


def calculate_average_trace(traces: list[pl.DataFrame]) -> pl.DataFrame:
    combined_traces = pl.concat(traces)
    average_trace = combined_traces.group_by("Datetime").agg(
        [pl.col("Value").mean().alias("Value")]
    )
    return average_trace


def add_half_year_as_column(trace: pl.DataFrame) -> pl.DataFrame:
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
    chunk: pl.DataFrame,
    file_metadata: dict[str, str],
    half_year: tuple[str],
    output_directory: Path,
    write_output_filepath: callable,
) -> None:
    file_metadata["hy"] = half_year[0]
    data = chunk.drop("HY")
    path_in_output_directory = write_output_filepath(file_metadata)
    save_filepath = output_directory / path_in_output_directory
    save_filepath.parent.mkdir(parents=True, exist_ok=True)
    data.write_parquet(save_filepath)


def process_and_save_files(
    files: list[Path],
    file_metadata: dict[str, str],
    write_output_filepath: callable,
    output_directory: str | Path,
) -> None:
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


def get_metadata_that_matches_trace_names(
    trace_names: list[str] | str, all_input_file_metadata: dict[Path, dict[str, str]]
) -> dict[Path, dict[str, str]]:
    if isinstance(trace_names, str):
        trace_names = [trace_names]
    matching_meta_data = {
        f: metadata.copy()
        for f, metadata in all_input_file_metadata.items()
        if metadata["name"] in trace_names
    }
    return matching_meta_data


def get_unique_reference_years_in_metadata(
    metadata_for_trace_files: dict[Path, dict[str, str]],
) -> list[str]:
    return list(
        set(
            metadata["reference_year"] for metadata in metadata_for_trace_files.values()
        )
    )


def get_metadata_that_matches_reference_year(
    year: str, metadata_for_trace_files: dict[Path, dict[str, str]]
) -> dict[str | Path, dict[str, str]]:
    return {
        f: metadata
        for f, metadata in metadata_for_trace_files.items()
        if metadata["reference_year"] == year
    }


def get_metadata_for_writing_save_name(
    metadata_for_trace_files: dict[Path, dict[str, str]],
) -> dict[str, str]:
    return next(iter(metadata_for_trace_files.values()))


def overwrite_metadata_trace_name_with_output_name(
    metadata: dict[str, str], save_name: str
) -> dict[str, str]:
    metadata["name"] = save_name
    return metadata


def check_filter_by_metadata(
    metadata: dict[str, str], filters: BaseModel | None
) -> bool:
    if filters is None:
        return True

    for field, allowed_values in filters.model_dump(exclude_unset=True).items():
        if field in metadata and allowed_values is not None:
            if metadata[field] not in allowed_values:
                return False

    return True


def get_unique_project_and_area_names_in_input_files(
    metadata_for_trace_files: dict[Path, dict[str, str]],
) -> list[str]:
    names = []
    for filepath, meta_data in metadata_for_trace_files.items():
        names.append(meta_data["name"])
    return list(set(names))


def filter_mapping_by_names_in_input_files(
    name_mapping: dict[str, str | list[str]], names_in_input_files: list[str]
) -> dict[str, str | list[str]]:
    filtered_mapping = {}
    for output_name, input_name in name_mapping.items():
        if isinstance(input_name, list):
            if input_name[0] in names_in_input_files:
                filtered_mapping[output_name] = input_name
        else:
            if input_name in names_in_input_files:
                filtered_mapping[output_name] = input_name
    return filtered_mapping


def get_just_filepaths(metadata_for_files):
    return [file for file, metadata in metadata_for_files.items()]
