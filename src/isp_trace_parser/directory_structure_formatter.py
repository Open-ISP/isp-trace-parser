from pathlib import Path
import re

import polars as pl
from polars.polars import dtype_cols

from isp_trace_parser.trace_formatter import trace_formatter_pl


def get_all_filepaths(dir):
    return [path for path in Path(dir).rglob('*.csv') if path.is_file()]


def map_to_workbook_names(name):
    return name


def add_half_year_as_column(df):

    def calculate_half_year(dt):
        if dt.month < 7:
            half_year = f"{dt.year}-1"
        else:
            half_year = f"{dt.year}-2"
        return half_year

    df = df.with_columns(
        (pl.col("Datetime").map_elements(calculate_half_year, pl.String).alias("HY"))
    )

    return df


def extract_solar_trace_meta_data(filename):
    # Pattern for pulling out generator name, followed by capitalised acronym specifying a
    # technology, and then the reference year at then end.
    pattern = r"^([A-Za-z0-9_\-]+)_([A-Z]+)_RefYear(\d{4})\.csv$"

    match = re.match(pattern, filename)

    if match:
        name = match.group(1)
        technology = match.group(2)
        year = match.group(3)
    else:
        raise ValueError('Failed to extract solar file name components.')

    file_type = 'project'
    if 'REZ' in name:
        file_type = 'area'

    return file_type, name, technology, year


def write_new_solar_filepath(name, file_type, technology, year, hy):
    name = map_to_workbook_names(name)
    return f"RefYear{year}/{file_type.capitalize()}/{name}/RefYear{year}_{name}_{technology}_HalfYear{hy}.parquet"


def solar_directory_restructure(old_directory, new_directory):
    c = 0
    for file in get_all_filepaths(old_directory):
        if c > 2:
            break
        c += 1
        file_type, name, technology, year = extract_solar_trace_meta_data(file.name)
        print(file)
        pl_types = [pl.Int64] * 3 + [pl.Float64] * 48
        trace_data = pl.read_csv(file, schema_overrides=pl_types)
        trace_data = trace_formatter_pl(trace_data)
        trace_data = add_half_year_as_column(trace_data)
        for half_year, data in trace_data.group_by("HY"):
            path_in_new_directory = write_new_solar_filepath(name, file_type, technology, year, half_year[0])
            save_filepath = Path(new_directory) / path_in_new_directory
            save_filepath.parent.mkdir(parents=True, exist_ok=True)
            data = data.drop("HY")
            data.write_parquet(save_filepath)


def extract_wind_trace_meta_data(filename):
    # Pattern for pulling out generator name, followed by capitalised acronym specifying a
    # technology, and then the reference year at then end.
    pattern = r"^([A-Za-z0-9_\-]+)_([A-Z]+)_RefYear(\d{4})\.csv$"

    match = re.match(pattern, filename)

    if match:
        generator_name = match.group(1)
        technology = match.group(2)
        year = match.group(3)
    else:
        raise ValueError('Failed to extract solar file name components.')

    return generator_name, technology, year

old_dir = "/media/nick/Samsung_T5/isp_2024_data/trace_data/solar"
new_dir = "/media/nick/Samsung_T5/isp_2024_data/reformatted_trace_data/solar"
solar_directory_restructure(old_dir, new_dir)



