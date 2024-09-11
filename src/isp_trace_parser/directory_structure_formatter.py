from pathlib import Path
import re
from time import time
from concurrent.futures import ProcessPoolExecutor
import os
import functools

import polars as pl

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
    pattern = re.compile(r"^([A-Za-z0-9_\-]+)_([A-Z]+)_RefYear(\d{4})\.csv$")

    match = pattern.match(filename)

    if match:
        meta_data = {
            'name': match.group(1),
            'technology': match.group(2),
            'year': match.group(3)
        }
    else:
        raise ValueError(f"Filename '{filename}' does not match the expected pattern")

    meta_data['file_type'] = 'project'
    if 'REZ' in meta_data['name']:
        meta_data['file_type'] = 'area'

    return meta_data


def extract_wind_trace_meta_data(filename):
    # Case 1: Match filenames that have a simple name followed by RefYear
    pattern1 = re.compile(r"^(.*)_RefYear(\d{4})\.csv$")

    # Case 2: Match filenames that have a resource type and a name followed by RefYear
    pattern2 = re.compile(r"^[A-Z0-9_]*_(W[A-Z]+)_([A-Za-z_\-]+)_RefYear(\d{4})\.csv$")

    # Try to match with pattern 2 first
    match2 = pattern2.match(filename)
    if match2:
        resource_type, name, year = match2.groups()
        return {"file_type": "area", "resource_type": resource_type, "name": name, "year": year}

    # Otherwise, try to match with pattern 1 (just name and year)
    match1 = pattern1.match(filename)
    if match1:
        name, year = match1.groups()
        return {"file_type": "project", "name": name, "year": year}

    raise ValueError(f"Filename '{filename}' does not match the expected pattern")


def extract_demand_trace_meta_data(filename):
    # Regex pattern to match the structure of the filename
    pattern = re.compile(
        r"^(?P<area>[A-Z]+)_RefYear_(?P<year>\d{4})_(?P<scenario>[A-Z_]+)_(?P<poe>POE\d{2})_(?P<descriptor>[A-Z_]+)\.csv$"
    )

    # Match the pattern against the filename
    match = pattern.match(filename)

    if match:
        # If the filename matches the pattern, return a dictionary of captured groups
        return match.groupdict()
    else:
        # If the pattern does not match, raise an error or return None
        raise ValueError(f"Filename '{filename}' does not match the expected pattern")


def write_new_solar_filepath(meta_data):
    m = meta_data
    name = map_to_workbook_names(m['name'])
    return (f"RefYear{m['year']}/{m['file_type'].capitalize()}/{name}/"
            f"RefYear{m['year']}_{name}_{m['technology']}_HalfYear{m['hy']}.parquet")


def write_new_wind_filepath(meta_data):
    m = meta_data
    name = map_to_workbook_names(m['name'])
    if 'resource_type' in m:
        return (f"RefYear{m['year']}/{m['file_type'].capitalize()}/{name}/{m['resource_type']}/"
                f"RefYear{m['year']}_{name}_{m['resource_type']}_HalfYear{m['hy']}.parquet")
    else:
        return (f"RefYear{m['year']}/{m['file_type'].capitalize()}/{name}/"
                f"RefYear{m['year']}_{name}_HalfYear{m['hy']}.parquet")


def write_new_demand_filepath(meta_data):
    m = meta_data
    return (f"{m['scenario']}/RefYear{m['year']}/{m['area']}/{m['poe']}/{m['descriptor']}/"
            f"{m['scenario']}_RefYear{m['year']}_{m['area']}_{m['poe']}_{m['descriptor']}_HalfYear{m['hy']}.parquet")


def restructure_file(meta_data_extractor, save_path_writer, new_directory, file):
    file_meta_data = meta_data_extractor(file.name)
    pl_types = [pl.Int64] * 3 + [pl.Float64] * 48
    trace_data = pl.read_csv(file, schema_overrides=pl_types)
    trace_data = trace_formatter_pl(trace_data)
    trace_data = add_half_year_as_column(trace_data)
    for half_year, data in trace_data.group_by("HY"):
        file_meta_data['hy'] = half_year[0]
        path_in_new_directory = save_path_writer(file_meta_data)
        save_filepath = Path(new_directory) / path_in_new_directory
        save_filepath.parent.mkdir(parents=True, exist_ok=True)
        data = data.drop("HY")
        data.write_parquet(save_filepath)


def restructure_directory(old_directory, new_directory, meta_data_extractor, save_path_writer):
    files = get_all_filepaths(old_directory)
    # files = files[0:1] + [f for f in files if 'REZ_N1' in str(f) and '2011' in str(f)]
    max_workers = os.cpu_count() - 2
    partial_func = functools.partial(restructure_file, meta_data_extractor, save_path_writer, new_directory)
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        executor.map(partial_func, files, chunksize=100)
    # for file in files:
    #     partial_func(file)



def restructure_solar_directory(old_directory, new_directory):
    restructure_directory(old_directory, new_directory, extract_solar_trace_meta_data, write_new_solar_filepath)


def restructure_wind_directory(old_directory, new_directory):
    restructure_directory(old_directory, new_directory, extract_wind_trace_meta_data, write_new_wind_filepath)


def restructure_demand_directory(old_directory, new_directory):
    restructure_directory(old_directory, new_directory, extract_demand_trace_meta_data, write_new_demand_filepath)


if __name__ == '__main__':
    old_dir = "D:/isp_2024_data/trace_data/solar"
    new_dir = "D:/isp_2024_data/trace_data/solar_example"
    t0 = time()
    restructure_solar_directory(old_dir, new_dir)
    print(time()-t0)


