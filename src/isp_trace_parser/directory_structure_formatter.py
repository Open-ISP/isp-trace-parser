from pathlib import Path
from time import time
from concurrent.futures import ProcessPoolExecutor
import os
import functools

import polars as pl
import yaml

from isp_trace_parser.meta_data_extractors import extract_solar_trace_meta_data, extract_wind_trace_meta_data, \
    extract_demand_trace_meta_data
from isp_trace_parser.trace_formatter import trace_formatter


def get_all_filepaths(dir):
    return [path for path in Path(dir).rglob('*.csv') if path.is_file()]


def write_new_solar_filepath(meta_data):
    m = meta_data
    name = m['name'].replace(' ', '_')

    if m['file_type'] == 'project':
        return (f"RefYear{m['year']}/{m['file_type'].capitalize()}/{name}/"
                f"RefYear{m['year']}_{name}_{m['technology']}_HalfYear{m['hy']}.parquet")
    else:
        return (f"RefYear{m['year']}/{m['file_type'].capitalize()}/{name}/{m['technology']}/"
                f"RefYear{m['year']}_{name}_{m['technology']}_HalfYear{m['hy']}.parquet")


def write_new_wind_filepath(meta_data):
    m = meta_data
    name = m['name'].replace(' ', '_')
    if 'resource_type' in m:
        return (f"RefYear{m['year']}/{m['file_type'].capitalize()}/{name}/{m['resource_type']}/"
                f"RefYear{m['year']}_{name}_{m['resource_type']}_HalfYear{m['hy']}.parquet")
    else:
        return (f"RefYear{m['year']}/{m['file_type'].capitalize()}/{name}/"
                f"RefYear{m['year']}_{name}_HalfYear{m['hy']}.parquet")


def write_new_demand_filepath(meta_data):
    m = meta_data
    area = m['area'].replace(' ', '_')
    scenario = m['scenario'].replace(' ', '_')

    return (f"{scenario}/RefYear{m['year']}/{area}/{m['poe']}/{m['descriptor']}/"
            f"{scenario}_RefYear{m['year']}_{area}_{m['poe']}_{m['descriptor']}_HalfYear{m['hy']}.parquet")


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
    average_trace = combined_traces.group_by("Datetime").agg([
        pl.col("Data").mean().alias("Data")
    ])
    return average_trace


def add_half_year_as_column(trace):

    def calculate_half_year(dt):
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


def save_half_year_chunk_of_trace(chunk, file_meta_data, half_year, new_directory, write_new_filepath):
    file_meta_data['hy'] = half_year[0]
    data = chunk.drop("HY")
    path_in_new_directory = write_new_filepath(file_meta_data)
    save_filepath = Path(new_directory) / path_in_new_directory
    save_filepath.parent.mkdir(parents=True, exist_ok=True)
    data.write_parquet(save_filepath)


def process_and_save_files(files, file_meta_data, write_new_filepath, new_directory):
    traces = read_and_format_traces(files)

    if len(traces) > 1:
        trace = calculate_average_trace(traces)
    else:
        trace = traces[0]

    trace = add_half_year_as_column(trace)

    for half_year, chunk in trace.group_by("HY"):
        save_half_year_chunk_of_trace(chunk, file_meta_data, half_year, new_directory, write_new_filepath)


def extract_meta_data_for_all_solar_files(filenames):
    file_meta_data = [extract_solar_trace_meta_data(str(f.name)) for f in filenames]
    return dict(zip(filenames, file_meta_data))


def extract_meta_data_for_all_wind_files(filenames):
    file_meta_data = [extract_wind_trace_meta_data(str(f.name)) for f in filenames]
    return dict(zip(filenames, file_meta_data))


def extract_meta_data_for_all_demand_files(filenames):
    file_meta_data = [extract_demand_trace_meta_data(str(f.name)) for f in filenames]
    return dict(zip(filenames, file_meta_data))


def get_meta_data_that_matches_trace_names(trace_names, all_old_file_meta_data):
    return {f: meta_data for f, meta_data in all_old_file_meta_data.items() if meta_data['name'] in trace_names}


def get_unique_reference_years_in_meta_data(meta_data_for_trace_files):
    return list(set(meta_data['year'] for meta_data in meta_data_for_trace_files.values()))


def get_meta_data_that_matches_reference_year(year, meta_data_for_trace_files):
    return {f: meta_data for f, meta_data in meta_data_for_trace_files.items() if meta_data['year'] == year}


def get_unique_techs_in_meta_data(meta_data_for_trace_files):
    return list(set(meta_data['technology'] for meta_data in meta_data_for_trace_files.values()))


def get_unique_resource_types_in_meta_data(meta_data_for_trace_files):
    return list(set(meta_data['resource_type'] for meta_data in meta_data_for_trace_files.values()))


def get_meta_data_that_matches_tech(tech, meta_data_for_trace_files):
    return {f: meta_data for f, meta_data in meta_data_for_trace_files.items() if meta_data['technology'] == tech}


def get_meta_data_that_matches_resource_type(resource_type, meta_data_for_trace_files):
    return {f: meta_data for f, meta_data in meta_data_for_trace_files.items() if meta_data['resource_type'] ==
            resource_type}


def get_example_meta_data_for_writing_save_name(meta_data_for_trace_files):
    return next(iter(meta_data_for_trace_files.values()))


def overwrite_meta_data_trace_name_with_save_name(example_meta_data, save_name):
    example_meta_data['name'] = save_name
    return example_meta_data


def restructure_solar_files(save_name, original_trace_names, all_old_file_meta_data, new_directory, write_new_filepath):
    meta_data_for_trace_files = get_meta_data_that_matches_trace_names(original_trace_names, all_old_file_meta_data)
    reference_years = get_unique_reference_years_in_meta_data(meta_data_for_trace_files)
    for year in reference_years:
        files_for_year = get_meta_data_that_matches_reference_year(year, meta_data_for_trace_files)
        techs = get_unique_techs_in_meta_data(files_for_year)
        for tech in techs:
            files_for_tech = get_meta_data_that_matches_tech(tech, meta_data_for_trace_files)
            example_meta_data = get_example_meta_data_for_writing_save_name(files_for_tech)
            example_meta_data = overwrite_meta_data_trace_name_with_save_name(example_meta_data, save_name)
            process_and_save_files(files_for_tech, example_meta_data, write_new_filepath, new_directory)


def restructure_wind_area_files(save_name, original_trace_names, all_old_file_meta_data, new_directory,
                                write_new_filepath):
    meta_data_for_trace_files = get_meta_data_that_matches_trace_names(original_trace_names, all_old_file_meta_data)
    reference_years = get_unique_reference_years_in_meta_data(meta_data_for_trace_files)
    for year in reference_years:
        files_for_year = get_meta_data_that_matches_reference_year(year, meta_data_for_trace_files)
        resource_types = get_unique_resource_types_in_meta_data(files_for_year)
        for resource_type in resource_types:
            files_for_tech = get_meta_data_that_matches_resource_type(resource_type, meta_data_for_trace_files)
            example_meta_data = get_example_meta_data_for_writing_save_name(files_for_tech)
            example_meta_data = overwrite_meta_data_trace_name_with_save_name(example_meta_data, save_name)
            process_and_save_files(files_for_tech, example_meta_data, write_new_filepath, new_directory)


def restructure_wind_project_files(save_name, original_trace_names, all_old_file_meta_data, new_directory,
                                   write_new_filepath):
    meta_data_for_trace_files = get_meta_data_that_matches_trace_names(original_trace_names, all_old_file_meta_data)
    reference_years = get_unique_reference_years_in_meta_data(meta_data_for_trace_files)
    for year in reference_years:
        files_for_year = get_meta_data_that_matches_reference_year(year, meta_data_for_trace_files)
        example_meta_data = get_example_meta_data_for_writing_save_name(files_for_year)
        example_meta_data = overwrite_meta_data_trace_name_with_save_name(example_meta_data, save_name)
        process_and_save_files(files_for_year, example_meta_data, write_new_filepath, new_directory)


def restructure_solar_directory(old_directory, new_directory, use_concurrency):
    files = get_all_filepaths(old_directory)
    file_meta_data = extract_meta_data_for_all_solar_files(files)
    with open(Path(__file__).parent.parent / Path('name_mapping/solar_project_mapping.yaml'), "r") as f:
        project_name_mapping = yaml.safe_load(f)
    with open(Path(__file__).parent.parent / Path('name_mapping/solar_area_mapping.yaml'), "r") as f:
        area_name_mapping = yaml.safe_load(f)
    name_mappings = {**project_name_mapping, **area_name_mapping}

    save_names, old_trace_names = zip(*name_mappings.items())

    partial_func = functools.partial(restructure_solar_files, all_old_file_meta_data=file_meta_data,
                                     new_directory=new_directory, write_new_filepath=write_new_solar_filepath)

    if use_concurrency:
        max_workers = os.cpu_count() - 2
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            results = executor.map(partial_func, save_names, old_trace_names)
            # Iterate through results to raise any errors that occurred.
            for result in results:
                result
    else:
        for save_name, old_trace_name in zip(save_names, old_trace_names):
            partial_func(save_name, old_trace_name)


def restructure_wind_project_mapping(project_name_mapping):
    return {name: mapping_data['CSVFile'] for name, mapping_data in project_name_mapping.items()}


def restructure_wind_directory(old_directory, new_directory, use_concurrency=True):
    files = get_all_filepaths(old_directory)
    file_meta_data = extract_meta_data_for_all_wind_files(files)

    with open(Path(__file__).parent.parent / Path('name_mapping/wind_project_mapping.yaml'), "r") as f:
        project_name_mappings = yaml.safe_load(f)

    project_name_mappings = restructure_wind_project_mapping(project_name_mappings)

    with open(Path(__file__).parent.parent / Path('name_mapping/wind_area_mapping.yaml'), "r") as f:
        area_name_mappings = yaml.safe_load(f)

    area_save_names, area_old_trace_names = zip(*area_name_mappings.items())

    project_save_names, project_old_trace_names = zip(*project_name_mappings.items())

    area_partial_func = functools.partial(restructure_wind_area_files, all_old_file_meta_data=file_meta_data,
                                          new_directory=new_directory, write_new_filepath=write_new_wind_filepath)

    project_partial_func = functools.partial(restructure_wind_project_files, all_old_file_meta_data=file_meta_data,
                                             new_directory=new_directory, write_new_filepath=write_new_wind_filepath)

    if use_concurrency:

        max_workers = os.cpu_count() - 2

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            results = executor.map(area_partial_func, area_save_names, area_old_trace_names)
            # Iterate through results to raise any errors that occurred.
            for result in results:
                result

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            results = executor.map(project_partial_func, project_save_names, project_old_trace_names)
            # Iterate through results to raise any errors that occurred.
            for result in results:
                result

    else:
        for save_name, old_trace_name in zip(area_save_names, area_old_trace_names):
            area_partial_func(save_name, old_trace_name)

        for save_name, old_trace_name in zip(project_save_names, project_old_trace_names):
            project_partial_func(save_name, old_trace_name)


def get_save_scenario_for_demand_trace(file_meta_data, demand_scenario_mapping):
    return demand_scenario_mapping[file_meta_data['scenario']]


def restructure_demand_file(file, demand_scenario_mapping, new_directory):
    file_meta_data = extract_demand_trace_meta_data(file.name)
    file_meta_data['scenario'] = get_save_scenario_for_demand_trace(file_meta_data, demand_scenario_mapping)
    trace = read_trace_csv(file)
    trace = trace_formatter(trace)
    trace = add_half_year_as_column(trace)
    for half_year, chunk in trace.group_by("HY"):
        save_half_year_chunk_of_trace(chunk, file_meta_data, half_year, new_directory, write_new_demand_filepath)


def restructure_demand_directory(old_directory, new_directory, use_concurrency):
    files = get_all_filepaths(old_directory)

    with open(Path(__file__).parent.parent / Path('name_mapping/demand_scenario_mapping.yaml'), "r") as f:
        demand_scenario_mapping = yaml.safe_load(f)

    partial_func = functools.partial(restructure_demand_file, demand_scenario_mapping=demand_scenario_mapping,
                                     new_directory=new_directory)

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


if __name__ == '__main__':
    t0 = time()
    old_dir = "D:/isp_2024_data/trace_data/solar"
    new_dir = "D:/isp_2024_data/trace_data/solar_parsed3"
    restructure_solar_directory(old_dir, new_dir, use_concurrency=True)
    print(time()-t0)

    t0 = time()
    old_dir = "D:/isp_2024_data/trace_data/wind"
    new_dir = "D:/isp_2024_data/trace_data/wind_parsed3"
    restructure_wind_directory(old_dir, new_dir, use_concurrency=True)
    print(time()-t0)

    t0 = time()
    old_dir = "D:/isp_2024_data/trace_data/demand"
    new_dir = "D:/isp_2024_data/trace_data/demand_parsed3"
    restructure_demand_directory(old_dir, new_dir, use_concurrency=False)
    print(time()-t0)


