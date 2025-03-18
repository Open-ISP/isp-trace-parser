import itertools
import multiprocessing as mp
from pathlib import Path

import end_to_end_test_data_config as config
import pytest
import yaml
from create_end_to_end_test_data import (
    create_demand_csvs,
    create_solar_csvs,
    create_wind_csvs,
)

from isp_trace_parser import parse_demand_traces, parse_solar_traces, parse_wind_traces

mp.set_start_method("spawn", force=True)


@pytest.mark.parametrize("use_concurrency", [True, False])
def test_solar_directory_restructure(tmp_path, use_concurrency):
    old_dir = tmp_path / Path("solar_old_directory")
    old_dir.mkdir()
    new_dir = tmp_path / Path("solar_new_directory")
    new_dir.mkdir()
    create_solar_csvs(old_dir)
    parse_solar_traces(old_dir, new_dir, use_concurrency=False)
    project_mapping_yaml = Path(__file__).parent.parent / Path(
        "src/isp_trace_name_mapping_configs/solar_project_mapping.yaml"
    )
    with open(project_mapping_yaml) as f:
        project_name_mapping = yaml.safe_load(f)

    project_name_mapping = {v: k for k, v in project_name_mapping.items()}

    combos = itertools.product(
        config.reference_years,
        range(config.start, config.end),
        config.half_years,
        config.solar_projects,
    )
    for ry, y, half_year, project in combos:
        project = project_name_mapping[project].replace(" ", "_")
        parquet_file = Path(
            f"RefYear{ry}/Project/{project}/RefYear{ry}_{project}_FFP_HalfYear{y}-"
            f"{half_year}.parquet"
        )
        assert (new_dir / parquet_file).is_file()

    combos = itertools.product(
        config.reference_years,
        range(config.start, config.end),
        config.half_years,
        config.areas,
        config.area_techs,
    )
    for ry, y, half_year, area, tech in combos:
        parquet_file = Path(
            f"RefYear{ry}/Area/{area}/{tech}/RefYear{ry}_{area}_{tech}_HalfYear{y}-"
            f"{half_year}.parquet"
        )
        assert (new_dir / parquet_file).is_file()


@pytest.mark.parametrize("use_concurrency", [True, False])
def test_wind_directory_restructure(tmp_path, use_concurrency):
    old_dir = tmp_path / Path("wind_old_directory")
    old_dir.mkdir()
    new_dir = tmp_path / Path("wind_new_directory")
    new_dir.mkdir()
    create_wind_csvs(old_dir)
    parse_wind_traces(old_dir, new_dir, use_concurrency=use_concurrency)

    combos = itertools.product(
        config.reference_years,
        range(config.start, config.end),
        config.half_years,
        config.wind_projects.keys(),
    )
    for ry, y, half_year, project in combos:
        project = project.replace(" ", "_")
        parquet_file = Path(
            f"RefYear{ry}/Project/{project}/RefYear{ry}_{project}_HalfYear{y}-"
            f"{half_year}.parquet"
        )
        assert (new_dir / parquet_file).is_file()

    combos = itertools.product(
        config.reference_years,
        range(config.start, config.end),
        config.half_years,
        config.areas,
        config.area_wind_resources,
    )
    for ry, y, half_year, area, resource in combos:
        parquet_file = Path(
            f"RefYear{ry}/Area/{area}/{resource}/RefYear{ry}_{area}_{resource}_HalfYear{y}-"
            f"{half_year}.parquet"
        )
        assert (new_dir / parquet_file).is_file()


@pytest.mark.parametrize("use_concurrency", [True, False])
def test_demand_directory_restructure(tmp_path, use_concurrency):
    old_dir = tmp_path / Path("demand_old_directory")
    old_dir.mkdir()
    new_dir = tmp_path / Path("demand_new_directory")
    new_dir.mkdir()
    create_demand_csvs(old_dir)
    parse_demand_traces(old_dir, new_dir, use_concurrency=use_concurrency)
    demand_scenario_mapping_yaml = Path(__file__).parent.parent / Path(
        "src/isp_trace_name_mapping_configs/demand_scenario_mapping.yaml"
    )
    with open(demand_scenario_mapping_yaml) as f:
        demand_scenario_mapping = yaml.safe_load(f)

    combos = itertools.product(
        config.reference_years,
        range(config.start, config.end),
        config.half_years,
        config.sub_regions,
        config.poe,
        config.demand_type,
        config.scenarios,
    )
    for ry, y, half_year, subregion, poe, demand_type, scenario in combos:
        scenario = demand_scenario_mapping[scenario].replace(" ", "_")
        parquet_file = Path(
            f"{scenario}/RefYear{ry}/{subregion}/{poe}/{demand_type}/"
            f"{scenario}_RefYear{ry}_{subregion}_{poe}_{demand_type}_HalfYear{y}-{half_year}.parquet"
        )
        assert (new_dir / parquet_file).is_file()
