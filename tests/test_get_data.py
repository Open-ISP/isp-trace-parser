import datetime
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from isp_trace_parser.get_data import (
    _year_range_to_dt_range,
    get_project_multiple_reference_years,
    get_project_single_reference_year,
    get_zone_multiple_reference_years,
    get_zone_single_reference_year,
    wind_project_single_reference_year,
)

TEST_DATA = Path(__file__).parent / "test_data"


def test_year_range_to_dt_range_fy():
    """Test financial year conversion."""
    start_dt, end_dt = _year_range_to_dt_range(2022, 2024, year_type="fy")

    assert start_dt == datetime.datetime(2021, 7, 1, 0, 0)
    assert end_dt == datetime.datetime(2024, 7, 1, 0, 0)


def test_year_range_to_dt_range_calendar():
    """Test calendar year conversion."""
    start_dt, end_dt = _year_range_to_dt_range(2022, 2024, year_type="calendar")

    assert start_dt == datetime.datetime(2022, 1, 1, 0, 0)
    assert end_dt == datetime.datetime(2025, 1, 1, 0, 0)


@pytest.mark.parametrize("year_type", ["fy", "calendar"])
def test_get_zone_single_reference_year(parsed_trace_trace_directory: Path, year_type):
    test_df_lazy = pl.scan_parquet(TEST_DATA / "output" / "RefYear2022_N2_CST.parquet")

    start_dt, end_dt = _year_range_to_dt_range(2023, 2024, year_type=year_type)

    test_df = (
        test_df_lazy.filter(
            (pl.col("datetime") > start_dt) & (pl.col("datetime") <= end_dt)
        )
        .select(["datetime", "value"])
        .collect()
        .to_pandas()
    )

    df = get_zone_single_reference_year(
        start_year=2023,
        end_year=2024,
        reference_year=2022,
        zone="N2",
        resource_type="CST",
        directory=parsed_trace_trace_directory / "zone_optimised",
        year_type=year_type,
    )

    pd.testing.assert_frame_equal(test_df, df)


def test_get_zone_multiple_reference_year(parsed_trace_trace_directory: Path):
    test_df_lazy = pl.scan_parquet(TEST_DATA / "output" / "RefYear2022_N1_WM.parquet")

    test_df = (
        test_df_lazy.filter(
            (pl.col("datetime") > datetime.datetime(2028, 7, 1))
            & (pl.col("datetime") <= datetime.datetime(2030, 7, 1))
        )
        .select(["datetime", "value"])
        .collect()
        .to_pandas()
    )

    df = get_zone_multiple_reference_years(
        reference_year_mapping={2029: 2022, 2030: 2022},
        zone="N1",
        resource_type="WM",
        directory=parsed_trace_trace_directory / "zone_optimised",
        year_type="fy",
    )

    pd.testing.assert_frame_equal(test_df, df)


def test_get_project_single_reference_year(parsed_trace_trace_directory: Path):
    test_df_lazy = pl.scan_parquet(
        TEST_DATA / "output" / "RefYear2022_Bodangora_Wind_Farm.parquet"
    )

    test_df = (
        test_df_lazy.filter(
            (pl.col("datetime") > datetime.datetime(2022, 7, 1))
            & (pl.col("datetime") <= datetime.datetime(2024, 7, 1))
        )
        .select(["datetime", "value"])
        .collect()
        .to_pandas()
    )

    df = get_project_single_reference_year(
        start_year=2023,
        end_year=2024,
        reference_year=2022,
        project="Bodangora Wind Farm",
        directory=parsed_trace_trace_directory / "project_optimised",
        year_type="fy",
    )

    pd.testing.assert_frame_equal(test_df, df)


def test_get_project_multiple_reference_year(parsed_trace_trace_directory: Path):
    test_df_lazy = pl.scan_parquet(
        TEST_DATA / "output" / "RefYear2022_Broken_Hill_Solar_Farm_FFP.parquet"
    )

    test_df = (
        test_df_lazy.filter(
            (pl.col("datetime") > datetime.datetime(2028, 7, 1))
            & (pl.col("datetime") <= datetime.datetime(2030, 7, 1))
        )
        .select(["datetime", "value"])
        .collect()
        .to_pandas()
    )

    df = get_project_multiple_reference_years(
        reference_year_mapping={2029: 2022, 2030: 2022},
        project="Broken Hill Solar Farm",
        directory=parsed_trace_trace_directory / "project_optimised",
        year_type="fy",
    )

    pd.testing.assert_frame_equal(test_df, df)


def test_explicit_select_columns(parsed_trace_trace_directory):
    df = get_zone_single_reference_year(
        start_year=2023,
        end_year=2024,
        reference_year=2022,
        zone="N2",
        resource_type="CST",
        directory=parsed_trace_trace_directory / "zone_optimised",
        select_columns=["datetime", "value", "zone"],
    )
    assert list(df.columns) == ["datetime", "value", "zone"]


def test_multi_value_filter(parsed_trace_trace_directory):
    df = get_zone_single_reference_year(
        start_year=2023,
        end_year=2024,
        reference_year=2022,
        zone=["N1", "N2"],  # List with multiple values
        resource_type="WM",
        directory=parsed_trace_trace_directory / "zone_optimised",
    )
    # Should have both zone column included in output
    assert "zone" in df.columns


def test_wind_project_single_reference_year(parsed_trace_trace_directory):
    test_df_lazy = pl.scan_parquet(
        TEST_DATA / "output" / "RefYear2022_Bodangora_Wind_Farm.parquet"
    )

    test_df = (
        test_df_lazy.filter(
            (pl.col("datetime") > datetime.datetime(2022, 7, 1))
            & (pl.col("datetime") <= datetime.datetime(2024, 7, 1))
        )
        .select(["datetime", "value"])
        .collect()
        .to_pandas()
    )

    df = wind_project_single_reference_year(
        start_year=2023,
        end_year=2024,
        reference_year=2022,
        project="Bodangora Wind Farm",
        directory=parsed_trace_trace_directory / "project_optimised",
        year_type="fy",
    )
    pd.testing.assert_frame_equal(test_df, df)
