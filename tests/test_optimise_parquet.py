from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from isp_trace_parser import optimise_parquet

TEST_DATA = Path(__file__).parent / "test_data"


@pytest.mark.parametrize(
    "expected_data, file_type",
    [("zone_data_0.parquet", "zone"), ("project_data_0.parquet", "project")],
)
def test_optimisation(parsed_trace_trace_directory, expected_data, file_type):
    """Test wind trace parsing produces expected parquet outputs (both for a sample wind project and wind zone)"""
    test_output_parquet = TEST_DATA / "output" / expected_data

    output_file = parsed_trace_trace_directory / f"{file_type}_optimised"

    optimise_parquet.partition_traces_by_columns(
        input_directory=parsed_trace_trace_directory / file_type,
        output_directory=output_file,
        partition_cols=["reference_year"],
    )

    parquet_output = pl.read_parquet(
        output_file / "reference_year=2022" / "data_0.parquet"
    )
    parquet_test = pl.read_parquet(test_output_parquet)

    sort_columns = [file_type, "resource_type", "datetime"]

    assert_frame_equal(
        parquet_test.sort(sort_columns), parquet_output.sort(sort_columns)
    )
