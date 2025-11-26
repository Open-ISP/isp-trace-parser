import tempfile
from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from isp_trace_parser import demand_traces

TEST_DATA = Path(__file__).parent / "test_data"


@pytest.mark.parametrize("use_concurrency", [True, False])
def test_demand_traces(use_concurrency: bool):
    """Test demand trace parsing produces expected parquet output."""
    test_demand_csv_directory = TEST_DATA / "demand"
    expected_filename = (
        "Green_Energy_Exports_RefYear2011_CNSW_POE10_OPSO_MODELLING.parquet"
    )
    test_demand_output_parquet = TEST_DATA / "output" / expected_filename

    with tempfile.TemporaryDirectory() as tmp_parsed_directory:
        tmp_parsed_directory = Path(tmp_parsed_directory)

        demand_traces.parse_demand_traces(
            input_directory=test_demand_csv_directory,
            parsed_directory=tmp_parsed_directory,
            use_concurrency=use_concurrency,
        )

        output_file = tmp_parsed_directory / expected_filename

        parquet_output = pl.read_parquet(output_file)

    parquet_test = pl.read_parquet(test_demand_output_parquet)

    assert_frame_equal(parquet_test, parquet_output)
