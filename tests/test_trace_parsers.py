import tempfile
from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from isp_trace_parser import demand_traces, solar_traces, wind_traces

TEST_DATA = Path(__file__).parent / "test_data"


@pytest.mark.parametrize("use_concurrency", [True, False])
def test_demand_trace_parsing(use_concurrency: bool):
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
        assert output_file.exists(), f"Expected file {expected_filename} not found"

        parquet_output = pl.read_parquet(output_file)

    parquet_test = pl.read_parquet(test_demand_output_parquet)

    assert_frame_equal(parquet_test, parquet_output)


@pytest.fixture(params=[True, False], ids=["concurrent", "sequential"])
def parsed_wind_traces(request):
    """Fixture that parses wind traces once and yields the output directory."""
    use_concurrency = request.param

    with tempfile.TemporaryDirectory() as tmp_parsed_directory:
        tmp_parsed_directory = Path(tmp_parsed_directory)

        wind_traces.parse_wind_traces(
            input_directory=TEST_DATA / "wind",
            parsed_directory=tmp_parsed_directory,
            use_concurrency=use_concurrency,
        )

        yield tmp_parsed_directory


@pytest.fixture(params=[True, False], ids=["concurrent", "sequential"])
def parsed_solar_traces(request):
    """Fixture that parses solar traces once and yields the output directory."""
    use_concurrency = request.param

    with tempfile.TemporaryDirectory() as tmp_parsed_directory:
        tmp_parsed_directory = Path(tmp_parsed_directory)

        solar_traces.parse_solar_traces(
            input_directory=TEST_DATA / "solar",
            parsed_directory=tmp_parsed_directory,
            use_concurrency=use_concurrency,
        )

        yield tmp_parsed_directory


@pytest.mark.parametrize(
    "expected_filename",
    [
        "RefYear2022_Bodangora_Wind_Farm.parquet",
        "RefYear2022_N1_WM.parquet",
    ],
)
def test_wind_trace_parsing(parsed_wind_traces, expected_filename):
    """Test wind trace parsing produces expected parquet outputs (both for a sample wind project and wind zone)"""
    test_output_parquet = TEST_DATA / "output" / expected_filename

    output_file = parsed_wind_traces / expected_filename

    parquet_output = pl.read_parquet(output_file)
    parquet_test = pl.read_parquet(test_output_parquet)

    assert_frame_equal(parquet_test, parquet_output)


@pytest.mark.parametrize(
    "expected_filename",
    [
        "RefYear2022_N2_CST.parquet",
        "RefYear2022_Broken_Hill_Solar_Farm_FFP.parquet",
    ],
)
def test_solar_trace_parsing(parsed_solar_traces, expected_filename):
    """Test solar trace parsing produces expected parquet output (both for a sample solar project and solar zone)"""
    test_output_parquet = TEST_DATA / "output" / expected_filename

    output_file = parsed_solar_traces / expected_filename

    parquet_output = pl.read_parquet(output_file)
    parquet_test = pl.read_parquet(test_output_parquet)

    assert_frame_equal(parquet_test, parquet_output)
