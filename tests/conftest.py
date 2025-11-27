import tempfile
from pathlib import Path

import pytest

from isp_trace_parser import solar_traces, wind_traces

TEST_DATA = Path(__file__).parent / "test_data"


@pytest.fixture(params=[True, False], ids=["concurrent", "sequential"])
def parsed_wind_trace_directory(request):
    """Fixture that performs parsing of wind trace directory once, providing
    the output directory to multiple test cases that validate different files.

    Automatically cleans up temporary resources after all tests complete.
    """

    use_concurrency = request.param

    with tempfile.TemporaryDirectory() as tmp_parsed_directory:
        tmp_parsed_directory = Path(tmp_parsed_directory)

        for file_type in ["zone", "project"]:
            filters = wind_traces.WindMetadataFilter(file_type=[file_type])
            wind_traces.parse_wind_traces(
                input_directory=TEST_DATA / "wind",
                parsed_directory=tmp_parsed_directory / file_type,
                use_concurrency=use_concurrency,
                filters=filters,
            )

        yield tmp_parsed_directory


@pytest.fixture(params=[True, False], ids=["concurrent", "sequential"])
def parsed_solar_trace_directory(request):
    """Fixture that performs parsing of solar trace directory once, providing
    the output directory to multiple test cases that validate different files.

    Automatically cleans up temporary resources after all tests complete.
    """
    use_concurrency = request.param

    with tempfile.TemporaryDirectory() as tmp_parsed_directory:
        tmp_parsed_directory = Path(tmp_parsed_directory)

        for file_type in ["zone", "project"]:
            filters = solar_traces.SolarMetadataFilter(file_type=[file_type])
            solar_traces.parse_solar_traces(
                input_directory=TEST_DATA / "solar",
                parsed_directory=tmp_parsed_directory / file_type,
                use_concurrency=use_concurrency,
                filters=filters,
            )

        yield tmp_parsed_directory
