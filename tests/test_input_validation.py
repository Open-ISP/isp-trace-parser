from pathlib import Path

import pytest
from pydantic import ValidationError

from isp_trace_parser import (
    DemandMetadataFilter,
    SolarMetadataFilter,
    WindMetadataFilter,
    construct_reference_year_mapping,
    input_validation,
    parse_demand_traces,
    parse_solar_traces,
    parse_wind_traces,
)


# Tests for Pydantic classes
@pytest.mark.parametrize(
    "valid_input",
    [
        {"name": ["A", "B"]},
        {"file_type": ["area", "project"]},
        {"technology": ["SAT", "FFP", "CST"]},
        {"reference_year": [2011, 2012]},
        {
            "name": ["A"],
            "file_type": ["area"],
            "technology": ["SAT"],
            "reference_year": [2011],
        },
    ],
)
def test_solar_metadata_filter_valid(valid_input):
    assert SolarMetadataFilter(**valid_input)


@pytest.mark.parametrize(
    "invalid_input,expected_error",
    [
        ({"file_type": ["invalid"]}, "Input should be 'area' or 'project'"),
        ({"technology": ["invalid"]}, "Input should be 'SAT', 'FFP' or 'CST'"),
        ({"reference_year": ["invalid"]}, "Input should be a valid integer"),
        ({"name": 123}, "Input should be a valid list"),
    ],
)
def test_solar_metadata_filter_invalid(invalid_input, expected_error):
    with pytest.raises(ValidationError, match=expected_error):
        SolarMetadataFilter(**invalid_input)


@pytest.mark.parametrize(
    "valid_input",
    [
        {"name": ["A", "B"]},
        {"file_type": ["area", "project"]},
        {"resource_quality": ["WH", "WM", "WL", "WX"]},
        {"reference_year": [2011, 2012]},
        {
            "name": ["A"],
            "file_type": ["area"],
            "resource_quality": ["WH"],
            "reference_year": [2011],
        },
    ],
)
def test_wind_metadata_filter_valid(valid_input):
    assert WindMetadataFilter(**valid_input)


@pytest.mark.parametrize(
    "invalid_input,expected_error",
    [
        ({"file_type": ["invalid"]}, "Input should be 'area' or 'project'"),
        ({"resource_quality": ["invalid"]}, "Input should be 'WH', 'WM', 'WL' or 'WX'"),
        ({"reference_year": ["invalid"]}, "Input should be a valid integer"),
        ({"name": 123}, "Input should be a valid list"),
    ],
)
def test_wind_metadata_filter_invalid(invalid_input, expected_error):
    with pytest.raises(ValidationError, match=expected_error):
        WindMetadataFilter(**invalid_input)


@pytest.mark.parametrize(
    "valid_input",
    [
        {"subregion": ["CNSW", "TAS"]},
        {"scenario": ["Step Change", "Progressive Change", "Green Energy Exports"]},
        {"poe": ["POE50", "POE10"]},
        {"demand_type": ["OPSO_MODELLING", "OPSO_MODELLING_PVLITE", "PV_TOT"]},
        {"reference_year": [2011, 2012]},
        {
            "subregion": ["CNSW"],
            "scenario": ["Step Change"],
            "poe": ["POE50"],
            "demand_type": ["OPSO_MODELLING"],
            "reference_year": [2011],
        },
    ],
)
def test_demand_metadata_filter_valid(valid_input):
    assert DemandMetadataFilter(**valid_input)


@pytest.mark.parametrize(
    "invalid_input,expected_error",
    [
        (
            {"scenario": ["invalid"]},
            "Input should be 'Step Change', 'Progressive Change' or 'Green Energy Exports'",
        ),
        ({"poe": ["invalid"]}, "Input should be 'POE50' or 'POE10'"),
        (
            {"demand_type": ["invalid"]},
            "Input should be 'OPSO_MODELLING', 'OPSO_MODELLING_PVLITE' or 'PV_TOT'",
        ),
        ({"reference_year": ["invalid"]}, "Input should be a valid integer"),
        ({"subregion": 123}, "Input should be a valid list"),
    ],
)
def test_demand_metadata_filter_invalid(invalid_input, expected_error):
    with pytest.raises(ValidationError, match=expected_error):
        DemandMetadataFilter(**invalid_input)


# Tests for @validate_call decorator
@pytest.mark.parametrize(
    "invalid_input",
    [
        {"input_directory": 123, "parsed_directory": "valid_path"},
        {"input_directory": "valid_path", "parsed_directory": 123},
        {
            "input_directory": "valid_path",
            "parsed_directory": "valid_path",
            "use_concurrency": "not_a_bool",
        },
        {
            "input_directory": "valid_path",
            "parsed_directory": "valid_path",
            "filters": "not_a_filter",
        },
    ],
)
def test_parse_traces_validation(invalid_input):
    with pytest.raises(ValidationError):
        parse_solar_traces(**invalid_input)
    with pytest.raises(ValidationError):
        parse_wind_traces(**invalid_input)
    with pytest.raises(ValidationError):
        parse_demand_traces(**invalid_input)


@pytest.mark.parametrize(
    "invalid_input",
    [
        {"start_year": "x", "end_year": 2035, "reference_years": [2011, 2013, 2018]},
        {"start_year": 2030, "end_year": "x", "reference_years": [2011, 2013, 2018]},
        {"start_year": 2030, "end_year": 2035, "reference_years": "not_a_list"},
        {"start_year": 2030, "end_year": 2035, "reference_years": [2011, "x", 2018]},
    ],
)
def test_construct_reference_year_mapping_validation_invalid(invalid_input):
    with pytest.raises(ValidationError):
        construct_reference_year_mapping(**invalid_input)


def test_construct_reference_year_mapping_validation_valid():
    result = construct_reference_year_mapping(
        start_year=2030, end_year=2035, reference_years=[2011, 2013, 2018]
    )
    assert isinstance(result, dict)
    assert len(result) == 6
    assert all(isinstance(k, int) and isinstance(v, int) for k, v in result.items())


# Tests for custom input validation functions
def test_input_directory(tmp_path):
    valid_dir = tmp_path / "valid_dir"
    valid_dir.mkdir()
    assert input_validation.input_directory(valid_dir) == valid_dir

    with pytest.raises(ValueError, match="Directory .* does not exist"):
        input_validation.input_directory(tmp_path / "non_existent_dir")


@pytest.mark.parametrize(
    "valid_path",
    [
        "/valid/path",
        Path("/valid/path"),
    ],
)
def test_parsed_directory_valid(valid_path):
    result = input_validation.parsed_directory(valid_path)
    assert isinstance(result, Path)


@pytest.mark.parametrize(
    "invalid_path",
    [
        123,
        None,
        [],
    ],
)
def test_parsed_directory_invalid(invalid_path):
    with pytest.raises(ValueError, match="Invalid parsed directory path"):
        input_validation.parsed_directory(invalid_path)


@pytest.mark.parametrize(
    "valid_path",
    [
        "/valid/path",
        Path("/valid/path"),
    ],
)
def test_is_valid_path_valid(valid_path):
    result = input_validation.is_valid_path(valid_path)
    assert isinstance(result, Path)


@pytest.mark.parametrize(
    "invalid_path",
    [
        123,
        None,
        [],
    ],
)
def test_is_valid_path_invalid(invalid_path):
    with pytest.raises(ValueError, match="Invalid parsed directory path"):
        input_validation.is_valid_path(invalid_path)


@pytest.mark.parametrize(
    "start,end",
    [
        (2020, 2025),
        (2020, 2020),
        (-10, 0),
    ],
)
def test_start_year_before_end_year_valid(start, end):
    assert input_validation.start_year_before_end_year(start, end) is None


@pytest.mark.parametrize(
    "start,end",
    [
        (2025, 2020),
        (0, -10),
        (2020, 2019),
    ],
)
def test_start_year_before_end_year_invalid(start, end):
    with pytest.raises(ValueError, match="Start year .* < end year"):
        input_validation.start_year_before_end_year(start, end)
