import isp_trace_parser


def test_write_solar_save_names():
    meta_data = {
        "name": "a",
        "reference_year": "1",
        "technology": "x",
        "file_type": "project",
        "hy": "2",
    }

    save_filepath = isp_trace_parser.solar_traces.write_output_solar_filepath(meta_data)

    assert str(save_filepath) == "RefYear1/Project/a/RefYear1_a_x_HalfYear2.parquet"

    meta_data = {
        "name": "a",
        "reference_year": "1",
        "technology": "x",
        "file_type": "area",
        "hy": "2",
    }

    save_filepath = isp_trace_parser.solar_traces.write_output_solar_filepath(meta_data)

    assert str(save_filepath) == "RefYear1/Area/a/x/RefYear1_a_x_HalfYear2.parquet"


def test_write_wind_save_names():
    meta_data = {"name": "a", "reference_year": "1", "file_type": "project", "hy": "2"}

    save_filepath = isp_trace_parser.wind_traces.write_output_wind_project_filepath(
        meta_data
    )

    assert str(save_filepath) == "RefYear1/Project/a/RefYear1_a_HalfYear2.parquet"

    meta_data = {
        "name": "a",
        "reference_year": "1",
        "resource_quality": "x",
        "file_type": "area",
        "hy": "2",
    }

    save_filepath = isp_trace_parser.wind_traces.write_output_wind_area_filepath(
        meta_data
    )

    assert str(save_filepath) == "RefYear1/Area/a/x/RefYear1_a_x_HalfYear2.parquet"


def test_write_demand_save_names():
    meta_data = {
        "scenario": "a",
        "reference_year": "1",
        "subregion": "x",
        "poe": "poe10",
        "demand_type": "y",
        "hy": "2",
    }

    save_filepath = isp_trace_parser.demand_traces.write_new_demand_filepath(meta_data)

    assert (
        str(save_filepath)
        == "a/RefYear1/x/poe10/y/a_RefYear1_x_poe10_y_HalfYear2.parquet"
    )
