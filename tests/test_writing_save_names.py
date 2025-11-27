import isp_trace_parser


def test_write_solar_save_names():
    meta_data = {
        "name": "a",
        "reference_year": "1",
        "resource_type": "x",
        "file_type": "project",
    }

    save_filepath = isp_trace_parser.solar_traces.write_output_solar_filename(meta_data)

    assert str(save_filepath) == "RefYear1_a_x.parquet"

    meta_data = {
        "name": "a",
        "reference_year": "1",
        "resource_type": "x",
        "file_type": "zone",
    }

    save_filepath = isp_trace_parser.solar_traces.write_output_solar_filename(meta_data)

    assert str(save_filepath) == "RefYear1_a_x.parquet"


def test_write_wind_save_names():
    meta_data = {
        "name": "a",
        "reference_year": "1",
        "file_type": "project",
        "resource_type": "wind",
    }

    save_filepath = isp_trace_parser.wind_traces.write_output_wind_project_filename(
        meta_data
    )

    assert str(save_filepath) == "RefYear1_a.parquet"

    meta_data = {
        "name": "a",
        "reference_year": "1",
        "resource_type": "x",
        "file_type": "zone",
    }

    save_filepath = isp_trace_parser.wind_traces.write_output_wind_zone_filename(
        meta_data
    )

    assert str(save_filepath) == "RefYear1_a_x.parquet"


def test_write_demand_save_names():
    meta_data = {
        "scenario": "a",
        "reference_year": "1",
        "subregion": "x",
        "poe": "poe10",
        "demand_type": "y",
    }

    save_filepath = isp_trace_parser.demand_traces.write_new_demand_filename(meta_data)

    assert str(save_filepath) == "a_RefYear1_x_poe10_y.parquet"
