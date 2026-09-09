# Copyright (C) 2026 University of New South Wales
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.

import isp_trace_parser


def test_write_solar_save_names() -> None:
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


def test_write_wind_save_names() -> None:
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
