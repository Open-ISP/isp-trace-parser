# Copyright (C) 2026 University of New South Wales
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.

start = 2030
end = 2032
half_years = [1, 2]
reference_years = [2011, 2012]
solar_projects = ["Adelaide_Desal", "Wellington_North", "Murray_Bridge-Onkaparinga_2"]
wind_projects = {
    "Snowtown S2 Wind Farm": ["SNOWSTH1", "SNOWNTH1"],
    "Bango 973 Wind Farm": "BANGOWF1",
    "Goyder South Wind Farm 1A": "Goyder_South",  # Added the two Goyders as regression test because they both use the same CSV which was causing a bug.
    "Goyder South Wind Farm 1B": "Goyder_South",
}
zones = ["N12", "T3"]
sub_regions = ["CNSW", "TAS"]
zone_techs = ["AB", "CD"]
zone_wind_resources = ["WH", "WM"]
scenarios = ["STEP_CHANGE", "PROGRESSIVE_CHANGE"]
poe = ["POE10", "POE50"]
demand_type = ["OPSO", "PV_TOT"]
