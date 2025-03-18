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
areas = ["N12", "T3"]
sub_regions = ["CNSW", "TAS"]
area_techs = ["AB", "CD"]
area_wind_resources = ["WH", "WM"]
scenarios = ["STEP_CHANGE", "PROGRESSIVE_CHANGE"]
poe = ["POE10", "POE50"]
demand_type = ["OPSO", "PV_TOT"]
