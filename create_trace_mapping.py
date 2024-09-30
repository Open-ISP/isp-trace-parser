import yaml

from nemosis import static_table

from isp_trace_parser.generator_to_trace_draft_mapper import (
    get_all_generators,
    gets_rezs,
    draft_wind_rez_mapping,
    draft_solar_generator_to_trace_mapping,
    draft_wind_generator_to_trace_mapping,
    draft_solar_rez_mapping,
)


workbook = "D:/isp_2024_data/2024-isp-inputs-and-assumptions-workbook.xlsx"
all_generators = get_all_generators(workbook)

solar_traces = "D:/isp_2024_data/trace_data/solar/solar_2023"
solar_gens = all_generators[
    all_generators["Technology type"] == "Large scale Solar PV"
].copy()
solar_generator_mapping = draft_solar_generator_to_trace_mapping(
    solar_gens, solar_traces
)
with open("draft_solar_generator_mapping.yaml", "w") as file:
    yaml.dump(solar_generator_mapping, file, default_flow_style=False)


solar_traces = "/media/nick/Samsung_T5/isp_2024_data/trace_data/solar/solar_2023"
rezs = gets_rezs(workbook)
solar_rez_mapping = draft_solar_rez_mapping(rezs, solar_traces)
with open("solar_area_mapping.yaml", "w") as file:
    yaml.dump(solar_rez_mapping, file, default_flow_style=False)

duids_and_station_names = static_table(
    "Generators and Scheduled Loads",
    "D:/nemosis_data_cache",
    select_columns=["Station Name", "DUID", "Fuel Source - Primary"],
    update_static_file=True,
)
wind_duids_and_station_names = duids_and_station_names[
    duids_and_station_names["Fuel Source - Primary"] == "Wind"
]
wind_duids_and_station_names = wind_duids_and_station_names.drop(
    columns=["Fuel Source - Primary"]
)
wind_traces = "D:/isp_2024_data/trace_data/wind/wind_2023"
wind_gens = all_generators[all_generators["Technology type"] == "Wind"].copy()
wind_gens = wind_gens.drop(columns=["Technology type"])
wind_generator_mapping = draft_wind_generator_to_trace_mapping(
    wind_gens, wind_duids_and_station_names, wind_traces
)
with open("draft_wind_generator_mapping.yaml", "w") as file:
    yaml.dump(wind_generator_mapping, file, default_flow_style=False, sort_keys=False)


wind_traces = "D:/isp_2024_data/trace_data/wind/wind_2023"
rezs = gets_rezs(workbook)
wind_rez_mapping = draft_wind_rez_mapping(rezs, wind_traces)
with open("draft_wind_rez_mapping.yaml", "w") as file:
    yaml.dump(wind_rez_mapping, file, default_flow_style=False)
