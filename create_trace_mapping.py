import yaml

from isp_trace_parser.generator_to_trace_draft_mapper import (draft_solar_generator_to_trace_mapping,
                                                              get_all_generators, gets_rezs, draft_solar_rez_mapping)


workbook = '2024-isp-inputs-and-assumptions-workbook.xlsx'
all_generators = get_all_generators(workbook)

# solar_traces = '/media/nick/Samsung_T5/isp_2024_data/trace_data/solar/solar_2023'
# solar_gens = all_generators[all_generators['Technology type'] == 'Large scale Solar PV'].copy()
# solar_generator_mapping = draft_solar_generator_to_trace_mapping(solar_gens, solar_traces)
# with open('draft_solar_generator_mapping.yaml', 'w') as file:
#     yaml.dump(solar_generator_mapping, file, default_flow_style=False)


solar_traces = '/media/nick/Samsung_T5/isp_2024_data/trace_data/solar/solar_2023'
rezs = gets_rezs(workbook)
solar_rez_mapping = draft_solar_rez_mapping(rezs, solar_traces)
with open('solar_rez_mapping.yaml', 'w') as file:
    yaml.dump(solar_rez_mapping, file, default_flow_style=False)