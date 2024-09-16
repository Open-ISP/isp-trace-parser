import os
import re

import pandas as pd
from fuzzywuzzy import process

from isp_workbook_parser import Parser, TableConfig


def get_all_generators(workbook_filepath):

    workbook = Parser(workbook_filepath)
    existing_gens = workbook.get_table("existing_generator_summary")
    existing_gens['Status'] = 'existing'
    committed_gens = workbook.get_table("committed_generator_summary")
    committed_gens['Status'] = 'committed'
    anticipated_gens = workbook.get_table("anticipated_projects_summary")
    anticipated_gens['Status'] = 'anticipated'
    additional_gens = workbook.get_table("additional_projects_summary")
    additional_gens['Status'] = 'additional'

    existing_gens = existing_gens.rename(columns={existing_gens.columns.values[0]: 'Generator'})
    committed_gens = committed_gens.rename(columns={committed_gens.columns.values[0]: 'Generator'})
    anticipated_gens = anticipated_gens.rename(columns={anticipated_gens.columns.values[0]: 'Generator'})
    additional_gens = additional_gens.rename(columns={additional_gens.columns.values[0]: 'Generator'})

    all_gens = pd.concat([existing_gens, committed_gens, anticipated_gens, additional_gens])

    all_gens = all_gens.loc[:, ['Generator', 'Technology type']]

    return all_gens


def gets_rezs(workbook_filepath):
    table_config = TableConfig(
        name="rezs",
        sheet_name="Renewable Energy Zones",
        header_rows=7,
        end_row=50,
        column_range="B:G"
    )
    workbook = Parser(workbook_filepath)
    rezs = workbook.get_table_from_config(table_config)
    rezs = rezs.loc[:, ['Name']]
    return rezs



def find_best_match(plant_name, csv_files):
    best_match = process.extractOne(plant_name, csv_files)
    best_match = best_match[0] if best_match else None
    best_match = best_match
    return best_match


def draft_solar_generator_to_trace_mapping(solar_generators, solar_trace_directory):
    # Remove the technology type, reference year, and file type from the filename
    csv_file_generator_names = [f[:-20] for f in os.listdir(solar_trace_directory) if f.endswith('.csv')]
    # Filter out REZ trace files
    csv_file_generator_names = [f for f in csv_file_generator_names if 'REZ' not in f]
    solar_generators['CSVFile'] = \
        solar_generators['Generator'].apply(lambda x: find_best_match(x, csv_file_generator_names))
    solar_generators = solar_generators.set_index('Generator')['CSVFile'].to_dict()
    return solar_generators


def extract_solar_rez_name(filename):
    pattern = re.compile(r"^(?P<filler>[A-Z0-9_]+)_(?P<area>[A-Za-z_\-]+)_(?P<tech>[A-Z]+)_RefYear(?P<year>\d{4})\.csv$")
    return pattern.match(filename).groupdict()['area']


def draft_solar_rez_mapping(rezs, rezs_trace_directory):
    csv_file_rez_names = [extract_solar_rez_name(f) for f in os.listdir(rezs_trace_directory) if 'REZ' in f]
    csv_file_rez_names = list(set(csv_file_rez_names))
    rezs['CSVFile'] = rezs['Name'].apply(lambda x: find_best_match(x, csv_file_rez_names))
    rezs = rezs.set_index('Name')['CSVFile'].to_dict()
    return rezs

def draft_wind_generator_to_trace_mapping(solar_generators, solar_trace_directory):
    # Remove the technology type, reference year, and file type from the filename
    csv_file_generator_names = [f[:-20] for f in os.listdir(solar_trace_directory) if f.endswith('.csv')]
    # Filter out REZ trace files
    csv_file_generator_names = [f for f in csv_file_generator_names if 'REZ' not in f]
    solar_generators['CSVFile'] = \
        solar_generators['Generator'].apply(lambda x: find_best_match(x, csv_file_generator_names))
    solar_generators = solar_generators.set_index('Generator')['CSVFile'].to_dict()
    return solar_generators


def extract_wind_rez_name(filename):
    pattern = re.compile(r"^(?P<filler>[A-Z0-9_]*)_(?P<resource>W[A-Z]+)_(?P<area>[A-Za-z_\-]+)_RefYear(?P<year>\d{"
                          r"4})\.csv$")
    match = pattern.match(filename)

    if match is not None:
        return match.groupdict()['area']
    else:
        return None

def draft_wind_rez_mapping(rezs, rezs_trace_directory):
    csv_file_rez_names = [extract_wind_rez_name(f) for f in os.listdir(rezs_trace_directory)]
    csv_file_rez_names = [f for f in csv_file_rez_names if f is not None]
    csv_file_rez_names = list(set(csv_file_rez_names))
    rezs['CSVFile'] = rezs['Name'].apply(lambda x: find_best_match(x, csv_file_rez_names))
    rezs = rezs.set_index('Name')['CSVFile'].to_dict()
    return rezs