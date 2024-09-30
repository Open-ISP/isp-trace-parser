import os

import pandas as pd
from fuzzywuzzy import process, fuzz
from isp_workbook_parser import Parser, TableConfig

from isp_trace_parser.metadata_extractors import (
    extract_solar_trace_metadata,
    extract_wind_trace_metadata,
)


def get_all_generators(workbook_filepath):
    workbook = Parser(workbook_filepath)
    existing_gens = workbook.get_table("existing_generator_summary")
    existing_gens["Status"] = "existing"
    committed_gens = workbook.get_table("committed_generator_summary")
    committed_gens["Status"] = "committed"
    anticipated_gens = workbook.get_table("anticipated_projects_summary")
    anticipated_gens["Status"] = "anticipated"
    additional_gens = workbook.get_table("additional_projects_summary")
    additional_gens["Status"] = "additional"

    existing_gens = existing_gens.rename(
        columns={existing_gens.columns.values[0]: "Generator"}
    )
    committed_gens = committed_gens.rename(
        columns={committed_gens.columns.values[0]: "Generator"}
    )
    anticipated_gens = anticipated_gens.rename(
        columns={anticipated_gens.columns.values[0]: "Generator"}
    )
    additional_gens = additional_gens.rename(
        columns={additional_gens.columns.values[0]: "Generator"}
    )

    all_gens = pd.concat(
        [existing_gens, committed_gens, anticipated_gens, additional_gens]
    )

    all_gens = all_gens.loc[:, ["Generator", "Technology type"]]

    return all_gens


def gets_rezs(workbook_filepath):
    table_config = TableConfig(
        name="rezs",
        sheet_name="Renewable Energy Zones",
        header_rows=7,
        end_row=50,
        column_range="B:G",
    )
    workbook = Parser(workbook_filepath)
    rezs = workbook.get_table_from_config(table_config)
    rezs = rezs.loc[:, ["Name"]]
    return rezs


def find_best_match(plant_name, csv_files):
    best_match = process.extractOne(plant_name, csv_files, scorer=fuzz.token_set_ratio)
    best_match = best_match[0] if best_match else None
    best_match = best_match
    return best_match


def find_best_match_two_columns(row, csv_files):
    match1 = process.extractOne(row["Generator"], csv_files)
    best_match_plant_name = match1[0] if match1 else None
    score_plant_name = match1[1] if match1 else None

    match2 = process.extractOne(row["DUID"], csv_files)
    best_match_duid = match2[0] if match2 else None
    score_duid = match2[1] if match2 else None

    if score_plant_name > score_duid:
        best_match = best_match_plant_name
    else:
        best_match = best_match_duid
    return best_match


def draft_solar_generator_to_trace_mapping(solar_generators, solar_trace_directory):
    csv_file_names = [
        f for f in os.listdir(solar_trace_directory) if f.endswith(".csv")
    ]
    csv_file_metadata = [extract_solar_trace_metadata(f) for f in csv_file_names]
    csv_project_names = [
        f["name"] for f in csv_file_metadata if f["file_type"] == "project"
    ]
    solar_generators["CSVFile"] = solar_generators["Generator"].apply(
        lambda x: find_best_match(x, csv_project_names)
    )
    solar_generators = solar_generators.set_index("Generator")["CSVFile"].to_dict()
    return solar_generators


def draft_solar_rez_mapping(rezs, rezs_trace_directory):
    csv_file_names = [f for f in os.listdir(rezs_trace_directory) if f.endswith(".csv")]
    csv_file_metadata = [extract_solar_trace_metadata(f) for f in csv_file_names]
    csv_rez_names = [f["name"] for f in csv_file_metadata if f["file_type"] == "area"]
    rezs["CSVFile"] = rezs["Name"].apply(lambda x: find_best_match(x, csv_rez_names))
    rezs = rezs.set_index("Name")["CSVFile"].to_dict()
    return rezs


def draft_wind_generator_to_trace_mapping(
    wind_generators, wind_duids_and_station_names, wind_trace_directory
):
    csv_file_names = [f for f in os.listdir(wind_trace_directory) if f.endswith(".csv")]
    csv_file_metadata = [extract_wind_trace_metadata(f) for f in csv_file_names]
    csv_project_names = [
        f["name"] for f in csv_file_metadata if f["file_type"] == "project"
    ]

    wind_station_names = list(wind_duids_and_station_names["Station Name"])

    wind_generators["Station Name"] = wind_generators["Generator"].apply(
        lambda x: find_best_match(x, wind_station_names)
    )
    wind_generators = pd.merge(
        wind_generators, wind_duids_and_station_names, how="left", on="Station Name"
    )
    wind_generators = wind_generators.drop_duplicates(["Generator"])

    wind_generators["CSVFile"] = wind_generators.apply(
        lambda x: find_best_match_two_columns(x, csv_project_names), axis=1
    )

    wind_generators = wind_generators.loc[
        :, ["Generator", "Station Name", "DUID", "CSVFile"]
    ]

    wind_generators = wind_generators.set_index("Generator").to_dict(orient="index")
    return wind_generators


def draft_wind_rez_mapping(rezs, rezs_trace_directory):
    csv_file_names = [f for f in os.listdir(rezs_trace_directory) if f.endswith(".csv")]
    csv_file_metadata = [extract_wind_trace_metadata(f) for f in csv_file_names]
    csv_rez_names = [f["name"] for f in csv_file_metadata if f["file_type"] == "area"]
    rezs["CSVFile"] = rezs["Name"].apply(lambda x: find_best_match(x, csv_rez_names))
    rezs = rezs.set_index("Name")["CSVFile"].to_dict()
    return rezs
