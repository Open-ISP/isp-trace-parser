from pathlib import Path
import itertools

import pandas as pd
import numpy as np


import end_to_end_test_data_config as config


def generate_random_data(start_year, end_year):
    # Generate date range from July 1st of the start year to July 1st of the end year (excluding end)
    date_range = pd.date_range(start=f'{start_year}-01-01', end=f'{end_year}-01-01', freq='D', inclusive="left")

    # Create a DataFrame for the date components
    df = pd.DataFrame({
        'Year': date_range.year,
        'Month': date_range.month,
        'Day': date_range.day
    })

    # Generate random data for 48 half-hour periods (for each day in the date_range)
    random_data = np.random.rand(len(date_range), 48)

    # Create column names '01', '02', ..., '48'
    half_hour_columns = [f'{i:02d}' for i in range(1, 49)]

    # Combine the date components with the random data
    df = pd.concat([df, pd.DataFrame(random_data, columns=half_hour_columns)], axis=1)
    return df


data = generate_random_data(start_year=config.start, end_year=config.end)


def create_solar_csvs(directory):
    combos = itertools.product(config.reference_years, config.solar_projects)
    for y, project in combos:
        data.to_csv(directory / Path(f"{project}_FFP_RefYear{y}.csv"), index=False)

    combos = itertools.product(config.reference_years, config.areas, config.area_techs)
    for y, area, tech in combos:
        data.to_csv(directory / Path(f"REZ_{area}_blah_{tech}_RefYear{y}.csv"), index=False)


def create_wind_csvs(directory):

    combos = itertools.product(config.reference_years, config.wind_projects)
    for y, project in combos:
        data = generate_random_data(start_year=config.start, end_year=config.end)
        data.to_csv(directory / Path(f"{project}_RefYear{y}.csv"), index=False)

    combos = itertools.product(config.reference_years, config.areas, config.area_wind_resources)
    for y, area, resource in combos:
        data = generate_random_data(start_year=config.start, end_year=config.end)
        data.to_csv(directory / Path(f"{area}_{resource}_blah_RefYear{y}.csv"), index=False)


def create_demand_csvs(directory):
    combos = itertools.product(config.reference_years, config.sub_regions, config.poe, config.demand_type,
                               config.scenarios)
    for y, area, poe, demand_type, scenario in combos:
        data.to_csv(directory / Path(f"{area}_RefYear_{y}_{scenario}_{poe}_{demand_type}.csv"), index=False)
