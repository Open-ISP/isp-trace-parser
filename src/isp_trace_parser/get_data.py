import itertools
from pathlib import Path


import polars as pl
import pandas as pd


def solar_project_single_reference_year(
    start_year, end_year, reference_year, project, directory, year_type="fy"
):
    """Reads solar project trace data from an output directory created by isp_trace_parser.restructure_solar_directory.

    Examples:

    >>> solar_project_single_reference_year(
    ... start_year=2022,
    ... end_year=2024,
    ... reference_year=2011,
    ... project='Adelaide Desalination Plant Solar Farm',
    ... directory='example_parsed_data/solar')
                     Datetime  Value
    0     2021-07-01 00:30:00    0.0
    1     2021-07-01 01:00:00    0.0
    2     2021-07-01 01:30:00    0.0
    3     2021-07-01 02:00:00    0.0
    4     2021-07-01 02:30:00    0.0
    ...                   ...   ...
    52603 2024-06-30 22:00:00    0.0
    52604 2024-06-30 22:30:00    0.0
    52605 2024-06-30 23:00:00    0.0
    52606 2024-06-30 23:30:00    0.0
    52607 2024-07-01 00:00:00    0.0
    <BLANKLINE>
    [52608 rows x 2 columns]

    Args:
        start_year: int, start of time window to return trace data for.
        end_year: int, end of time window (inclusive) to return trace data for.
        reference_year: int, the reference year of the trace data to retrieve.
        project: str, the name of solar project (generator) to return data for. Names should as given in the IASR
            workbook.
        directory: str or pathlib.Path, the directory were the trace data is stored. Trace data needs to be in the
            format as produced by isp_trace_parser.restructure_solar_directory.
        year_type: str, 'fy' or 'calendar', if 'fy' then time filtering is by financial year with start_year and
            end_year specifiying the financial year to return data for, using year ending nomenclature (2016 ->
            FY2015/2016). If 'calendar', then filtering is by calendar year.

    Returns: pd.DataFrame with columns Datetime andValue
    """
    kwargs = {"project": project}
    return generic_single_reference_year(
        "solar_project",
        start_year,
        end_year,
        reference_year,
        year_type,
        directory,
        **kwargs,
    )


def solar_project_multiple_reference_years(
    reference_years, project, directory, year_type="fy"
):
    """Reads solar project trace data from an output directory created by isp_trace_parser.restructure_solar_directory.

    Examples:

    >>> solar_project_multiple_reference_years(
    ... reference_years={2022: 2011, 2024: 2012},
    ... project='Adelaide Desalination Plant Solar Farm',
    ... directory='example_parsed_data/solar')
                     Datetime  Value
    0     2021-07-01 00:30:00    0.0
    1     2021-07-01 01:00:00    0.0
    2     2021-07-01 01:30:00    0.0
    3     2021-07-01 02:00:00    0.0
    4     2021-07-01 02:30:00    0.0
    ...                   ...   ...
    17563 2024-06-30 22:00:00    0.0
    17564 2024-06-30 22:30:00    0.0
    17565 2024-06-30 23:00:00    0.0
    17566 2024-06-30 23:30:00    0.0
    17567 2024-07-01 00:00:00    0.0
    <BLANKLINE>
    [35088 rows x 2 columns]


    Args:
        reference_years: dict{int: int}, a mapping of the which reference year (value) to retrieve data from for
            each financial or calendar year (value).
        project: str, the name of solar project (generator) to return data for. Names should as given in the IASR
            workbook.
        directory: str or pathlib.Path, the directory were the trace data is stored. Trace data needs to be in the
            format as produced by isp_trace_parser.restructure_solar_directory.
        year_type: str, 'fy' or 'calendar', if 'fy' then time filtering is by financial year with start_year and
            end_year specifiying the financial year to return data for, using year ending nomenclature (2016 ->
            FY2015/2016). If 'calendar', then filtering is by calendar year.

    Returns: pd.DataFrame with columns Datetime andValue
    """
    kwargs = {"project": project}
    return generic_multi_reference_year_mapping(
        "solar_project", reference_years, year_type, directory, **kwargs
    )


def solar_area_single_reference_year(
    start_year, end_year, reference_year, area, technology, directory, year_type="fy"
):
    """Reads solar area trace data from an output directory created by isp_trace_parser.restructure_solar_directory.

    Examples:

    >>> solar_area_single_reference_year(
    ... start_year=2022,
    ... end_year=2024,
    ... reference_year=2011,
    ... area='Q1',
    ... technology='SAT',
    ... directory='example_parsed_data/solar')
                     Datetime  Value
    0     2021-07-01 00:30:00    0.0
    1     2021-07-01 01:00:00    0.0
    2     2021-07-01 01:30:00    0.0
    3     2021-07-01 02:00:00    0.0
    4     2021-07-01 02:30:00    0.0
    ...                   ...   ...
    52603 2024-06-30 22:00:00    0.0
    52604 2024-06-30 22:30:00    0.0
    52605 2024-06-30 23:00:00    0.0
    52606 2024-06-30 23:30:00    0.0
    52607 2024-07-01 00:00:00    0.0
    <BLANKLINE>
    [52608 rows x 2 columns]


    Args:
        start_year: int, start of time window to return trace data for.
        end_year: int, end of time window (inclusive) to return trace data for.
        reference_year: int, the reference year of the trace data to retrieve.
        area: str, the ISP code for the area (typically a REZ) to return data for. Codes need to be as given in the IASR
            workbook.
        technology: str, the technology to return trace data for.
        directory: str or pathlib.Path, the directory were the trace data is stored. Trace data needs to be in the
            format as produced by isp_trace_parser.restructure_solar_directory.
        year_type: str, 'fy' or 'calendar', if 'fy' then time filtering is by financial year with start_year and
            end_year specifiying the financial year to return data for, using year ending nomenclature (2016 ->
            FY2015/2016). If 'calendar', then filtering is by calendar year.

    Returns: pd.DataFrame with columns Datetime andValue
    """
    kwargs = {"area": area, "technology": technology}
    return generic_single_reference_year(
        "solar_area",
        start_year,
        end_year,
        reference_year,
        year_type,
        directory,
        **kwargs,
    )


def solar_area_multiple_reference_years(
    reference_years, area, technology, directory, year_type="fy"
):
    """Reads solar area trace data from an output directory created by isp_trace_parser.restructure_solar_directory.

    Examples:

    >>> solar_area_multiple_reference_years(
    ... reference_years={2022: 2011, 2024: 2012},
    ... area='Q1',
    ... technology='SAT',
    ... directory='example_parsed_data/solar')
                     Datetime  Value
    0     2021-07-01 00:30:00    0.0
    1     2021-07-01 01:00:00    0.0
    2     2021-07-01 01:30:00    0.0
    3     2021-07-01 02:00:00    0.0
    4     2021-07-01 02:30:00    0.0
    ...                   ...   ...
    17563 2024-06-30 22:00:00    0.0
    17564 2024-06-30 22:30:00    0.0
    17565 2024-06-30 23:00:00    0.0
    17566 2024-06-30 23:30:00    0.0
    17567 2024-07-01 00:00:00    0.0
    <BLANKLINE>
    [35088 rows x 2 columns]


    Args:
        reference_years: dict{int: int}, a mapping of the which reference year (value) to retrieve data from for
            each financial or calendar year (value).
        area: str, the ISP code for the area (typically a REZ) to return data for. Codes need to be as given in the IASR
            workbook.
        technology: str, the technology to return trace data for.
        directory: str or pathlib.Path, the directory were the trace data is stored. Trace data needs to be in the
            format as produced by isp_trace_parser.restructure_solar_directory.
        year_type: str, 'fy' or 'calendar', if 'fy' then time filtering is by financial year with start_year and
            end_year specifiying the financial year to return data for, using year ending nomenclature (2016 ->
            FY2015/2016). If 'calendar', then filtering is by calendar year.

    Returns: pd.DataFrame with columns Datetime andValue
    """
    kwargs = {"area": area, "technology": technology}
    return generic_multi_reference_year_mapping(
        "solar_area", reference_years, year_type, directory, **kwargs
    )


def wind_project_single_reference_year(
    start_year, end_year, reference_year, project, directory, year_type="fy"
):
    """Reads wind project trace data from an output directory created by isp_trace_parser.restructure_solar_directory.

    Examples:

    >>> wind_project_single_reference_year(
    ... start_year=2022,
    ... end_year=2024,
    ... reference_year=2011,
    ... project='Bango 973 Wind Farm',
    ... directory='example_parsed_data/wind')
                     Datetime     Value
    0     2021-07-01 00:30:00  0.162167
    1     2021-07-01 01:00:00  0.153542
    2     2021-07-01 01:30:00  0.131597
    3     2021-07-01 02:00:00  0.106550
    4     2021-07-01 02:30:00  0.086972
    ...                   ...       ...
    52603 2024-06-30 22:00:00  0.044304
    52604 2024-06-30 22:30:00  0.046103
    52605 2024-06-30 23:00:00  0.046103
    52606 2024-06-30 23:30:00  0.057853
    52607 2024-07-01 00:00:00  0.076900
    <BLANKLINE>
    [52608 rows x 2 columns]


    Args:
        start_year: int, start of time window to return trace data for.
        end_year: int, end of time window (inclusive) to return trace data for.
        reference_year: int, the reference year of the trace data to retrieve.
        project: str, the name of solar project (generator) to return data for. Names should as given in the IASR
            workbook.
        directory: str or pathlib.Path, the directory were the trace data is stored. Trace data needs to be in the
            format as produced by isp_trace_parser.restructure_solar_directory.
        year_type: str, 'fy' or 'calendar', if 'fy' then time filtering is by financial year with start_year and
            end_year specifiying the financial year to return data for, using year ending nomenclature (2016 ->
            FY2015/2016). If 'calendar', then filtering is by calendar year.

    Returns: pd.DataFrame with columns Datetime andValue
    """
    kwargs = {"project": project}
    return generic_single_reference_year(
        "wind_project",
        start_year,
        end_year,
        reference_year,
        year_type,
        directory,
        **kwargs,
    )


def wind_project_multiple_reference_years(
    reference_years, project, directory, year_type="fy"
):
    """Reads wind project trace data from an output directory created by isp_trace_parser.restructure_solar_directory.

    Examples:

    >>> wind_project_multiple_reference_years(
    ... reference_years={2022: 2011, 2024: 2012},
    ... project='Bango 973 Wind Farm',
    ... directory='example_parsed_data/wind')
                     Datetime     Value
    0     2021-07-01 00:30:00  0.162167
    1     2021-07-01 01:00:00  0.153542
    2     2021-07-01 01:30:00  0.131597
    3     2021-07-01 02:00:00  0.106550
    4     2021-07-01 02:30:00  0.086972
    ...                   ...       ...
    17563 2024-06-30 22:00:00  0.932596
    17564 2024-06-30 22:30:00  0.956507
    17565 2024-06-30 23:00:00  0.950307
    17566 2024-06-30 23:30:00  0.958626
    17567 2024-07-01 00:00:00  0.943996
    <BLANKLINE>
    [35088 rows x 2 columns]


    Args:
        reference_years: dict{int: int}, a mapping of the which reference year (value) to retrieve data from for
            each financial or calendar year (value).
        project: str, the name of solar project (generator) to return data for. Names should as given in the IASR
            workbook.
        directory: str or pathlib.Path, the directory were the trace data is stored. Trace data needs to be in the
            format as produced by isp_trace_parser.restructure_solar_directory.
        year_type: str, 'fy' or 'calendar', if 'fy' then time filtering is by financial year with start_year and
            end_year specifiying the financial year to return data for, using year ending nomenclature (2016 ->
            FY2015/2016). If 'calendar', then filtering is by calendar year.

    Returns: pd.DataFrame with columns Datetime andValue
    """
    kwargs = {"project": project}
    return generic_multi_reference_year_mapping(
        "wind_project", reference_years, year_type, directory, **kwargs
    )


def wind_area_single_reference_year(
    start_year,
    end_year,
    reference_year,
    area,
    resource_quality,
    directory,
    year_type="fy",
):
    """Reads wind area trace data from an output directory created by isp_trace_parser.restructure_solar_directory.

    Examples:

    >>> wind_area_single_reference_year(
    ... start_year=2022,
    ... end_year=2024,
    ... reference_year=2011,
    ... area='Q1',
    ... resource_quality='WH',
    ... directory='example_parsed_data/wind')
                     Datetime     Value
    0     2021-07-01 00:30:00  0.790868
    1     2021-07-01 01:00:00  0.816555
    2     2021-07-01 01:30:00  0.762126
    3     2021-07-01 02:00:00  0.787833
    4     2021-07-01 02:30:00  0.821139
    ...                   ...       ...
    52603 2024-06-30 22:00:00  0.842419
    52604 2024-06-30 22:30:00  0.850748
    52605 2024-06-30 23:00:00  0.850466
    52606 2024-06-30 23:30:00  0.853165
    52607 2024-07-01 00:00:00  0.816184
    <BLANKLINE>
    [52608 rows x 2 columns]


    Args:
        start_year: int, start of time window to return trace data for.
        end_year: int, end of time window (inclusive) to return trace data for.
        reference_year: int, the reference year of the trace data to retrieve.
        area: str, the ISP code for the area (typically a REZ) to return data for. Codes need to be as given in the IASR
            workbook.
        resource_quality: str, the resource quality of the trace to retrieve usaual 'WH' or 'WM'.
        directory: str or pathlib.Path, the directory were the trace data is stored. Trace data needs to be in the
            format as produced by isp_trace_parser.restructure_solar_directory.
        year_type: str, 'fy' or 'calendar', if 'fy' then time filtering is by financial year with start_year and
            end_year specifiying the financial year to return data for, using year ending nomenclature (2016 ->
            FY2015/2016). If 'calendar', then filtering is by calendar year.

    Returns: pd.DataFrame with columns Datetime andValue
    """
    kwargs = {"area": area, "resource_type": resource_quality}
    return generic_single_reference_year(
        "wind_area",
        start_year,
        end_year,
        reference_year,
        year_type,
        directory,
        **kwargs,
    )


def wind_area_multiple_reference_years(
    reference_years, area, resource_quality, directory, year_type="fy"
):
    """Reads wind area trace data from an output directory created by isp_trace_parser.restructure_solar_directory.

    Examples:

    >>> wind_area_multiple_reference_years(
    ... reference_years={2022: 2011, 2024: 2012},
    ... area='Q1',
    ... resource_quality='WH',
    ... directory='example_parsed_data/wind')
                     Datetime     Value
    0     2021-07-01 00:30:00  0.790868
    1     2021-07-01 01:00:00  0.816555
    2     2021-07-01 01:30:00  0.762126
    3     2021-07-01 02:00:00  0.787833
    4     2021-07-01 02:30:00  0.821139
    ...                   ...       ...
    17563 2024-06-30 22:00:00  0.500298
    17564 2024-06-30 22:30:00  0.468695
    17565 2024-06-30 23:00:00  0.531441
    17566 2024-06-30 23:30:00  0.548219
    17567 2024-07-01 00:00:00  0.534083
    <BLANKLINE>
    [35088 rows x 2 columns]


    Args:
        reference_years: dict{int: int}, a mapping of the which reference year (value) to retrieve data from for
            each financial or calendar year (value).
        area: str, the ISP code for the area (typically a REZ) to return data for. Codes need to be as given in the IASR
            workbook.
        resource_quality: str, the resource quality of the trace to retrieve usaual 'WH' or 'WM'.
        directory: str or pathlib.Path, the directory were the trace data is stored. Trace data needs to be in the
            format as produced by isp_trace_parser.restructure_solar_directory.
        year_type: str, 'fy' or 'calendar', if 'fy' then time filtering is by financial year with start_year and
            end_year specifiying the financial year to return data for, using year ending nomenclature (2016 ->
            FY2015/2016). If 'calendar', then filtering is by calendar year.

    Returns: pd.DataFrame with columns Datetime andValue
    """
    kwargs = {"area": area, "resource_type": resource_quality}
    return generic_multi_reference_year_mapping(
        "wind_area", reference_years, year_type, directory, **kwargs
    )


def demand_single_reference_year(
    start_year,
    end_year,
    reference_year,
    subregion,
    scenario,
    poe,
    descriptor,
    directory,
    year_type="fy",
):
    """Reads demand trace data from an output directory created by isp_trace_parser.restructure_solar_directory.

    Examples:

    >>> demand_single_reference_year(
    ... start_year=2024,
    ... end_year=2024,
    ... reference_year=2011,
    ... subregion='CNSW',
    ... scenario='Green Energy Exports',
    ... poe='POE10',
    ... descriptor='OPSO_MODELLING',
    ... directory='example_parsed_data/demand')
                     Datetime        Value
    0     2023-07-01 00:30:00  1021.534634
    1     2023-07-01 01:00:00   997.293145
    2     2023-07-01 01:30:00   971.978426
    3     2023-07-01 02:00:00   942.272701
    4     2023-07-01 02:30:00   899.031012
    ...                   ...          ...
    17563 2024-06-30 22:00:00  1042.242398
    17564 2024-06-30 22:30:00  1028.134356
    17565 2024-06-30 23:00:00   996.066288
    17566 2024-06-30 23:30:00   968.414059
    17567 2024-07-01 00:00:00   945.461810
    <BLANKLINE>
    [17568 rows x 2 columns]


    Args:
        start_year: int, start of time window to return trace data for.
        end_year: int, end of time window (inclusive) to return trace data for.
        reference_year: int, the reference year of the trace data to retrieve.
        subregion: str, the ISP ID for the subregion to return data for. ID need to be as given in the IASR
            workbook.
        scenario: str, the scenario to return data for.
        poe: str, the poe level to return data for.
        descriptor: str, the type of demand data to return.
        directory: str or pathlib.Path, the directory were the trace data is stored. Trace data needs to be in the
            format as produced by isp_trace_parser.restructure_solar_directory.
        year_type: str, 'fy' or 'calendar', if 'fy' then time filtering is by financial year with start_year and
            end_year specifiying the financial year to return data for, using year ending nomenclature (2016 ->
            FY2015/2016). If 'calendar', then filtering is by calendar year.

    Returns: pd.DataFrame with columns Datetime andValue
    """
    kwargs = {
        "area": subregion,
        "scenario": scenario,
        "poe": poe,
        "descriptor": descriptor,
    }
    return generic_single_reference_year(
        "demand", start_year, end_year, reference_year, year_type, directory, **kwargs
    )


def demand_multiple_reference_years(
    reference_years, subregion, scenario, poe, descriptor, directory, year_type="fy"
):
    """Reads wind area trace data from an output directory created by isp_trace_parser.restructure_solar_directory.

    Examples:

    >>> demand_multiple_reference_years(
    ... reference_years={2024: 2011},
    ... subregion='CNSW',
    ... scenario='Green Energy Exports',
    ... poe='POE10',
    ... descriptor='OPSO_MODELLING',
    ... directory='example_parsed_data/demand')
                     Datetime        Value
    0     2023-07-01 00:30:00  1021.534634
    1     2023-07-01 01:00:00   997.293145
    2     2023-07-01 01:30:00   971.978426
    3     2023-07-01 02:00:00   942.272701
    4     2023-07-01 02:30:00   899.031012
    ...                   ...          ...
    17563 2024-06-30 22:00:00  1042.242398
    17564 2024-06-30 22:30:00  1028.134356
    17565 2024-06-30 23:00:00   996.066288
    17566 2024-06-30 23:30:00   968.414059
    17567 2024-07-01 00:00:00   945.461810
    <BLANKLINE>
    [17568 rows x 2 columns]


    Args:
        reference_years: dict{int: int}, a mapping of the which reference year (value) to retrieve data from for
            each financial or calendar year (value).
        subregion: str, the ISP ID for the subregion to return data for. ID need to be as given in the IASR
            workbook.
        scenario: str, the scenario to return data for.
        poe: str, the poe level to return data for.
        descriptor: str, the type of demand data to return.
        directory: str or pathlib.Path, the directory were the trace data is stored. Trace data needs to be in the
            format as produced by isp_trace_parser.restructure_solar_directory.
        year_type: str, 'fy' or 'calendar', if 'fy' then time filtering is by financial year with start_year and
            end_year specifiying the financial year to return data for, using year ending nomenclature (2016 ->
            FY2015/2016). If 'calendar', then filtering is by calendar year.

    Returns: pd.DataFrame with columns Datetime andValue
    """
    kwargs = {
        "area": subregion,
        "scenario": scenario,
        "poe": poe,
        "descriptor": descriptor,
    }
    return generic_multi_reference_year_mapping(
        "demand", reference_years, year_type, directory, **kwargs
    )


def get_years_and_half_years(start_year, end_year, year_type):
    if year_type == "calendar":
        years = range(start_year, end_year + 1)
        half_years = [1, 2]
        years_and_half_years = itertools.product(years, half_years)
    elif year_type == "fy":
        years = range(start_year - 1, end_year + 1)
        half_years = [1, 2]
        years_and_half_years = list(itertools.product(years, half_years))[1:-1]
    else:
        raise ValueError(f"The year_type {year_type} is not recognised.")
    return years_and_half_years


def generic_multi_reference_year_mapping(
    data_type, reference_year_mapping, year_type, directory, **kwargs
):
    data = []
    for year, reference_year in reference_year_mapping.items():
        data.append(
            generic_single_reference_year(
                data_type, year, year, reference_year, year_type, directory, **kwargs
            )
        )
    data = pd.concat(data)
    return data


def generic_single_reference_year(
    data_type, start_year, end_year, reference_year, year_type, directory, **kwargs
):
    years_and_half_years = get_years_and_half_years(start_year, end_year, year_type)
    data = []
    for i, (year, half_year) in enumerate(years_and_half_years):
        filepath_args = {
            "year": year,
            "half_year": half_year,
            "reference_year": reference_year,
            "directory": directory,
        }
        filepath_args.update(kwargs)
        filepath = filepath_writers[data_type](**filepath_args)
        data.append(pl.read_parquet(filepath))
    data = pl.concat(data)
    return data.to_pandas()


def get_solar_project_filepath(year, half_year, reference_year, directory, project):
    filepath_template = "RefYear{ry}/Project/{project}/RefYear{ry}_{project}_FFP_HalfYear{y}-{hy}.parquet"
    project = project.replace(" ", "_").replace("*", "")
    return Path(directory) / filepath_template.format(
        ry=reference_year, project=project, y=year, hy=half_year
    )


def get_solar_area_filepath(
    year, half_year, reference_year, directory, area, technology
):
    filepath_template = "RefYear{ry}/Area/{area}/{tech}/RefYear{ry}_{area}_{tech}_HalfYear{y}-{hy}.parquet"
    return Path(directory) / filepath_template.format(
        ry=reference_year, area=area, y=year, hy=half_year, tech=technology
    )


def get_wind_project_filepath(year, half_year, reference_year, directory, project):
    filepath_template = (
        "RefYear{ry}/Project/{project}/RefYear{ry}_{project}_HalfYear{y}-{hy}.parquet"
    )
    project = project.replace(" ", "_").replace("*", "")
    return Path(directory) / filepath_template.format(
        ry=reference_year, project=project, y=year, hy=half_year
    )


def get_wind_area_filepath(
    year, half_year, reference_year, directory, area, resource_type
):
    filepath_template = (
        "RefYear{ry}/Area/{area}/{resource_type}/"
        "RefYear{ry}_{area}_{resource_type}_HalfYear{y}-{hy}.parquet"
    )
    return Path(directory) / filepath_template.format(
        ry=reference_year, area=area, y=year, hy=half_year, resource_type=resource_type
    )


def get_demand_file_path(
    year, half_year, reference_year, directory, area, scenario, poe, descriptor
):
    scenario = scenario.replace(" ", "_")
    filepath_template = (
        "{scenario}/RefYear{ry}/{area}/{poe}/{descriptor}/"
        "{scenario}_RefYear{ry}_{area}_{poe}_{descriptor}_HalfYear{y}-{hy}.parquet"
    )
    return Path(directory) / filepath_template.format(
        ry=reference_year,
        area=area,
        y=year,
        hy=half_year,
        scenario=scenario,
        poe=poe,
        descriptor=descriptor,
    )


filepath_writers = {
    "solar_project": get_solar_project_filepath,
    "solar_area": get_solar_area_filepath,
    "wind_project": get_wind_project_filepath,
    "wind_area": get_wind_area_filepath,
    "demand": get_demand_file_path,
}
