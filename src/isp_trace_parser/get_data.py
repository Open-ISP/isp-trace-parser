import datetime
from pathlib import Path
from typing import List, Literal

import pandas as pd
import polars as pl
from pydantic import validate_call


def _year_range_to_dt_range(
    start_year: int, end_year: int, year_type: Literal["fy", "calendar"] = "fy"
):
    """
    Converting years to datetimes (for more efficient filters / selects)
    """

    ##Need to make a call on end dates

    if year_type == "fy":
        return datetime.datetime(start_year - 1, 7, 1), datetime.datetime(
            end_year, 7, 1
        )

    elif year_type == "calendar":
        return datetime.datetime(start_year, 1, 1), datetime.datetime(
            end_year + 1, 1, 1
        )


def _format_string_filters(filters: dict) -> None:
    """Format string filters by replacing spaces with underscores.

    Currently handles: Project, Scenario
    """
    for key in ["Project", "Scenario"]:
        if key in filters:
            if isinstance(filters[key], list):
                filters[key] = [i.replace(" ", "_") for i in filters[key]]
            else:
                filters[key] = filters[key].replace(" ", "_")


def _query_parquet_single_reference_year(
    start_year: int,
    end_year: int,
    reference_year: int,
    directory: str | Path,
    filters: dict[str, any] = {},
    select_columns: list[str] = ["Datetime", "Value"],
    year_type: Literal["fy", "calendar"] = "fy",
) -> pd.DataFrame:
    """
    Generic function to query parquet files with flexible column filters.

    Args:
        start_year: Start of time window
        end_year: End of time window (inclusive)
        reference_year: Reference year for the trace data
        directory: Directory containing parquet files
        filters: Dictionary of column_name: value or column_name: list_of_values.
                Single values use equality (==), lists use membership (.is_in())
        select_columns: Columns to return in the result. Defaults to ["Datetime", "Value"]
        year_type: 'fy' or 'calendar'

    Returns:
        pd.DataFrame with selected columns, sorted by Datetime
    """
    _format_string_filters(filters)
    start_dt, end_dt = _year_range_to_dt_range(start_year, end_year, year_type)

    df_lazy = pl.scan_parquet(directory)

    filter_expr = (
        (pl.col("RefYear") == reference_year)
        & (pl.col("Datetime") > start_dt)
        & (pl.col("Datetime") <= end_dt)
    )

    for col, value in filters.items():
        if isinstance(value, list):
            filter_expr &= pl.col(col).is_in(value)
        else:
            filter_expr &= pl.col(col) == value

    df = df_lazy.filter(filter_expr).select(*select_columns).sort("Datetime").collect()

    return df.to_pandas()


def _query_parquet_multiple_reference_years(
    reference_year_mapping: dict[int, int], **kwargs: any
) -> pd.DataFrame:
    data = []
    for year, reference_year in reference_year_mapping.items():
        data.append(
            _query_parquet_single_reference_year(
                start_year=year, end_year=year, reference_year=reference_year, **kwargs
            )
        )
    data = pd.concat(data)
    return data


@validate_call
def get_project_single_reference_year(
    start_year: int,
    end_year: int,
    reference_year: int,
    project: str | List,
    directory: str | Path,
    year_type: Literal["fy", "calendar"] = "fy",
    select_columns: list[str] = ["Datetime", "Value"],
):
    return _query_parquet_single_reference_year(
        start_year=start_year,
        end_year=end_year,
        reference_year=reference_year,
        directory=directory,
        filters={"Project": project},
        year_type=year_type,
        select_columns=select_columns,
    )


@validate_call
def get_zone_single_reference_year(
    start_year: int,
    end_year: int,
    reference_year: int,
    zone: str | List,
    tech: str | List,
    directory: str | Path,
    year_type: Literal["fy", "calendar"] = "fy",
    select_columns: list[str] = ["Datetime", "Value"],
):
    return _query_parquet_single_reference_year(
        start_year=start_year,
        end_year=end_year,
        reference_year=reference_year,
        directory=directory,
        filters={"Zone": zone, "Tech": tech},
        year_type=year_type,
        select_columns=select_columns,
    )


@validate_call
def get_demand_single_reference_year(
    start_year: int,
    end_year: int,
    reference_year: int,
    scenario: str | List,
    subregion: str | List,
    category: str | List,
    poe: str | List,
    directory: str | Path,
    year_type: Literal["fy", "calendar"] = "fy",
    select_columns: list[str] = ["Datetime", "Value"],
):
    return _query_parquet_single_reference_year(
        start_year=start_year,
        end_year=end_year,
        reference_year=reference_year,
        directory=directory,
        filters={
            "Scenario": scenario,
            "Subregion": subregion,
            "Category": category,
            "POE": poe,
        },
        year_type=year_type,
        select_columns=select_columns,
    )


@validate_call
def get_project_multiple_reference_years(
    reference_year_mapping: dict[int, int],
    project: str | List,
    directory: str | Path,
    year_type: Literal["fy", "calendar"] = "fy",
    select_columns: list[str] = ["Datetime", "Value"],
):
    return _query_parquet_multiple_reference_years(
        reference_year_mapping=reference_year_mapping,
        directory=directory,
        filters={"Project": project},
        year_type=year_type,
        select_columns=select_columns,
    )


@validate_call
def get_zone_multiple_reference_years(
    reference_year_mapping: dict[int, int],
    zone: str | List,
    tech: str,
    directory: str | Path,
    year_type: Literal["fy", "calendar"] = "fy",
    select_columns: list[str] = ["Datetime", "Value"],
):
    return _query_parquet_multiple_reference_years(
        reference_year_mapping=reference_year_mapping,
        directory=directory,
        filters={"Zone": zone, "Tech": tech},
        year_type=year_type,
        select_columns=select_columns,
    )


@validate_call
def get_demand_multiple_reference_years(
    reference_year_mapping: dict[int, int],
    scenario: str,
    subregion: str,
    category: str,
    poe: str,
    directory: str | Path,
    year_type: Literal["fy", "calendar"] = "fy",
):
    return _query_parquet_multiple_reference_years(
        reference_year_mapping=reference_year_mapping,
        directory=directory,
        filters={
            "Scenario": scenario,
            "Subregion": subregion,
            "Category": category,
            "POE": poe,
        },
        year_type=year_type,
    )


"""
This section is just passthrough functions from original API. This includes:
 - the use of "area" rather than "zones"
 - (similarly "demand_type", rather than "category")
 - technology specific calls
 - "reference_years" (as distinct from 'reference_year_mapping')
"""


@validate_call
def solar_project_single_reference_year(*args, **kwargs):
    """
    Pass-through function to keep backwards capability with previos API
    """
    return get_project_single_reference_year(*args, **kwargs)


@validate_call
def wind_project_single_reference_year(*args, **kwargs):
    """
    Pass-through function to keep backwards capability with previos API

    Examples:

    >>> wind_project_single_reference_year(
    ... start_year=2022,
    ... end_year=2024,
    ... reference_year=2011,
    ... project='Bango 973 Wind Farm',
    ... directory='example_parsed_data/wind') # doctest: +SKIP
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

    Returns: pd.DataFrame with columns Datetime and Value
    """
    return get_project_single_reference_year(*args, **kwargs)


@validate_call
def solar_project_multiple_reference_years(
    reference_years: dict[int, int],
    project: str,
    directory: str | Path,
    year_type: Literal["fy", "calendar"] = "fy",
) -> pd.DataFrame:
    """
    Pass-through function to keep backwards capability with previos API
    
    Reads solar project trace data from an output directory created by isp_trace_parser.solar_trace_parser.


    Examples:

    >>> solar_project_multiple_reference_years(
    ... reference_years={2022: 2011, 2024: 2012},
    ... project='Adelaide Desalination Plant Solar Farm',
    ... directory='example_parsed_data/solar') # doctest: +SKIP
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

    Returns: pd.DataFrame with columns Datetime and Value
    """    
    return get_project_multiple_reference_years(reference_year_mapping=reference_years,
        project=project,
        directory=directory,
        year_type=year_type)

def solar_area_single_reference_year(
    start_year: int,
    end_year: int,
    reference_year: int,
    area: str,
    technology: str,
    directory: str | Path,
    year_type: Literal["fy", "calendar"] = "fy",
) -> pd.DataFrame:

    """
    Pass-through function to keep backwards capability with previos API

    Reads solar area trace data from an output directory created by isp_trace_parser.solar_trace_parser.

    Examples:

    >>> solar_area_single_reference_year(
    ... start_year=2022,
    ... end_year=2024,
    ... reference_year=2011,
    ... area='Q1',
    ... technology='SAT',
    ... directory='example_parsed_data/solar') # doctest: +SKIP
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

    Returns: pd.DataFrame with columns Datetime and Value

    """
    
    return get_zone_single_reference_year(
    start_year= start_year,
    end_year= end_year,
    reference_year= reference_year,
    zone= area,
    tech=technology,
    directory= directory,
    year_type=year_type)

@validate_call
def solar_area_multiple_reference_years(
    reference_years: dict[int, int],
    area: str,
    technology: str,
    directory: str | Path,
    year_type: Literal["fy", "calendar"] = "fy",
) -> pd.DataFrame:

    """
    Pass-through function to keep backwards capability with previos API
    
    Reads solar area trace data from an output directory created by isp_trace_parser.solar_trace_parser.

    Examples:
    Examples:

    >>> solar_area_multiple_reference_years(
    ... reference_years={2022: 2011, 2024: 2012},
    ... area='Q1',
    ... technology='SAT',
    ... directory='example_parsed_data/solar') # doctest: +SKIP
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

    Returns: pd.DataFrame with columns Datetime and Value
    """

    return get_zone_multiple_reference_years(reference_year_mapping=reference_years,
    zone=area,
    tech=technology,
    directory = directory,
    year_type=year_type)     


@validate_call
def wind_project_multiple_reference_years(
    reference_years: dict[int, int],
    project: str,
    directory: str | Path,
    year_type: Literal["fy", "calendar"] = "fy",
) -> pd.DataFrame:

    """
    Pass-through function to keep backwards capability with previos API
    
    Reads wind project trace data from an output directory created by isp_trace_parser.wind_trace_parser.

    Examples:

    >>> wind_project_multiple_reference_years(
    ... reference_years={2022: 2011, 2024: 2012},
    ... project='Bango 973 Wind Farm',
    ... directory='example_parsed_data/wind') # doctest: +SKIP
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

    Returns: pd.DataFrame with columns Datetime and Value
    """

    return get_project_multiple_reference_years(
    reference_year_mapping=reference_years,
    project=project,
    directory= directory,
    year_type=year_type)


@validate_call
def wind_area_single_reference_year(
    start_year: int,
    end_year: int,
    reference_year: int,
    area: str,
    resource_quality: str,
    directory: str | Path,
    year_type: Literal["fy", "calendar"] = "fy",
) -> pd.DataFrame:
    """
    Pass-through function to keep backwards capability with previos API
    Reads wind area trace data from an output directory created by isp_trace_parser.wind_trace_parser.

    Examples:

    >>> wind_area_single_reference_year(
    ... start_year=2022,
    ... end_year=2024,
    ... reference_year=2011,
    ... area='Q1',
    ... resource_quality='WH',
    ... directory='example_parsed_data/wind') # doctest: +SKIP
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
        resource_quality: str, the resource quality of the trace to retrieve usual 'WH' or 'WM'.
        directory: str or pathlib.Path, the directory were the trace data is stored. Trace data needs to be in the
            format as produced by isp_trace_parser.restructure_solar_directory.
        year_type: str, 'fy' or 'calendar', if 'fy' then time filtering is by financial year with start_year and
            end_year specifiying the financial year to return data for, using year ending nomenclature (2016 ->
            FY2015/2016). If 'calendar', then filtering is by calendar year.

    Returns: pd.DataFrame with columns Datetime and Value
    """


    return get_zone_single_reference_year(start_year=start_year,
    end_year=end_year,
    reference_year=reference_year,
    zone=area,
    tech=resource_quality,
    directory=directory,
    year_type=year_type)


def demand_multiple_reference_years(
    reference_years: dict[int, int],
    subregion: str,
    scenario: str,
    poe: str,
    demand_type: str,
    directory: str | Path,
    year_type: Literal["fy", "calendar"] = "fy",
) -> pd.DataFrame:
    """Pass-through function to keep backwards capability with previos API
    
    Reads wind area trace data from an output directory created by isp_trace_parser.demand_trace_parser.

    Examples:

    >>> demand_multiple_reference_years(
    ... reference_years={2024: 2011},
    ... subregion='CNSW',
    ... scenario='Green Energy Exports',
    ... poe='POE10',
    ... demand_type='OPSO_MODELLING',
    ... directory='example_parsed_data/demand') # doctest: +SKIP
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
        demand_type: str, the type of demand data to return.
        directory: str or pathlib.Path, the directory were the trace data is stored. Trace data needs to be in the
            format as produced by isp_trace_parser.restructure_solar_directory.
        year_type: str, 'fy' or 'calendar', if 'fy' then time filtering is by financial year with start_year and
            end_year specifiying the financial year to return data for, using year ending nomenclature (2016 ->
            FY2015/2016). If 'calendar', then filtering is by calendar year.

    Returns: pd.DataFrame with columns Datetime and Value
    """
    
    return get_demand_multiple_reference_years(
    reference_year_mapping= reference_years,
    scenario=scenario,
    subregion=subregion,
    category= demand_type,
    poe=poe,
    directory=directory,
    year_type=year_type)

@validate_call
def wind_area_multiple_reference_years(
    reference_years: dict[int, int],
    area: str,
    resource_quality: str,
    directory: str | Path,
    year_type: Literal["fy", "calendar"] = "fy",
) -> pd.DataFrame:
    
    """
    Reads wind area trace data from an output directory created by isp_trace_parser.restructure_solar_directory.

    Examples:

    >>> wind_area_multiple_reference_years(
    ... reference_years={2022: 2011, 2024: 2012},
    ... area='Q1',
    ... resource_quality='WH',
    ... directory='example_parsed_data/wind') # doctest: +SKIP
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
        resource_quality: str, the resource quality of the trace to retrieve usual 'WH' or 'WM'.
        directory: str or pathlib.Path, the directory were the trace data is stored. Trace data needs to be in the
            format as produced by isp_trace_parser.restructure_solar_directory.
        year_type: str, 'fy' or 'calendar', if 'fy' then time filtering is by financial year with start_year and
            end_year specifiying the financial year to return data for, using year ending nomenclature (2016 ->
            FY2015/2016). If 'calendar', then filtering is by calendar year.

    Returns: pd.DataFrame with columns Datetime and Value
    """

    return get_zone_multiple_reference_years(
    reference_year_mapping=reference_years,
    zone=area,
    tech=resource_quality,
    directory=directory,
    year_type=year_type)

@validate_call
def demand_single_reference_year(
    start_year: int,
    end_year: int,
    reference_year: int,
    subregion: str,
    scenario: str,
    poe: str,
    demand_type: str,
    directory: str | Path,
    year_type: Literal["fy", "calendar"] = "fy",
) -> pd.DataFrame:
    """
    Pass-through function to keep backwards capability with previos API
    
    Reads demand trace data from an output directory created by isp_trace_parser.demand_trace_parser.

    Examples:

    >>> demand_single_reference_year(
    ... start_year=2024,
    ... end_year=2024,
    ... reference_year=2011,
    ... subregion='CNSW',
    ... scenario='Green Energy Exports',
    ... poe='POE10',
    ... demand_type='OPSO_MODELLING',
    ... directory='example_parsed_data/demand') # doctest: +SKIP
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
        demand_type: str, the type of demand data to return.
        directory: str or pathlib.Path, the directory were the trace data is stored. Trace data needs to be in the
            format as produced by isp_trace_parser.restructure_solar_directory.
        year_type: str, 'fy' or 'calendar', if 'fy' then time filtering is by financial year with start_year and
            end_year specifiying the financial year to return data for, using year ending nomenclature (2016 ->
            FY2015/2016). If 'calendar', then filtering is by calendar year.

    Returns: pd.DataFrame with columns Datetime and Value
    """

    return get_demand_single_reference_year(
    start_year= start_year,
    end_year=end_year,
    reference_year=reference_year,
    scenario=scenario,
    subregion=subregion,
    category=demand_type,
    poe=poe,
    directory=directory,
    year_type= year_type)
