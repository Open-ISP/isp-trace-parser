import itertools
from pathlib import Path
from typing import Literal

import pandas as pd
import polars as pl
from pydantic import validate_call

from isp_trace_parser import input_validation


@validate_call
def solar_project_single_reference_year(
    start_year: int,
    end_year: int,
    reference_year: int,
    project: str,
    directory: str | Path,
    year_type: Literal["fy", "calendar"] = "fy",
) -> pd.DataFrame:
    """Reads solar project trace data from an output directory created by isp_trace_parser.solar_trace_parser.

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

    Returns: pd.DataFrame with columns Datetime and Value
    """
    directory = input_validation.parsed_directory(directory)
    input_validation.start_year_before_end_year(start_year, end_year)

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


@validate_call
def solar_project_multiple_reference_years(
    reference_years: dict[int, int],
    project: str,
    directory: str | Path,
    year_type: Literal["fy", "calendar"] = "fy",
) -> pd.DataFrame:
    """Reads solar project trace data from an output directory created by isp_trace_parser.solar_trace_parser.

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

    Returns: pd.DataFrame with columns Datetime and Value
    """
    directory = input_validation.parsed_directory(directory)
    kwargs = {"project": project}
    return generic_multi_reference_year_mapping(
        "solar_project", reference_years, year_type, directory, **kwargs
    )


@validate_call
def solar_area_single_reference_year(
    start_year: int,
    end_year: int,
    reference_year: int,
    area: str,
    technology: str,
    directory: str | Path,
    year_type: Literal["fy", "calendar"] = "fy",
) -> pd.DataFrame:
    """Reads solar area trace data from an output directory created by isp_trace_parser.solar_trace_parser.

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

    Returns: pd.DataFrame with columns Datetime and Value
    """
    directory = input_validation.parsed_directory(directory)
    input_validation.start_year_before_end_year(start_year, end_year)
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


@validate_call
def solar_area_multiple_reference_years(
    reference_years: dict[int, int],
    area: str,
    technology: str,
    directory: str | Path,
    year_type: Literal["fy", "calendar"] = "fy",
) -> pd.DataFrame:
    """Reads solar area trace data from an output directory created by isp_trace_parser.solar_trace_parser.

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

    Returns: pd.DataFrame with columns Datetime and Value
    """
    directory = input_validation.parsed_directory(directory)
    kwargs = {"area": area, "technology": technology}
    return generic_multi_reference_year_mapping(
        "solar_area", reference_years, year_type, directory, **kwargs
    )


@validate_call
def wind_project_single_reference_year(
    start_year: int,
    end_year: int,
    reference_year: int,
    project: str,
    directory: str | Path,
    year_type: Literal["fy", "calendar"] = "fy",
) -> pd.DataFrame:
    """Reads wind project trace data from an output directory created by isp_trace_parser.wind_trace_parser.

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

    Returns: pd.DataFrame with columns Datetime and Value
    """
    directory = input_validation.parsed_directory(directory)
    input_validation.start_year_before_end_year(start_year, end_year)
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


@validate_call
def wind_project_multiple_reference_years(
    reference_years: dict[int, int],
    project: str,
    directory: str | Path,
    year_type: Literal["fy", "calendar"] = "fy",
) -> pd.DataFrame:
    """Reads wind project trace data from an output directory created by isp_trace_parser.wind_trace_parser.

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

    Returns: pd.DataFrame with columns Datetime and Value
    """
    directory = input_validation.parsed_directory(directory)
    kwargs = {"project": project}
    return generic_multi_reference_year_mapping(
        "wind_project", reference_years, year_type, directory, **kwargs
    )


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
    """Reads wind area trace data from an output directory created by isp_trace_parser.wind_trace_parser.

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
        resource_quality: str, the resource quality of the trace to retrieve usual 'WH' or 'WM'.
        directory: str or pathlib.Path, the directory were the trace data is stored. Trace data needs to be in the
            format as produced by isp_trace_parser.restructure_solar_directory.
        year_type: str, 'fy' or 'calendar', if 'fy' then time filtering is by financial year with start_year and
            end_year specifiying the financial year to return data for, using year ending nomenclature (2016 ->
            FY2015/2016). If 'calendar', then filtering is by calendar year.

    Returns: pd.DataFrame with columns Datetime and Value
    """
    directory = input_validation.parsed_directory(directory)
    input_validation.start_year_before_end_year(start_year, end_year)
    kwargs = {"area": area, "resource_quality": resource_quality}
    return generic_single_reference_year(
        "wind_area",
        start_year,
        end_year,
        reference_year,
        year_type,
        directory,
        **kwargs,
    )


@validate_call
def wind_area_multiple_reference_years(
    reference_years: dict[int, int],
    area: str,
    resource_quality: str,
    directory: str | Path,
    year_type: Literal["fy", "calendar"] = "fy",
) -> pd.DataFrame:
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
        resource_quality: str, the resource quality of the trace to retrieve usual 'WH' or 'WM'.
        directory: str or pathlib.Path, the directory were the trace data is stored. Trace data needs to be in the
            format as produced by isp_trace_parser.restructure_solar_directory.
        year_type: str, 'fy' or 'calendar', if 'fy' then time filtering is by financial year with start_year and
            end_year specifiying the financial year to return data for, using year ending nomenclature (2016 ->
            FY2015/2016). If 'calendar', then filtering is by calendar year.

    Returns: pd.DataFrame with columns Datetime and Value
    """
    directory = input_validation.parsed_directory(directory)
    kwargs = {"area": area, "resource_quality": resource_quality}
    return generic_multi_reference_year_mapping(
        "wind_area", reference_years, year_type, directory, **kwargs
    )


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
    """Reads demand trace data from an output directory created by isp_trace_parser.demand_trace_parser.

    Examples:

    >>> demand_single_reference_year(
    ... start_year=2024,
    ... end_year=2024,
    ... reference_year=2011,
    ... subregion='CNSW',
    ... scenario='Green Energy Exports',
    ... poe='POE10',
    ... demand_type='OPSO_MODELLING',
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
        demand_type: str, the type of demand data to return.
        directory: str or pathlib.Path, the directory were the trace data is stored. Trace data needs to be in the
            format as produced by isp_trace_parser.restructure_solar_directory.
        year_type: str, 'fy' or 'calendar', if 'fy' then time filtering is by financial year with start_year and
            end_year specifiying the financial year to return data for, using year ending nomenclature (2016 ->
            FY2015/2016). If 'calendar', then filtering is by calendar year.

    Returns: pd.DataFrame with columns Datetime and Value
    """
    directory = input_validation.parsed_directory(directory)
    input_validation.start_year_before_end_year(start_year, end_year)
    kwargs = {
        "area": subregion,
        "scenario": scenario,
        "poe": poe,
        "demand_type": demand_type,
    }
    return generic_single_reference_year(
        "demand", start_year, end_year, reference_year, year_type, directory, **kwargs
    )


@validate_call
def demand_multiple_reference_years(
    reference_years: dict[int, int],
    subregion: str,
    scenario: str,
    poe: str,
    demand_type: str,
    directory: str | Path,
    year_type: Literal["fy", "calendar"] = "fy",
) -> pd.DataFrame:
    """Reads wind area trace data from an output directory created by isp_trace_parser.demand_trace_parser.

    Examples:

    >>> demand_multiple_reference_years(
    ... reference_years={2024: 2011},
    ... subregion='CNSW',
    ... scenario='Green Energy Exports',
    ... poe='POE10',
    ... demand_type='OPSO_MODELLING',
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
        demand_type: str, the type of demand data to return.
        directory: str or pathlib.Path, the directory were the trace data is stored. Trace data needs to be in the
            format as produced by isp_trace_parser.restructure_solar_directory.
        year_type: str, 'fy' or 'calendar', if 'fy' then time filtering is by financial year with start_year and
            end_year specifiying the financial year to return data for, using year ending nomenclature (2016 ->
            FY2015/2016). If 'calendar', then filtering is by calendar year.

    Returns: pd.DataFrame with columns Datetime and Value
    """
    directory = input_validation.parsed_directory(directory)
    kwargs = {
        "area": subregion,
        "scenario": scenario,
        "poe": poe,
        "demand_type": demand_type,
    }
    return generic_multi_reference_year_mapping(
        "demand", reference_years, year_type, directory, **kwargs
    )


def get_years_and_half_years(
    start_year: int, end_year: int, year_type: str
) -> list[tuple]:
    """
    Generate a list of year and half-year tuples for the given range and year type.

    This function creates a list of tuples, each containing a year and a half-year (1 or 2),
    based on the specified start year, end year, and year type. The year type determines how
    the years and half-years are calculated.

    Notes:
        - For 'calendar' year type, the function includes both halves of each year from
          start_year to end_year, inclusive.
        - For 'fy' (fiscal year) type, the function includes the second half of the year
          before start_year and the first half of end_year. This ensures that complete
          fiscal years are covered for the specified range.

    Examples:

        Using 'calendar' year type:

        >>> list(get_years_and_half_years(2022, 2023, 'calendar'))
        [(2022, 1), (2022, 2), (2023, 1), (2023, 2)]

        This returns all half-years for 2022 and 2023 in calendar year format.

        Using 'fy' (fiscal year) type:

        >>> list(get_years_and_half_years(2022, 2023, 'fy'))
        [(2021, 2), (2022, 1), (2022, 2), (2023, 1)]

        This returns fiscal years 2021/22 and 2022/23. Note that:
        - (2021, 2) represents July-December 2021 (first half of FY 2021/22)
        - (2022, 1) represents January-June 2022 (second half of FY 2021/22)
        - (2022, 2) represents July-December 2022 (first half of FY 2022/23)
        - (2023, 1) represents January-June 2023 (second half of FY 2022/23)
    """
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
    data_type: str,
    reference_year_mapping: dict[int, int],
    year_type: str,
    directory: str | Path,
    **kwargs,
) -> pd.DataFrame:
    """
    Retrieve data for multiple reference years for various types of energy data.

    This function is a generic method to fetch data for solar projects, solar areas,
    wind projects, wind areas, or demand, for multiple reference years. It allows
    for mapping different years to specific reference years.

    Examples:
        Retrieving solar project data for multiple years:
        >>> df = generic_multi_reference_year_mapping(
        ...     'solar_project',
        ...     reference_year_mapping={2022: 2011, 2023: 2012, 2024: 2011},
        ...     year_type='fy',
        ...     directory='path/to/solar/data',
        ...     project='Solar Farm A'
        ... )  # doctest: +SKIP

        Retrieving wind area data for multiple years:
        >>> df = generic_multi_reference_year_mapping(
        ...     'wind_area',
        ...     reference_year_mapping={2022: 2010, 2023: 2010, 2024: 2011},
        ...     year_type='calendar',
        ...     directory='path/to/wind/data',
        ...     area='Wind Area B',
        ...     resource_quality='high'
        ... )  # doctest: +SKIP

        Retrieving demand data for multiple years:
        >>> df = generic_multi_reference_year_mapping(
        ...     'demand',
        ...     reference_year_mapping={2022: 2011, 2023: 2011, 2024: 2012},
        ...     year_type='fy',
        ...     directory='path/to/demand/data',
        ...     subregion='Region C',
        ...     scenario='Step Change',
        ...     poe='POE50',
        ...     descriptor='OPSO_MODELLING'
        ... )  # doctest: +SKIP

    Args:
        data_type (str): Type of data to retrieve. Must be one of 'solar_project',
             'solar_area', 'wind_project', 'wind_area', or 'demand'.
        reference_year_mapping (dict[int, int]): A dictionary mapping years to their
             corresponding reference years.
        year_type (str): Type of year to use, either 'fy' (fiscal year) or 'calendar'.
        directory (str | Path): The directory where the data files are stored.
        **kwargs: Additional keyword arguments specific to each data type:
            - For solar_project and wind_project: project (str)
            - For solar_area: area (str), technology (str)
            - For wind_area: area (str), resource_quality (str)
            - For demand: subregion (str), scenario (str), poe (str), descriptor (str)

    Returns:
        pd.DataFrame: A DataFrame containing the requested data for all specified years, with 'Datetime' and 'Value'
        columns.
    """
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
    data_type: str,
    start_year: int,
    end_year: int,
    reference_year: int,
    year_type: str,
    directory: str | Path,
    **kwargs,
) -> pd.DataFrame:
    """
    Retrieve data for a single reference year for various types of energy data.

    This function is a generic method to fetch data for solar projects, solar areas,
    wind projects, wind areas, or demand, for a single reference year. It handles
    the complexities of file paths and data reading for different data types.

    Args:
        data_type (str): Type of data to retrieve. Must be one of 'solar_project',
                         'solar_area', 'wind_project', 'wind_area', or 'demand'.
        start_year (int): The start year for the data range.
        end_year (int): The end year for the data range (inclusive).
        reference_year (int): The reference year for the data.
        year_type (str): Type of year to use, either 'fy' (fiscal year) or 'calendar'.
        directory (str | Path): The directory where the data files are stored.
        **kwargs: Additional keyword arguments specific to each data type:
            - For solar_project and wind_project: project (str)
            - For solar_area: area (str), technology (str)
            - For wind_area: area (str), resource_quality (str)
            - For demand: subregion (str), scenario (str), poe (str), demand_type (str)

    Returns:
        pd.DataFrame: A DataFrame containing the requested data, with 'Datetime' and 'Value' columns.

    Examples:
        Retrieving solar project data:
        >>> df = generic_single_reference_year(
        ...     'solar_project',
        ...     start_year=2022,
        ...     end_year=2023,
        ...     reference_year=2011,
        ...     year_type='fy',
        ...     directory='path/to/solar/data',
        ...     project='Solar Farm A'
        ... ) # doctest: +SKIP

        Retrieving wind area data:
        >>> df = generic_single_reference_year(
        ...     'wind_area',
        ...     start_year=2022,
        ...     end_year=2023,
        ...     reference_year=2011,
        ...     year_type='calendar',
        ...     directory='path/to/wind/data',
        ...     area='Wind Area B',
        ...     resource_quality='WH'
        ... ) # doctest: +SKIP

        Retrieving demand data:
        >>> df = generic_single_reference_year(
        ...     'demand',
        ...     start_year=2022,
        ...     end_year=2023,
        ...     reference_year=2011,
        ...     year_type='fy',
        ...     directory='path/to/demand/data',
        ...     subregion='VIC',
        ...     scenario='Step Change',
        ...     poe='POE50',
        ...     demand_type='OPSO_MODELLING'
        ... ) # doctest: +SKIP

    Notes:
        - The function uses the appropriate file path structure and naming convention
          for each data type.
        - For fiscal year ('fy') calculations, the function includes data from July
          of the year before start_year to June of end_year.
        - The resulting DataFrame includes data for all half-hourly intervals within
          the specified date range.
    """
    years_and_half_years = get_years_and_half_years(start_year, end_year, year_type)
    data = []
    for i, (year, half_year) in enumerate(years_and_half_years):
        filepath_args = {
            "year": year,
            "half_year": half_year,
            "reference_year": reference_year,
        }
        filepath_args.update(kwargs)
        filepath = filepath_writer(data_type, directory, **filepath_args)
        data.append(pl.read_parquet(filepath))
    data = pl.concat(data)
    return data.to_pandas()


def filepath_writer(data_type: str, directory: Path, **kwargs):
    for k, v in kwargs.items():
        if isinstance(v, str):
            kwargs[k] = kwargs[k].replace(" ", "_").replace("*", "")
    return directory / filepath_templates[data_type].format(**kwargs)


filepath_templates = {
    "solar_project": (
        "RefYear{reference_year}/Project/{project}/RefYear{reference_year}_{project}_*_HalfYear{year}-"
        "{half_year}.parquet"
    ),
    "solar_area": (
        "RefYear{reference_year}/Area/{area}/{technology}/RefYear{reference_year}_{area}_{technology}_HalfYear{year}-"
        "{half_year}.parquet"
    ),
    "wind_project": (
        "RefYear{reference_year}/Project/{project}/RefYear{reference_year}_{project}_HalfYear{year}-{half_year}.parquet"
    ),
    "wind_area": (
        "RefYear{reference_year}/Area/{area}/{resource_quality}/"
        "RefYear{reference_year}_{area}_{resource_quality}_HalfYear{year}-{half_year}.parquet"
    ),
    "demand": (
        "{scenario}/RefYear{reference_year}/{area}/{poe}/{demand_type}/"
        "{scenario}_RefYear{reference_year}_{area}_{poe}_{demand_type}_HalfYear{year}-{half_year}.parquet"
    ),
}
