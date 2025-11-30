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
    Convert year range to datetime boundaries for efficient time filtering.

    Handles both financial year (FY) and calendar year conventions. For FY, uses
    year-ending nomenclature where FY2022 spans July 1, 2021 to July 1, 2022.

    Args:
        start_year: Start year of the range
        end_year: End year of the range (inclusive)
        year_type: 'fy' for financial year or 'calendar' for calendar year

    Returns:
        Tuple of (start_datetime, end_datetime)

    Examples:
        >>> _year_range_to_dt_range(2022, 2024, year_type="fy")
        (datetime.datetime(2021, 7, 1, 0, 0), datetime.datetime(2024, 7, 1, 0, 0))

        >>> _year_range_to_dt_range(2022, 2024, year_type="calendar")
        (datetime.datetime(2022, 1, 1, 0, 0), datetime.datetime(2025, 1, 1, 0, 0))
    """

    if year_type == "fy":
        return datetime.datetime(start_year - 1, 7, 1), datetime.datetime(
            end_year, 7, 1
        )

    elif year_type == "calendar":
        return datetime.datetime(start_year, 1, 1), datetime.datetime(
            end_year + 1, 1, 1
        )


def _query_parquet_single_reference_year(
    start_year: int,
    end_year: int,
    reference_year: int,
    directory: str | Path,
    filters: dict[str, any] = None,
    select_columns: list[str] = None,
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
                Single values use equality (==), lists use membership (.is_in()).
                If None, no additional filters are applied.
        select_columns: Columns to return in the result. If None, column selection
                depends on filters: all columns if no filters, ["datetime", "value"]
                plus multi-value filter columns if filters provided.
        year_type: 'fy' or 'calendar'

    Returns:
        pd.DataFrame with selected columns, sorted by datetime
    """
    start_dt, end_dt = _year_range_to_dt_range(start_year, end_year, year_type)

    df_lazy = pl.scan_parquet(directory)

    # Build filter expression
    filter_expr = (
        (pl.col("reference_year") == reference_year)
        & (pl.col("datetime") > start_dt)
        & (pl.col("datetime") <= end_dt)
    )

    if filters:
        for col, value in filters.items():
            if isinstance(value, list):
                filter_expr &= pl.col(col).is_in(value)
            else:
                filter_expr &= pl.col(col) == value

    # Determine which columns to select
    if select_columns is not None:
        # Based on choice, if provided
        columns_to_select = select_columns
    elif filters:
        # or on filters
        columns_to_select = ["datetime", "value"]
        for col, value in filters.items():
            if isinstance(value, list) and len(value) > 1:
                columns_to_select.append(col)
    else:
        # Otherwise select all columns
        columns_to_select = df_lazy.columns

    df = (
        df_lazy.filter(filter_expr)
        .select(*columns_to_select)
        .sort("datetime")
        .collect()
    )

    return df.to_pandas()


def _query_parquet_multiple_reference_years(
    reference_year_mapping: dict[int, int], **kwargs: any
) -> pd.DataFrame:
    """
    Query parquet files across multiple reference years.

    Iteratively calls _query_parquet_single_reference_year for each year-reference_year
    pair and concatenates the results.

    Args:
        reference_year_mapping: Mapping of year to reference_year (e.g., {2022: 2011, 2024: 2012})
        **kwargs: Additional arguments passed to _query_parquet_single_reference_year

    Returns:
        pd.DataFrame with concatenated results from all years
    """
    data = []
    for year, reference_year in reference_year_mapping.items():
        data.append(
            _query_parquet_single_reference_year(
                start_year=year, end_year=year, reference_year=reference_year, **kwargs
            )
        )
    data = pd.concat(data).reset_index(drop=True)
    return data


@validate_call
def get_project_single_reference_year(
    start_year: int,
    end_year: int,
    reference_year: int,
    project: str | List,
    directory: str | Path,
    year_type: Literal["fy", "calendar"] = "fy",
    select_columns: list[str] = None,
):
    """
    Query project trace data for a single reference year.

    Retrieves trace data for one or more projects within a specified time window.
    When querying multiple projects (as a list), the 'project' column is automatically
    included in the output to distinguish between projects.

    Args:
        start_year: Start of time window (inclusive)
        end_year: End of time window (inclusive)
        reference_year: Reference year of the trace data
        project: Project name (str) or list of project names
        directory: Directory containing parquet files
        year_type: 'fy' for financial year or 'calendar' for calendar year.
            Default is 'fy' (financial year ending nomenclature).
        select_columns: Optional list of columns to return. If None, returns
            ["datetime", "value"] for single project, or ["datetime", "value", "project"]
            for multiple projects.

    Returns:
        pd.DataFrame with trace data sorted by datetime

    Examples:
        Query single project:

        >>> get_project_single_reference_year(
        ...     start_year=2023,
        ...     end_year=2024,
        ...     reference_year=2011,
        ...     project="Bango 973 Wind Farm",
        ...     directory="parsed_data/project"
        ... ) # doctest: +SKIP
                         datetime     value
        0     2022-07-01 00:30:00  0.000000
        1     2022-07-01 01:00:00  0.000000
        2     2022-07-01 01:30:00  0.000000
        3     2022-07-01 02:00:00  0.000000
        4     2022-07-01 02:30:00  0.000000
        ...                   ...       ...
        35083 2024-06-30 22:00:00  0.044304
        35084 2024-06-30 22:30:00  0.046103
        35085 2024-06-30 23:00:00  0.046103
        35086 2024-06-30 23:30:00  0.057853
        35087 2024-07-01 00:00:00  0.076900
        <BLANKLINE>
        [35088 rows x 2 columns]

        Query multiple projects:

        >>> get_project_single_reference_year(
        ...     start_year=2023,
        ...     end_year=2024,
        ...     reference_year=2011,
        ...     project=["Bango 973 Wind Farm", "Bodangora Wind Farm"],
        ...     directory="parsed_data/project"
        ... ) # doctest: +SKIP
                         datetime     value              project
        0     2022-07-01 00:30:00  0.001436  Bodangora Wind Farm
        1     2022-07-01 00:30:00  0.000000  Bango 973 Wind Farm
        2     2022-07-01 01:00:00  0.001436  Bodangora Wind Farm
        3     2022-07-01 01:00:00  0.000000  Bango 973 Wind Farm
        4     2022-07-01 01:30:00  0.000000  Bango 973 Wind Farm
        ...                   ...       ...                  ...
        70171 2024-06-30 23:00:00  0.046103  Bango 973 Wind Farm
        70172 2024-06-30 23:30:00  0.449124  Bodangora Wind Farm
        70173 2024-06-30 23:30:00  0.057853  Bango 973 Wind Farm
        70174 2024-07-01 00:00:00  0.512703  Bodangora Wind Farm
        70175 2024-07-01 00:00:00  0.076900  Bango 973 Wind Farm
        <BLANKLINE>
        [70176 rows x 3 columns]
    """
    return _query_parquet_single_reference_year(
        start_year=start_year,
        end_year=end_year,
        reference_year=reference_year,
        directory=directory,
        filters={"project": project},
        year_type=year_type,
        select_columns=select_columns,
    )


@validate_call
def get_zone_single_reference_year(
    start_year: int,
    end_year: int,
    reference_year: int,
    zone: str | List,
    resource_type: str | List,
    directory: str | Path,
    year_type: Literal["fy", "calendar"] = "fy",
    select_columns: list[str] = None,
):
    """
    Query zone trace data for a single reference year.

    Retrieves trace data for one or more zones and resource types within a specified
    time window. When querying multiple zones (as a list), the 'zone' column is
    automatically included in the output to distinguish between zones.

    Args:
        start_year: Start of time window (inclusive)
        end_year: End of time window (inclusive)
        reference_year: Reference year of the trace data
        zone: Zone name (str) or list of zone names (e.g., "N2" or ["N1", "N2", "N3"])
        resource_type: Resource type (str) or list of resource types (e.g., "SAT", "WM")
        directory: Directory containing parquet files
        year_type: 'fy' for financial year or 'calendar' for calendar year.
            Default is 'fy' (financial year ending nomenclature).
        select_columns: Optional list of columns to return. If None, returns
            ["datetime", "value"] for single zone, or ["datetime", "value", "zone"]
            for multiple zones.

    Returns:
        pd.DataFrame with trace data sorted by datetime

    Examples:
        Query single zone:

        >>> get_zone_single_reference_year(
        ...     start_year=2023,
        ...     end_year=2024,
        ...     reference_year=2022,
        ...     zone="N2",
        ...     resource_type="SAT",
        ...     directory="parsed_data/zone"
        ... ) # doctest: +SKIP
                         datetime  value
        0     2022-07-01 00:30:00    0.0
        1     2022-07-01 01:00:00    0.0
        2     2022-07-01 01:30:00    0.0
        3     2022-07-01 02:00:00    0.0
        4     2022-07-01 02:30:00    0.0
        ...                   ...    ...
        35083 2024-06-30 22:00:00    0.0
        35084 2024-06-30 22:30:00    0.0
        35085 2024-06-30 23:00:00    0.0
        35086 2024-06-30 23:30:00    0.0
        35087 2024-07-01 00:00:00    0.0
        <BLANKLINE>
        [35088 rows x 2 columns]

        Query multiple zones:

        >>> get_zone_single_reference_year(
        ...     start_year=2023,
        ...     end_year=2024,
        ...     reference_year=2022,
        ...     zone=["N1", "N2", "N3"],
        ...     resource_type="SAT",
        ...     directory="parsed_data/zone"
        ... ) # doctest: +SKIP
                         datetime  value zone
        0     2022-07-01 00:30:00    0.0   N3
        1     2022-07-01 00:30:00    0.0   N2
        2     2022-07-01 00:30:00    0.0   N1
        3     2022-07-01 01:00:00    0.0   N3
        4     2022-07-01 01:00:00    0.0   N2
        ...                   ...    ...  ...
        105259 2024-06-30 23:30:00    0.0   N2
        105260 2024-06-30 23:30:00    0.0   N1
        105261 2024-07-01 00:00:00    0.0   N3
        105262 2024-07-01 00:00:00    0.0   N2
        105263 2024-07-01 00:00:00    0.0   N1
        <BLANKLINE>
        [105264 rows x 3 columns]
    """
    return _query_parquet_single_reference_year(
        start_year=start_year,
        end_year=end_year,
        reference_year=reference_year,
        directory=directory,
        filters={"zone": zone, "resource_type": resource_type},
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
    demand_type: str | List,
    poe: str | List,
    directory: str | Path,
    year_type: Literal["fy", "calendar"] = "fy",
    select_columns: list[str] = None,
):
    """
    Query demand trace data for a single reference year.

    Retrieves demand trace data for specified scenario, subregion, demand type, and
    probability of exceedance (POE) within a time window. When querying with multiple
    values for any parameter (as a list), those columns are automatically included in
    the output.

    Args:
        start_year: Start of time window (inclusive)
        end_year: End of time window (inclusive)
        reference_year: Reference year of the trace data
        scenario: Scenario name (str) or list of scenarios (e.g., "Green Energy Exports")
        subregion: Subregion code (str) or list of subregions (e.g., "VIC", ["VIC", "CSA"])
        demand_type: Demand type (str) or list of types (e.g., "OPSO_MODELLING", "PV_TOT")
        poe: Probability of exceedance (str) or list (e.g., "POE10", ["POE10", "POE50"])
        directory: Directory containing parquet files
        year_type: 'fy' for financial year or 'calendar' for calendar year.
            Default is 'fy' (financial year ending nomenclature).
        select_columns: Optional list of columns to return. If None, returns
            ["datetime", "value"] for single values, or ["datetime", "value"] plus
            any multi-value filter columns (sorted alphabetically).

    Returns:
        pd.DataFrame with trace data sorted by datetime

    Examples:
        Query single demand configuration:

        >>> get_demand_single_reference_year(
        ...     start_year=2024,
        ...     end_year=2025,
        ...     reference_year=2018,
        ...     scenario="Green Energy Exports",
        ...     subregion="VIC",
        ...     demand_type="OPSO_MODELLING",
        ...     poe="POE10",
        ...     directory="parsed_data/demand"
        ... ) # doctest: +SKIP
                         datetime        value
        0     2023-07-01 00:30:00  5612.328685
        1     2023-07-01 01:00:00  5386.638540
        2     2023-07-01 01:30:00  5192.704179
        3     2023-07-01 02:00:00  4969.665691
        4     2023-07-01 02:30:00  4763.455486
        ...                   ...          ...
        35083 2025-06-30 22:00:00  6065.453897
        35084 2025-06-30 22:30:00  5783.557418
        35085 2025-06-30 23:00:00  5624.744198
        35086 2025-06-30 23:30:00  5760.429165
        35087 2025-07-01 00:00:00  5708.627939
        <BLANKLINE>
        [35088 rows x 2 columns]

        Query multiple subregions and demand types:

        >>> get_demand_single_reference_year(
        ...     start_year=2024,
        ...     end_year=2025,
        ...     reference_year=2018,
        ...     scenario="Green Energy Exports",
        ...     subregion=["CSA", "VIC"],
        ...     demand_type=["OPSO_MODELLING", "PV_TOT"],
        ...     poe="POE10",
        ...     directory="parsed_data/demand"
        ... ) # doctest: +SKIP
                         datetime        value     demand_type subregion
        0     2023-07-01 00:30:00     0.000000          PV_TOT       VIC
        1     2023-07-01 00:30:00     0.000000          PV_TOT       CSA
        2     2023-07-01 00:30:00  1813.726159  OPSO_MODELLING       CSA
        3     2023-07-01 00:30:00  5612.328685  OPSO_MODELLING       VIC
        4     2023-07-01 01:00:00     0.000000          PV_TOT       VIC
        ...                   ...          ...             ...       ...
        140347 2025-06-30 23:30:00  1767.772016  OPSO_MODELLING       CSA
        140348 2025-07-01 00:00:00     0.000000          PV_TOT       VIC
        140349 2025-07-01 00:00:00     0.000000          PV_TOT       CSA
        140350 2025-07-01 00:00:00  5708.627939  OPSO_MODELLING       VIC
        140351 2025-07-01 00:00:00  1952.508153  OPSO_MODELLING       CSA
        <BLANKLINE>
        [140352 rows x 4 columns]
    """
    return _query_parquet_single_reference_year(
        start_year=start_year,
        end_year=end_year,
        reference_year=reference_year,
        directory=directory,
        filters={
            "scenario": scenario,
            "subregion": subregion,
            "demand_type": demand_type,
            "poe": poe,
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
    select_columns: list[str] = None,
):
    """
    Query project trace data across multiple reference years.

    Retrieves trace data for one or more projects across different years, each
    potentially using a different reference year. Results from all years are
    concatenated. When querying multiple projects (as a list), the 'project' column
    is automatically included in the output.

    Note: By default, the reference_year column is not included in the output. Use
    select_columns to explicitly include it if needed to identify which reference year
    was used for each row.

    Args:
        reference_year_mapping: Mapping of year to reference_year. For example,
            {2024: 2011, 2025: 2012} retrieves FY2024 data using 2011 reference year
            and FY2025 data using 2012 reference year.
        project: Project name (str) or list of project names
        directory: Directory containing parquet files
        year_type: 'fy' for financial year or 'calendar' for calendar year.
            Default is 'fy' (financial year ending nomenclature).
        select_columns: Optional list of columns to return. If None, returns
            ["datetime", "value"] plus any multi-value filter columns.

    Returns:
        pd.DataFrame with concatenated trace data from all years, sorted by datetime

    Examples:
        Query multiple years with multiple projects:

        >>> get_project_multiple_reference_years(
        ...     reference_year_mapping={2024: 2011, 2025: 2012},
        ...     project=["Bango 973 Wind Farm", "Bodangora Wind Farm"],
        ...     directory="parsed_data/project"
        ... ) # doctest: +SKIP
                         datetime     value              project
        0     2023-07-01 00:30:00  0.244017  Bodangora Wind Farm
        1     2023-07-01 00:30:00  0.436084  Bango 973 Wind Farm
        2     2023-07-01 01:00:00  0.273899  Bodangora Wind Farm
        3     2023-07-01 01:00:00  0.489390  Bango 973 Wind Farm
        4     2023-07-01 01:30:00  0.340990  Bodangora Wind Farm
        ...                   ...       ...                  ...
        70171 2025-06-30 23:00:00  0.065651  Bango 973 Wind Farm
        70172 2025-06-30 23:30:00  0.182044  Bodangora Wind Farm
        70173 2025-06-30 23:30:00  0.049964  Bango 973 Wind Farm
        70174 2025-07-01 00:00:00  0.218949  Bodangora Wind Farm
        70175 2025-07-01 00:00:00  0.037577  Bango 973 Wind Farm
        <BLANKLINE>
        [70176 rows x 3 columns]

        Include reference_year column to identify which reference year was used:

        >>> get_project_multiple_reference_years(
        ...     reference_year_mapping={2024: 2011, 2025: 2012},
        ...     project=["Bango 973 Wind Farm", "Bodangora Wind Farm"],
        ...     directory="parsed_data/project",
        ...     select_columns=["datetime", "value", "project", "reference_year"]
        ... ) # doctest: +SKIP
                         datetime     value              project  reference_year
        0     2023-07-01 00:30:00  0.244017  Bodangora Wind Farm            2011
        1     2023-07-01 00:30:00  0.436084  Bango 973 Wind Farm            2011
        2     2023-07-01 01:00:00  0.273899  Bodangora Wind Farm            2011
        3     2023-07-01 01:00:00  0.489390  Bango 973 Wind Farm            2011
        4     2023-07-01 01:30:00  0.340990  Bodangora Wind Farm            2011
        ...                   ...       ...                  ...             ...
        70171 2025-06-30 23:00:00  0.065651  Bango 973 Wind Farm            2012
        70172 2025-06-30 23:30:00  0.182044  Bodangora Wind Farm            2012
        70173 2025-06-30 23:30:00  0.049964  Bango 973 Wind Farm            2012
        70174 2025-07-01 00:00:00  0.218949  Bodangora Wind Farm            2012
        70175 2025-07-01 00:00:00  0.037577  Bango 973 Wind Farm            2012
        <BLANKLINE>
        [70176 rows x 4 columns]
    """
    return _query_parquet_multiple_reference_years(
        reference_year_mapping=reference_year_mapping,
        directory=directory,
        filters={"project": project},
        year_type=year_type,
        select_columns=select_columns,
    )


@validate_call
def get_zone_multiple_reference_years(
    reference_year_mapping: dict[int, int],
    zone: str | List,
    resource_type: str | List,
    directory: str | Path,
    year_type: Literal["fy", "calendar"] = "fy",
    select_columns: list[str] = None,
):
    """
    Query zone trace data across multiple reference years.

    Retrieves trace data for one or more zones and resource types across different
    years, each potentially using a different reference year. Results from all years
    are concatenated. When querying multiple zones (as a list), the 'zone' column is
    automatically included in the output.

    Note: By default, the reference_year column is not included in the output. Use
    select_columns to explicitly include it if needed to identify which reference year
    was used for each row.

    Args:
        reference_year_mapping: Mapping of year to reference_year. For example,
            {2024: 2011, 2025: 2012} retrieves FY2024 data using 2011 reference year
            and FY2025 data using 2012 reference year.
        zone: Zone name (str) or list of zone names (e.g., "N2" or ["N1", "N2", "N3"])
        resource_type: Resource type (str) or list of resource types (e.g., "SAT", "WM")
        directory: Directory containing parquet files
        year_type: 'fy' for financial year or 'calendar' for calendar year.
            Default is 'fy' (financial year ending nomenclature).
        select_columns: Optional list of columns to return. If None, returns
            ["datetime", "value"] plus any multi-value filter columns.

    Returns:
        pd.DataFrame with concatenated trace data from all years, sorted by datetime

    Examples:
        Query multiple years with multiple zones:

        >>> get_zone_multiple_reference_years(
        ...     reference_year_mapping={2024: 2011, 2025: 2012},
        ...     zone=["N1", "N2", "N3"],
        ...     resource_type="SAT",
        ...     directory="parsed_data/zone"
        ... ) # doctest: +SKIP
                         datetime  value zone
        0     2023-07-01 00:30:00    0.0   N3
        1     2023-07-01 00:30:00    0.0   N1
        2     2023-07-01 00:30:00    0.0   N2
        3     2023-07-01 01:00:00    0.0   N3
        4     2023-07-01 01:00:00    0.0   N1
        ...                   ...    ...  ...
        105259 2025-06-30 23:30:00    0.0   N3
        105260 2025-06-30 23:30:00    0.0   N1
        105261 2025-07-01 00:00:00    0.0   N2
        105262 2025-07-01 00:00:00    0.0   N3
        105263 2025-07-01 00:00:00    0.0   N1
        <BLANKLINE>
        [105264 rows x 3 columns]

        Include reference_year column to identify which reference year was used:

        >>> get_zone_multiple_reference_years(
        ...     reference_year_mapping={2024: 2011, 2025: 2012},
        ...     zone=["N1", "N2", "N3"],
        ...     resource_type="SAT",
        ...     directory="parsed_data/zone",
        ...     select_columns=["datetime", "value", "zone", "reference_year"]
        ... ) # doctest: +SKIP
                         datetime  value zone  reference_year
        0     2023-07-01 00:30:00    0.0   N3            2011
        1     2023-07-01 00:30:00    0.0   N1            2011
        2     2023-07-01 00:30:00    0.0   N2            2011
        3     2023-07-01 01:00:00    0.0   N3            2011
        4     2023-07-01 01:00:00    0.0   N1            2011
        ...                   ...    ...  ...             ...
        105259 2025-06-30 23:30:00    0.0   N3            2012
        105260 2025-06-30 23:30:00    0.0   N1            2012
        105261 2025-07-01 00:00:00    0.0   N2            2012
        105262 2025-07-01 00:00:00    0.0   N3            2012
        105263 2025-07-01 00:00:00    0.0   N1            2012
        <BLANKLINE>
        [105264 rows x 4 columns]
    """
    return _query_parquet_multiple_reference_years(
        reference_year_mapping=reference_year_mapping,
        directory=directory,
        filters={"zone": zone, "resource_type": resource_type},
        year_type=year_type,
        select_columns=select_columns,
    )


@validate_call
def get_demand_multiple_reference_years(
    reference_year_mapping: dict[int, int],
    scenario: str | List,
    subregion: str | List,
    demand_type: str | List,
    poe: str | List,
    directory: str | Path,
    year_type: Literal["fy", "calendar"] = "fy",
    select_columns: list[str] = None,
):
    """
    Query demand trace data across multiple reference years.

    Retrieves demand trace data for specified scenario, subregion, demand type, and
    probability of exceedance (POE) across different years, each potentially using a
    different reference year. Results from all years are concatenated. When querying
    with multiple values for any parameter (as a list), those columns are automatically
    included in the output.

    Note: By default, the reference_year column is not included in the output. Use
    select_columns to explicitly include it if needed to identify which reference year
    was used for each row.

    Args:
        reference_year_mapping: Mapping of year to reference_year. For example,
            {2024: 2011, 2025: 2012} retrieves FY2024 data using 2011 reference year
            and FY2025 data using 2012 reference year.
        scenario: Scenario name (str) or list of scenarios (e.g., "Green Energy Exports")
        subregion: Subregion code (str) or list of subregions (e.g., "VIC", ["VIC", "CSA"])
        demand_type: Demand type (str) or list of types (e.g., "OPSO_MODELLING", "PV_TOT")
        poe: Probability of exceedance (str) or list (e.g., "POE10", ["POE10", "POE50"])
        directory: Directory containing parquet files
        year_type: 'fy' for financial year or 'calendar' for calendar year.
            Default is 'fy' (financial year ending nomenclature).
        select_columns: Optional list of columns to return. If None, returns
            ["datetime", "value"] plus any multi-value filter columns.

    Returns:
        pd.DataFrame with concatenated trace data from all years, sorted by datetime

    Examples:
        Query multiple years with multiple subregions:

        >>> get_demand_multiple_reference_years(
        ...     reference_year_mapping={2024: 2011, 2025: 2012},
        ...     scenario="Green Energy Exports",
        ...     subregion=["CSA", "VIC"],
        ...     demand_type="OPSO_MODELLING",
        ...     poe="POE10",
        ...     directory="parsed_data/demand"
        ... ) # doctest: +SKIP
                         datetime        value subregion
        0     2023-07-01 00:30:00  1919.673822       CSA
        1     2023-07-01 00:30:00  5215.504431       VIC
        2     2023-07-01 01:00:00  1852.922186       CSA
        3     2023-07-01 01:00:00  5050.995415       VIC
        4     2023-07-01 01:30:00  1725.958348       CSA
        ...                   ...          ...       ...
        70171 2025-06-30 23:00:00  5457.607049       VIC
        70172 2025-06-30 23:30:00  1759.593902       CSA
        70173 2025-06-30 23:30:00  5638.540111       VIC
        70174 2025-07-01 00:00:00  1950.356725       CSA
        70175 2025-07-01 00:00:00  5659.380906       VIC
        <BLANKLINE>
        [70176 rows x 3 columns]

        Include reference_year column to identify which reference year was used:

        >>> get_demand_multiple_reference_years(
        ...     reference_year_mapping={2024: 2011, 2025: 2012},
        ...     scenario="Green Energy Exports",
        ...     subregion=["CSA", "VIC"],
        ...     demand_type="OPSO_MODELLING",
        ...     poe="POE10",
        ...     directory="parsed_data/demand",
        ...     select_columns=["datetime", "value", "subregion", "reference_year"]
        ... ) # doctest: +SKIP
                         datetime        value subregion  reference_year
        0     2023-07-01 00:30:00  1919.673822       CSA            2011
        1     2023-07-01 00:30:00  5215.504431       VIC            2011
        2     2023-07-01 01:00:00  1852.922186       CSA            2011
        3     2023-07-01 01:00:00  5050.995415       VIC            2011
        4     2023-07-01 01:30:00  1725.958348       CSA            2011
        ...                   ...          ...       ...             ...
        70171 2025-06-30 23:00:00  5457.607049       VIC            2012
        70172 2025-06-30 23:30:00  1759.593902       CSA            2012
        70173 2025-06-30 23:30:00  5638.540111       VIC            2012
        70174 2025-07-01 00:00:00  1950.356725       CSA            2012
        70175 2025-07-01 00:00:00  5659.380906       VIC            2012
        <BLANKLINE>
        [70176 rows x 4 columns]
    """
    return _query_parquet_multiple_reference_years(
        reference_year_mapping=reference_year_mapping,
        directory=directory,
        filters={
            "scenario": scenario,
            "subregion": subregion,
            "demand_type": demand_type,
            "poe": poe,
        },
        year_type=year_type,
        select_columns=select_columns,
    )


"""
This section is just passthrough functions from original API. This includes:
 - the use of "area" rather than "zones"
 - (similarly "demand_type", rather than "category")
 - technology specific calls
 - "reference_years" (as distinct from 'reference_year_mapping')
"""


@validate_call
def solar_project_single_reference_year(
    start_year: int,
    end_year: int,
    reference_year: int,
    project: str,
    directory: str | Path,
    year_type: Literal["fy", "calendar"] = "fy",
) -> pd.DataFrame:
    """
    Pass-through function to keep backwards capability with previos API

    Reads solar project trace data from an output directory created by isp_trace_parser.solar_trace_parser.

    Examples:

    >>> solar_project_single_reference_year(
    ... start_year=2022,
    ... end_year=2024,
    ... reference_year=2011,
    ... project='Adelaide Desalination Plant Solar Farm',
    ... directory='example_parsed_data/solar') # doctest: +SKIP
                     datetime  value
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

    Returns: pd.DataFrame with columns datetime and value
    """

    return get_project_single_reference_year(
        start_year=start_year,
        end_year=end_year,
        reference_year=reference_year,
        project=project,
        directory=directory,
        year_type=year_type,
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
    """
    Pass-through function to keep backwards capability with previos API

    Examples:

    >>> wind_project_single_reference_year(
    ... start_year=2022,
    ... end_year=2024,
    ... reference_year=2011,
    ... project='Bango 973 Wind Farm',
    ... directory='example_parsed_data/wind') # doctest: +SKIP
                     datetime     value
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

    Returns: pd.DataFrame with columns datetime and value
    """
    return get_project_single_reference_year(
        start_year=start_year,
        end_year=end_year,
        reference_year=reference_year,
        project=project,
        directory=directory,
        year_type=year_type,
    )


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
                     datetime  value
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

    Returns: pd.DataFrame with columns datetime and value
    """
    return get_project_multiple_reference_years(
        reference_year_mapping=reference_years,
        project=project,
        directory=directory,
        year_type=year_type,
    )


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
                     datetime  value
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

    Returns: pd.DataFrame with columns datetime and value

    """

    return get_zone_single_reference_year(
        start_year=start_year,
        end_year=end_year,
        reference_year=reference_year,
        zone=area,
        resource_type=technology,
        directory=directory,
        year_type=year_type,
    )


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

    >>> solar_area_multiple_reference_years(
    ... reference_years={2022: 2011, 2024: 2012},
    ... area='Q1',
    ... technology='SAT',
    ... directory='example_parsed_data/solar') # doctest: +SKIP
                     datetime  value
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

    Returns: pd.DataFrame with columns datetime and value
    """

    return get_zone_multiple_reference_years(
        reference_year_mapping=reference_years,
        zone=area,
        resource_type=technology,
        directory=directory,
        year_type=year_type,
    )


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
                     datetime     value
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

    Returns: pd.DataFrame with columns datetime and value
    """

    return get_project_multiple_reference_years(
        reference_year_mapping=reference_years,
        project=project,
        directory=directory,
        year_type=year_type,
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
                     datetime     value
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

    Returns: pd.DataFrame with columns datetime and value
    """

    return get_zone_single_reference_year(
        start_year=start_year,
        end_year=end_year,
        reference_year=reference_year,
        zone=area,
        resource_type=resource_quality,
        directory=directory,
        year_type=year_type,
    )


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
                     datetime        value
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

    Returns: pd.DataFrame with columns datetime and value
    """

    return get_demand_multiple_reference_years(
        reference_year_mapping=reference_years,
        scenario=scenario,
        subregion=subregion,
        demand_type=demand_type,
        poe=poe,
        directory=directory,
        year_type=year_type,
    )


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
                     datetime     value
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

    Returns: pd.DataFrame with columns datetime and value
    """

    return get_zone_multiple_reference_years(
        reference_year_mapping=reference_years,
        zone=area,
        resource_type=resource_quality,
        directory=directory,
        year_type=year_type,
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
                     datetime        value
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

    Returns: pd.DataFrame with columns datetime and value
    """

    return get_demand_single_reference_year(
        start_year=start_year,
        end_year=end_year,
        reference_year=reference_year,
        scenario=scenario,
        subregion=subregion,
        demand_type=demand_type,
        poe=poe,
        directory=directory,
        year_type=year_type,
    )
