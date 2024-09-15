from datetime import timedelta


import pandas as pd
import polars as pl


def trace_formatter(trace_data: pd.DataFrame) -> pd.DataFrame:
    """Takes trace data in the AEMO format and converts it to Datetime column and data column format.

    AEMO provides ISP trace data in a format where there are separates columns specifying the year, month, and day,
    and then a different data column for each half hour in the day (labeled 01, 02 . . . 48). This function converts
    that data format into one where a single column specifies the datatime of data point, with the datetime
    specifying the end of the sample period, and another column specifies the data point value.

    Examples

    In this example we just specify three of the half hour columns for brevity.

    >>> aemo_format_data = pd.DataFrame({
    ... 'Year': [2024, 2024],
    ... 'Month': [6, 6],
    ... 'Day':[1, 2],
    ... '01': [11.2, 15.3],
    ... '02': [30.7, 20.4],
    ... '48': [17.1, 18.9]
    ... })

    >>> aemo_format_data
       Year  Month  Day    01    02    48
    0  2024      6    1  11.2  30.7  17.1
    1  2024      6    2  15.3  20.4  18.9

    >>> trace_formatter(aemo_format_data)
                 Datetime  Data
    0 2024-06-01 00:30:00  11.2
    1 2024-06-01 01:00:00  30.7
    2 2024-06-02 00:00:00  17.1
    3 2024-06-02 00:30:00  15.3
    4 2024-06-02 01:00:00  20.4
    5 2024-06-03 00:00:00  18.9

    Args:
        trace_data: DataFrame with columns 'Hour', 'Month', 'Year', and '01', '02', . . . '48'. The hour, month and year
            columns specify the day which the data pertains to, and the 01, 02 . . . 48 contain data for each half
            for each half hour of the day, starting with the time period ending 00:30:00 and ending with the time period
            ending 00:00:00.

    Returns: DataFrame with a column 'Datetime' specifying the end of the sample period, ending on the half hour, and a
        column specifying the data for each sample period.
    """

    value_vars = [f"{i:02d}" for i in range(1, 49)]
    value_vars = [v for v in value_vars if v in trace_data.columns]

    trace_data = trace_data.melt(
        id_vars=["Year", "Month", "Day"],
        value_vars=value_vars,
        var_name="time_label",
        value_name="Data",
    )

    def get_hour(time_label):
        return timedelta(hours=int(time_label) // 2)

    def get_minute(time_label):
        return timedelta(minutes=int(time_label) % 2 * 30)

    trace_data["Hour"] = trace_data["time_label"].apply(lambda x: get_hour(x))

    trace_data["Minute"] = trace_data["time_label"].apply(lambda x: get_minute(x))

    trace_data["Datetime"] = pd.to_datetime(
        trace_data["Year"].astype(str).str.zfill(2)
        + "-"
        + trace_data["Month"].astype(str).str.zfill(2)
        + "-"
        + trace_data["Day"].astype(str).str.zfill(2)
        + " "
        + "00:00:00"
    )

    trace_data["Datetime"] = (
        trace_data["Datetime"] + trace_data["Hour"] + trace_data["Minute"]
    )

    trace_data = (
        trace_data[["Datetime", "Data"]].sort_values("Datetime").reset_index(drop=True)
    )

    return trace_data


def trace_formatter_pl(trace_data: pl.DataFrame) -> pl.DataFrame:
    """
    Takes trace data in the AEMO format and converts it to a format with 'Datetime' and 'Data' columns.

    AEMO provides ISP trace data with separate columns for 'Year', 'Month', and 'Day', and individual data columns
    labeled '01', '02', ..., '48', representing half-hour intervals. This function converts that data format into
    one where a single 'Datetime' column specifies the end of each half-hour period, and another 'Data' column contains
    the corresponding values.

    Example:

    Input format:
    >>> aemo_format_data = pl.DataFrame({
    ... 'Year': [2024, 2024],
    ... 'Month': [6, 6],
    ... 'Day': [1, 2],
    ... '01': [11.2, 15.3],
    ... '02': [30.7, 20.4],
    ... '48': [17.1, 18.9]
    ... })

    >>> trace_formatter(aemo_format_data)
    shape: (6, 2)
    ┌─────────────────────┬──────┐
    │ Datetime            │ Data │
    │ ---                 │ ---  │
    │ datetime[ns]        │ f64  │
    ├─────────────────────┼──────┤
    │ 2024-06-01 00:30:00 │ 11.2 │
    │ 2024-06-01 01:00:00 │ 30.7 │
    │ 2024-06-02 00:00:00 │ 17.1 │
    │ 2024-06-02 00:30:00 │ 15.3 │
    │ 2024-06-02 01:00:00 │ 20.4 │
    │ 2024-06-03 00:00:00 │ 18.9 │
    └─────────────────────┴──────┘

    Args:
        trace_data: A `polars.DataFrame` with 'Year', 'Month', 'Day', and columns labeled '01' to '48' representing
                    half-hour intervals.

    Returns:
        A `polars.DataFrame` with:
        - 'Datetime': A column specifying the end time of each half-hour period.
        - 'Data': A column containing the data for each half-hour period.
    """

    # Need both padded 1-9 and not padded because AEMO data files can have both.
    value_vars = [f"{i:02d}" for i in range(1, 49)] + [str(i) for i in range(1, 10)]
    value_vars = [v for v in value_vars if v in trace_data.columns]

    trace_data = trace_data.melt(
        id_vars=["Year", "Month", "Day"],
        value_vars=value_vars,
        variable_name="time_label",
        value_name="Data"
    )

    def get_hour(time_label):
        return timedelta(hours=int(time_label) // 2)

    def get_minute(time_label):
        return timedelta(minutes=int(time_label) % 2 * 30)

    trace_data = trace_data.with_columns([
        pl.col("time_label").map_elements(get_hour, return_dtype=pl.Duration).alias("Hour"),
        pl.col("time_label").map_elements(get_minute, return_dtype=pl.Duration).alias("Minute"),
        (pl.col("Year").cast(pl.Utf8).str.zfill(2)
         + "-"
         + pl.col("Month").cast(pl.Utf8).str.zfill(2)
         + "-"
         + pl.col("Day").cast(pl.Utf8).str.zfill(2)
         + " 00:00:00").str.strptime(pl.Datetime).alias("Datetime")
    ])

    trace_data = trace_data.with_columns([
        (pl.col("Datetime") + pl.col("Hour") + pl.col("Minute")).alias("Datetime")
    ]).select(["Datetime", "Data"]).sort("Datetime")

    return trace_data
