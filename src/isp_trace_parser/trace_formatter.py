from datetime import timedelta

import polars as pl
from pydantic import config, validate_call


@validate_call(config=config.ConfigDict(arbitrary_types_allowed=True))
def trace_formatter(trace_data: pl.DataFrame) -> pl.DataFrame:
    """
    Takes trace data in the AEMO format and converts it to a format with 'Datetime' and 'Data' columns.

    AEMO provides ISP trace data with separate columns for 'Year', 'Month', and 'Day', and individual data columns
    labeled '01', '02', ..., '48', representing half-hour intervals. This function converts that data format into
    one where a single 'Datetime' column specifies the end of each half-hour period, and another 'Data' column contains
    the corresponding values.

    Example:

    Input format (example):

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
    ┌─────────────────────┬───────┐
    │ Datetime            ┆ Value │
    │ ---                 ┆ ---   │
    │ datetime[μs]        ┆ f64   │
    ╞═════════════════════╪═══════╡
    │ 2024-06-01 00:30:00 ┆ 11.2  │
    │ 2024-06-01 01:00:00 ┆ 30.7  │
    │ 2024-06-02 00:00:00 ┆ 17.1  │
    │ 2024-06-02 00:30:00 ┆ 15.3  │
    │ 2024-06-02 01:00:00 ┆ 20.4  │
    │ 2024-06-03 00:00:00 ┆ 18.9  │
    └─────────────────────┴───────┘



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

    trace_data = trace_data.unpivot(
        index=["Year", "Month", "Day"],
        on=value_vars,
        variable_name="time_label",
        value_name="Value",
    )

    def get_hour(time_label):
        return timedelta(hours=int(time_label) // 2)

    def get_minute(time_label):
        return timedelta(minutes=int(time_label) % 2 * 30)

    trace_data = trace_data.with_columns(
        [
            pl.col("time_label")
            .map_elements(get_hour, return_dtype=pl.Duration)
            .alias("Hour"),
            pl.col("time_label")
            .map_elements(get_minute, return_dtype=pl.Duration)
            .alias("Minute"),
            (
                pl.col("Year").cast(pl.Utf8).str.zfill(2)
                + "-"
                + pl.col("Month").cast(pl.Utf8).str.zfill(2)
                + "-"
                + pl.col("Day").cast(pl.Utf8).str.zfill(2)
                + " 00:00:00"
            )
            .str.strptime(pl.Datetime)
            .alias("Datetime"),
        ]
    )

    trace_data = (
        trace_data.with_columns(
            [(pl.col("Datetime") + pl.col("Hour") + pl.col("Minute")).alias("Datetime")]
        )
        .select(["Datetime", "Value"])
        .sort("Datetime")
    )

    return trace_data
