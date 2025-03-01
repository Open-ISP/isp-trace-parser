import polars as pl
from polars.testing import assert_frame_equal

from isp_trace_parser import trace_formatter, trace_restructure_helper_functions


def test_trace_formatter():
    # Test trace formatting works by using formatting function works by performing formatting and then
    # reversing the formatting changes and checking the result matches the original data.
    filepath = (
        "tests/test_data/demand/demand_CNSW_Green Energy Exports"
        "/CNSW_RefYear_2011_HYDROGEN_EXPORT_POE10_OPSO_MODELLING.csv"
    )
    original_trace_data = trace_restructure_helper_functions.read_trace_csv(filepath)
    formatted_data = trace_formatter(original_trace_data)

    formatted_data = formatted_data.with_columns(
        [
            pl.col("Datetime")
            .dt.offset_by("-1s")
            .dt.year()
            .cast(pl.Int64)
            .alias("Year"),
            pl.col("Datetime")
            .dt.offset_by("-1s")
            .dt.month()
            .cast(pl.Int64)
            .alias("Month"),
            pl.col("Datetime").dt.offset_by("-1s").dt.day().cast(pl.Int64).alias("Day"),
            (pl.col("Datetime").dt.hour()).cast(pl.Int64).alias("hm"),
            pl.col("Datetime").dt.minute().cast(pl.Int64).alias("mm"),
        ]
    )
    formatted_data = formatted_data.with_columns(
        [(pl.col("hm") * 60 + pl.col("mm")).alias("m")]
    )
    formatted_data = formatted_data.with_columns([(pl.col("m") // 30).alias("Period")])
    formatted_data = formatted_data.with_columns(
        [
            pl.when(pl.col("Period") == 0)
            .then(pl.lit(48))
            .otherwise(pl.col("Period"))
            .alias("Period")
        ]
    )
    formatted_data = formatted_data.with_columns(
        [
            pl.col("Period")
            .map_elements(lambda x: f"{x:02}", return_dtype=pl.String)
            .alias("Period")
        ]
    )

    formatted_data = formatted_data.sort(by=["Year", "Month", "Day", "Period"])

    formatted_data = formatted_data.pivot(
        index=["Year", "Month", "Day"], on="Period", values="Value"
    )

    assert_frame_equal(original_trace_data, formatted_data)
