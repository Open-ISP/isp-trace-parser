from itertools import product
from pathlib import Path
from typing import Optional

import duckdb
from pydantic import validate_call


def _delete_source_files(input_directory: str | Path) -> None:
    """Delete all parquet files in the input directory.

    Args:
        input_directory: Directory containing parquet files to delete
    """
    input_path = Path(input_directory)
    files = list(input_path.rglob("*.parquet"))
    for file in files:
        file.unlink()


@validate_call
def partition_traces_by_columns(
    input_directory: str | Path,
    output_directory: str | Path,
    partition_cols: list[str],
    sort_by: Optional[list[str]] = ["datetime"],
) -> None:
    """Partition parquet traces by specified columns with optional sorting.

    Queries distinct values for each partition column, generates all combinations,
    and writes partitioned parquet files with filtering and optional within-partition
    sorting (to optimize for different performance or disk space outcomes). Individual
    partitions are queried separately to avoid memory or disk limits of large sorts.

    Args:
        input_directory: Directory containing source parquet files
        output_directory: Directory to write partitioned parquet files
        partition_cols: List of column names to partition by
        sort_by: Optional list of column names to sort by (defaults to ["datetime"])

    Examples:
        Partition resource traces by reference year:
        >>> partition_traces_by_columns(
        ...     "parsed_zone/",
        ...     "optimised_zone/",
        ...     partition_cols=["reference_year"]
        ... ) # doctest: +SKIP

        Partition demand traces by scenario and reference year:
        >>> partition_traces_by_columns(
        ...     "parsed_demand/",
        ...     "optimized_demand/",
        ...     partition_cols=["scenario", "reference_year"]
        ... ) # doctest: +SKIP
    """
    output_path = Path(output_directory)
    output_path.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()

    # Partition values
    distinct_values = []
    for col in partition_cols:
        values = con.execute(f"""
            SELECT DISTINCT {col}
            FROM read_parquet('{input_directory}')
        """).fetchall()
        distinct_values.append(values)

    partitions = [tuple(val[0] for val in vals) for vals in product(*distinct_values)]

    for partition_values in partitions:
        # print(*partition_values)

        conditions = []
        for col, val in zip(partition_cols, partition_values):
            if isinstance(val, str):
                conditions.append(f"{col}='{val}'")
            else:
                conditions.append(f"{col}={val}")

        where_clause = " AND ".join(conditions)
        query = f"SELECT * FROM read_parquet('{input_directory}') WHERE {where_clause}"

        if sort_by:
            query += f" ORDER BY {', '.join(sort_by)}"

        partition_clause = f"PARTITION_BY ({', '.join(partition_cols)})"

        con.execute(f"""COPY ({query})
                    TO '{output_directory}'
                    (FORMAT PARQUET, {partition_clause}, OVERWRITE_OR_IGNORE TRUE)
                    """)

    con.close()
