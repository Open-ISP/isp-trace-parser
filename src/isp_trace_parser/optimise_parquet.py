from pathlib import Path
from typing import Literal, Optional

import duckdb
from pydantic import BaseModel, validate_call


class OptimisationConfig(BaseModel):
    partition_cols: list[str] = ["reference_year"]
    sort_by: Optional[list[str]] = ["datetime"]
    delete_source: bool = False


def _delete_source_files(input_directory: str | Path) -> None:
    input_path = Path(input_directory)
    files = list(input_path.rglob("*.parquet"))
    for file in files:
        file.unlink()


def partition_traces(
    input_directory: str | Path,
    output_directory: str | Path,
    config: OptimisationConfig = None,
):
    if config is None:
        config = OptimisationConfig()

    output_path = Path(output_directory)
    output_path.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()

    query = f"SELECT * FROM read_parquet('{input_directory}')"

    if config.sort_by:
        query += f" ORDER BY {', '.join(config.sort_by)}"

    partition_clause = f"PARTITION_BY ({', '.join(config.partition_cols)})"

    con.execute(f"""COPY ({query})
                TO '{output_directory}'
                (FORMAT PARQUET, {partition_clause})
                """)

    con.close()


def partition_traces_partial(
    input_directory: str | Path,
    output_directory: str | Path,
    config: OptimisationConfig = None,
):
    if config is None:
        config = OptimisationConfig()

    output_path = Path(output_directory)
    output_path.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()

    for year in range(2011, 2024):
        print(year)

        query = f"SELECT * FROM read_parquet('{input_directory}') WHERE reference_year={year}"

        if config.sort_by:
            query += f" ORDER BY {', '.join(config.sort_by)}"

        partition_clause = f"PARTITION_BY ({', '.join(config.partition_cols)})"

        con.execute(f"""COPY ({query})
                    TO '{output_directory}'
                    (FORMAT PARQUET, {partition_clause},  OVERWRITE_OR_IGNORE TRUE)
                    """)

    con.close()
