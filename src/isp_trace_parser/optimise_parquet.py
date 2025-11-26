from pathlib import Path
from typing import Literal, Optional

import duckdb
from pydantic import BaseModel, validate_call


class OptimisationConfig(BaseModel):
    partition_cols: Optional[list[str]] = None
    sort_by: Optional[list[str]] = None
    delete_source: bool = True


def partition_traces(
    input_directory: str | Path,
    output_directory: str | Path,
    config: OptimisationConfig | None = None,
):
    if config is None:
        config = OptimisationConfig()

    output_path = Path(output_directory)
    output_path.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()

    query = f"SELECT * FROM read_parquet('{input_directory}')"

    if config.sort_by:
        query += f" ORDER BY {', '.join(config.sort_by)}"

    partition_clause = (
        f"PARTITION_BY ({', '.join(config.partition_cols)})"
        if config.partition_cols
        else ""
    )

    con.execute(f"""COPY ({query})
                TO '{output_directory}'
                (FORMAT PARQUET{', ' + partition_clause if partition_clause else ''})
                """)

    con.close()
