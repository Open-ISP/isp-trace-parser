import datetime
import tempfile
import timeit
import zipfile
from pathlib import Path

import duckdb

from isp_trace_parser import (
    demand_traces,
    get_data,
    optimise_parquet,
    solar_traces,
    wind_traces,
)


def list_files_in_zips(directory: str | Path) -> list[str]:
    """Returns a list of all filenames contained within zip files in a directory.

    Args:
        directory: Path to directory containing zip files.

    Returns:
        List of filenames from all zip files.
    """
    directory = Path(directory)
    all_files = []

    for zip_path in directory.glob("*.zip"):
        with zipfile.ZipFile(zip_path, "r") as zf:
            all_files.extend(f for f in zf.namelist() if not f.endswith("/"))

    return all_files


def process_resource_zips():
    a = datetime.datetime.now()

    root = Path("/home/dylan/Data/openISP/archive/openISP-server/isp_2024/")
    solar_dir = root / "solar"
    wind_dir = root / "wind"

    tmp_path = Path("/home/dylan/Data/openISP/archive/openISP-server/tmp")

    # Unpack all zips into tmp directory
    for zip_path in solar_dir.glob("*.zip"):
        print(zip_path)
        with tempfile.TemporaryDirectory() as tmpdir:
            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extractall(tmpdir)

                for file_type in ["zone", "project"]:
                    filters = solar_traces.SolarMetadataFilter(file_type=[file_type])
                    solar_traces.parse_solar_traces(
                        input_directory=tmpdir,
                        parsed_directory=tmp_path / file_type,
                        filters=filters,
                    )

    for zip_path in wind_dir.glob("*.zip"):
        print(zip_path)
        with tempfile.TemporaryDirectory() as tmpdir:
            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extractall(tmpdir)

                for file_type in ["zone", "project"]:
                    filters = wind_traces.WindMetadataFilter(file_type=[file_type])
                    wind_traces.parse_wind_traces(
                        input_directory=tmpdir,
                        parsed_directory=tmp_path / file_type,
                        filters=filters,
                    )

    b = datetime.datetime.now()

    print(a)
    print(b)
    print(b - a)


def optimise(file_type="project"):
    a = datetime.datetime.now()

    root = Path("/home/dylan/Data/openISP/archive/openISP-server/")
    tmp_path = Path("/home/dylan/Data/openISP/archive/openISP-server/tmp")

    try:
        optimise_parquet.partition_traces_by_columns(
            input_directory=tmp_path / file_type,
            output_directory=root / "processed" / file_type,
            partition_cols=["reference_year"],
        )
    except Exception as E:
        print(E)

    b = datetime.datetime.now()
    print(a)
    print(b)
    print(b - a)


# processed = full set
# processed2 = by partion, sorted
# processed3 = by parition, no sort
# raw = ~2k files


def test_get():
    root = Path("/home/dylan/Data/openISP/trace_archive/")
    n = 10
    zones = ["N0", "N11", "N1", "N10", "N2", "N3", "N4", "N5", "N6", "N7", "N8"]
    # zones = ["Q1"]
    directories = [
        root / subdir / "zone"
        for subdir in ["processed", "processed2", "processed3", "tmp"]
    ]
    for path in directories:
        timer = timeit.Timer(
            lambda: get_data.get_zone_single_reference_year(
                start_year=2023,
                end_year=2024,
                reference_year=2018,
                zone=zones,
                resource_type="SAT",
                directory=path,
            )
        )
        runs = timer.repeat(repeat=n, number=n)
        print(path, sum(runs) / n)


def process_demand_zips():
    a = datetime.datetime.now()

    root = Path("/home/dylan/Data/openISP/archive/openISP-server/isp_2024/")
    demand_dir = root / "demand"

    tmp_path = Path("/home/dylan/Data/openISP/archive/openISP-server/tmp")

    # Unpack all zips into tmp directory
    for zip_path in demand_dir.glob("*.zip"):
        print(zip_path)
        with tempfile.TemporaryDirectory() as tmpdir:
            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extractall(tmpdir)
                demand_traces.parse_demand_traces(
                    input_directory=tmpdir,
                    parsed_directory=tmp_path / "demand",
                    use_concurrency=True,
                )

    b = datetime.datetime.now()

    print(a)
    print(b)
    print(b - a)


def optimise_demand():
    a = datetime.datetime.now()
    root = Path("/home/dylan/Data/openISP/archive/openISP-server/")
    tmp_path = Path("/home/dylan/Data/openISP/archive/openISP-server/tmp")
    file_type = "demand"
    try:
        optimise_parquet.partition_traces_by_columns(
            input_directory=tmp_path / file_type,
            output_directory=root / "processed" / file_type,
            partition_cols=["scenario", "reference_year"],
        )
    except Exception as E:
        print(E)

    b = datetime.datetime.now()
    print(a)
    print(b)
    print(b - a)


def resource_splitter(file_type="project", split=275):
    root_dir = Path("/home/dylan/Data/openISP/archive/openISP-server")
    original_dir = root_dir / "processed" / file_type
    split_dir = root_dir / "split" / file_type

    # assumed one level of partioning
    for reference_year_dir in sorted(original_dir.iterdir()):
        input_file = reference_year_dir / "data_0.parquet"
        output_dir = split_dir / reference_year_dir.stem

        with duckdb.connect() as con:
            # Run the COPY statement
            con.execute(f"""
                        COPY
                        (SELECT * FROM read_parquet('{input_file}') ORDER BY DATETIME)
                        TO '{output_dir}' (FORMAT PARQUET, FILE_SIZE_BYTES '{split}MB') """)
        print(reference_year_dir.stem)


def demand_splitter():
    file_type = "demand"
    root_dir = Path("/home/dylan/Data/openISP/archive/openISP-server")
    original_dir = root_dir / "processed" / file_type
    split_dir = root_dir / "split" / file_type

    # assumes two level of partioning
    for scenario_dir in sorted(original_dir.iterdir()):
        for reference_year_dir in scenario_dir.iterdir():
            input_file = reference_year_dir / "data_0.parquet"

            (split_dir / scenario_dir.stem).mkdir(exist_ok=True)

            output_dir = split_dir / scenario_dir.stem / reference_year_dir.stem

            with duckdb.connect() as con:
                # Run the COPY statement
                con.execute(f"""
                            COPY
                            (SELECT * FROM read_parquet('{input_file}') ORDER BY DATETIME)
                            TO '{output_dir}' (FORMAT PARQUET, FILE_SIZE_BYTES '275MB') """)
            print(scenario_dir.stem, reference_year_dir.stem)
