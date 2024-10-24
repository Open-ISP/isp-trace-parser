from isp_trace_parser import solar_traces
import tempfile
import requests
from urllib.parse import urljoin
import zipfile

BASE_URL = "https://data.openisp.au"


def get_archive_trace(
    trace_type: str = "solar",
    version: str = "isp_2024",
    refyear: int = 2011,
    filepath: str = "isp_wind_traces_r2011.zip",
):
    """
    Function to get trace zipfile of particular `trace_type`, `version` and `refyear` and save to local `filepath`.
    """

    url = urljoin(
        BASE_URL,
        f"archive/traces/{trace_type}/{version}/isp_{trace_type}_traces_r{refyear}.zip",
    )
    with requests.get(url, stream=True) as response:
        with open(filepath, "wb") as file:
            for chunk in response.iter_content(chunk_size=1048576):
                file.write(chunk)


def unzip_trace_archive(zipfilepath: str, destination_dir: str):
    """
    Simple function to extracts all files from a zip archive to a destination directory.
    """

    with zipfile.ZipFile(zipfilepath, "r") as zip_ref:
        zip_ref.extractall(destination_dir)


def unzip_and_restructure(
    zipfilepath: str,
    parsed_directory: str,
):
    """
    Function to extact a zipfile to temp directory and restructure
    """

    with tempfile.TemporaryDirectory() as temp_dir:
        unzip_trace_archive(zipfilepath, temp_dir)
        solar_traces.parse_solar_traces(temp_dir, parsed_directory)


def get_unzip_and_resturcture(
    trace_type: str = "solar",
    version: str = "isp_2024",
    refyear: int = 2018,
    parsed_directory: str = "/home/dylan/Data/openISP/restructured_solar",
):
    """
    Function to download  zipfile to tempfile, extract to temp directory and restructure
    """

    with tempfile.NamedTemporaryFile() as temp_file:
        get_archive_trace(trace_type, version, refyear, filepath=temp_file.name)
        unzip_and_restructure(temp_file.name, parsed_directory)


def get_solar_project_multiple_reference_years(
    reference_years: dict[int, int],
    project: str,
    directory: str,
    year_type: str = "fy",
):
    """
    Directly access restructured files from object store (not implemented etc)
    (Idea would be to directly mimic / copy get_data functions).
    """
    pass
