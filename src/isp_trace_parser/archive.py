from pathlib import Path
from typing import Callable, Dict
import yaml
from isp_trace_parser import solar_traces, wind_traces, demand_traces
import tempfile
import requests
from urllib.parse import urljoin
import zipfile
import logging

from pydantic.dataclasses import dataclass

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

OBJECT_STORE = "https://data.openisp.au"

def get_trace_index():
    url = urljoin(OBJECT_STORE, "archive/archive-index.yml")
    response = requests.get(url)
    return yaml.safe_load(response.text)["traces"]

@dataclass
class ArchiveTrace:
    trace_type: str
    version: str
    zipfile: str

    @property
    def key(self):
        return f"archive/traces/{self.trace_type}/{self.version}/{self.zipfile}"
    
    @property
    def restructure_function(self):
        
        handlers: Dict[str, Callable] = {
            "demand" : demand_traces.parse_demand_traces,
            "solar" : solar_traces.parse_solar_traces,
            "wind": wind_traces.parse_wind_traces
            }
        return handlers[self.trace_type]


def get_archive(
    archive_trace: ArchiveTrace,
    filepath: str,
):
    """
    Function to get trace zipfile of particular `trace_type`, `version` and `refyear` and save to local `filepath`.
    """

    url = urljoin(OBJECT_STORE,archive_trace.key)
    logger.info(f"Downloading {archive_trace.version} {archive_trace.zipfile}")

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
    restructure_func: Callable
):
    """
    Function to extact a zipfile to temp directory and restructure
    """

    with tempfile.TemporaryDirectory() as temp_dir:
        unzip_trace_archive(zipfilepath, temp_dir)
        restructure_func(temp_dir, parsed_directory)


def get_unzip_and_restructure(
    archive_trace: ArchiveTrace,
    parsed_directory: str
):
    """
    Function to download  zipfile to tempfile, extract to temp directory and restructure
    """

    with tempfile.NamedTemporaryFile() as temp_file:
        get_archive(archive_trace=archive_trace, filepath=temp_file.name)
        logger.info(f"Processing {archive_trace.version} {archive_trace.zipfile}")
        unzip_and_restructure(temp_file.name, parsed_directory, archive_trace.restructure_function)

def get_all(parsed_trace_data_dir: str,
            version: str = "isp_2024"):
    
    store_keys = get_trace_index()
    _dir = Path(parsed_trace_data_dir)

    archive_traces = [
    ArchiveTrace(trace_type=trace_type, version=version, zipfile=zipfile)
    for trace_type, versions in store_keys.items()
    for version, zipfiles in versions.items()
    for zipfile in zipfiles
    ]

    for archive_trace in archive_traces[48:]:
        trace_dir = _dir / archive_trace.trace_type
        Path.mkdir(trace_dir, exist_ok=True)
        get_unzip_and_restructure(archive_trace, parsed_directory=trace_dir)
