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
    """
    Fetches and parses an index file from the object store (a YAML file containng the keys of objects in the object store 
    
    Returns a A dictionary containing the "traces" section of the index.
    """

    url = urljoin(OBJECT_STORE, "archive/archive-index.yml")
    response = requests.get(url)
    return yaml.safe_load(response.text)["traces"]

@dataclass
class ArchiveTrace:
    """
    Represents an archived trace file with a specific type and version.

    Attributes:
        trace_type (str): The type of trace (e.g., "demand", "solar", "wind").
        version (str): The version of the trace data.
        zipfilename (str): The name of the zip file containing the trace data.
    
    Properties:
        key (str): The storage key for the archive trace in the format 
                   'archive/traces/{trace_type}/{version}/{zipfilename}'.
        restructure_function (Callable): A function to process the trace data based on its type.
    """
    trace_type: str
    version: str
    zipfilename: str

    @property
    def key(self):
        return f"archive/traces/{self.trace_type}/{self.version}/{self.zipfilename}"
    
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
    logger.info(f"Downloading {archive_trace.version} {archive_trace.zipfilename}")

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
    output_directory: str
):
    """
    Function to download  zipfile to tempfile, extract to temp directory and restructure
    """

    with tempfile.NamedTemporaryFile() as temp_file:
        get_archive(archive_trace=archive_trace, filepath=temp_file.name)
        logger.info(f"Processing {archive_trace.version} {archive_trace.zipfilename}")
        unzip_and_restructure(temp_file.name, output_directory, archive_trace.restructure_function)

def get_all(archive_directory: str,
            version: str = "isp_2024"):
    
    store_keys = get_trace_index()
    _dir = Path(archive_directory)

    archive_traces = [
    ArchiveTrace(trace_type=trace_type, version=version, zipfilename=zipfilename)
    for trace_type in ["solar", "wind", "demand"]
    for zipfilename in store_keys[trace_type][version]
    ]

    for archive_trace in archive_traces:
        trace_dir = _dir / archive_trace.trace_type
        Path.mkdir(trace_dir, exist_ok=True)
        filepath = trace_dir / archive_trace.zipfilename
        if not filepath.exists():
            get_archive(archive_trace,  filepath=filepath)

def process_all(archive_directory: str,
                restructured_directory: str,
                version: str = "isp_2024"):
    
    input_dir = Path(archive_directory)
    output_dir =Path(restructured_directory)

    for trace_type in ["solar", "wind", "demand"]:
        input_trace_dir = input_dir / trace_type
        output_trace_dir = output_dir / trace_type 
        Path.mkdir(output_trace_dir, exist_ok=True)

        for zipfilepath in input_trace_dir.glob("*.zip"):
            archive_trace =ArchiveTrace(trace_type=trace_type, version=version, zipfilename=zipfilepath.name)
            logger.info(f"Processing {archive_trace.version} {archive_trace.zipfilename}")
            unzip_and_restructure(zipfilepath, output_trace_dir, archive_trace.restructure_function)
            
