from pathlib import Path
from typing import Callable, Dict, Literal
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
    Fetches and parses an index file from the object store (a YAML file containng the keys of
    objects in the object store

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
        version (str): The version of the trace data (e.g. "isp_2024")
        zipfilename (str): The name of the zip file containing the trace data.

    Properties:
        key (str): The key for the archive trace in the object store
        restructure_function (Callable): A function to process the trace data based on its type.
    """

    trace_type: Literal["solar", "wind", "demand"]
    version: Literal["isp_2024"]
    zipfilename: str

    @property
    def key(self):
        return f"archive/traces/{self.trace_type}/{self.version}/{self.zipfilename}"

    @property
    def restructure_function(self):
        handlers: Dict[str, Callable] = {
            "demand": demand_traces.parse_demand_traces,
            "solar": solar_traces.parse_solar_traces,
            "wind": wind_traces.parse_wind_traces,
        }
        return handlers[self.trace_type]


def get_archive(
    archive_trace: ArchiveTrace,
    filepath: str,
):
    """
    Downloads a specific trace zipfile from the archive and saves it to a local file.

    This function retrieves a zipfile (represented in the`ArchiveTrace` object)
    from the object store and saves it to the specified local `filepath`.

    Args:
        archive_trace (ArchiveTrace): An object containing details about the trace,
            including its type, version, and filename.
        filepath (str): The local file path where the downloaded zipfile will be saved.
    """

    url = urljoin(OBJECT_STORE, archive_trace.key)
    logger.info(f"Downloading {archive_trace.version} {archive_trace.zipfilename}")

    with requests.get(url, stream=True) as response:
        with open(filepath, "wb") as file:
            for chunk in response.iter_content(chunk_size=1048576):
                file.write(chunk)


def unzip_trace_archive(zipfilepath: str, destination_dir: str):
    """
    Simple function that xtracts all files from a zip archive to a specified destination directory.

    Args:
        zipfilepath (str): The path to the zip archive file to be extracted.
        destination_dir (str): The directory where the contents of the zip file will be extracted.
    """

    with zipfile.ZipFile(zipfilepath, "r") as zip_ref:
        zip_ref.extractall(destination_dir)


def unzip_and_restructure(
    zipfilepath: str, output_directory: str, restructure_function: Callable
):
    """
    Extracts a zipfile to a temporary directory and restructures its contents, by appling a
    restructuring function to process the extracted data, and saving it to a specified directory.


    Args:
        zipfilepath (str): The path to the zip file to be extracted.
        output_directory (str): The target directory where the restructured data will be saved.
        restructure_func (Callable): A callable function that processes the contents of the
        temporary directory and saves the restructured data to `output_directory`
    """

    with tempfile.TemporaryDirectory() as temp_dir:
        unzip_trace_archive(zipfilepath, temp_dir)
        restructure_function(temp_dir, output_directory)


def get_zipfiles(archive_directory: str, version: str = "isp_2024"):
    """
    Downloads and organizes a specific version of archived zip files.

    This function retrieves the archive index to determine the zip files
    corresponding to the given version and trace types ("solar", "wind", and "demand").
    It downloads these zip files from the object store and saves them in the specified
    archive directory, organized by trace type. If a zip file already exists in the
    specified location,it is not downloaded again .

    Args:
        archive_directory (str): The local directory where the trace zip files will be stored.
        version (str, optional): The version of the trace data to download. Defaults to "isp_2024".

    Example:
        get_zipfiles("/path/to/archive_dir", version="isp_2024")
    """
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
            get_archive(archive_trace, filepath=filepath)


def process_zipfiles(
    archive_directory: str, restructured_directory: str, version: str = "isp_2024"
):
    input_dir = Path(archive_directory)
    output_dir = Path(restructured_directory)

    """
    Processes zip files from a directory containing archived zipfiles and restructures their contents.

    This function iterates through zip files for the specified trace types ("solar", "wind", "demand")
    in the given archive directory. It extracts their contents to a corresponding directory in the
    restructured directory and applies a restructuring function based on the trace type.

    Args:
        archive_directory (str): The path to the directory containing trace zip files organized by trace type.
        restructured_directory (str): The path to the directory where restructured files will be saved,
            organized by trace type.
        version (str, optional): The version of the trace data being processed. Defaults to "isp_2024".

    Example:
        process_zipfiles("/path/to/archive", "/path/to/restructured", version="isp_2024")
    """

    for trace_type in ["solar", "wind", "demand"]:
        input_trace_dir = input_dir / trace_type
        output_trace_dir = output_dir / trace_type
        Path.mkdir(output_trace_dir, exist_ok=True)

        for zipfilepath in input_trace_dir.glob("*.zip"):
            archive_trace = ArchiveTrace(
                trace_type=trace_type, version=version, zipfilename=zipfilepath.name
            )
            logger.info(
                f"Processing {archive_trace.version} {archive_trace.zipfilename}"
            )
            unzip_and_restructure(
                zipfilepath, output_trace_dir, archive_trace.restructure_function
            )
