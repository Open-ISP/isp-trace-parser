from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from isp_trace_parser.remote import download

TEST_URL = "https://data.openisp.au/test/test/test_file.txt"
TEST_EXPECTED_CONTENT = b"ISP Trace Parser Test File\n"


def test_download_test_file():
    """Test download with actual server file."""

    with TemporaryDirectory() as tmp_path:
        tmp_path = Path(tmp_path)
        download._download_file(TEST_URL, tmp_path, strip_levels=0)
        downloaded = tmp_path / "test" / "test" / "test_file.txt"

        assert downloaded.exists()
        assert downloaded.read_bytes() == TEST_EXPECTED_CONTENT


def test_download_with_retry():
    """Test retry logic with real server."""

    with TemporaryDirectory() as tmp_path:
        tmp_path = Path(tmp_path)
        download._download_with_retry(TEST_URL, tmp_path, strip_levels=0, max_retries=3)

        assert (tmp_path / "test" / "test" / "test_file.txt").exists()


def test_fetch_trace_data_with_test_manifest(monkeypatch):
    """Test downloading from a small, test manifest.
    The testing manifest, while still named "full_isp_2024" here, is just a test manifest
    with containing a single url ("https://data.openisp.au/test/test/test_file.txt")
    """

    with TemporaryDirectory() as tmp_path:
        tmp_path = Path(tmp_path)

        # Point to test fixtures instead of production manifests
        def mock_files(package):
            return Path(__file__).parent / "fixtures" / "manifests"

        monkeypatch.setattr("isp_trace_parser.remote.download.files", mock_files)

        download._download_from_manifest(
            "archive/full_isp_2024", tmp_path, strip_levels=2
        )

        assert len(list(tmp_path.iterdir())) == 1

        downloaded = tmp_path / "test_file.txt"

        assert downloaded.exists()
        assert downloaded.read_bytes() == TEST_EXPECTED_CONTENT


def test_manifest_not_found():
    """Test downloading from a small, test manifest."""

    with pytest.raises(FileNotFoundError):
        download._download_from_manifest(
            "archive/missing_manifest", "/", strip_levels=2
        )


@pytest.mark.parametrize("unquote", [True, False])
def test_fetch_trace_data(unquote: bool, monkeypatch):
    """Test downloading via fetch_trace_data with test fixtures.
    This, while still download a dataset name "full", is just a pointing to a test manifest
    manifest with containing a single url ("https://data.openisp.au/test/test/test_file.txt")
    """

    with TemporaryDirectory() as tmp_path:
        tmp_path = Path(tmp_path)

        # Point to test manifests instead of production manifests
        def mock_files(package):
            return Path(__file__).parent / "fixtures" / "manifests"

        monkeypatch.setattr("isp_trace_parser.remote.download.files", mock_files)

        download.fetch_trace_data(
            "full", "isp_2024", tmp_path, "archive", unquote_path=unquote
        )

        assert len(list(tmp_path.iterdir())) == 1

        downloaded = tmp_path / "test_file.txt"

        assert downloaded.exists()
        assert downloaded.read_bytes() == TEST_EXPECTED_CONTENT


def test_wrong_source():
    # no ISP 2025 data
    with pytest.raises(ValueError):
        download.fetch_trace_data("test", "isp_2025", "/", "archive")


def test_wrong_format():
    # only archive or processed data (not other)
    with pytest.raises(ValueError):
        download.fetch_trace_data("test", "isp_2024", "/", "other")


def test_wrong_type():
    # only full or example type
    with pytest.raises(ValueError):
        download.fetch_trace_data("other", "isp_2024", "/", "archive")


def test_empty_manifest(monkeypatch):
    """Test that empty manifest raises ValueError."""
    from importlib.resources import files

    with TemporaryDirectory() as tmp_path:
        tmp_path = Path(tmp_path)

        # Point to test manifest instead of production manifests
        def mock_files(package):
            return Path(__file__).parent / "fixtures" / "manifests"

        monkeypatch.setattr("isp_trace_parser.remote.download.files", mock_files)

        with pytest.raises(ValueError, match="No URLs found in manifest"):
            download._download_from_manifest("empty_manifest", tmp_path, strip_levels=0)


def test_strip_levels_too_high():
    """Test that strip_levels >= path parts raises ValueError."""
    with TemporaryDirectory() as tmp_path:
        tmp_path = Path(tmp_path)

        # TEST_URL has path "test/test/test_file.txt" = 3 parts
        with pytest.raises(ValueError, match="Cannot strip .* levels"):
            download._download_file(TEST_URL, tmp_path, strip_levels=10)
