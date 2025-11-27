from pathlib import Path
from tempfile import TemporaryDirectory

from isp_trace_parser.remote import download

TEST_URL = "https://data.openisp.au/test/test_file.txt"
TEST_EXPECTED_CONTENT = b"ISP Trace Parser Test File\n"


def test_download_test_file():
    """Test download with actual server file."""

    with TemporaryDirectory() as tmp_path:
        tmp_path = Path(tmp_path)
        download._download_file(TEST_URL, tmp_path, strip_levels=0)
        downloaded = tmp_path / "test" / "test_file.txt"

        assert downloaded.exists()
        assert downloaded.read_bytes() == TEST_EXPECTED_CONTENT

    with TemporaryDirectory() as tmp_path:
        tmp_path = Path(tmp_path)


def test_download_with_retry():
    """Test retry logic with real server."""

    with TemporaryDirectory() as tmp_path:
        tmp_path = Path(tmp_path)
        download._download_with_retry(TEST_URL, tmp_path, strip_levels=0, max_retries=3)

        assert (tmp_path / "test" / "test_file.txt").exists()


def test_fetch_trace_data_with_test_manifest():
    """Test downloading from a small, test manifest."""

    with TemporaryDirectory() as tmp_path:
        tmp_path = Path(tmp_path)
        download._download_from_manifest(
            "archive/test_isp_2024", tmp_path, strip_levels=1
        )

        assert len(list(tmp_path.iterdir())) == 1

        downloaded = tmp_path / "test_file.txt"

        assert downloaded.exists()
        assert downloaded.read_bytes() == TEST_EXPECTED_CONTENT
