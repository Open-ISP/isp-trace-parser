from pathlib import Path

import pytest

from isp_trace_parser import resource_trace_metadata


def test_build():
    """One test covers function logic compared with regex approach

    Solar zones / wind zones / extra reference years add no new code-path
    coverage (they're different YAML rows, not different code)
    """
    files = [
        Path("Adelaide_Desal_FFP_RefYear2011.csv"),
        Path("BLUFF1_RefYear2011.csv"),
    ]
    metadata = resource_trace_metadata.build(files, version="2024")

    assert metadata[files[0]] == {
        "name": "Adelaide_Desal",
        "reference_year": 2011,
        "resource_type": "FFP",
        "file_type": "project",
    }
    assert metadata[files[1]]["resource_type"] == "WIND"


@pytest.mark.parametrize(
    "filename",
    [
        "Adelaide_Desal.csv",  # missing _RefYear separator
        "Adelaide_Desal_RefYear.csv",  # missing year
        "Adelaide_Desal_RefYear2011a.csv",  # non-digit year
        "Mystery_Plant_RefYear2011.csv",  # stem not in mapping
    ],
)
def test_build_rejects_unexpected_filename(filename):
    with pytest.raises(ValueError, match="Unexpected trace filename"):
        resource_trace_metadata.build([Path(filename)], version="2024")
