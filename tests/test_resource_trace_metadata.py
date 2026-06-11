from pathlib import Path

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
