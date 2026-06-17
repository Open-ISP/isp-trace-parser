from pathlib import Path

import pytest

from isp_trace_parser import demand_trace_metadata


def test_build():
    """Two examples spanning different scenario / poe / demand_type /
    subregion values. Every combination resolves through the same single
    dict lookup, so two are enough for testing.
    """
    files = [
        Path("VIC_RefYear_2011_STEP_CHANGE_POE10_OPSO_MODELLING.csv"),
        Path("CNSW_RefYear_2023_HYDROGEN_EXPORT_POE50_OPSO_MODELLING_PVLITE.csv"),
    ]
    metadata = demand_trace_metadata.build(files, version="2024")

    assert metadata[files[0]] == {
        "subregion": "VIC",
        "reference_year": 2011,
        "scenario": "STEP_CHANGE",
        "poe": "POE10",
        "demand_type": "OPSO_MODELLING",
    }
    assert metadata[files[1]] == {
        "subregion": "CNSW",
        "reference_year": 2023,
        "scenario": "HYDROGEN_EXPORT",
        "poe": "POE50",
        "demand_type": "OPSO_MODELLING_PVLITE",
    }


@pytest.mark.parametrize(
    "filename",
    [
        "VIC_2011_STEP_CHANGE_POE10_OPSO_MODELLING.csv",  # missing _RefYear_
        "VIC_RefYear_201a_STEP_CHANGE_POE10_OPSO_MODELLING.csv",  # non-digit year
        "VIC_RefYear_2011_MYSTERY_POE10_OPSO_MODELLING.csv",  # lookup miss
    ],
)
def test_build_rejects_unexpected_filename(filename):
    with pytest.raises(ValueError, match="Unexpected trace filename"):
        demand_trace_metadata.build([Path(filename)], version="2024")
