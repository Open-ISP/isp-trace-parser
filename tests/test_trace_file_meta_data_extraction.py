from isp_trace_parser import metadata_extractors


def test_demand_trace_metadata_extraction():
    file_name = "VIC_RefYear_2011_STEP_CHANGE_POE10_OPSO_MODELLING.csv"
    metadata = metadata_extractors.extract_demand_trace_metadata(file_name)
    assert metadata["subregion"] == "VIC"
    assert metadata["reference_year"] == 2011
    assert metadata["scenario"] == "STEP_CHANGE"
    assert metadata["poe"] == "POE10"
    assert metadata["demand_type"] == "OPSO_MODELLING"
