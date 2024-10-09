from isp_trace_parser import metadata_extractors


def test_solar_trace_metadata_extraction():
    file_name = "Woolooga_SAT_RefYear2023.csv"
    metadata = metadata_extractors.extract_solar_trace_metadata(file_name)
    assert metadata["name"] == "Woolooga"
    assert metadata["technology"] == "SAT"
    assert metadata["reference_year"] == 2023
    assert metadata["file_type"] == "project"

    file_name = "Darling_Downs_FFP_RefYear2023.csv"
    metadata = metadata_extractors.extract_solar_trace_metadata(file_name)
    assert metadata["name"] == "Darling_Downs"
    assert metadata["technology"] == "FFP"
    assert metadata["reference_year"] == 2023
    assert metadata["file_type"] == "project"

    file_name = "REZ_N0_NSW_Non-REZ_CST_RefYear2023.csv"
    metadata = metadata_extractors.extract_solar_trace_metadata(file_name)
    assert metadata["name"] == "N0"
    assert metadata["technology"] == "CST"
    assert metadata["reference_year"] == 2023
    assert metadata["file_type"] == "area"


def test_wind_trace_metadata_extraction():
    file_name = "ARWF1_RefYear2023.csv"
    metadata = metadata_extractors.extract_wind_trace_metadata(file_name)
    assert metadata["name"] == "ARWF1"
    assert metadata["reference_year"] == 2023
    assert metadata["file_type"] == "project"

    file_name = "CAPTL_WF_RefYear2023.csv"
    metadata = metadata_extractors.extract_wind_trace_metadata(file_name)
    assert metadata["name"] == "CAPTL_WF"
    assert metadata["reference_year"] == 2023
    assert metadata["file_type"] == "project"

    file_name = "N8_WH_Cooma-Monaro_RefYear2023.csv"
    metadata = metadata_extractors.extract_wind_trace_metadata(file_name)
    assert metadata["name"] == "N8"
    assert metadata["resource_quality"] == "WH"
    assert metadata["reference_year"] == 2023
    assert metadata["file_type"] == "area"


def test_demand_trace_metadata_extraction():
    file_name = "VIC_RefYear_2011_STEP_CHANGE_POE10_OPSO_MODELLING.csv"
    metadata = metadata_extractors.extract_demand_trace_metadata(file_name)
    assert metadata["subregion"] == "VIC"
    assert metadata["reference_year"] == 2011
    assert metadata["scenario"] == "STEP_CHANGE"
    assert metadata["poe"] == "POE10"
    assert metadata["demand_type"] == "OPSO_MODELLING"
