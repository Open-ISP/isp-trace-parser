from isp_trace_parser import meta_data_extractors


def test_solar_trace_meta_data_extraction():
    file_name = "Woolooga_SAT_RefYear2023.csv"
    meta_data = meta_data_extractors.extract_solar_trace_meta_data(file_name)
    assert meta_data['name'] == 'Woolooga'
    assert meta_data['technology'] == 'SAT'
    assert meta_data['year'] == '2023'
    assert meta_data['file_type'] == 'project'

    file_name = "Darling_Downs_FFP_RefYear2023.csv"
    meta_data = meta_data_extractors.extract_solar_trace_meta_data(file_name)
    assert meta_data['name'] == 'Darling_Downs'
    assert meta_data['technology'] == 'FFP'
    assert meta_data['year'] == '2023'
    assert meta_data['file_type'] == 'project'

    file_name = "REZ_N0_NSW_Non-REZ_CST_RefYear2023.csv"
    meta_data = meta_data_extractors.extract_solar_trace_meta_data(file_name)
    assert meta_data['name'] == 'N0'
    assert meta_data['technology'] == 'CST'
    assert meta_data['year'] == '2023'
    assert meta_data['file_type'] == 'area'


def test_wind_trace_meta_data_extraction():
    file_name = "ARWF1_RefYear2023.csv"
    meta_data = meta_data_extractors.extract_wind_trace_meta_data(file_name)
    assert meta_data['name'] == 'ARWF1'
    assert meta_data['year'] == '2023'
    assert meta_data['file_type'] == 'project'

    file_name = "CAPTL_WF_RefYear2023.csv"
    meta_data = meta_data_extractors.extract_wind_trace_meta_data(file_name)
    assert meta_data['name'] == 'CAPTL_WF'
    assert meta_data['year'] == '2023'
    assert meta_data['file_type'] == 'project'

    file_name = "N8_WH_Cooma-Monaro_RefYear2023.csv"
    meta_data = meta_data_extractors.extract_wind_trace_meta_data(file_name)
    assert meta_data['name'] == 'N8'
    assert meta_data['resource_type'] == 'WH'
    assert meta_data['year'] == '2023'
    assert meta_data['file_type'] == 'area'


def test_demand_trace_meta_data_extraction():
    file_name = "VIC_RefYear_2011_STEP_CHANGE_POE10_OPSO_MODELLING.csv"
    meta_data = meta_data_extractors.extract_demand_trace_meta_data(file_name)
    assert meta_data['area'] == 'VIC'
    assert meta_data['year'] == '2011'
    assert meta_data['scenario'] == 'STEP_CHANGE'
    assert meta_data['poe'] == 'POE10'
    assert meta_data['descriptor'] == 'OPSO_MODELLING'

