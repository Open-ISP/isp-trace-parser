from scratch import new_dir

# isp-trace-parser

A Python package for reformatting and accessing demand, solar, and wind time series data used by the Australian Energy
Market Operator in their Integrated System Plan (ISP) modelling study. Currently, the trace parser only data in the
format of 2024 ISP.

## Examples

The trace parser has two core functionalities. Firstly reformatted or parsing raw AEMO trace data into a more
standard programmatic format. Secondly querying the parsed data.

### Reformatting (parsing) trace data

If AEMO trace data is downloaded onto a local machine it can be reformatted using isp_trace_parser. To perform the
restructuring solar, wind, and demand data should each be store in separate directories, then the following code can be
used to parse the data. No exact directory structure within solar, wind, and demand subdirectories needs to be followed.

```python
from isp_trace_parser import parse_solar_traces, parse_wind_traces, parse_demand_traces

parse_solar_traces(
    input_directory='<path/to/aemo/solar/traces>',
    new_directory='<path/to/store/solar/output>',
)

parse_wind_traces(
    input_directory='<path/to/aemo/wind/traces>',
    parsed_directory='<path/to/store/wind/output>',
)

parse_solar_traces(
    input_directory='<path/to/aemo/demand/traces>',
    new_directory='<path/to/store/demand/output>',
)
```

### Querying parsed trace data

Once trace data has been parsed it can be queried using the following API functionality.

```python
from isp_trace_parser import get_data

solar_project_trace_single_reference_year = get_data.solar_project_single_reference_year(
    start_year=2022,
    end_year=2024,
    reference_year=2011,
    project='Adelaide Desalination Plant Solar Farm',
    directory='<path/to/parsed/solar/data>'
)

solar_project_trace_many_reference_years = get_data.solar_project_multiple_reference_years(
    reference_years={2022: 2011, 2024: 2012},
    project='Adelaide Desalination Plant Solar Farm',
    directory='example_parsed_data/solar'
)

solar_rez_trace_single_reference_years = get_data.solar_area_single_reference_year(
    start_year=2022,
    end_year=2024,
    reference_year=2011,
    area='Q1',
    technology='SAT',
    directory='example_parsed_data/solar'
)

solar_rez_trace_many_reference_years = get_data.solar_area_multiple_reference_years(
    reference_years={2022: 2011, 2024: 2012},
    area='Q1',
    technology='SAT',
    directory='example_parsed_data/solar'
)

wind_project_trace_single_reference_years = get_data.wind_project_single_reference_year(
    start_year=2022,
    end_year=2024,
    reference_year=2011,
    project='Bango 973 Wind Farm',
    directory='example_parsed_data/wind'
)

wind_project_trace_many_reference_years = get_data.wind_project_multiple_reference_years(
    reference_years={2022: 2011, 2024: 2012},
    project='Bango 973 Wind Farm',
    directory='example_parsed_data/wind'
)

wind_rez_trace_single_reference_years = get_data.wind_area_single_reference_year(
    start_year=2022,
    end_year=2024,
    reference_year=2011,
    area='Q1',
    resource_quality='WH',
    directory='example_parsed_data/wind'
)

wind_rez_trace_many_reference_years = get_data.wind_area_multiple_reference_years(
    reference_years={2022: 2011, 2024: 2012},
    area='Q1',
    resource_quality='WH',
    directory='example_parsed_data/wind'
)

demand_subregion_trace_single_reference_years = get_data.demand_single_reference_year(
    start_year=2024,
    end_year=2024,
    reference_year=2011,
    subregion='CNSW',
    scenario='Green Energy Exports',
    poe='POE10',
    descriptor='OPSO_MODELLING',
    directory='example_parsed_data/demand'
)

demand_subregion_trace_many_reference_years = get_data.demand_multiple_reference_years(
    reference_years={2024: 2011},
    subregion='CNSW',
    scenario='Green Energy Exports',
    poe='POE10',
    descriptor='OPSO_MODELLING',
    directory='example_parsed_data/demand'
)

```

## Contributing

Interested in contributing to the source code or adding table configurations? Check out the [contributing instructions](./CONTRIBUTING.md), which also includes steps to install `package_name` for development.

Please note that this project is released with a [Code of Conduct](./CONDUCT.md). By contributing to this project, you agree to abide by its terms.

## License

`package_name` was created as a part of the [OpenISP project](https://github.com/Open-ISP). It is licensed under the terms of [GNU GPL-3.0-or-later](LICENSE) licences.
