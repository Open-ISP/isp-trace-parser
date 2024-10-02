# AEMO Integrated System Plan Trace Parser

[![PyPI version](https://badge.fury.io/py/isp-trace-parser.svg)](https://badge.fury.io/py/isp-trace-parser)
[![Continuous Integration and Deployment](https://github.com/Open-ISP/isp-trace-parser/actions/workflows/cicd.yml/badge.svg)](https://github.com/Open-ISP/isp-trace-parser/actions/workflows/cicd.yml)
[![codecov](https://codecov.io/gh/Open-ISP/isp-trace-parser/graph/badge.svg?token=HLRLX78WHP)](https://codecov.io/gh/Open-ISP/isp-trace-parser)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/Open-ISP/isp-trace-parser/main.svg)](https://results.pre-commit.ci/latest/github/Open-ISP/isp-trace-parser/main)
[![Rye](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/rye/main/artwork/badge.json)](https://rye.astral.sh)

A Python package for reformatting and accessing demand, solar, and wind time series data used by the Australian Energy
Market Operator in their Integrated System Plan (ISP) modelling study. Currently, the trace parser only data in the
format of the 2024 ISP.

## Table of contents

- [Install](#install)
- [How the package works](#how-the-package-works)
- [Examples](#examples)
    - [Parsing trace data](#parsing-trace-data)
    - [Querying parsed trace data](#querying-parsed-trace-data)
    - [Constructing reference year mapping](#constructing-reference-year-mapping)
    - [Dataframe trace parsing](#dataframe-trace-parsing)
- [Contributing](#contributing)
- [License](#license)

## Install

```
pip install isp-trace-parser
```

## How the package works

1. Parse raw AEMO trace data using the functions `parse_wind_traces`, `parse_solar_traces`, and
   `parse_demand_traces`. This reformats the data, saving it to a specified directory. The data reformatting
   restructures the data to a standard time series format with a 'Datetime' column and 'Values' column. Additionally,
   the data is saved in half-yearly chunks in parquet files, which significantly improves read from disk speeds. For
   further information (via the API docs) you can run `help` in the Python console, e.g. `help(parse_wind_traces)`.
2. Query the parsed data using the naming conventions for generators, REZs, and subregions established in the
   IASR workbook using the `get_data` functions, see the [Querying parsed trace data](https://github.com/Open-ISP/isp-trace-parser#querying-parsed-trace-data).
   For further information on querying run `help` in the Python console, e.g.
  `help(solar_project_trace_single_reference_year)`.

## Examples

### Parsing trace data

If AEMO trace data is downloaded onto a local machine it can be reformatted using isp_trace_parser. To perform the
restructuring solar, wind, and demand data should each be store in separate directories, then the following code can be
used to parse the data. No exact directory structure within solar, wind, and demand subdirectories needs to be followed.

```python
from isp_trace_parser import parse_solar_traces, parse_wind_traces, parse_demand_traces

parse_solar_traces(
    input_directory='<path/to/aemo/solar/traces>',
    parsed_directory='<path/to/store/solar/output>',
)

parse_wind_traces(
    input_directory='<path/to/aemo/wind/traces>',
    parsed_directory='<path/to/store/wind/output>',
)

parse_demand_traces(
    input_directory='<path/to/aemo/demand/traces>',
    parsed_directory='<path/to/store/demand/output>',
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
    directory='example_parsed_data/solar'
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
    demand_type='OPSO_MODELLING',
    directory='example_parsed_data/demand'
)

demand_subregion_trace_many_reference_years = get_data.demand_multiple_reference_years(
    reference_years={2024: 2011},
    subregion='CNSW',
    scenario='Green Energy Exports',
    poe='POE10',
    demand_type='OPSO_MODELLING',
    directory='example_parsed_data/demand'
)

```

### Constructing reference year mapping

A helper function is provided to allow user to construct reference year mappings for use with the `get_data` functions.
The sequence of reference years specified is cycled to construct a mapping between the start and end year.

```python
from isp_trace_parser import construct_reference_year_mapping

mapping = construct_reference_year_mapping(
    start_year=2030,
    end_year=2035,
    reference_years=[2011, 2013, 2018],
)
print(mapping)
# {2030: 2011, 2031: 2013, 2032: 2018, 2033: 2011, 2034: 2013, 2035: 2018}
```

### Dataframe trace parsing

isp-trace-parse also exposes functionality for transforming trace data, as a polars DataFrame, from AEMO format to
"Datetime" and "Values" format. The polar package also provides functionality for converting to and from pandas if
required.

```python
import polars as pl
from isp_trace_parser import trace_formatter

aemo_format_data = pl.DataFrame({
    'Year': [2024, 2024],
    'Month': [6, 6],
    'Day': [1, 2],
    '01': [11.2, 15.3],
    '02': [30.7, 20.4],
    '48': [17.1, 18.9]
})

print(trace_formatter(aemo_format_data))
# shape: (6, 2)
# ┌─────────────────────┬───────┐
# │ Datetime            ┆ Value │
# │ ---                 ┆ ---   │
# │ datetime[μs]        ┆ f64   │
# ╞═════════════════════╪═══════╡
# │ 2024-06-01 00:30:00 ┆ 11.2  │
# │ 2024-06-01 01:00:00 ┆ 30.7  │
# │ 2024-06-02 00:00:00 ┆ 17.1  │
# │ 2024-06-02 00:30:00 ┆ 15.3  │
# │ 2024-06-02 01:00:00 ┆ 20.4  │
# │ 2024-06-03 00:00:00 ┆ 18.9  │
# └─────────────────────┴───────┘
```

## Contributing

Interested in contributing to the source code or adding table configurations? Check out the [contributing instructions](https://github.com/Open-ISP/isp-trace-parser/blob/main/CONTRIBUTING.md), which also includes steps to install `package_name` for development.

Please note that this project is released with a [Code of Conduct](https://github.com/Open-ISP/isp-trace-parser/blob/main/CONDUCT.md). By contributing to this project, you agree to abide by its terms.

## License

`package_name` was created as a part of the [OpenISP project](https://github.com/Open-ISP). It is licensed under the terms of [GNU GPL-3.0-or-later](https://github.com/Open-ISP/isp-trace-parser/blob/main/LICENSE) licences.
