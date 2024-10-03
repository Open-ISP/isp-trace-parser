# AEMO Integrated System Plan Trace Parser

[![PyPI version](https://badge.fury.io/py/isp-trace-parser.svg)](https://badge.fury.io/py/isp-trace-parser)
[![Continuous Integration and Deployment](https://github.com/Open-ISP/isp-trace-parser/actions/workflows/cicd.yml/badge.svg)](https://github.com/Open-ISP/isp-trace-parser/actions/workflows/cicd.yml)
[![codecov](https://codecov.io/gh/Open-ISP/isp-trace-parser/graph/badge.svg?token=HLRLX78WHP)](https://codecov.io/gh/Open-ISP/isp-trace-parser)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/Open-ISP/isp-trace-parser/main.svg)](https://results.pre-commit.ci/latest/github/Open-ISP/isp-trace-parser/main)
[![Rye](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/rye/main/artwork/badge.json)](https://rye.astral.sh)

A Python package for reformatting and accessing demand, solar generation and wind generation time series data used by the Australian Energy
Market Operator (AEMO) in their Integrated System Plan (ISP) modelling study.

> [!IMPORTANT]
> Currently, `isp-trace-parser` only supports trace data in the format of the 2024 ISP.

## Table of contents

- [Install](#install)
- [How the package works](#how-the-package-works)
- [Key terminology](#key-terminology)
- [Examples](#examples)
    - [Parsing trace data](#parsing-trace-data)
    - [Querying parsed trace data](#querying-parsed-trace-data)
    - [Constructing reference year mapping](#constructing-reference-year-mapping)
    - [Dataframe trace parsing](#dataframe-trace-parsing)
- [Contributing](#contributing)
- [License](#license)

## Install

```bash
pip install isp-trace-parser
```

## How the package works

1. Parse raw AEMO trace data using the functions `parse_wind_traces`, `parse_solar_traces`, and
   `parse_demand_traces`.
   - These functions reformat and restructure the data to a specified directory.
     - *Reformatting* puts the data in a standard time series format (i.e. with a `Datetime` column and `Values` column).
     - The data is *restructured* into half-yearly chunks in [Parquet](https://parquet.apache.org/) files, which significantly improves the speed at which data can be read from disk.
   - To access the full documentation for these functions, you can run `help` in the Python console, e.g. `help(parse_wind_traces)`.

2. Query the parsed data using the naming conventions for generators, renewable energy zones (REZs), and subregions established in the
   AEMO Inputs and Assumptions workbook (see [`isp-workbook-parser`](https://github.com/Open-ISP/isp-workbook-parser)) using the `get_data` functions
    - Refer to the [Querying parsed trace data example](https://github.com/Open-ISP/isp-trace-parser#querying-parsed-trace-data).
    - To access the full documentation for these functions, you can run `help` in the Python console, e.g.
  `help(get_data.solar_project_trace_single_reference_year)`.

## Key terminology

### Solar/wind

- _**Project**_: Traces for a specific solar/wind project
- _**Area**_: Traces for an area, e.g. a renewable energy zone
- _**Reference year**_: A historical weather year that is used to produce the generation trace.
  - Modelled years are mapped to reference years, e.g. generation data for one or multiple years can be mapped to a single reference year, or generation data for each year can be mapped to different reference years (refer to the [Querying parsed trace data example](https://github.com/Open-ISP/isp-trace-parser#querying-parsed-trace-data)).
- _**Technology**_ (solar): Fixed flat plate (FFP), single-axis tracking (SAT), concentrated solar thermal (CST).
- _**Resource quality**_ (wind): Wind High (WH), Wind Low (WL).

### Demand
- _**Reference year**_: A historical weather year that is used to produce the demand trace.
  - Modelled years are mapped to reference years, e.g. demand data for one or multiple years can be mapped to a single reference year, or demand data for each year can be mapped to different reference years (refer to the [Querying parsed trace data example](https://github.com/Open-ISP/isp-trace-parser#querying-parsed-trace-data)).
- _**Subregion**_: ISP subregion (refer to the [ISP methodology](https://aemo.com.au/-/media/files/stakeholder_consultation/consultations/nem-consultations/2023/isp-methodology-2023/isp-methodology_june-2023.pdf?la=en))
- _**Scenario**_: ISP scenario (refer to the [ISP methodology](https://aemo.com.au/-/media/files/stakeholder_consultation/consultations/nem-consultations/2023/isp-methodology-2023/isp-methodology_june-2023.pdf?la=en))
- _**POE**_: Probability of exceedance (refer to [AEMO Demand Terms documentation](https://aemo.com.au/-/media/Files/Electricity/NEM/Security_and_Reliability/Dispatch/Policy_and_Process/Demand-terms-in-EMMS-Data-Model.pdf)). Generally either POE10 or POE50.
- _**Demand type**_: `OPSO_MODELLING`, `OPSO_MODELLING_PVLITE` or `PV_TOT`. Refer to [this ESOO document](https://aemo.com.au/-/media/files/electricity/nem/planning_and_forecasting/nem_esoo/2024/model-instructions-2024-esoo.pdf?la=en) for a description of each.

## Examples

### Parsing trace data

If AEMO trace data is downloaded onto a local machine, it can be reformatted using `isp_trace_parser`.

To perform the reformatting and restructuring, the solar, wind and demand data should each be stored in separate directories (though no exact directory structure within the solar, wind and demand subdirectories needs to be followed).

The following code can then be used to parse the data:

### Parsing all files in a directory

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

### Filtering which files get parsed

```python
from isp_trace_parser import (
    parse_solar_traces,
    SolarMetadataFilter,
    parse_wind_traces,
    WindMetadataFilter,
    parse_demand_traces,
    DemandMetadataFilter
)

# Note: to not filter on a component of the metadata it can be excluded from the filter definition.

solar_filters = SolarMetadataFilter(
    name=['N1'],
    file_type=['area'],
    technology=['SAT'],
    reference_year=[2011]
)

parse_solar_traces(
    input_directory='<path/to/aemo/solar/traces>',
    parsed_directory='<path/to/store/solar/output>',
    filters=solar_filters
)

wind_filters = WindMetadataFilter(
    name=['N1'],
    file_type=['area'],
    resouce_quality=['WH'],
    reference_year=[2011]
)

parse_wind_traces(
    input_directory='<path/to/aemo/wind/traces>',
    parsed_directory='<path/to/store/wind/output>',
    filters=wind_filters
)

demand_filters = DemandMetadataFilter(
    scenario=['Green Energy Exports'],
    subregion=['CNSW'],
    poe=['POE50'],
    demand_type=['OPSO_MODELLING'],
    reference_year=[2011]
)

parse_demand_traces(
    input_directory='<path/to/aemo/demand/traces>',
    parsed_directory='<path/to/store/demand/output>',
    filters=demand_filters
)
```

### Querying parsed trace data

Once trace data has been parsed it can be queried using the following API functionality.

<details>
<summary>Solar project traces from 2022 to 2024 (for a single reference year), and for 2022 and 2024 (multiple reference years)</summary>
<br>

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
```

</details>

<details>
<summary>Solar area/REZ traces from 2022 to 2024 (for a single reference year), and for 2022 and 2024 (multiple reference years)</summary>
<br>

```python
from isp_trace_parser import get_data
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
```

</details>

<details>
<summary>Wind project traces from 2022 to 2024 (for a single reference year), and for 2022 and 2024 (multiple reference years)</summary>
<br>

```python
from isp_trace_parser import get_data
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
```

</details>

<details>
<summary>Wind area/REZ traces from 2022 to 2024 (for a single reference year), and for 2022 and 2024 (multiple reference years)</summary>
<br>

```python
from isp_trace_parser import get_data
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
```

</details>

<details>
<summary>OPSO_MODELLING POE10 traces from 2022 to 2024 (for a single reference year), and for 2024 (multiple reference years) from the "Green Energy Exports" scenario</summary>
<br>

```python
from isp_trace_parser import get_data
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

</details>

### Constructing a reference year mapping

A helper function is provided to allow you to construct reference year mappings for use with the `get_data` multiple reference year functions.

The sequence of reference years specified is cycled from first to last to construct and mapoped to data years starting from `start_year` and ending in `end_year`.

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

### polars Dataframe trace parsing

`isp-trace-parser` also exposes functionality for transforming input trace data (in a [`polars`](https://pola.rs/)
`DataFrame`) in the AEMO format to a standard time series format (i.e. "Datetime" and "Values" columns). As shown
below, the polars package also provides [functionality for converting to and from `pandas`](https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.to_pandas.html).


```python
import polars as pl
import pandas as pd
from isp_trace_parser import trace_formatter

aemo_format_data = pd.DataFrame({
    'Year': [2024, 2024],
    'Month': [6, 6],
    'Day': [1, 2],
    '01': [11.2, 15.3],
    '02': [30.7, 20.4],
    '48': [17.1, 18.9]
})

aemo_format_data_as_polars = pl.from_pandas(aemo_format_data)

trace_parser_format_data = trace_formatter(aemo_format_data_as_polars)

print(trace_parser_format_data.to_pandas())
#              Datetime  Value
# 0 2024-06-01 00:30:00   11.2
# 1 2024-06-01 01:00:00   30.7
# 2 2024-06-02 00:00:00   17.1
# 3 2024-06-02 00:30:00   15.3
# 4 2024-06-02 01:00:00   20.4
# 5 2024-06-03 00:00:00   18.9
```

## Contributing

Interested in contributing to the source code? Check out the [contributing instructions](https://github.com/Open-ISP/isp-trace-parser/blob/main/CONTRIBUTING.md), which also includes steps to install `isp-trace-parser` for development.


Please note that this project is released with a [Code of Conduct](https://github.com/Open-ISP/isp-trace-parser/blob/main/CONDUCT.md). By contributing to this project, you agree to abide by its terms.

## License

`isp-trace-parser` was created as a part of the [OpenISP project](https://github.com/Open-ISP). It is licensed under the terms of [GNU GPL-3.0-or-later](https://github.com/Open-ISP/isp-trace-parser/blob/main/LICENSE) licences.
