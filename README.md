# AEMO Integrated System Plan Trace Parser

[![PyPI version](https://badge.fury.io/py/isp-trace-parser.svg)](https://badge.fury.io/py/isp-trace-parser)
[![Continuous Integration and Deployment](https://github.com/Open-ISP/isp-trace-parser/actions/workflows/cicd.yml/badge.svg)](https://github.com/Open-ISP/isp-trace-parser/actions/workflows/cicd.yml)
[![codecov](https://codecov.io/gh/Open-ISP/isp-trace-parser/graph/badge.svg?token=HLRLX78WHP)](https://codecov.io/gh/Open-ISP/isp-trace-parser)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/Open-ISP/isp-trace-parser/main.svg)](https://results.pre-commit.ci/latest/github/Open-ISP/isp-trace-parser/main)
[![UV](https://camo.githubusercontent.com/4ab8b0cb96c66d58f1763826bbaa0002c7e4aea0c91721bdda3395b986fe30f2/68747470733a2f2f696d672e736869656c64732e696f2f656e64706f696e743f75726c3d68747470733a2f2f7261772e67697468756275736572636f6e74656e742e636f6d2f61737472616c2d73682f75762f6d61696e2f6173736574732f62616467652f76302e6a736f6e)](https://github.com/astral-sh/uv)

A Python package for reformatting and accessing demand, solar generation and wind generation time series data used by the Australian Energy
Market Operator (AEMO) in their Integrated System Plan (ISP) modelling study.

> [!IMPORTANT]
> Currently, `isp-trace-parser` only supports trace data in the format of the 2024 ISP.

## Table of contents

- [Install](#install)
- [How the package works](#how-the-package-works)
- [Accessing raw trace data](#accessing-raw-trace-data)
- [Key terminology](#key-terminology)
- [Examples](#examples)
    - [Parsing trace data](#parsing-trace-data)
    - [Querying parsed trace data](#querying-parsed-trace-data)
    - [Querying trace data for a sets of generators, areas or subregions](#querying-trace-data-for-a-sets-of-generators-areas-or-subregions)
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

## Accessing raw trace data

Currently, AEMO trace data needs to be downloaded from the [AEMO website](https://aemo.com.au/en/energy-systems/major-publications/integrated-system-plan-isp/2024-integrated-system-plan-isp)
and unzipped manually before the trace parser can be used.

> [!Note]
> However, it is likely future versions of the trace parser will automate this process by using a third party platform to host the trace data.

## Key terminology

### Solar/wind

- _**Project**_: Traces for a specific solar/wind project
- _**Area**_: Traces for an area, e.g. a renewable energy zone
- _**Reference year**_: A historical weather year that is used to produce the generation trace.
  - Modelled years are mapped to reference years, e.g. generation data for one or multiple years can be mapped to a single reference year, or generation data for each year can be mapped to different reference years (refer to the [Querying parsed trace data example](https://github.com/Open-ISP/isp-trace-parser#querying-parsed-trace-data)).
- _**Technology**_ (solar): Fixed flat plate (FFP), single-axis tracking (SAT), concentrated solar thermal (CST).
- _**Resource quality**_ (wind):
    - Onshore wind: Wind High (WH) and Wind Low (WL)
    - Offshore wind: Wind Offshore Fixed (WFX) and Wind Offshore Floating (WFL)

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

### Querying trace data for a sets of generators, areas or subregions

Often modelling or analysis will require a set of traces. For example, all the existing solar generators traces, all
the wind REZ traces, or all the subregion demand traces. To query a set of traces the names of generators, REZ IDs,
or subregion IDs can be retrieved from the IASR workbook using the
[isp-workbook-parser](https://github.com/Open-ISP/isp-workbook-parser). Using isp-workbook-parser the workbook data can
be exported to CSVs, and then required names, REZ IDs, or subregion IDs extracted, as shown below:

<details>
<summary>Wind and solar project traces</summary>
<br>

```python
from pathlib import Path

import pandas as pd
from isp_trace_parser import get_data


# Define location of parsed data.

parsed_workbook_data = Path(
    "/path/to/parsed/workbook/data"
)

parsed_solar_data = Path('path/to/parsed/solar/traces')

# Wind and solar generator names are stored across four IASR workbook tables

existing_generators = pd.read_csv(
    parsed_workbook_data / Path("existing_generator_summary.csv")
)

committed_generators = pd.read_csv(
    parsed_workbook_data / Path("committed_generator_summary.csv")
)

anticipated_generators = pd.read_csv(
    parsed_workbook_data / Path("anticipated_projects_summary.csv")
)

additional_generators = pd.read_csv(
    parsed_workbook_data / Path("additional_projects_summary.csv")
)


# Before combining the data tables we need to standardise the generator name column

generator_tables = [
    existing_generators,
    committed_generators,
    anticipated_generators,
    additional_generators
]

for table in generator_tables:
    table.rename(
        columns={table.columns.values[0]: "Generator"},
        inplace=True
    )

generator_data = pd.concat(generator_tables)


# The names of solar and wind projects/generators can be retrieved by filtering

solar_generators = generator_data[generator_data['Technology type'] == 'Large scale Solar PV']

solar_generator_names = list(solar_generators['Generator'])

print(solar_generator_names)
# ['Avonlie Solar Farm', 'Beryl Solar Farm', 'Bomen Solar Farm', 'Broken Hill Solar Farm' . . .

wind_generators = generator_data[generator_data['Technology type'] == 'Wind']

wind_generator_names = list(wind_generators['Generator'])

print(wind_generator_names)
# ['Bango 973 Wind Farm', 'Bango 999 Wind Farm', 'Boco Rock Wind Farm', 'Bodangora Wind Farm' . . .


# These names can be used to retrieve trace data

solar_generator_traces = {}

for generator_name in solar_generator_names:
    trace_for_generator = get_data.solar_project_single_reference_year(
        start_year=2025,
        end_year=2030,
        reference_year=2011,
        project=generator_name,
        directory=parsed_solar_data
    )
    solar_generator_traces[generator_name] = trace_for_generator
```

</details>


<details>
<summary>Wind area traces</summary>
<br>

```python
from pathlib import Path

import pandas as pd
from isp_trace_parser import get_data


# Define location of parsed data.

parsed_workbook_data = Path(
    "/path/to/parsed/workbook/data"
)

parsed_wind_data = Path('path/to/parsed/wind/traces')

# ISP REZ IDs and wind resource types can be retrieved from the parsed workbook data

build_limits = pd.read_csv(
    parsed_workbook_data / Path("initial_build_limits.csv")
)

# If a unit has a non-nan offshore floating build limit then it will have the wind
# resource qualities WFL and WFX (wind offshore floating and wind offshore fixed).

offshore_rezs = build_limits[~build_limits["Wind generation total limits (MW)_Offshore -floating"].isna()]

print(list(offshore_rezs['REZ ID']))
# ['N10', 'N11', 'V7', 'V8', 'S10', 'T4']

# If a unit has a nonzero high build limit then it will be an on shore REZ and have the wind
# resource qualities WH and WM (wind high and wind medium).

onshore_rezs = build_limits[build_limits["Wind generation total limits (MW)_High"] > 0.1]

print(list(onshore_rezs['REZ ID']))
# ['Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', . . .

# These sets of onshore and offshore REZ IDs then can be used to retrieve trace data

wind_offshore_rez_traces = {}

for rez in list(offshore_rezs['REZ ID']):
    trace_for_rez = get_data.wind_area_single_reference_year(
        start_year=2025,
        end_year=2026,
        reference_year=2011,
        area=rez,
        resource_quality='WFL',
        directory=parsed_wind_data
    )
    wind_offshore_rez_traces[rez] = trace_for_rez

wind_onshore_rez_traces = {}

for rez in list(onshore_rezs['REZ ID']):
    trace_for_rez = get_data.wind_area_single_reference_year(
        start_year=2025,
        end_year=2026,
        reference_year=2011,
        area=rez,
        resource_quality='WH',
        directory=parsed_wind_data
    )
    wind_onshore_rez_traces[rez] = trace_for_rez
```

</details>


<details>
<summary>Solar area traces</summary>
<br>

```python
from pathlib import Path

import pandas as pd
from isp_trace_parser import get_data


# Define location of parsed data.

parsed_workbook_data = Path(
    "/path/to/parsed/workbook/data"
)

parsed_solar_data = Path('path/to/parsed/wind/traces')

# ISP REZ IDs and types can be retrieved from the parsed workbook data

build_limits = pd.read_csv(
    parsed_workbook_data / Path("initial_build_limits.csv")
)

# If a unit has a nonzero high build limit then it will be an onshore REZ and have the
# solar traces for SAT (single axis tracking) and CST (concentrating solar thermal).

onshore_solar_rezs = build_limits[build_limits["Solar PV plus Solar thermal Limits (MW)_Solar"] > 0.1]

print(list(onshore_solar_rezs['REZ ID']))
# ['Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', . . .

# These sets of onshore and offshore REZ IDs then can be used to retrieve trace data

single_axis_tracking_traces = {}

for rez in list(onshore_solar_rezs['REZ ID']):
    trace_for_rez = get_data.solar_area_single_reference_year(
        start_year=2025,
        end_year=2026,
        reference_year=2011,
        area=rez,
        technology='SAT',
        directory=parsed_solar_data
    )
    single_axis_tracking_traces[rez] = trace_for_rez
```

</details>


<details>
<summary>Demand subregion traces</summary>
<br>

```python
from pathlib import Path

import pandas as pd
from isp_trace_parser import get_data


# Define location of parsed data.

parsed_workbook_data = Path(
    "/path/to/parsed/workbook/data"
)

parsed_demand_data  = Path('path/to/parsed/demand/traces')

# ISP Subregion ID can be retrieved from renewable energy zones table

rez_definitions = pd.read_csv(
    parsed_workbook_data / Path("renewable_energy_zones.csv")
)

subregions = list(set(rez_definitions["ISP Sub-region"]))
print(subregions)
# ['CSA', 'SESA', 'CQ', 'NQ', 'NNSW', 'CNSW', 'SNSW', 'SNW', 'TAS', 'VIC', 'SQ']

# This set of subregions can then can be used to retrieve demand trace data

demand_traces = {}

for subregion in subregions:
    trace = get_data.demand_single_reference_year(
        start_year=2025,
        end_year=2026,
        reference_year=2011,
        subregion=subregion,
        demand_type='OPSO_MODELLING',
        poe='POE50',
        scenario='Step Change',
        directory=parsed_demand_data
    )
    demand_traces[subregion] = trace

```

</details>

### Constructing a reference year mapping

A helper function is provided to allow you to construct reference year mappings for use with the `get_data` multiple reference year functions.

The sequence of reference years specified is cycled from first to last and mapped to data years starting
from `start_year` and ending in `end_year`.

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
below, the data can be converted to polars from pandas before performing Dataframe trace parsing, and back to pandas
after the parsing is complete, the polars package provides [functionality for converting to and from `pandas`](https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.to_pandas.html).


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
