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

> [!WARNING]
> **Version 2.0 breaking change:** Version 2.0 of `isp-trace-parser` stores and reads parsed data in a new hive-partitioned storage format. Data originally parsed with version 1.x is **not compatible** with version 2.0.
While the original API itself remains largely the same, upgrading from version 1.x requires either [re-parsing your raw AEMO trace data](#parsing-trace-data) or [downloading pre-processed data](#pre-processed-trace-data).

  ## Table of contents

- [Install](#install)
- [How the package works](#how-the-package-works)
- [Accessing raw trace data](#accessing-raw-trace-data)
- [Key terminology](#key-terminology)
- [Examples](#examples)
    - [Parsing trace data](#parsing-trace-data)
    - [Querying parsed trace data using alternative approach](#querying-parsed-trace-data-using-alternative-approach)
    - [Querying trace data for sets of projects, zones or subregions](#querying-trace-data-for-sets-of-projects-zones-or-subregions)
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
     - *Reformatting* puts the data in a standard time series format (i.e. with a `datetime` column and `value` column).
     - The data is *restructured* into [Parquet](https://parquet.apache.org/) files, which significantly improves the speed at which data can be read from disk.
   - To access the full documentation for these functions, you can run `help` in the Python console, e.g. `help(parse_wind_traces)`.

2. Query the parsed data using the naming conventions for generators, renewable energy zones (REZs), and subregions established in the
   AEMO Inputs and Assumptions workbook (see [`isp-workbook-parser`](https://github.com/Open-ISP/isp-workbook-parser)) using the `get_data` functions
    - Refer to the [Querying parsed trace data example](https://github.com/Open-ISP/isp-trace-parser#querying-parsed-trace-data).
    - To access the full documentation for these functions, you can run `help` in the Python console, e.g.
  `help(get_data.solar_project_trace_single_reference_year)`.

## Accessing trace data

### Original trace data
Currently, AEMO trace data needs to be downloaded from the [AEMO website](https://aemo.com.au/en/energy-systems/major-publications/integrated-system-plan-isp/2024-integrated-system-plan-isp)
and unzipped manually before the trace parser can be used.

The zipped data is also archived in publicly accessible object storage ([data.openisp.au](https://data.openisp.au)). This can be downloaded by:

```python
from isp_trace_parser.remote import fetch_trace_data

fetch_trace_data("full", dataset_src="isp_2024", save_directory="data/archive", data_format="archive")
```

This will download all the archived zip files into the provided directory with the following structure:

```bash
archive/
└── isp_2024/
  ├── solar/
  ├── wind/
  └── demand/
```

### Pre-processed trace data

Trace data that has been processed into the hive-partitioned format is also available for download from the object store. Both "full" and "example" datasets are available (the example dataset contains only data for the 2018 reference year):

```python
from isp_trace_parser.remote import fetch_trace_data

# Download example dataset (2018 reference year only)
fetch_trace_data("example", dataset_src="isp_2024", save_directory="data/trace_data", data_format="processed")
```

This will download the processed parquet files with the following structure:

```bash
trace_data/
  ├── project/
  │   └── reference_year=<year>/
  ├── zone/
  │   └── reference_year=<year>/
  └── demand/
      └── scenario=<scenario_name>/
          └── reference_year=<year>/
```

## Key terminology

### Solar/wind

- _**Project**_: Traces for a specific solar/wind project
- _**Zone**_: Traces for a zone, e.g. a renewable energy zone
- _**Reference year**_: A historical weather year that is used to produce the generation trace.
  - Modelled years are mapped to reference years, e.g. generation data for one or multiple years can be mapped to a single reference year, or generation data for each year can be mapped to different reference years (refer to the [Querying parsed trace data example](https://github.com/Open-ISP/isp-trace-parser#querying-parsed-trace-data)).
- _**Resource type**_ : This is used to categorise types of resource data:
    - Solar:
        - FFP: fixed flat plate
        - SAT: single-axis tracking.
        - CST: concentrated solar thermal
    - Wind:
        - WH: onshore wind (high)
        - WL: onshore wind (low)
        - WFX: offshore wind (fixed)
        - WFL: offshore wind (floating)
        - WIND: (existing project)

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

The following code can then be used to parse out the `project`, `zone` or `demand` data, by making use of appropriate filters.

### Parsing all files in a directory

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

filters = SolarMetadataFilter(file_type=["project"])
parse_solar_traces(
    input_directory='<path/to/aemo/solar/traces>',
    parsed_directory='<path/to/store/project>',
    filters = filters,
)

filters = WindMetadataFilter(file_type=["project"])
parse_wind_traces(
    input_directory='<path/to/aemo/wind/traces>',
    parsed_directory='<path/to/store/project>',
    filters = filters,
)

filters = SolarMetadataFilter(file_type=["zone"])
parse_solar_traces(
    input_directory='<path/to/aemo/solar/traces>',
    parsed_directory='<path/to/store/zone>',
    filters = filters,
)

filters = WindMetadataFilter(file_type=["zone"])
parse_wind_traces(
    input_directory='<path/to/aemo/wind/traces>',
    parsed_directory='<path/to/store/zone>',
    filters = filters,
)

parse_demand_traces(
    input_directory='<path/to/aemo/demand/traces>',
    parsed_directory='<path/to/store/demand>',
)
```

### Optimising stored data
The following code illustrates how the parsed parquet files can be consolidated and optimised with `optimise_parquet.py`

> [!NOTE]
> There _may_ be an issue with this step on some architectures (see issue https://github.com/Open-ISP/isp-trace-parser/issues/23).

```python
from isp_trace_parser import optimise_parquet

# For optimising `zone` and `project`, suggest partitioning on reference year
optimise_parquet.partition_traces_by_columns(input_directory="<path/to/store/zone|project>",
                                             output_directory="<path/to/store/optimised_zone|optimised_project>",
                                             partition_cols=["reference_year"])

# For optimising `demand`, suggest partitioning on scenario and reference year
optimise_parquet.partition_traces_by_columns(input_directory="<path/to/store/demand>",
                                             output_directory="<path/to/store/optimised_demand>",
                                             partition_cols=["scenario", "reference_year"])
```


### Querying trace data for sets of projects, zones or subregions

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


# The project names can be used to retrieves a dataframe containing all project traces, which can be filtered by project name using the 'project' column"

solar_traces = get_data.get_project_single_reference_year(
    start_year=2025,
    end_year=2030,
    reference_year=2011,
    project=solar_generator_names,
    directory="parsed_project_data"
    )

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

# These sets of onshore and offshore REZ IDs can the be used to retrieve a dataframes containing all relevant traces, which can be filtered by REZ name using the 'zone' column"

wind_offshore_rez_traces = get_data.get_zone_single_reference_year(
    start_year=2025,
    end_year=2026,
    reference_year=2011,
    zone=list(offshore_rezs['REZ ID']),
    resource_type="WFL",
    directory="parsed_zone_data"
)

wind_onshore_rez_traces = get_data.get_zone_single_reference_year(
    start_year=2025,
    end_year=2026,
    reference_year=2011,
    zone=list(onshore_rezs['REZ ID']),
    resource_type="WH",
    directory="parsed_zone_data"
)

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

# The set of REZ IDs can be used to retrieves a dataframe containing all REZ traces, which can be filtered by REZ name using the 'zone' column"

single_axis_tracking_traces = get_data.get_zone_single_reference_year(
    start_year=2025,
    end_year=2026,
    reference_year=2011,
    zone=onshore_solar_rezs['REZ ID'],
    resource_type="SAT",
    directory="parsed_zone_data"
)

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

# "The list of subregion names retrieves a dataframe containing all subregion traces, which can be filtered or accessed by subregion name using the 'subregion' column"

demand_trace = get_demand_single_reference_year(
    start_year=2025,
    end_year=2026,
    reference_year=2011,
    scenario="Step Change",
    subregion=subregions,
    demand_type="OPSO_MODELLING",
    poe="POE50",
    directory="parsed_data/demand"
     )
```

</details>

### Querying parsed trace data using alternative approach

Once trace data has been parsed it can also queried using legacy API functionality (based on around querying technologies, areas, rather for example).

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
    directory='example_project_data/'
)

solar_project_trace_many_reference_years = get_data.solar_project_multiple_reference_years(
    reference_years={2022: 2011, 2024: 2012},
    project='Adelaide Desalination Plant Solar Farm',
    directory='example_project_data/'
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
    directory='example_rez_data/'
)

solar_rez_trace_many_reference_years = get_data.solar_area_multiple_reference_years(
    reference_years={2022: 2011, 2024: 2012},
    area='Q1',
    technology='SAT',
    directory='example_rez_data/'
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
    directory='parsed_project_data/'
)

wind_project_trace_many_reference_years = get_data.wind_project_multiple_reference_years(
    reference_years={2022: 2011, 2024: 2012},
    project='Bango 973 Wind Farm',
    directory='parsed_project_data/'
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
    directory='parsed_rez_data/'
)

wind_rez_trace_many_reference_years = get_data.wind_area_multiple_reference_years(
    reference_years={2022: 2011, 2024: 2012},
    area='Q1',
    resource_quality='WH',
    directory='parsed_rez_data/'
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
    directory='parsed_demand_data/'
)

demand_subregion_trace_many_reference_years = get_data.demand_multiple_reference_years(
    reference_years={2024: 2011},
    subregion='CNSW',
    scenario='Green Energy Exports',
    poe='POE10',
    demand_type='OPSO_MODELLING',
    directory='parsed_demand_data/'
)

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

### Polars DataFrame trace parsing

`isp-trace-parser` also exposes functionality for transforming input trace data (in a [`Polars`](https://pola.rs/)
`DataFrame`) in the AEMO format to a standard time series format (i.e. "datetime" and "value" columns). As shown
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
#              datetime  value
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
