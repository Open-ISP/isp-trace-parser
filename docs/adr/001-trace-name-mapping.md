# ADR-001: Trace name mapping convention

## Status

Proposed — 2026-05-28

Would supersede the implicit conventions encoded in `metadata_extractors.py` and
the split solar/wind × project/zone YAML files under `src/isp_trace_parser/mappings/`.

This ADR *only* relates to format of trace *mapping* data - not it's downstream use (e.g. filtering, creating hive partitioned data, etc - though those uses kept in mind while developing this )

## Context

The 2026 ISP trace data introduces new filename patterns (DREZs, Distributed Resources, split Q8 zones - see issue #36) that the existing regex-based metadata extractors
misclassify (issue #40).

Updating / integrating these new traces presents an opportunity to streamline and improve the current approach.

Characteristics of the current approach:

- **Filename reconstruction in code.** Metadata is extracted from input filenames via regex, then output filenames are reconstructed from that metadata. Every new AEMO filename pattern requires a regex change.
- **Partial mapping also exits:**  Mapping does exist, but for trace names to IASR names only)
		- **Four mapping files per ISP version.** `solar_project_mapping.yaml`,`wind_project_mapping.yaml`, `solar_zone_mapping.yaml`,`wind_zone_mapping.yaml` (each with its own conventions - and the zone_maps being largely redundant)
		- (**Coupling to IASR naming.** - downstream issue, but parsed output filenames are rewritten to IASR conventions, which couples)
- **Project/zone split.** The parser distinguishes "project"  (named generator) from "zone" (REZ) traces, but the CSVs   are all co-located in a single folder per reference year (the distinction is also re-derived from filename shape).
- **Silent misclassification & misses classificiation.** Some patterns match
  *any* `*_RefYear<year>.csv`, so unrecognised filenames silently fall
  through with `file_type="project"`, `resource_type="WIND"` (see # issue 36). Some trace

### Issues with current approach and new data:
1) The 2026 data includes new  that don't fit the project/zone split cleanly: Distributed Resources are at ISP subregion resolution; DREZs are REZ-like but distinct.

2) AEMO's conventions also differ across ISP versions:
	- **ISP-2024**: no formal IASR identifier. Generators are referred to by  a human-readable name (e.g. `"Bango 973 Wind Farm"`).
	- **ISP-2026**: introduces a formal IASR ID, in addition to the human-readable name.

- 3) Maintaining regex patterns that work across multiple / new AEMO formats sound like a literal nightmare (noting that we will continue to get  updated traces in the future)

- 4)  Maintaining 4 resource maps per ISP also annoying (noting that IASR ID doesn't exist for 2024 ISP, and the mappings are already not internally consistent structures anyway)

We already do (partial) mapping with - this ADR proposes a full ***explicit*** mapping for each ISP (or source) version

This ADR sets out proposed mapping convention for ISP traces. ISP-2024  will be migrated to the same convention first.  This is done for backwards compatability - but actually mainly because of testing and incremental implementation (i.e. there is no existing tests / safeguards for just trying on the 2026 data)

## Decision

### 1. One mapping file per ISP version, per concern

```
src/isp_trace_parser/mappings/
├── 2024/
│   ├── resources.yaml
│   ├── demand.yaml
│   └── topography.yaml
├── 2026/
│   ├── resources.yaml
│   ├── demand.yaml
│   └── topography.yaml
```

Three files per version, regardless of technology or geographic resolution. The project/zone split is removed; the solar/wind split is removed.  Technology and location-type are carried as fields on each entry in the yaml (see below).

Add includes a separate topographic hierarchy (region → subregion → zone) into `topography.yaml`. ( rationale discuss below)

### 2. Resources YAML: trace-stem-keyed, IASR aliases as values

```yaml
# mappings/2026/resources.yaml

BLUFF1:
  location_type: project
  location: BLUFF1
  resource_type: wind
  zone: S3                              # required: topography doesn't track projects
  iasr_aliases: ["Hallett 5 The Bluff Wind Farm"]

Coleambally_SAT:
  location_type: project
  location: Coleambally
  resource_type: solar_sat
  subregion: SNSW                       # no formal REZ- subregion is the tightest known parent
  iasr_aliases: ["Coleambally Solar Farm"]

Walla_Walla_SAT:
  location_type: project
  location: Walla_Walla
  resource_type: solar_sat
  subregion: SNSW
  iasr_aliases: ['WLWLSF1', 'WLWLSF2']      # many IASR identifiers share one trace file

REZ_N6_Wagga_Wagga_CST:
  location_type: zone
  location: N6                          # no `zone:` - parent subregion resolved via topography
  resource_type: solar_cst
  iasr_aliases: []                      # no IASR id / generator equivalent in 2024 IASR


# example of many traces feed one IASR name
CLRKCWF1:
  location_type: project
  location: CLRKCWF1
  resource_type: wind
  zone: Q4
  iasr_aliases: ["Clarke Creek Wind Farm"]

CLRKCWF2:
  location_type: project
  location: CLRKCWF2
  resource_type: wind
  zone: Q4
  iasr_aliases: ["Clarke Creek Wind Farm"]
```

**Key:** the trace filename stem (the part before `_RefYear<year>.csv`), including any resource_type suffix.

**Filename convention assumption.** This ADR assumes AEMO's `<stem>_RefYear<year>.csv` filename pattern.

The parser would extract `reference_year` by a single `rpartition("_RefYear")`split; everything else comes from the mapping. If AEMO happened to changes the refyear  convention in a future ISP release  this function helper needs updating in one place (but the mapping itself is unaffected)

**Fields:**

| Field           | Required          | Allowed values                                                                                                                                                                                                      |
| --------------- | ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `location_type` | yes               | `project`, `zone` (`drez`),  `subregion`,                                                                                                                                                                           |
| `location`      | yes               | Llocation identifier, distinct from the trace-stem key. For `project`, the bare project name (e.g. `Coleambally`); for `zone`/`drez`, the zone code (e.g. `N6`); for `subregion`, the subregion code (e.g. `SNSW`). |
| `resource_type` | yes               | strings: `wind`, `wind_high`, `wind_medium`, `solar_sat`, `solar_cst`, `solar_ffp`, …                                                                                                                               |
| `zone`          | conditional       | REZ code from `topography.yaml`. <br>Set `zone` when the project sits inside a formal REZ. <br>Omitted on `zone`/`drez`/`subregion` entries.                                                                        |
| `subregion`     | conditional       | ISP subregion code from `topography.yaml`. <br>Set on a `project` entry **only when** the project lies outside any formal REZ <br>Omitted on `zone`/`drez`/`subregion` entries.                                     |
| `iasr_aliases`  | yes (may be `[]`) | list of identifiers under which this trace is known in IASR/external sources                                                                                                                                        |

### Notes:

1) **Asymmetric parent storage.** Only `project` entries carry an explicit parent (`zone:` or `subregion:`, whichever is tightest). Every other entry type omits a parent field — the parent is resolved by looking the entry's `location` up in `topography.yaml`:
	- **project**  `zone:` on the entry when the project sits inside a   formal REZ; otherwise `subregion:` as the tightest known parent.   Topography does not track projects, so resources must carry this.
	- **zone** / **drez**: parent subregion is `topography["zones"][location]`.
	- **subregion**: parent region is `topography["subregions"][location]`.

	The resolver would be a single small python function (not yet implemented).
	- **Schema validation could be implemented**:  (in a way that a parallels the resolver)

2) **`iasr_aliases` is always a list, including the empty list**.  Idea is that keeps downstream code on a single branch and distinguishes "no IASR equivalent" (empty list) from "schema error" (missing key).
	1) Chose  more neutral "iasr_alias" -  "iasr_id" is new (and maybe might change? - and there is no "iasr_id" in 2024 ISP, but there is human readable generator names). Ie. the type of identifier not encoded in scheme .


### 3. Topography YAML: normalised hierarchy

```yaml
# mappings/2026/topography.yaml

regions: [NSW, VIC, QLD, SA, TAS]

subregions:
  CNSW: NSW
  NNSW: NSW
  SNSW: NSW
  VIC:  VIC
  CQ:   QLD
  NQ:   QLD
  SQ:   QLD
  CSA:  SA
  SESA: SA
  TAS:  TAS

zones:
  N0: CNSW  # AEMO non-REZ NSW
  V0: VIC   # AEMO non-REZ VIC
  Q1: NQ  # Far North QLD
  Q2: NQ  # North Qld Clean Energy Hub
  Q3: NQ  # Northern Qld
  Q4: CQ  # Isaac
  Q5: CQ  # Barcaldine
  # …
```

Each subregion/zone entry's value is its parent in the hierarchy
	- in future, if something else needed here (e.g. display names / human readable names can be easily added - some extra details includes as comments for now )

AEMO's non-REZ  zones (`N0`, `V0`) parent at a real  subregion like every other zone (AEMO attributes "NSW Non-REZ" to `CNSW` and "Victoria Non-REZ" to `VIC` in its new-entrants modelling
inputs). This keeps every zone on the same `zone -> subregion -> region`
chain with no special case in the resolver.

### 4. Demand YAML

```yaml
# mappings/2026/demand.yaml

scenarios:
  "Step Change": step_change
  "Progressive Change": progressive_change
  "Green Energy Exports": green_energy_exports

poe_levels: [POE10, POE50]

demand_types: [OPSO_MODELLING, OPSO_MODELLING_PVLITE]
```

Demand filenames are a  product / combination of these options, as well as`reference_year` and `subregion`. The YAML declares the  each possible value rather than enumerating ~100+ valid combinations.

The `subregion` axis is **not** redeclared here — it's resolved from `topography.yaml` (every subregion defined there is assumed to have a demand trace). This avoids a second list of subregion codes that has to stay in sync with the geography file.

At load time the loader expands the vocabularies into a stem-keyed dict so the parser's ingest loop has the same shape as for resources. The  asymmetry is in the YAML format (which matches each dataset's actual shape - and parser pipeline is the same).

### 5. Ingest pipeline: mapping-driven, no regex

Metadata extractors would be removed entirely. The ingest path becomes:

```
zip → ZipFile.namelist() → dict-lookup against mapping (fail loud on unknown)
    → ZipFile.open(member) → polars.scan_csv → write parquet to output
```

The only thing extracted from the filename is `reference_year`, via a more simple `rpartition("_RefYear")` split.

Unknown filenames raise `KeyError` rather than silently misclassifying. The mapping file is the canonical list of traces the parser supports.

## Consequences

### Positive

- **No regex maintenance.** New AEMO filename patterns require a YAML edit,   not a code change.
- **Loud failures.** Unmapped filenames raise; silent misclassification   (#40) becomes impossible.
- **Single source of truth per version.** Three YAMLs per ISP version,   all declarative; geography defined once and referenced.
- **Normalised geography.** Region → subregion → zone is defined once   per ISP version. Resource entries carry only their tightest known  parent; no per-entry duplication of upstream geography.
- **Uniform parser pipeline.** Resources and demand share the ingest loop;   the difference is in two small loader functions.
- **Decoupled from IASR naming.** The parser is faithful to AEMO source  data; IASR lookup is an optional layer via the reverse index built from  `iasr_aliases`.
- **Reviewable.** YAML diffs are easy to inspect; adding a trace is one   block.
- **Resolution-agnostic.** DREZs, Distributed Resources, and any future  new entity kinds are accommodated by adding a `location_type` value,   not by special-casing filename shapes.
### Negative

- **Update cost.** New ISP versions require writing the resources  YAML up front.
	- (though easier than updating regex!)
- **Redundancy on moderately common case.** For 1:1 traces, the key duplicates
  one entry of `iasr_aliases`. Acceptable redundancy
- **Different YAML shapes.** Resources are entity-keyed; demand is  option-keyed; geography is a small dimension file.
- **Mutual-exclusion check on projects.** A `project` entry must carry   exactly one of `zone:` / `subregion:` (both present, or neither,   is a schema error. A trivial load-time check.
-  **Cross-file integrity.** `zone:` and `subregion:` values in   `resources.yaml` must exist in `topography.yaml`; this is now a load-time   check rather than an implicit assumption.

## Alternatives considered

### IASR-identifier-keyed YAML

Key by IASR generator/REZ identifier; trace filenames as values.

Rejected because:

- Most 2024 entries have no IASR  generator identifier, forcing the something else to be the key anyway.
- The parser ingest path is filename → metadata
	- (trace-keyed gives   that direct, IASR-keyed requires a reverse index).
	- Checks and look up between YAML and `ZipFile.namelist()` are easier  when keyed by trace stem.

The reverse index (alias → trace stems) needed by user-facing `get_data`is straight forward for uses that want to select processed data by IASR names or ID's (iasr_alias)

### Separate YAMLs for projects and zones

Rejected because the source CSVs are co-located, and the project-vs-zone classification problem at parse time is kind of the exactly the regex problem this ADR tries removes.   With `location_type` (i.e project/zone) adds more files / schemas for no gain (afact)

### Denormalised geography (region/subregion/zone on every entry)

Earlier discussion considered carrying `region`, `subregion`, and `rez` as fields on every resource entry. Rejected because:

- Region duplicates information already implied by subregion (10 → 5
  values) - essentially a DRY argument.
- Subregion duplicates information already implied by zone (≥40 → 10
  values) - also DRY.
- Three fields per entry must be kept consistent with each other by
  convention; with hundreds of entries the duplication is a real
  maintenance and review surface (potential maintainance issue and/or easier to make a mistake.

The original idea of keeping it all co-location / together explicitly was essentially for
review-ability / ease of reading.  Considered that  `topography.yaml` itself actually easier to review / maintain  (for example, any diffs to heirachy only change one file in one place, rather than hundreds of lines in a a file ).

Replaced by `topography.yaml` plus a single tightest-parent field on each entry.

### Parent-pointer shape on resource entries (e.g. `parent: {zone: N6}`)

Considered as a way to keep the schema uniform - every resource entry has exactly one `parent:` map regardless of whether it points at a zone or a subregion.

Rejected in favour of flat `zone:` / `subregion:` fields  because flat fields are more skimmable / readable (imho) and the uniformity argument matters more in `topography.yaml` than in `resources.yaml`  (which is read/reviewed/edited by humans - hopefully).

### Replacing `zone:` / `subregion:` with `location:`

Considered as a way to give every resource entry an identical shape regardless of `location_type`(i.e.  drop `zone` and `subregion` from resources entirely and have a single `location` field resolved via `topography.yaml`'s `projects` / `zones` / `subregions` sections to derive the geographic parent).

Rejected because the "complexity" it removes is essentially avoided a two-line helper:

```python
def parent_id(entry):
    return entry.get("zone") or entry.get("subregion")
```


Centralising it behind a `location` field would:

- Add a new `projects:` section to `topography.yaml` mapping each   project to its zone, duplicating the project namespace across   `resources.yaml` (as keys) and `topography.yaml` (as `projects:`   keys), which then have to agree.
- Replace a direct, self-contained item with two entries (ie. a reader/reviewer would have to jump to  `topography.yaml` for every project entry entry)
- Provide no additional information that isn't already derivable from the direct fields ( the helper above is does that).

### All geographic fields on every entry (nullable)

Considered as a way to make every entry uniform *without* the above issues - that is  every entry carries both `zone:` and `subregion:` (and any future levels), with values explicitly null where they don't apply.

Rejected because of additional verbosity *and* raises a potential consistency problem: with both `zone` and `subregion` storable on the  same entry, the two can drift (someone updates one, forgets / doesn't properly update the other, or sets a zone whose subregion contradicts the stored value).

`topography.yaml` already encodes the zone → subregion relationship canonically; replicating it inline on every entry adds complexity for no readability gain.

### Explicit demand `subregions:` list in demand YAML

Also discuess a  enumerated list of the demand-trace subregions in `demand.yaml`.

Rejected once `topography.yaml` exists, because the list would have to stay in sync with the subregions defined there.

## References

- GitHub discussions
  [#41](https://github.com/Open-ISP/isp-trace-parser/discussions/41),
  [#42](https://github.com/Open-ISP/isp-trace-parser/discussions/42)
- Issues #36, #39, #40
