# MAES Summary Dataset Schema

This document describes the four Apache Parquet datasets written by `Summaries2.py` during the summarization phase of a MAES simulation run.

---

## Overview

The four datasets form a pipeline:

```
InstEmissions  ──►  SiteSummary  ──►  SimSummary
               ──►  EventSummary
```

- **`InstEmissions`** — raw per-event, per-MC-run emission records; the timeseries source for all downstream summaries.
- **`SiteSummary`** — annual emission statistics (kg/year and unit variants) aggregated across MC runs, at multiple grouping levels per site.
- **`EventSummary`** — event-level statistics (count, duration, rate) aggregated across MC runs, at multiple grouping levels per site.
- **`SimSummary`** — simulation-wide statistics rolled up across all sites, derived by reading `SiteSummary`.

## Config Keys and Resolved Paths

| Dataset | Config key | Resolved path |
|---|---|---|
| `InstEmissions` | `parquetNewInstEmissions` | `{parquetDir}/SummaryNew/InstEmissions` |
| `SiteSummary` | `parquetNewSummary` | `{parquetDir}/SummaryNew/SiteSummary` |
| `EventSummary` | `parquetNewEventSummary` | `{parquetDir}/SummaryNew/EventSummary` |
| `SimSummary` | `parquetNewSimSummary` | `{parquetDir}/SummaryNew/SimSummary` |

`parquetDir` resolves to `{simulationRoot}/parquet` (e.g. `output/<studyName>/MC_<timestamp>/parquet`). The `SummaryNew` subdirectory name is subject to change.

---

## Partition Scheme

| Dataset | Partition columns |
|---|---|
| `InstEmissions` | `site` |
| `SiteSummary` | `site` |
| `EventSummary` | `site` |
| `SimSummary` | *(none — single unpartitioned file)* |

---

## Dataset Schemas

### `InstEmissions` — per-event, per-MC-run emission records

One row per emission event per MC iteration. This is the timeseries source; no rollups are performed. The `mcRun` column is present and contains every iteration value.

| Column | Type | Notes |
|---|---|---|
| `mcRun` | int64 | MC iteration number (0-based) |
| `site` | string | Facility/site identifier (also the partition key) |
| `species` | string | Gas species (e.g. `METHANE`, `ETHANE`) |
| `operator` | string | Operator name; empty string if not set |
| `psno` | string | Permit/source number; empty string if not set |
| `emitterID` | string | Individual emitter identifier |
| `timestamp_s` | int64 | Event start time (seconds from simulation start) |
| `duration_s` | float64 | Event duration (seconds) |
| `emission_kgPerS` | float64 | Instantaneous emission rate (kg/s) |
| `totalEmission_kg` | float64 | Total mass emitted for this event (`emission_kgPerS × duration_s`) |
| `METype` | string | Major equipment type |
| `unitID` | string | Equipment unit identifier |
| `modelReadableName` | string | Human-readable equipment name |
| `modelEmissionCategory` | string | Emission category: `VENTED`, `FUGITIVE`, etc. |

---

### `SiteSummary` — annual emission statistics per site

Cross-MC statistics; no per-MC-run rows are stored. Each row represents one grouping level × one unit conversion.

#### `CICategory` values and their grouping levels

| `CICategory` | Groupby columns (beyond `SUMMARY_KEY_COLS`) | Description |
|---|---|---|
| `METype` | `METype` | Per major equipment type |
| `METype` | *(none)* | Site-level rollup for METype hierarchy |
| `unitID` | `unitID` | Per equipment unit |
| `unitID` | *(none)* | Site-level rollup for unitID hierarchy |
| `modelEmissionCategory` | `modelEmissionCategory` | Per emission category (VENTED, FUGITIVE, etc.) |
| `modelEmissionCategory` | *(none)* | Site-level rollup for modelEmissionCategory hierarchy |
| `modelReadableName` | `modelReadableName`, `unitID`, `METype` | Per readable name × unit × equipment type |
| `modelReadableName` | `unitID`, `METype` | Rollup: readable name removed |
| `modelReadableName` | `METype` | Rollup: readable name and unitID removed |
| `modelReadableName` | *(none)* | Site-level rollup for modelReadableName hierarchy |
| `instantEmissionsByModelReadableName` | `METype`, `unitID`, `modelReadableName` | Instantaneous emissions, full detail |
| `instantEmissionsByModelReadableName` | `METype`, `unitID` | Rollup: readable name removed |
| `instantEmissionsByModelReadableName` | `METype` | Rollup: readable name and unitID removed |
| `instantEmissionsByModelReadableName` | *(none)* | Site-level rollup |

`SUMMARY_KEY_COLS` = `['site', 'species', 'operator', 'psno']`. Site-level rollup rows have no value in the extra groupby columns (those columns are absent from the row).

#### Columns

| Column | Type | Notes |
|---|---|---|
| `site` | string | Partition key |
| `species` | string | Gas species |
| `operator` | string | |
| `psno` | string | |
| `CICategory` | string | Grouping level identifier (see table above) |
| `units` | string | Emission units for this row (see Unit Conventions) |
| `includeFugitive` | bool | `True` = all categories included; `False` = FUGITIVE excluded |
| `confidenceLevel` | int64 | CI confidence level (95) |
| `total` | float64 | Sum of per-MC-run totals across all MC runs |
| `count` | int64 | Number of MC iterations contributing (= `monteCarloIterations`) |
| `mean` | float64 | Mean emission corrected for MC iterations (`total / monteCarloIterations`) |
| `min` | float64 | Minimum per-MC-run total |
| `max` | float64 | Maximum per-MC-run total |
| `lowerQuintile` | float64 | 25th percentile of per-MC-run totals |
| `upperQuintile` | float64 | 75th percentile of per-MC-run totals |
| `lowerCI` | float64 | Lower confidence interval bound (2.5th percentile at 95% CI) |
| `upperCI` | float64 | Upper confidence interval bound (97.5th percentile at 95% CI) |
| `readings` | list\<float64\> | Per-MC-run total values (length = `monteCarloIterations`) |
| `rawCount` | float64 | Raw number of emitter-level observations before MC rollup |
| `rawMean` | float64 | Raw mean before MC mean correction |
| `METype` | string | Present when `CICategory` groups by METype; absent at site-level rollup rows |
| `unitID` | string | Present when `CICategory` groups by unitID; absent at site-level rollup rows |
| `modelEmissionCategory` | string | Present when `CICategory` groups by modelEmissionCategory; absent at site-level rollup rows |
| `modelReadableName` | string | Present when `CICategory` groups by modelReadableName; absent at site-level rollup rows |

#### C2/C1 ratio rows in `SiteSummary`

In addition to the per-species rows, `SiteSummary` contains C2/C1 ethane-to-methane ratio rows written by `calculateC2C1Ratios`. These rows share the same schema as the main rows with the following fixed values and differences:

| Column | Value / Notes |
|---|---|
| `species` | `'C2/C1'` |
| `units` | `'unitless'` (no other unit variants are produced) |
| `total` | `total_ETHANE / total_METHANE` |
| `readings` | Per-MC-run `emission_ETHANE / emission_METHANE` ratios; `NaN` where METHANE emission is zero |
| `mean` | `nanmean` of `readings` |
| `min` / `max` | `nanmin` / `nanmax` of `readings` |
| `lowerQuintile` / `upperQuintile` | 25th / 75th nanpercentile of `readings` |
| `lowerCI` / `upperCI` | 2.5th / 97.5th nanpercentile of `readings` |
| `rawCount` | `rawCount` from the METHANE source row |
| `rawMean` | `rawMean_ETHANE / rawMean_METHANE` |
| `count`, `CICategory`, groupby cols | Inherited from the METHANE source row |

Ratio rows are only present when both METHANE and ETHANE rows exist for a given groupby key. They appear for all `CICategory` and `includeFugitive` combinations that have matching METHANE/ETHANE pairs, but only for `units='kg/year'` source rows (since the ratio is dimensionless, no further conversions are applied).

---

### `EventSummary` — event statistics per site

Cross-MC event aggregation; no per-MC-run rows are stored. Each row represents one grouping level × one emission rate unit.

#### Grouping levels

| Level | Groupby columns (beyond `SUMMARY_KEY_COLS`) | Description |
|---|---|---|
| Equipment-level | `unitID`, `modelReadableName` | Per emitter across all MC runs |
| Site-level | *(none)* | All emitters combined across all MC runs |

Each level appears twice: once with `emissionRateUnits='kg/s'` and once with `emissionRateUnits='kg/h'`.

#### Columns

| Column | Type | Notes |
|---|---|---|
| `site` | string | Partition key |
| `species` | string | Gas species |
| `operator` | string | |
| `psno` | string | |
| `unitID` | string | Present at equipment level; absent at site level |
| `modelReadableName` | string | Present at equipment level; absent at site level |
| `CICategory` | string | Always `eventSummary` |
| `mcRuns` | int64 | Number of MC iterations (`monteCarloIterations`) |
| `emissionRateUnits` | string | `kg/s` or `kg/h` |
| `eventCount` | int64 | Total events across all MC runs |
| `totalEmission_kg` | float64 | Total mass emitted across all events and MC runs (kg) |
| `totalEventDuration_s` | float64 | Total event duration across all events and MC runs (seconds) |
| `meanEventDuration_s` | float64 | Mean event duration (seconds) |
| `eventsPerMCRun` | float64 | `eventCount / mcRuns` |
| `meanEmissionRate` | float64 | `totalEmission_kg / totalEventDuration_s`, in `emissionRateUnits` |
| `durationEvents` | list\<float64\> | Per-event durations (seconds) across all MC runs |
| `totalEmissionEvents` | list\<float64\> | Per-event total emissions (kg) across all MC runs |
| `includeFugitive` | bool | `True` = all categories included; `False` = FUGITIVE excluded |

---

### `SimSummary` — simulation-wide summary (no site partition)

Derived from `SiteSummary` by `summarizeSimulation`. One file covering all sites.

#### `CICategory` values

| `CICategory` | Pivot field | Description |
|---|---|---|
| `modelEmissionCategory` | `modelEmissionCategory` | Cross-site totals per emission category |
| `modelReadableName` | `modelReadableName` | Cross-site totals per readable name |
| `unitID` | `unitID` | Cross-site totals per unit |
| `METype` | `METype` | Cross-site totals per equipment type |
| `pneumatic` | `METype` | Pneumatic-filtered cross-site totals per equipment type |
| `simulation` | *(none)* | Single simulation-wide total per species/units/includeFugitive |

#### Columns

| Column | Type | Notes |
|---|---|---|
| `species` | string | Gas species |
| `units` | string | Emission units |
| `includeFugitive` | bool | `True` = all categories; `False` = FUGITIVE excluded |
| `CICategory` | string | Grouping level (see table above) |
| `total` | float64 | Sum of per-site totals |
| `count` | int64 | Number of contributing rows |
| `mean` | float64 | Mean of per-site totals |
| `min` | float64 | Minimum per-site total |
| `max` | float64 | Maximum per-site total |
| `lower` | float64 | 25th percentile of per-site totals |
| `upper` | float64 | 75th percentile of per-site totals |
| `lowerCI` | float64 | 2.5th percentile of per-site totals |
| `upperCI` | float64 | 97.5th percentile of per-site totals |
| `readings` | list\<float64\> | Per-site total values |
| `modelEmissionCategory` | string | Present for relevant `CICategory` values |
| `modelReadableName` | string | Present for relevant `CICategory` values |
| `unitID` | string | Present for relevant `CICategory` values |
| `METype` | string | Present for relevant `CICategory` values |

---

## `includeFugitive` Semantics

Every row in `SiteSummary` and `EventSummary` appears twice:

| `includeFugitive` | Source data |
|---|---|
| `True` | All emission events, including `modelEmissionCategory = FUGITIVE` |
| `False` | Emission events with `modelEmissionCategory != FUGITIVE` only |

---

## Unit Conventions

`SiteSummary` rows appear in multiple unit variants, produced by `applyConversions`:

| `units` | Conversion | Used in |
|---|---|---|
| `kg/year` | Base unit (no conversion) | `calculateAnnualSummaries`, `calculateEmissionSummary` |
| `US tons/year` | `× KG_TO_SHORT_TONS` | `calculateAnnualSummaries` |
| `mt/year` | `× 0.001` | `calculateAnnualSummaries` |
| `kg/hour` | `× (1 / HOURS_PER_YEAR)` | `calculateEmissionSummary` only |

`EventSummary` uses `kg/s` (base) and `kg/h` (`× SECONDS_PER_HOUR`) for `meanEmissionRate`.

---

## MC Iteration Handling

### Mean correction

The raw cross-MC mean (`sum / n_groups`) overstates the per-iteration mean when some MC runs produce zero emissions for a group. `SiteSummary` corrects this:

```
mean = total / monteCarloIterations
count = monteCarloIterations
```

`rawCount` and `rawMean` retain the uncorrected values.

### Per-dataset MC granularity

| Dataset | MC granularity | Rationale |
|---|---|---|
| `InstEmissions` | One row per event per MC run | Timeseries source; full granularity required |
| `SiteSummary` | Cross-MC statistics only | Consumers need aggregate stats, not per-run rows |
| `EventSummary` | Cross-MC statistics only | Same rationale |
| `SimSummary` | Cross-site statistics only | Derived from `SiteSummary`; already cross-MC |

---

## Design Notes

### `_doAggHierarchy` — unified aggregation hierarchy

All cross-MC summary hierarchies in `Summaries2.py` are produced by a single function:

```python
_doAggHierarchy(df, aggColumnList, mcIterations, varCol, detailGroupbyCols, rollupCols)
```

It always executes the same three-phase pattern:

1. **Level 0 (internal)** — group by `[*detailGroupbyCols, 'mcRun']` to produce per-MC-run totals. Never written to output.
2. **Level 1 (cross-MC)** — group Level 0 results by `detailGroupbyCols`. Apply mean correction: `mean = total / mcIterations`, `count = mcIterations`. Preserve raw values in `rawCount` / `rawMean`. Append to output.
3. **Rollup levels** — for each column in `rollupCols`, drop it from the active groupby and re-aggregate from the previous level's `total`. Append each level to output.

Passing `rollupCols=[]` suppresses all rollup levels and returns only the cross-MC Level 1 rows.

### `calculateC2C1Ratios` — ethane/methane ratio rows

After all emission summaries are assembled, `calculateC2C1Ratios` is called in `summarizeSingleSite`. It:

1. Filters `SiteSummary` to `units = 'kg/year'` rows only.
2. Joins METHANE and ETHANE rows on all non-statistical columns (i.e. `site`, `operator`, `psno`, `CICategory`, `includeFugitive`, `confidenceLevel`, and whichever groupby columns are present such as `METype`, `unitID`, `modelEmissionCategory`, `modelReadableName`).
3. Computes per-MC-run ethane/methane ratios from the `readings` lists, then derives the same statistical columns as the main summary.
4. Appends the resulting rows to `SiteSummary` with `species='C2/C1'` and `units='unitless'`.

The ratio rows are only produced when both METHANE and ETHANE data exist for a matched groupby key. They inherit the same `CICategory` and groupby-column values as the underlying species rows, but `units` is always `'unitless'` (no further unit conversions are applied).

See the [C2/C1 Ratio Rows](#c2c1-ratio-rows-in-sitesummary) subsection under Dataset Schemas for the full column list.

### `COMBINED` modelEmissionCategory — the relabeling trick

Adding a synthetic `COMBINED` category (sum of COMBUSTION + FUGITIVE + VENTED) required no changes to `_doAggHierarchy`. The pattern is:

```python
combinedDF = aggregatedEmissionsByEmitterID.assign(modelEmissionCategory='COMBINED')
resultDFList.append(
    _doAggHierarchy(combinedDF, aggColumnList, mcIterations,
                    varCol='modelEmissionCategory',
                    detailGroupbyCols=[*SUMMARY_KEY_COLS, 'modelEmissionCategory'],
                    rollupCols=[])
)
```

Because every row is relabeled `'COMBINED'` before the call, Level 0 collapses all real categories into one total per `[*SUMMARY_KEY_COLS, 'COMBINED', 'mcRun']`. Level 1 then produces the correct cross-MC statistics for the combined total. `rollupCols=[]` keeps the output to just those cross-MC rows — there is no further site-level rollup because the per-category site-level rollup already covers that need.

Unit conversions flow through `applyConversions` automatically — no special handling is required for COMBINED rows. The `includeFugitive` split is also handled transparently: COMBINED over `instEmissionDF` sums all categories; COMBINED over `instEmissionNoFugitiveDF` sums only non-fugitive categories.
