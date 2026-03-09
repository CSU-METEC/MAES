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
| `eventCount` | int64 | Total events across all MC runs, including zero-emission events |
| `nonZeroEventCount` | int64 | Total events with `emission_kgPerS > 0` across all MC runs |
| `totalEmission_kg` | float64 | Total mass emitted across all events and MC runs (kg) |
| `totalEventDuration_s` | float64 | Total event duration across all events and MC runs (seconds) |
| `meanEventDuration_s` | float64 | Mean event duration (seconds) |
| `eventsPerMCRun` | float64 | `eventCount / mcRuns` — includes zero-emission events |
| `nonZeroEventsPerMCRun` | float64 | `nonZeroEventCount / mcRuns` — matches legacy `Summaries.py` `avg_event_count` (see note below) |
| `meanEmissionRate` | float64 | Duration-weighted mean rate: `totalEmission_kg / totalEventDuration_s`, in `emissionRateUnits` |
| `simpleMean` | float64 | Simple arithmetic mean of per-event emission rates, in `emissionRateUnits` (see note below) |
| `durationEvents` | list\<float64\> | Per-event durations (seconds) across all MC runs |
| `totalEmissionEvents` | list\<float64\> | Per-event total emissions (kg) across all MC runs |
| `includeFugitive` | bool | `True` = all categories included; `False` = FUGITIVE excluded |

#### `meanEmissionRate` vs. `simpleMean`

`meanEmissionRate` is the physically correct duration-weighted average rate: total mass emitted divided by total event duration. This weights each event by how long it lasted.

`simpleMean` is the simple arithmetic mean of per-event emission rates, matching the calculation used in the legacy `Summaries.py` (`AvgEmissionRatesAndDurations` output). It is retained for comparison purposes only. It overweights short, high-rate events and underweights long, low-rate events, producing results that can differ substantially from `meanEmissionRate` when event durations vary widely.

**Example:** two events — 10 kg/h for 1 hour and 1 kg/h for 100 hours:
- `simpleMean` = (10 + 1) / 2 = **5.5 kg/h**
- `meanEmissionRate` = (10 + 100) / 101 ≈ **1.09 kg/h**

This discrepancy is the primary known cause of differences between `Summaries2.py` and `Summaries.py` results for `AvgEmissionRatesAndDurations`.

#### `eventsPerMCRun` vs. `nonZeroEventsPerMCRun`

`eventsPerMCRun` counts **all** emission events regardless of rate (`eventCount / mcRuns`). It includes MC runs where the gas composition sampled a zero fraction for a species, producing emission events with `emission_kgPerS = 0`.

`nonZeroEventsPerMCRun` counts only events with `emission_kgPerS > 0` (`nonZeroEventCount / mcRuns`). The denominator is always `mcRuns` (total MC iterations), so MC runs with all-zero emissions for a given group contribute 0 to the numerator — exactly matching the legacy `Summaries.py` behavior (`processInstantEquipEmissions` filters `emission > 0` before counting).

##### Root cause of discrepancy in `Summaries.py` validation

For equipment like `Compressor Dry Seal Vent (OP)`, some MC runs sample a zero gas-composition fraction for a species (e.g., METHANE). Those runs still produce `EMISSION` command events but with `emission_kgPerS = 0`. `Summaries.py` silently drops these via its `emission > 0` filter, then averages over all MC iterations, depressing the mean. `Summaries2.py` `eventsPerMCRun` includes them; `nonZeroEventsPerMCRun` reproduces the legacy result.

**Example (Bluestone_Gas_Processing_Plant, `comp_15939.0`, METHANE):**
- 17 of 100 MC runs sampled zero METHANE fraction → all-zero emission events in those runs
- `eventsPerMCRun` = 12.64 (all events counted)
- `nonZeroEventsPerMCRun` = 10.49 (matches `Summaries.py` `avg_event_count` exactly)

**`SummaryTest.py` uses `nonZeroEventsPerMCRun`** when comparing against the legacy `avg_event_count` column, so that zero-emission events do not produce spurious `eventOutOfRangeCount` failures. `eventsPerMCRun` is retained in the dataset for reference but is not used in validation.

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
| `total` | float64 | Sum of per-site corrected means (see [Mean correction](#mean-correction)) |
| `count` | int64 | Number of unique sites in the simulation (`len(fullSummaryDF['site'].unique())`) |
| `mean` | float64 | `total / count` — mean of per-site corrected means (mirrors `SiteSummary` mean correction: `mean = total / count`) |
| `min` | float64 | Minimum per-site corrected mean |
| `max` | float64 | Maximum per-site corrected mean |
| `lower` | float64 | 25th percentile of per-site corrected means |
| `upper` | float64 | 75th percentile of per-site corrected means |
| `lowerCI` | float64 | 2.5th percentile of per-site corrected means (at 95% CI) |
| `upperCI` | float64 | 97.5th percentile of per-site corrected means (at 95% CI) |
| `readings` | list\<float64\> | Per-site corrected mean values |
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

### Flare equipment and the `includeFugitive=False` filter

> **Design note — legacy bug, not present in `Summaries2.py`**
>
> Flare equipment (`tnk_flare`, `LinkedProductionTankFlare`) has emitters in multiple emission categories:
>
> | Readable name | Emission category |
> |---|---|
> | Flared Gas Operating | `COMBUSTION` |
> | Flared Gas Malfunction | `FUGITIVE` |
> | Unflared Gas from Flare | `FUGITIVE` |
> | Flare Component Leak | `FUGITIVE` |
>
> The `includeFugitive=False` split is intended to exclude only true fugitive leaks from the totals, but "Flared Gas Malfunction" and "Unflared Gas from Flare" are assigned `FUGITIVE` because the gas escapes without combustion — they are legitimately excluded from the non-fugitive totals.
>
> **Legacy `Summaries.py` bug:** the `abnormal=off` filter in the old code operated on emitter ID membership rather than `modelEmissionCategory`. This caused FUGITIVE-category flare emitters ("Flared Gas Malfunction", "Unflared Gas from Flare") to be retained in the `includeFugitive=False` totals, inflating the `Flare` METype by approximately 38% relative to the correct value.
>
> **`Summaries2.py`** filters cleanly as `instEmissionDF['modelEmissionCategory'] != 'FUGITIVE'` (`Summaries2.py:354`), so all FUGITIVE-category events — including flare malfunction and unflared gas — are correctly excluded when `includeFugitive=False`. No code change was required; the correct behaviour was present from the start.

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

##### CI bounds and `readings` are not zero-filled

The mean correction accounts for absent MC runs, but `readings` only contains values from MC runs where the group had non-zero emissions. `lowerCI`, `upperCI`, and the quintile columns are therefore computed from a list that may be shorter than `monteCarloIterations` when any MC run produces zero emissions for a given group. CI bounds will be optimistically narrow for low-prevalence groups. A future fix would pad `readings` with zeros to length `monteCarloIterations` before computing percentiles.

`SimSummary` applies the same `mean = total / count` pattern at the cross-site level. `count` is the number of unique sites in the simulation, computed explicitly as `len(fullSummaryDF['site'].unique())` before any aggregation and passed into `_filterAndPivot` — it is not derived from the groupby. `total` is the sum of per-site corrected means, and `mean = total / count`. Statistics (`min`, `max`, percentiles, `readings`) are derived from the per-site corrected mean values.

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

### C2/C1 ratio rows in `SimSummary`

`SimSummary` contains C2/C1 rows computed by `_computeSimC2C1` in `summarizeSimulation`. These are **not** derived by aggregating the per-site C2/C1 rows from `SiteSummary`; instead they are recomputed from scratch from the aggregated METHANE and ETHANE totals:

```
total_CH4  = Σ site_mean_METHANE  (for sites where CICategory and groupby key match)
total_C2H6 = Σ site_mean_ETHANE   (same filter)
C2/C1      = total_C2H6 / total_CH4
```

This produces an **emission-weighted** simulation-wide ratio: sites that emit more METHANE contribute proportionally more to the denominator and therefore have proportionally more influence on the result. The legacy `Summaries.py` instead averaged the per-site C2/C1 ratios without weighting, treating each site equally regardless of its emission volume.

This is structurally the same discrepancy as `simpleMean` vs `meanEmissionRate` in `EventSummary`:

| | `EventSummary` emission rate | `SimSummary` C2/C1 |
|---|---|---|
| **Simple / legacy** | arithmetic mean of per-event rates (each event equally weighted) | arithmetic mean of per-site C2/C1 ratios (each site equally weighted) |
| **Weighted / new** | `total_mass / total_duration` (weighted by event duration) | `total_C2H6 / total_CH4` (weighted by methane emission volume) |
| **Bias** | overweights short, high-rate events | overweights high-C2/C1 sites that emit little methane |
| **Correct approach** | duration-weighted | emission-weighted |

`SimSummary` C2/C1 rows omit `min`, `max`, `lower`, `upper`, `lowerCI`, `upperCI`, and `readings` (set to `NaN` / empty list). The ratio is deterministic given the site-level means; per-site distribution information is not propagated.

**SummaryTest validation:** The `emissionRateOutOfRangeCount=1` flag for `AggregatedSimulationEmissions / unitID / on / simulation` (species `C2/C1`, `unitID=tnk_flare`, ~10.6% relative delta) is an **expected divergence** caused by this methodological difference. The new emission-weighted value is physically correct; the legacy unweighted value is not a defect target.

### `COMBINED` vs `TOTAL` — legacy naming difference

The legacy `Summaries.py` pipeline emits a `TOTAL` category row in `aggregated_sim_emissions_by_category_*.csv` for the `abnormal=ON` case. `Summaries2.py` uses `COMBINED` for the same concept. `SummaryTest.py` normalises this before comparison by replacing `TOTAL` with `COMBINED` in the old summary's `modelEmissionCategory` column.

The legacy pipeline does **not** emit a `TOTAL` row for `abnormal=OFF` (it only writes COMBUSTION and VENTED rows in that case), so no corresponding `COMBINED` match exists on the old side for `off` comparisons. `SummaryTest.py` reports these new-only `COMBINED` rows as `right_only_non_zero` warnings; this is expected and not a defect in `Summaries2.py`.

---

## Topics For Discussion

Sections of this document that capture known methodological differences, design trade-offs, or deferred work items:

- [`meanEmissionRate` vs. `simpleMean`](#meanemissionrate-vs-simplemean) — emission-rate averaging: duration-weighted (correct) vs. arithmetic mean (legacy)
- [`eventsPerMCRun` vs. `nonZeroEventsPerMCRun`](#eventsperMCRun-vs-nonzeroeventspermcrun) — event counting: all events vs. non-zero-emission events only
- [Root cause of discrepancy in `Summaries.py` validation](#root-cause-of-discrepancy-in-summariespy-validation) — zero-emission events from zero gas-composition MC runs silently filtered by legacy code
- [Mean correction](#mean-correction) — `mean = total / monteCarloIterations` corrects for MC runs with no emissions; `rawMean` retains the uncorrected value
- [CI bounds and `readings` are not zero-filled](#ci-bounds-and-readings-are-not-zero-filled) — percentile/CI columns computed from a shortened `readings` list when some MC runs produce zero emissions; bounds are optimistically narrow for low-prevalence groups
- [C2/C1 ratio rows in `SimSummary`](#c2c1-ratio-rows-in-simsummary) — simulation-wide C2/C1 recomputed from aggregated totals (emission-weighted) rather than averaged from per-site ratios (unweighted)
- [Flare equipment and the `includeFugitive=False` filter](#flare-equipment-and-the-includefugitivefalse-filter) — legacy `Summaries.py` retained FUGITIVE-category flare emitters in `abnormal=off` totals due to emitter-ID-based filtering; `Summaries2.py` is correct by construction
