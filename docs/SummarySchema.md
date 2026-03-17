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
| `lowerQuartile` | float64 | 25th percentile of per-MC-run totals |
| `upperQuartile` | float64 | 75th percentile of per-MC-run totals |
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
| `lowerQuartile` / `upperQuartile` | 25th / 75th nanpercentile of `readings` |
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
| `total` | float64 | Sum of cross-site run totals across all MC runs (true simulation grand total) |
| `count` | int64 | Number of MC iterations (`monteCarloIterations`) |
| `mean` | float64 | Mean of the cross-site run-total distribution (`total / mcIterations`) |
| `min` | float64 | Minimum cross-site run total across all MC runs |
| `max` | float64 | Maximum cross-site run total across all MC runs |
| `lowerQuartile` | float64 | 25th percentile of cross-site run totals |
| `upperQuartile` | float64 | 75th percentile of cross-site run totals |
| `lowerCI` | float64 | 2.5th percentile of cross-site run totals (95% CI lower bound) |
| `upperCI` | float64 | 97.5th percentile of cross-site run totals (95% CI upper bound) |
| `readings` | list\<float64\> | Cross-site run totals, one value per MC run |
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

`SimSummary` follows the approach in issue #27: for each MC run, the per-site values are summed across all sites to produce a distribution of cross-site run totals. All statistics are then computed from that distribution:

- `total` = sum of all cross-site run totals = simulation grand total across all MC runs
- `mean` = mean of the cross-site run-total distribution = `total / mcIterations`
- `min`, `max`, `lowerQuartile`, `upperQuartile`, `lowerCI`, `upperCI` = statistics of the cross-site run-total distribution
- `readings` = list of cross-site run totals, one per MC run
- `count` = `monteCarloIterations`

This guarantees `mean <= max` and `lowerCI <= mean <= upperCI` for any multi-site simulation.

The cross-site sums are derived by exploding the `readings` lists from `SiteSummary` and summing by positional index. Because `readings` in `SiteSummary` is not zero-filled (see [CI bounds and readings are not zero-filled](#ci-bounds-and-readings-are-not-zero-filled)), the positional index is an approximation of the MC run number for groups where some MC runs have zero emissions. The resulting cross-site sums are slightly misaligned in those cases; the effect is small for well-sampled groups.

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

`SimSummary` C2/C1 rows omit `min`, `max`, `lowerQuartile`, `upperQuartile`, `lowerCI`, `upperCI`, and `readings` (set to `NaN` / empty list). The ratio is deterministic given the site-level means; per-site distribution information is not propagated.

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
- [Minimum meaningful emission rate in CDF pipeline](#minimum-meaningful-emission-rate-in-cdf-pipeline) — open question: should very small emission rates be filtered before CDF construction?
- [Blowdown Event exclusion from PDF](#blowdown-event-exclusion-from-pdf) — legacy `Summaries.py` excluded Blowdown Events from all PDFs as "maintenance emissions"; `Summaries2.py` includes them; open question for external review
- [Sparse heater PDFs — KS sensitivity](#sparse-heater-pdfs--ks-sensitivity) — heater units with 3–6 PDF bins produce elevated KS statistics due to the discrete, step-function nature of very sparse CDFs; pending external review
- [SummaryTest blind spot](#summarytest-blind-spot--old-vs-new-comparison-cannot-detect-shared-bugs) — old-vs-new comparison cannot catch bugs shared by both implementations; mitigated by self-consistency checks on new output
- [Extreme outlier MC runs in SimSummary](#extreme-outlier-mc-runs-in-simsummary) — 2 MC runs in MPLX-Q4 produce cross-site METHANE totals ~34× the median; causes `mean > upperCI` for non-fugitive simulation rows; pending colleague review

---

### PDF and PDFCache datasets

#### Terminology

The **PDF** (Probability Mass Function) maps emission rate values to their duration-weighted probability mass across all MC runs. The **CDF** (Cumulative Distribution Function) is derived from the PDF by cumulative summation. Both forms are stored in the `PDF` parquet dataset — the PDF is the primary computational object; the CDF is included for convenience so downstream consumers need no additional computation.

The pipeline datasets were historically named `CDFCache`, `CDFPrecomputed`, and `CDF`. They have been renamed `PDFCache` and `PDF` to reflect the primary terminology. `CDFPrecomputed` has been removed; `createPDFCache` now writes directly to `PDF`.

#### Dataset descriptions

| Dataset | Config key | Resolved path | Description |
|---|---|---|---|
| `PDFCache` | `parquetNewPDFCache` | `{parquetDir}/SummaryNew/PDFCache` | Raw RLE interval data (start/end time, emission rate) per emitter per MC run, at all grouping levels. Input to PDF construction. |
| `PDF` | `parquetNewPDF` | `{parquetDir}/SummaryNew/PDF` | Computed probability distributions at all grouping levels. Stores both `probability` (PDF) and `cumulativeProbability` (CDF) columns. |

Both datasets are partitioned by `site`.

#### `PDF` schema

| Column | Type | Notes |
|---|---|---|
| `site` | string | Partition key |
| `species` | string | Gas species (METHANE, ETHANE) |
| `operator` | string | |
| `psno` | string | |
| `CICategory` | string | Grouping level: `site`, `METype`, `unitID`, or `modelReadableName` |
| `METype` | string | Present when `CICategory` is `METype` or `modelReadableName` |
| `unitID` | string | Present when `CICategory` is `unitID` or `modelReadableName` |
| `modelReadableName` | string | Present when `CICategory` is `modelReadableName` |
| `includeFugitive` | bool | `True` = all emission categories; `False` = FUGITIVE excluded |
| `emissionRate_kgPerH` | float64 | Emission rate bin value (kg/h), rounded to 6 decimal places |
| `probability` | float64 | Fraction of total duration spent at this emission rate (PDF value) |
| `cumulativeProbability` | float64 | Cumulative probability up to and including this bin (CDF value) |

#### Emission rate rounding convention (`_roundForPDF`)

All `emission_kgPerH` values stored in `PDFCache` and `PDF` are rounded to **6 decimal places** by `_roundForPDF` before storage. Rounding is applied after every `TimeseriesSet.sum()` call, at the point values are written into cache DataFrames.

**Why rounding is necessary:** `TimeseriesSet.sum()` performs floating-point arithmetic that introduces ULP-level noise (~1e-16 for rates in the 0.1–10 kg/h range). Without rounding, what is physically one emission rate may appear as 2–3 distinct float64 values differing only in the 15th–16th decimal place. `TimeseriesPDF.fromDataFrame` groups intervals by exact float64 value, so these ULP variants become separate PDF bins — creating multiple near-identical steps in the CDF.

**Why 6 decimal places:** This matches the precision of the legacy `Summaries.py` CSV output (`PDF_for_*` files), which rounded emission rates to 6 decimal places when writing. Using the same resolution ensures old and new CDFs share the same x-axis binning, keeping the KS validation statistic physically meaningful. Differences below 1e-6 kg/h are not physically significant for emissions reporting.

**Why rounding must happen after `sum()`:** Rounding inputs before `sum()` is not sufficient — `sum()` may reintroduce ULP noise when combining COMBUSTION and FUGITIVE timeseries intervals. Rounding must be applied at every cache write point.

### Minimum meaningful emission rate in CDF pipeline

`TimeseriesSet.sum()` filters floating-point residuals using an absolute threshold of `1e-10` (see comment in `Timeseries.py`). This is a mathematical noise floor, not a physical one — its sole purpose is to discard sub-picogram/hour artifacts from floating-point cancellation. Legitimate emission values in the simulation are many orders of magnitude larger.

**Open question:** Should a separate, physically motivated minimum emission rate threshold be applied earlier in the CDF pipeline — for example, in `_removeZeroEmissionEvents` in `Summaries2.py` — to prevent very small but non-zero emission rates from creating low-density PDF bins near zero that may not be physically meaningful?

Considerations:
- If such a threshold exists, what is the appropriate value and in what units (kg/s, kg/h)?
- Should it be a fixed constant, or derived from instrument detection limits / regulatory reporting thresholds?
- Applying a threshold here would affect the `CDFCache` and all downstream CDFs; it should not be set in `TimeseriesSet.sum()`, which is a general-purpose operation used outside the CDF pipeline.

### Blowdown Event exclusion from PDF

**Legacy behaviour (`Summaries.py`):** `generatePDFs` (line 1825) contains the explicit filter:

```python
df = df[df['modelReadableName'] != 'Blowdown Event']    # exclude maintenance emissions
```

This removes all Blowdown Event records before constructing any PDF, for both `abnormal=on` and `abnormal=off`. The comment labels blowdown events as "maintenance emissions."

**New behaviour (`Summaries2.py`):** No such filter exists. All VENTED events — including Blowdown Events — are included when `includeFugitive=False`. All VENTED + FUGITIVE events are included when `includeFugitive=True`.

**Observed impact on validation (2026-03-16):** For compressor units that have Blowdown Events, the legacy and new PDFs differ substantially:

- **Max emission rate:** Old PDF reaches ~10 kg/h (Compressor Rod Packing Vent only); new PDF reaches ~88 kg/h (Blowdown Events at 88.43 kg/h).
- **Distribution shape:** Blowdown Events co-occur with active packing-vent intervals. The combined timeseries rate during a blowdown is 88.43 + 0.006786 ≈ 88.43 kg/h, absorbing time that would otherwise be at 0.006786 kg/h. This shifts the CDF rightward and reduces the fraction of duration at low rates (e.g., CDF at 0.006786 kg/h drops from 13.1% to 0.8% for `comp_63723.0` at Timberlake).
- **KS statistic:** Typically 0.85–0.94 for affected compressor units in `off` mode. This is the primary source of the 210 remaining CDF validation failures after the ULP rounding fix.

**Open question for external review:** Should Blowdown Events be included in the emission rate PDF? The legacy exclusion reflects a policy decision that blowdowns are episodic maintenance activities and should not shape the "normal operation" emission rate distribution. The new code treats them as standard non-fugitive VENTED events. The correct treatment depends on how the PDF is intended to be used (e.g., compliance reporting, probabilistic risk assessment, equipment characterisation).

### Sparse heater PDFs — KS sensitivity

Three heater units produce very sparse PDFs (3–6 bins each) and exhibit elevated KS statistics in the old-vs-new comparison:

| Unit | Site | KS (on) | KS (off) | Old bins | New bins |
|------|------|---------|---------|---------|---------|
| `HTR_BLU` | Bluestone Gas Processing Plant | 0.465 | 0.465 | 4 | 4 |
| `HTR_SAR` | Sarsen Gas Processing Plant | 0.177 | 0.177 | 6 | 4 |
| `HTR_HUM` | Humphreys Compressor Station | 0.709 | 0.709 | 3 | 3 |

With so few bins, the CDF is a step function. A single bin boundary shifting by even one bin position produces a KS jump proportional to the probability mass of that bin. These are not ULP-noise failures — old and new row counts match (or differ by one), and no ULP variants are present in the PDF parquet. The elevated KS reflects a genuine shape difference in how the old and new code partition heater emission intervals into bins.

Root cause has not been fully diagnosed. No code change is proposed at this time. **Pending external review.**

---

## SimSummary Statistics — Implementation Note

**Status: Fixed (2026-03-17) per [issue #27](https://github.com/CSU-METEC/MAES/issues/27).**

**Previous bug:** `mean` was set to `total` (sum of per-site means), while `min`/`max`/CI were computed from the per-site mean distribution. This caused `mean > max` by construction for any multi-site simulation. Confirmed on a 26-site CNX run: 378 of 2035 rows affected.

**Fix:** `_filterAndPivot` now reconstructs per-run cross-site totals by exploding the `readings` lists from `SiteSummary` and summing by positional MC-run index. All statistics (`mean`, `min`, `max`, CI, `readings`) are computed from the resulting distribution of cross-site run totals, consistent with the approach described in issue #27. See [SimSummary columns](#columns-2) for the full updated column semantics.

---

## Extreme Outlier MC Runs in SimSummary

**Status: Pending colleague review.**

In the MPLX-Q4 simulation (50 sites, 100 MC runs), the `CICategory='simulation'`, `includeFugitive=False` rows for METHANE and ETHANE show `mean > upperCI`. Inspection of the `readings` distribution reveals 2 extreme outlier MC runs:

| Species | Typical run (mt/year) | Outlier run 1 | Outlier run 2 |
|---|---|---|---|
| METHANE | ~23,600 | ~426,419 | ~800,188 |
| ETHANE | proportional | proportional | proportional |

The 98 non-outlier runs are tightly clustered; the 2 outliers are 17–34× the median. This pulls `mean` (35,392) above `upperCI` (24,580 — the 97.5th percentile of the bulk distribution). `mean > upperCI` is **statistically valid** for this distribution shape — it is not a bug in the summarization code.

**Open question:** What simulation events in those 2 MC runs produce totals 17–34× higher than a typical run? Candidates include runaway tank flash events, flare malfunction sequences, or compressor blowdown timing coincidences across multiple sites. Flagged for colleague review before investigating further.

`SummaryTest` reports these as `ciWarningCount` (not `emissionRateOutOfRangeCount`) in the `SimSummaryConsistency` row.

---

## SummaryTest Blind Spot — Old-vs-New Comparison Cannot Detect Shared Bugs

`SummaryTest` validates correctness by comparing new `Summaries2.py` output against legacy `Summaries.py` output. This approach has a fundamental limitation: **if both implementations share the same structural mistake, SummaryTest gives a false clean signal.**

### Example: SimSummary `mean > max` bug

The SimSummary `mean > max` bug (fixed 2026-03-17, see [SimSummary Statistics](#simsummary-statistics--implementation-note)) was not caught by SummaryTest because the legacy `Summaries.py` set `mean = sum(site_means)` and computed `min`/`max`/CI from the per-site distribution — the same structural error as the original `_filterAndPivot`. Both implementations agreed with each other, so all SimSummary comparison rows passed. The bug was only discovered when a colleague directly inspected the `SimSummary-0.parquet` and noticed `mean > max` visually.

### Mitigation: self-consistency checks

`SummaryTest.py` now includes `checkSimSummaryConsistency`, which validates statistical invariants on the **new output alone**, without reference to the legacy values:

- `mean ≤ max` for all non-C2/C1 rows
- `lowerCI ≤ mean` for all non-C2/C1 rows
- `mean ≤ upperCI` for all non-C2/C1 rows

Violations appear in the results CSV as `summaryType=SimSummaryConsistency`, `by=self`, `abnormal=check`, with the violation count in `emissionRateOutOfRangeCount`.

This pattern — checking internal invariants independently of old-vs-new comparison — should be applied to other datasets (SiteSummary, EventSummary) as future validation coverage is extended.

---

## SummaryTest Validation Run Log

### Site 1 (2026-03-17, `SummaryTest_results_20260317_063000.csv`)

50 sites, 100 MC iterations.

**Non-CDF (508 rows):** 507 clean. 1 expected divergence: `AggregatedSimulationEmissions / category / off / simulation`, `roNonZeroCount=3` (COMBINED rows absent from legacy code in off mode — documented, not a defect).

**CDF (1,872 rows):** 1,666 clean (ksD ≤ 0.05), 206 failing. KS distribution: 119 in 0.05–0.10, 49 in 0.10–0.20, 11 in 0.20–0.30, 27 in 0.70–1.00. All failures attributable to the two open items above (Blowdown Event exclusion policy; sparse heater PDFs).

### Site 2 (2026-03-17, `SummaryTest_results_20260317_072906.csv`)

~19 sites compared.

**Non-CDF (263 rows):** 255 clean. 8 flagged:

- **Site2.Station1 / AvgEmissionRatesAndDurations / off:** `eventOutOfRangeCount=2` for `Blowdown Event` (old: 1.44 events/run, new: 1.30). Same root cause as CDF Blowdown Event open item — old code excludes blowdown events from event counts.
- **AggregatedSimulationEmissions:** Large `roNonZeroCount` across all groupings (`unitID/on=378`, `unitID/off=300`, `modelReadableName/on=45`, `METype/on=9`, etc.). Detail parquet shows zero `right_only` merge rows — these are `both` rows where `new_mean > 0` but `old_mean = 0`, i.e., emission categories present in new code that were zero in the legacy path. `maxRelativeDelta` values 70–404% reflect this structural difference. Not a defect.

**CDF (513 rows):** 449 clean, 64 failing. KS distribution: 39 in 0.05–0.10, 17 in 0.10–0.20, 4 in 0.20–0.30, 3 in 0.70–1.00. High-KS failures (0.70–0.92) are compressor `off` mode Blowdown Event cases (`comp_Marshall 3` KS=0.92, `comp_G90` KS=0.88, `comp_IAMS ENG2` KS=0.87). Remaining 61 failures driven by sparse heater PDFs (`WDT_HTR`, `BIG_HTR`, `BP__HTR`) and `Tank_battery_OIL` units (all ksD ≤ 0.21). No regressions.
