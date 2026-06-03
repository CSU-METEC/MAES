# CHANGELOG


## v0.4.0 (unreleased)

### Bug Fixes (2026-06-03)

- **SimRNG.py** / **SiteMain2.py** — site-aware MC seeding (issue #106). `runSim` seeded
  every site's simulation with the MC run number alone, so two sites with identical study
  definitions replayed bit-for-bit identical random streams and produced numerically
  identical per-site `SiteSummary` output. The seed is now composed by
  `SimRNG.composeSeed` as `[baseSeed?, crc32(siteName), mcRunNum]`: site identity
  separates sites within a run, the MC run number keeps iterations distinct (#69), and
  the optional `--randomSeed` base keeps whole-simulation reproducibility (#96).
  **Default-mode results change for every site** (the seed composition changed) — this is
  inherent to the fix, and re-runs remain deterministic.
- **AppUtils.py** — ported the `-rs/--randomSeed` CLI flag from Curated_Root
  (`ab45594`, issue #96): seed each MC run with `[randomSeed, …, mcRunNum]` when given;
  unset → default seeding. Applied explicitly so a seed of `0` is not dropped by the
  truthy CLI filter. The original commit's contract tests are not ported (they need
  `Testing/OutputEquivalence`, which is not on this branch).

### Tests (2026-06-03)

- **test/test_issue106_site_seeding.py**: red-green coverage for #106. Fast tier — the
  `composeSeed` contract (distinct per site, distinct per MC run, reproducible,
  `--randomSeed` prepended, crc32 site key, legacy fallback). Slow tier (`-m slow`,
  MAES conda env) — the canonical repro: two byte-identical study sheets run via
  `--directory` must produce different `SiteSummary` numbers; a default rerun stays
  bit-identical; `--randomSeed 42` shifts both sites without collapsing them. The `slow`
  marker is now registered in `pyproject.toml`.

### Schema Changes (2026-06-02)

- **defaultConfig.json**: `-dr` (directory) runs now consolidate their `Summary/` parquet
  tree into a single **job-level** location, matching bundle output. `summaryParquetDir`'s
  default changed from the per-study `{parquetDir}` to `{outputRoot}/MC_{scenarioTimestamp}/parquet`.
  Because every `parquetNew*` summary key already resolves through `summaryParquetDir`, this
  moves **both** the site-level datasets (`SiteSummary`, `InstEmissions`, `EventSummary`,
  `PDF`, `PDFCache`, hive-partitioned by `site`) and the sim-level datasets (`SimSummary`,
  `SimPDF`) — plus `SummaryLegacy` — from `<outputRoot>/<site>/MC_<ts>/parquet/Summary/` to
  the consolidated `<outputRoot>/MC_<ts>/parquet/Summary/`. Bundle mode is unaffected: it
  sets `summaryParquetDir` explicitly via `bundleSummaryParquetDir` and overrides the default.
  Non-summary parquet (`events`, `timeseries`, `gascomposition`, `metadata`, `eventList`)
  stays per-study under `{parquetDir}`. Consumers that located `-dr` Summary output under a
  per-site directory must read the job-level path instead.

### Tests (2026-06-02)

- **test/test_dr_consolidated_summary_layout.py**: asserts that a `-dr` run resolves every
  Summary dataset to a study-independent (job-level) path while per-site raw parquet stays
  study-specific, and that `summaryParquetDir` resolves to `{outputRoot}/MC_{scenarioTimestamp}/parquet`.

### Documentation (2026-06-02)

- **README.md**: removed stale, accidentally-committed merge-conflict markers (`<<<<<<<` /
  `=======` / `>>>>>>>`) around the `### Usage` section, present since the `eb2631f2` main
  merge (2025-01-24). Kept the Usage section.
### Validation (2026-05-27)

- **Testing/SummaryConsistency.py**: new directory-level consistency checker for a MAES
  `Summary/` parquet output. Unlike `SummaryTest.py` (new-vs-legacy comparison driven by a
  scenario config), it operates on a directory alone and is unit-testable on synthetic
  DataFrames. Two independently switchable tiers (`--structural` / `--cross-level` CLI
  flags; `--rtol`, `--warn-rtol` tolerances; non-zero exit only on violations):
  - **structural** — `InstEmissions`: timestamps/durations non-negative, events do not
    overrun the simulation window (issue #87), emissions non-negative, `totalEmission_kg`
    consistent with rate × duration; `SiteSummary`/`SimSummary`: `mean ≤ max` (violation),
    CI-bound ordering (warning); `PDF`/`SimPDF`: probability non-negative, CDF monotone
    and bounded by 1.
  - **cross-level** — `SimSummary` rollup: `simulation` total equals the sum over each of
    the modelReadableName / METype / unitID levels and the modelEmissionCategory COMBINED
    row (issue #77, a violation); per-site `siteTotals` PDF mean vs `SiteSummary`
    site-total mean (issue #74, reported as a **warning** because #74 is an open,
    unresolved discrepancy and the identity is not yet guaranteed). `simDurationSecs` is
    read from the `simDurationDays` column; absent datasets are skipped. The `mean ≤ max`
    / CI-bound comparisons use a small relative tolerance so a constant-rate emitter
    (whose `mean = sum/N` can land ~1 ULP above the exact `max` from float64 rounding) is
    not flagged as a spurious violation.

### Tests (2026-05-27)

- **test/test_summary_consistency.py**: new test file covering `SummaryConsistency.py`.
  Each check is exercised on a clean fixture (no violations) and an injected fault flagged
  at the correct severity — including a fixture mirroring the issue #77 triple-count
  (`simulation` = 3 × level totals) and confirmation that the issue #74 PDF-vs-summary
  mismatch is a warning, never a hard violation.

### Bug Fixes (2026-06-01)

- **SiteMain2.py** / **Summaries2.py** — the simulation-level summary now aggregates **all**
  sites (issue #101). The single `simSummary` workitem is generated once after the study
  loop, so it inherited whichever site's config the config manager was left on (the last
  study). As a result `summarizeSimulation` / `createSimPDF` read only that one site's
  `SiteSummary` / `PDF` (`SimPDF mixture: 1 sites`) and the `simulation`-level total
  reflected a single site rather than the whole simulation. `generateWorkitems` now
  accumulates every site's `SiteSummary` and `PDF` output directory and threads them onto
  the workitem (`allSiteSummaryDirs` / `allSitePDFDirs`); a new `_readSummaryAcrossSites`
  helper reads and concatenates across them (each per-site dataset is hive-partitioned by
  `site`, so the column is reconstructed and preserved). Falls back to the single
  configured path for single-study runs.

### Schema Changes (2026-06-01)

- **defaultConfig.json**: `SimSummary` and `SimPDF` are now written to a deterministic,
  job-level location (issue #101). Added `simulationParquetDir` =
  `{outputRoot}/MC_{scenarioTimestamp}/parquet`; `parquetNewSimSummary` and
  `parquetNewSimPDF` resolve under it (`{simulationParquetDir}/Summary/SimSummary` and
  `.../SimPDF`) instead of the per-study `{parquetDir}/Summary/...`. Previously these
  simulation-wide datasets landed under whichever site was processed last; they now live
  at the job root, independent of study order. Per-site datasets (`SiteSummary`, `PDF`,
  `InstEmissions`, `EventSummary`, `PDFCache`) are unchanged.

### Tests (2026-06-01)

- **test/test_issue101_simsummary_aggregation.py**: new test file (issue #101). Covers the
  `_readSummaryAcrossSites` helper aggregating across sites and its single-path fallback;
  `summarizeSimulation` producing a `simulation` total that sums all sites (not just the
  last); `createSimPDF` mixing every site's PDF (vs. the old single-site mixture); and the
  job-level, site-independent `SimSummary` / `SimPDF` write paths.

### Bug Fixes (2026-05-26)

- **Summaries2.py** — `summarizeSimulation`: fixed simulation-level triple-count (issue #77).
  The `'simulation'` SimSummary rows were built by feeding all `CICategory ==
  'modelEmissionCategory'` rows through `_filterAndPivot(..., pivotField='simulation')`.
  That tag is shared by three full-total aggregation levels from `calculateAnnualSummaries`
  — the per-category detail, the category-dropped rollup (NaN category), and the COMBINED
  total row — each of which already sums to the full per-site total. Because the
  `pivotField='simulation'` group key omits the category column, none were filtered out and
  all three were summed, inflating every simulation-level `mean`/`total` by exactly 3x.
  Symptom (issue #77): `sum(mean)` for `CICategory=='simulation'` was 3x the sum for
  `CICategory=='modelReadableName'`. Fixed by restricting to the COMBINED per-site totals
  before the cross-site rollup. Verified the simulation total now equals the
  modelReadableName total on both `includeFugitive` paths.

- **Summaries2.py** — `_createEmissionDF`: clip events at the simulation-window boundary
  (issue #87). The engine logs the full sampled `duration` of whatever state is in
  progress when simpy stops at `simDurationSecs`, so `timestamp + duration` can exceed the
  window. Left unclipped, the overrun added spurious emission credit past the window,
  biasing every rate-integrated quantity (annual summaries, PDFs) upward — verified at
  +67.8% on a real 41.5-day `Compressor Rod Packing Vent` overrun in `JennaBug2`.
  `_createEmissionDF` now takes `simDurationSecs` and clips each event's `duration_s` to
  `min(duration, simDurationSecs − timestamp)` (floored at 0), recomputing
  `totalEmission_kg` from the clipped duration; `emission_kgPerS` (the rate) is unchanged.
  This is the single point where the emission DataFrame is built and saved as
  `InstEmissions` (the dataset the PDF cascade reads back), so the rate summaries, event
  summaries, and PDFs are all consistent over `[0, simDurationSecs]`, restoring the
  identity `mean_pdf = mean_events`. Note: `calculateEventSummary`'s `meanEventDuration_s`
  / `totalEventDuration_s` now reflect in-window durations for overrunning events; the
  full sampled durations remain available in the raw events parquet. Independent of the
  additive `overrunSecs` / `underrunSecs` column proposal (#89).

### Tests (2026-05-26)

- **test/test_issue77_simulation_rollup.py**: new test file covering the simulation-level
  triple-count fix (issue #77). Builds a multi-site, multi-category `calculateAnnualSummaries`
  output and asserts the COMBINED-only simulation rollup equals both the true total and the
  `modelReadableName` total on both `includeFugitive` paths, plus a test that pins the
  pre-fix 3x behavior to guard against regression.

- **test/test_issue87_event_clipping.py**: new test file covering simulation-window event
  clipping (issue #87). Asserts that overrunning events are clipped to the in-window
  remainder with `totalEmission_kg` recomputed and the rate preserved; interior and
  exactly-at-boundary events are untouched; an event starting past the window yields zero
  (not negative) duration; `calculateAnnualSummaries` totals reflect only in-window
  emission; and `_buildMCRunTimeseries` → PDF mean equals the rate-integrated mean over
  `[0, T]` after clipping.

### Schema Changes (2026-03-31)

- **defaultConfig.json** / **ParquetLib.py**: renamed the new Parquet summary directory
  from `SummaryNew` to `Summary`. All `parquetNew*` config keys now resolve to
  `{parquetDir}/Summary/<dataset>` (e.g. `{parquetDir}/Summary/SiteSummary`).
  Existing `SummaryNew` output directories must be renamed or regenerated.

- **defaultConfig.json** / **ParquetLib.py**: renamed the legacy per-MC-run parquet
  summary directory from `simsummary` to `SummaryLegacy`. The `parquetSummaryDS` config
  key now resolves to `{parquetDir}/SummaryLegacy`. Existing `simsummary` output
  directories must be renamed or left in place (reads will fail until renamed or
  regenerated).

### Documentation (2026-03-31)

- **docs/SummarySchema.md**: added "Examples" section before Dataset Schemas, describing
  the five example programs in `src/Examples/` with a table mapping each file to its
  target dataset and the question it answers.

- **docs/SummarySchema.md**: added summarization hierarchy tables for `SiteSummary` and
  `SimSummary`. The `SiteSummary` table shows the level-by-level groupby columns for
  each `CICategory` value (from site rollup to most detailed), and documents the
  COMBINED-only Level 1 exception for the `modelEmissionCategory` hierarchy. The
  `SimSummary` table lists the populated column and description for each `CICategory`
  and explains that there is no within-category rollup.

- **docs/SummarySchema.md**: added a directory-tree example under Partition Scheme
  showing the full `Summary/` layout for a hypothetical site `MySite`, including correct
  dataset-named filenames (e.g. `SiteSummary-0.parquet`).

- **docs/SummarySchema.md**: moved "Config Keys and Resolved Paths" section to after
  Dataset Schemas; expanded it to include `PDF`, `PDFCache`, and `SimPDF` rows and a
  note about the `SummaryLegacy` path for the legacy dataset. Updated all resolved paths
  to reflect the `Summary` rename.

### Bug Fixes (2026-03-25)

- **Summaries2.py** — `_filterAndPivot`: fixed mean denominator bug (issue #33, Bug 2).
  `mean` was computed as `sum(readings) / len(readings)`, but `readings` only contains
  values from MC runs that produced non-zero emissions for the group. For low-prevalence
  sources firing in only k of N MC runs, this inflated the reported mean by N/k. Fixed
  by using `sum(readings) / mcIterations`, treating non-firing runs as zero — consistent
  with the mean correction already applied in `calculateEmissionSummary`.

- **Summaries2.py** — `createPDFCache` / `validatePDFCache`: replaced
  `pd.read_parquet(..., filters=[('site', '=', site_name)])` calls with a new
  `_read_parquet_site` helper that reads a site partition using PyArrow's native dataset
  API with an explicit Hive partitioning schema. The `pd.read_parquet` filter path
  failed when PyArrow could not infer the partition schema from an empty or schema-less
  directory. The new helper is robust to that case.

- **input/ModelFormulation/Compressor.json** — corrected typo in readable name:
  `"Compresor Single-Unit Vent Large Emitter"` → `"Compressor Single-Unit Vent Large
  Emitter"`.

### Tests (2026-03-25)

- **test/test_simsummary_mean.py**: new test file covering the `_filterAndPivot` mean
  denominator fix (issue #33, Bug 2). Tests construct minimal `SiteSummary`-shaped
  DataFrames with controlled `readings` lists and assert that `mean = total /
  mcIterations` in the output, including cases where fewer than `mcIterations` MC runs
  contributed readings.

### Bug Fixes (2026-03-17)

- **Summaries2.py** — `_filterAndPivot`: fixed incorrect SimSummary statistics (issue #27).
  The previous implementation aggregated the `mean` column across per-site rows, producing
  `total = simulation_total / mcIterations` and `mean = total`, while `min`, `max`, and CI
  columns were computed from the distribution of per-site means. This caused `mean > max` for
  any multi-site simulation. The function now explodes the per-site `readings` lists from
  `SiteSummary`, sums by positional MC-run index across sites, and computes all statistics from
  the resulting distribution of cross-site run totals. Validated on a 26-site and a 50-site
  simulation: 0 rows where `mean > max` or `upperCI < mean` in either case.

- **ModelClasses.py** — `MEETCompressor.checkForCorrectDriver`: the "Driver Type X does not
  have a close approximation of load equations" warning was emitted on every call, producing
  large numbers of identical log lines per simulation. Added `_driverTypeWarnIssued` instance
  flag so the warning fires at most once per compressor unit, matching the existing
  `_loadingWarnIssued` throttle pattern.

### Schema Changes (2026-03-17)

- **Summaries2.py** / **docs/SummarySchema.md** / **CLAUDE.md**: renamed `lowerQuintile` →
  `lowerQuartile` and `upperQuintile` → `upperQuartile` throughout. The columns compute the
  25th and 75th percentiles (Q1 and Q3), which are quartiles; the previous name was incorrect.
  Also corrected the percentile values documented in `CLAUDE.md` from 20th/80th to 25th/75th.
  `SiteSummary` and `SimSummary` parquets must be regenerated to pick up the new column names.

### Validation (2026-03-17)

- **SummaryTest.py** — `checkSimSummaryConsistency`: new function that validates statistical
  invariants on the new `SimSummary` output independently of old-vs-new comparison. Checks
  `mean ≤ max`, `lowerCI ≤ mean`, and `mean ≤ upperCI` for all non-C2/C1 rows.
  `mean > max` is counted as a hard violation (`emissionRateOutOfRangeCount`); CI bound
  violations are counted separately as warnings (`ciWarningCount`) since they can be
  statistically valid for heavily right-skewed distributions. Results appear as a
  `SimSummaryConsistency / self / check` row in the SummaryTest CSV output.

- **docs/SummarySchema.md**: documented the SummaryTest blind spot (old-vs-new comparison
  cannot detect bugs shared by both implementations), the SimSummary fix, the self-consistency
  check approach, and two new topics for discussion: extreme outlier MC runs causing
  `mean > upperCI` in MPLX-Q4, and the general recommendation to extend self-consistency
  checks to `SiteSummary` and `EventSummary`.

### Features (2026-03-17)

- **Summaries2.py** — `createSimPDF`: new function that builds simulation-level PDF summaries
  by computing the **mixture distribution** across all sites. For each grouping level, the
  per-site `PDF` parquet dataset is read and probabilities are averaged across the
  `(site, operator, psno)` components that contribute to each group:
  `p_sim(rate) = (1/N) × Σ_i p_i(rate)`. Four levels are produced: `simulation` (site totals
  mixed across all sites), `METype`, `unitID`, and `modelReadableName`. The result is written
  to the `SimPDF` parquet dataset (config key `parquetNewSimPDF`). `createSimPDF` is called
  from `summarizeSimulation` after `SimSummary` is written.

- **Summaries2.py** — `PDFCache` partitioning: added `cacheLevel` as a second partition column
  alongside `site`, so PyArrow can prune reads by cache level without loading the entire site
  partition. Renamed the `'site'` cache level to `'siteTotals'` to avoid confusion with the
  `site` partition column. `SIM_PDF_CACHE_LEVEL_MAP` replaced by `SIM_PDF_LEVEL_MAP` (no
  longer references PDFCache; operates on the `PDF` dataset instead).

- **SummaryTest.py** — `doSimPDFComparison`: compares the new `SimPDF` (`CICategory='simulation'`,
  `species='METHANE'`) against the legacy `aggregated_sim_PDFs_abnormal_*.csv` files in
  `summaries/AggregatedSimulationEmissions/` using the KS statistic. The legacy files store the
  PMF (`probability` sums to 1.0), so `doSimPDFComparison` computes the cumsum before
  interpolating — unlike `doPDFComparison`, which treats the per-site `probability` column as
  a CDF directly. KS distances of ~0.25–0.30 are expected and are **not** a bug: the new
  `SimPDF` is a mixture distribution ("typical site"), while the legacy files were computed by
  summing timeseries across all sites ("total across the simulation"). These answer different
  questions. See `docs/SummarySchema.md` § *SimPDF: mixture vs. sum* for full discussion.

- **SummaryTest.py** — `checkSimPDFConsistency`: new self-consistency check for the `SimPDF`
  dataset. For each `(CICategory, species, [METype], includeFugitive)` group, validates three
  invariants: probability sums ≈ 1, CDF is monotone non-decreasing, and CDF ends at ≈ 1.
  Results appear as a `SimPDFConsistency / self / check` row in the SummaryTest CSV. Validated
  on a 50-site simulation: 11,670 groups, 0 violations across all four levels (`simulation`,
  `METype`, `unitID`, `modelReadableName`).

- **SummaryTest.py** — `_readNewSimPDF`, `_readOldSimPDFs`: read the new `SimPDF` parquet
  dataset and the legacy `aggregated_sim_PDFs_abnormal_*.csv` files respectively.

- **MAESForTetra** — `moveLegacyPDFCache.py`: utility script that walks a mixed PDFCache
  directory (containing both old-style flat `PDFCache-0.parquet` files and new-style
  `cacheLevel=*/` subdirectories) and moves the old-style files to a `PDFCache.old` sibling
  directory, leaving the new-style partitioned files in place.

### Features

- **Summaries2.py** — PDF caching pipeline: added `createPDFCache`, which writes two new
  parquet datasets per simulation run — `PDFCache` (raw RLE interval data per emitter per MC
  run at four grouping levels: `site`, `METype`, `unitID`, `modelReadableName`) and `PDF`
  (computed probability distributions storing both `probability` and `cumulativeProbability`
  columns). Controlled by a new `createPDFCache` phase in `SiteMain2.py`. Config keys
  `parquetNewPDFCache` and `parquetNewPDF` added to `defaultConfig.json`.

- **Timeseries.py** — `TimeseriesCDF`: new class with `inverse(pts)` and `isempty()` methods.
  `TimeseriesPDF.toCDF()` now returns a `TimeseriesCDF` instance without mutating `self.data`.
  `TimeseriesPDF.inverse(pts)` added as a convenience wrapper for `toCDF().inverse(pts)`.

- **Timeseries.py** — `TimeseriesPDF.std()`: duration-weighted standard deviation of the PDF.

- **Summaries2.py** — `_roundForPDF(values, decimals=6)`: canonical rounding helper applied
  after every `TimeseriesSet.sum()` call before values are written into cache DataFrames or
  combined MC-run timeseries lists. Prevents ULP-variant emission rates from being treated as
  distinct PDF bins by `TimeseriesPDF.fromDataFrame`.

### Schema Changes

- **Summaries2.py** — `SimSummary` parquet: renamed columns `lower` → `lowerQuintile` and
  `upper` → `upperQuintile` in both `_filterAndPivot` and `_computeSimC2C1` to match the
  corresponding column names in `SiteSummary`. Updated `docs/SummarySchema.md` accordingly.

### Bug Fixes

- **ModelClasses.py** — `MEETCompressor.calcLoading`: the "Loading distribution > max rated
  load" warning was emitted on every overload sample, producing thousands of identical log lines
  per simulation. Added `_loadingWarnIssued` instance flag so the warning fires at most once per
  compressor unit per MC run.

- **Summaries2.py**: Exclude zero-emission events from event count and duration statistics to match
  the behaviour of the legacy `Summaries.py` path, which filtered `emissions_kgPerS > 0` before
  computing `avg_event_count`. Added `_removeZeroEmissionEvents` helper called by both
  `calculateEmissionSummary` and `calculateEventSummary`.

- **Summaries2.py**: Compute simulation-level C2/C1 ratio as an emission-weighted average rather
  than an unweighted mean of per-site ratios. Added `_computeSimC2C1` helper and updated
  `summarizeSimulation` to strip `species='C2/C1'` rows from the per-site summary before all
  `_filterAndPivot` calls, then recompute C2/C1 for each dimension from aggregated METHANE/ETHANE
  totals.

- **ParquetLib.py**: Changed `existing_data_behavior` from `overwrite_or_ignore` to
  `delete_matching` in both `toBaseParquet` and `toBaseParquetFullConfig`. The previous setting
  caused duplicate Parquet files to accumulate in a partition on repeated summarization runs,
  producing inflated totals in `siteEmissionsByCat` and `siteEmissionsByEquip`.

- **SummaryTest.py**: Normalise `modelEmissionCategory='TOTAL'` (emitted by legacy `Summaries.py`)
  to `'COMBINED'` (emitted by `Summaries2.py`) inside `doSimSummaryComparison` so that simulation-
  summary rows align correctly during validation comparisons.

- **Timeseries.py** — `TimeseriesRLE.__init__`: `MalformedTimeseriesError` was not raised until
  after the code attempted to access the missing column, producing an uninformative `KeyError`
  instead. The error is now raised immediately once any required column is found to be absent.

- **Timeseries.py** — `TimeseriesRLE.equal`: `np.allclose` was called without `equal_nan=True`,
  causing two timeseries that both contained NaN values to compare as unequal even when otherwise
  identical.

- **Timeseries.py** — `TimeseriesRLE.removeZeroDuration` / `removeErrorValues`: both methods
  mutated `self.df` in place and returned `self`. They now return a new `TimeseriesRLE` instance,
  leaving the original unchanged.

- **Timeseries.py** — `TimeseriesPDF.fromTS`: the `datascale` parameter was silently ignored
  because the internal call to `fromDataFrame` hard-coded `datascale=1`. The parameter is now
  forwarded correctly.

- **Timeseries.py** — `TimeseriesSet.sum()`: near-zero floating-point residuals (~1e-14) were
  retained after shared-endpoint cancellations, creating phantom intervals spanning large gaps
  between real events and corrupting downstream PDFs. Fixed by replacing `mask = intervalVals
  != 0.0` with an absolute-value threshold (`abs(val) < 1e-10`), which is four or more orders
  of magnitude above observed residuals and well below any physically meaningful emission rate.

- **Timeseries.py** — `TimeseriesPDF.add()`: adding two PDFs that shared emission-rate values
  produced duplicate rows rather than merging the counts. `add()` now re-groups by value after
  concatenation.

- **Timeseries.py** — `TimeseriesRLE.__init__`: added enforcement that all intervals satisfy
  `endTime > startTime`. A `MalformedTimeseriesError` is raised immediately on construction if
  any zero- or negative-duration interval is present, preventing silent data corruption
  downstream.

- **Summaries2.py** — `_buildMCRunTimeseries`: added a guard that logs a WARNING and filters
  out any `duration_s <= 0` rows in the instantaneous-emissions input before building emitter
  timeseries, preventing `MalformedTimeseriesError` from zero-duration events in raw data.

- **Summaries2.py** — `_buildPDFForGroupFromCache`: `_roundForPDF` was not applied to the
  per-MC-run summed timeseries produced inside this function. When multiple emission categories
  (e.g., COMBUSTION + FUGITIVE) are summed, the result can reintroduce ULP-level noise even
  though the individual cache entries were already rounded. `_roundForPDF` is now applied to the
  value column of both `fullTS` and `noFugTS` immediately after each `TimeseriesSet.sum()` call,
  before the timeseries is appended to the MC-run list. This eliminated 529 of 739 spurious CDF
  validation failures.

### Refactoring

- **Timeseries.py** — `TimeseriesPDF` / `TimeseriesCDF`: removed the `Method` enum and the
  `isinstance` dispatch in `__init__`. `TimeseriesPDF.__init__` now simply stores the provided
  DataFrame as `self.data`. The `probability` column is no longer stored internally (it is
  recomputed on demand in `toCDF()`). Dead code after the `return` in `fromTS` removed.
  `TimeseriesRLE.CDFInverse` updated to call `self.toPDF().inverse(pts)`; the standalone
  `cdfInverse(cdf_df, pts)` function removed.

- **Timeseries.py** — `TimeseriesRLE.sampleSquare`: replaced the original two-pass
  implementation with a single-pass binary search using `np.searchsorted`.

- **Summaries2.py** / **SiteMain2.py** / **SummaryTest.py** / **defaultConfig.json**: renamed
  all `CDF*` datasets, config keys, and functions to `PDF*` to reflect that the primary
  computational object is the Probability Mass Function, not the Cumulative Distribution
  Function. Specifically: `CDFCache` → `PDFCache`, `CDF` → `PDF`, `createCDFCache` →
  `createPDFCache`. The `CDFPrecomputed` intermediate dataset was removed; `createPDFCache` now
  writes directly to `PDF`. The `createCDF` phase and wrapper function in `SiteMain2.py` were
  also removed.

### Documentation

- **docs/SummarySchema.md**: added sections documenting the `PDFCache` and `PDF` parquet
  datasets (schema, config keys, resolved paths), the `_roundForPDF` rounding convention and
  its rationale, and two open items pending external review: (1) whether Blowdown Events should
  be included in emission-rate PDFs (legacy code excluded them as "maintenance emissions"; new
  code includes them as VENTED), and (2) KS-statistic sensitivity for heater units with sparse
  PDFs (3–6 bins).

- **docs/Timeseries.md**: updated `TimeseriesPDF` and `TimeseriesCDF` API tables to reflect
  the refactored interface. Also added module-level usage guide, caveats, and design notes
  covering `TimeseriesRLE`, `TimeseriesSet`, `TimeseriesPDF`, and `TimeseriesCDF`.

### Tests

- **UnitTests/test_Timeseries.py**: Fixed module imports to use the flat `src/` layout
  (`import Timeseries as ts`, `from Timer import Timer`) rather than a `timeseries` package that
  no longer exists. Added a `sys.path` insert so the test file can be run directly from the `src/`
  directory.

- **UnitTests/test_Timeseries.py**: Plotting-backend imports (`TsMatplotlib`, `TsBokeh`,
  `TsPlotly`) are now guarded with `try/except ImportError`; `TestPlotter` is skipped with
  `@unittest.skipUnless` when the backends are absent, allowing the remaining 72 tests to run.

- **UnitTests/test_Timeseries.py**: Corrected `LARGE_TSFILE` path from `tests/` to `UnitTests/`.

- **UnitTests/test_Timeseries.py**: Updated three `TestPDF` expected DataFrames to include the
  `probability` column that `TimeseriesPDF.fromDataFrame` has always produced.

- **UnitTests/test_Timeseries.py** — `TestTimeseriesCDF`: replaced the former `TestCdfInverse`
  suite with 7 tests covering `TimeseriesCDF.inverse()`, `TimeseriesCDF.isempty()`, and the
  non-mutation guarantee of `TimeseriesPDF.toCDF()`.

- **UnitTests/test_Timeseries.py** — `TestPDF`: extended to 15 tests, adding coverage for
  `TimeseriesPDF.std()`, `TimeseriesPDF.add()` (including the duplicate-row fix),
  `TimeseriesPDF.inverse()`, and `test_PDFStatsTable` (updated expected `stdDev` to 7.9368).

### Performance

- **Timeseries.py** — `TimeseriesSet.sum()`: replaced the sequential `addSquare` reduce
  (O(K²·M)) with an event-based algorithm (O(K·M log(K·M))). For each interval in each
  timeseries a `+value` event is emitted at the start time and a `−value` event at the end time;
  all events are concatenated, grouped by time, and cumsummed to produce the running total at
  every breakpoint. The old implementation is preserved as `TimeseriesSet.oldSum()` for
  reference. Benchmarks on 10 timeseries × 100 K intervals: ~52× speedup (17 s → 0.32 s).
  Full algorithm exposition in **docs/TimeseriesSum.md**.

- **Timeseries.py** — `TimeseriesSet.oldSum()`: the original `addSquare`-reduce implementation
  is retained under this name for comparison. Note that `oldSum()` also produced **incorrect
  totals** (overcount of ~34–46%) when timeseries overlapped in time, due to a boundary
  misassignment in `sampleSquare` that compounded across sequential accumulation steps.

### Tests

- **UnitTests/test_Timeseries.py** — `TestTimeseriesSetSum`: 15 new correctness tests for
  `TimeseriesSet.sum()` covering disjoint, adjacent, partial overlap, containment, all three
  breakpoint-alignment cases (start-aligned, end-aligned, both-aligned, A-end = B-start),
  multi-interval cases, three-way overlap, empty set, single-element set, empty member, and
  zero-value intervals.

- **UnitTests/test_Timeseries.py** — `TSSetScaleTest`: two timing tests asserting both
  correctness and throughput for `sum()`:
  - `test_sumFewWideSeries`: 10 timeseries × 1 M intervals (~8 s)
  - `test_sumManyNarrowSeries`: 1 K timeseries × 10 K intervals (~8 s)
  - `test_sumVsOldSum`: side-by-side timing comparison of `sum()` vs `oldSum()` on
    10 × 100 K intervals; asserts correctness of `sum()` only.

- **UnitTests/demo_oldSumVsNewSum.py**: standalone matplotlib demo showing the `oldSum()`
  overcounting bug. Generates random overlapping timeseries, runs both implementations, and
  produces a four-panel plot: individual timeseries, full-range overlay, 200-step zoom of the
  maximum-divergence window (with error region shaded), and a bar-chart total comparison using
  the large-scale dataset.


## v0.2.0 (2025-01-12)

### Features

- Build input study sheet artifacts & version
  ([`d380921`](https://github.com/CSU-METEC/MAES/commit/d3809214d3d0e54a09eaaf7031961fe6fc665dc6))


## v0.1.0 (2025-01-12)

### Features

- Build input study sheet artifacts & version
  ([`a34fee8`](https://github.com/CSU-METEC/MAES/commit/a34fee81894cab451f74db17b2d9303057f2bed0))


## v0.0.4 (2025-01-12)

### Bug Fixes

- Declare packages in pyproject.toml
  ([`1af5f16`](https://github.com/CSU-METEC/MAES/commit/1af5f16cb87cb9332fdd03a348eb1067ed80fcf0))


## v0.0.3 (2025-01-12)

### Bug Fixes

- Remove non-package mode
  ([`dda6ccf`](https://github.com/CSU-METEC/MAES/commit/dda6ccf7aac43069a66f259bc563984fb1724bfc))


## v0.0.2 (2025-01-12)

### Bug Fixes

- Automated input study sheet artifact storage
  ([`5be8536`](https://github.com/CSU-METEC/MAES/commit/5be85363628e801d36d9d4831d50be2c27ac11e2))


## v0.0.1 (2025-01-12)

### Bug Fixes

- Changelog file path
  ([`41931b3`](https://github.com/CSU-METEC/MAES/commit/41931b33730cb54143562fd62ca06324ed892f84))

- **env**: Added requirements file and initial README.md guide
  ([`d3dee5a`](https://github.com/CSU-METEC/MAES/commit/d3dee5a8b0372b4601ef6127ffc61cf5038961a8))

- **git**: Git ignored .idea files/folders
  ([`e3a2746`](https://github.com/CSU-METEC/MAES/commit/e3a274632f8e393a5b559436a440d306d054a4bd))

### Chores

- **release**: 1.0.0
  ([`feabd54`](https://github.com/CSU-METEC/MAES/commit/feabd54678fc73923f0aef514385c7693229db6d))

- **release**: 1.0.1
  ([`0c952a9`](https://github.com/CSU-METEC/MAES/commit/0c952a9ac8fd0978b7edfaaf38ec521b29428926))

### Continuous Integration

- Packaging and automated release with github actions
  ([`c1df4c0`](https://github.com/CSU-METEC/MAES/commit/c1df4c00cfc378e771ed78520e261bd12e8a9399))

- Poetry lock file for depency management
  ([`215102d`](https://github.com/CSU-METEC/MAES/commit/215102d1ca8f843ddd3502b1ecea5a3531b91b20))

- Poetry lock file for depency management
  ([`8867a58`](https://github.com/CSU-METEC/MAES/commit/8867a58c39397b5e1680cbdbaf4050508012980e))

### Documentation

- **changelog**: Added Initial release fork msg
  ([`086daf5`](https://github.com/CSU-METEC/MAES/commit/086daf5d484c1e53a0a8be5cd90b4eb4fa508af5))

- **readme**: Fix guide type
  ([`51f2761`](https://github.com/CSU-METEC/MAES/commit/51f27618d68f92dfcaab61a936a453b7096febde))
