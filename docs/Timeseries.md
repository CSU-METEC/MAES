# Timeseries Module

`Timeseries.py` provides classes for working with rate-valued timeseries in the MAES framework. The primary use case is representing emission rates over time, where intervals are sparse (most of the time the rate is zero) and arithmetic operations across many Monte Carlo timeseries are needed.

---

## Classes

### `TimeseriesRLE`

Run-length encoded timeseries. Each row in the underlying DataFrame represents a half-open interval `[startTime, endTime)` with a constant value. Gaps between intervals are implicitly zero.

**DataFrame columns** (defaults):

| Column | Default name | Description |
|--------|-------------|-------------|
| Start time | `timestamp` | Interval start (inclusive) |
| End time | `nextTS` | Interval end (exclusive) |
| Value | `tsValue` | Rate/value during interval |

Column names can be overridden via `startTimeColName`, `endTimeColName`, `valueColName` constructor arguments.

**Constructor validation** — `MalformedTimeseriesError` is raised if:
- Any required column is missing
- Any interval has `endTime <= startTime` (zero or negative duration)
- Any interval overlaps the next interval's start time
- The end times are not strictly increasing

**`filterZeros=True`** constructor argument silently drops rows where `tsValue == 0`.

#### Key methods

| Method | Description |
|--------|-------------|
| `total()` | Time-integrated sum: Σ (value × duration) |
| `totalDuration(omitZero=True)` | Total time covered by non-zero intervals |
| `mean()` | Duration-weighted mean, total duration, total value |
| `std()` | Duration-weighted standard deviation |
| `meanAndStd(omitZero, startTime, endTime)` | Mean and std over an optional sub-window |
| `startTime()` / `endTime()` | First start / last end time |
| `isempty()` | True if the DataFrame has no rows |
| `mask(ts2, fillZeros)` | Multiply by a mask timeseries (keep only periods where `ts2` is active) |
| `maskTS(tStart, tEnd, fill)` | Clip to a time window; optionally pad with zeros |
| `periodicAverage(intervals)` | Resample to fixed intervals using trapezoidal integration |
| `toFullTimeseries()` | Convert to a dense `TimeseriesFull` step function |
| `toPDF()` | Convert to a `TimeseriesPDF` (duration-weighted distribution) |
| `fillZeros(startTime, endTime)` | Return a new timeseries with explicit zero intervals filling gaps |
| `zeroPeriods(startTime, endTime)` | Return a timeseries representing the zero (gap) periods, valued 1 |
| `threshold(threshold)` | Return a timeseries of periods where value exceeds the threshold |
| `removeZerosNonDestructive()` | Return a new timeseries with zero-value rows dropped |
| `removeErrorValues(replace)` | Drop or replace NaN/inf values |
| `addSquare(ts2)` | Pairwise addition with another timeseries |
| `subtractSquare(ts2)` | Pairwise subtraction |
| `multiplySquare(ts2)` | Pairwise multiplication |
| `divideSquare(ts2)` | Pairwise division |
| `createConstant(value)` | Return a constant-valued timeseries spanning the same range |

#### Class methods

| Method | Description |
|--------|-------------|
| `fromCollections(starts, ends, values, ...)` | Construct from three equal-length sequences |
| `fromDictList(dictList, ...)` | Construct from a list of dicts |

---

### `TimeseriesSet`

Holds a list of `TimeseriesRLE` objects and supports set-level operations.

| Method | Description |
|--------|-------------|
| `sum()` | Correct O(M log M) event-based summation (see Caveats) |
| `mean()` | Element-wise mean: `sum() / N` |
| `toPDF()` | Concatenate all timeseries into a single `TimeseriesPDF` |
| `addTimeseries(ts)` | Append a timeseries to the set |

---

### `TimeseriesPDF`

Duration-weighted probability distribution derived from a `TimeseriesRLE`. Values are grouped and weighted by their interval durations. Internal representation is a DataFrame with columns `value` and `count` only — no `probability` column is stored.

**No-mutation policy:** Methods never modify `self.data` in place; each operation that produces new data returns a new object.

| Method | Description |
|--------|-------------|
| `fromTS(ts, tolerance, datascale)` | Class method — construct from a `TimeseriesRLE` |
| `fromDataFrame(df, ...)` | Class method — construct from a raw DataFrame |
| `add(pdfObj)` | Merge another `TimeseriesPDF` into this one, summing counts for matching values |
| `toCDF()` | Return a `TimeseriesCDF` (does **not** mutate `self.data`) |
| `inverse(pts)` | Convenience — delegates to `self.toCDF().inverse(pts)` |
| `std()` | Duration-weighted standard deviation |
| `statsTable(params)` | Summary stats: min, lower CI, mean, upper CI, max, std, median |
| `total()` | Σ (value × count) |
| `mean()` | Duration-weighted mean |
| `min()` / `max()` | Minimum and maximum values |
| `counts()` | Total duration (sum of counts) |

---

### `TimeseriesCDF`

Cumulative distribution function produced by `TimeseriesPDF.toCDF()`. Internal DataFrame has columns `value` and `cumulative_probability`.

| Method | Description |
|--------|-------------|
| `inverse(pts)` | Interpolate CDF at probability points in `[0, 1]`; returns `[None, ...]` for empty/NaN CDF |
| `isempty()` | True if the underlying DataFrame is empty |

---

### `TimeseriesFull`

Dense (non-RLE) timeseries where each row is a (timestamp, value) sample. Used internally as an intermediate representation by `toFullTimeseries()` and `periodicAverage()`. Not intended for direct construction in most cases.

---

### `TimeseriesCategorical`

Subclass of `TimeseriesRLE` for string-valued (categorical) timeseries such as equipment state machines.

---

## Caveats

### Zero-duration intervals are forbidden

`TimeseriesRLE.__init__` raises `MalformedTimeseriesError` if any row has `endTime <= startTime`. Zero-duration intervals must be removed from source data before constructing a `TimeseriesRLE`.

**Why this matters:** The pairwise arithmetic methods (`addSquare`, `subtractSquare`, etc.) use `sampleSquare()` internally, which unconditionally assigns an interval's value at its start time regardless of duration. A zero-duration interval `[t, t)` is assigned its value at breakpoint `t`, and `fromCollections` then promotes that point into a finite-width interval in the result. This causes phantom values in any downstream computation.

The `sum()` method on `TimeseriesSet` is immune to this bug by construction (opposing `+value`/`−value` events at the same time cancel exactly), but the pairwise methods are not. The constructor guard prevents the issue from arising.

**Generating valid timeseries from random data:** integer-cast random durations can floor to zero. Always clamp:

```python
durations = np.maximum(1, (rng.random(n) * deltas).astype(np.int64))
```

### `TimeseriesSet.sum()` vs `addSquare` accumulation

See [TimeseriesSum.md](TimeseriesSum.md) for a full exposition of the algorithm, correctness proof, complexity analysis, and annotated implementation.

`TimeseriesSet.sum()` uses an event-based O(M log M) algorithm: for each interval `[t_start, t_end)` with value `v`, it emits `+v` at `t_start` and `−v` at `t_end`, then cumsum over all sorted events. This is correct for any number of timeseries.

The legacy `addSquare`-based accumulation (`TimeseriesSet.oldSum()`, retained for reference) is incorrect when zero-duration intervals are present. Once zero-duration intervals are excluded (enforced by the constructor), `oldSum()` also produces correct results. The event-based `sum()` is preferred for sets of more than two timeseries.

**Performance:** Benchmarked on 10 timeseries × 100,000 intervals each (1M total events, measured on 2026-03-10):

| Method | Throughput |
|--------|-----------|
| `TimeseriesSet.sum()` | ~1.4M events/sec |
| `TimeseriesSet.oldSum()` | ~57K events/sec |

`sum()` is approximately 24× faster than `oldSum()` at this scale. The advantage grows with the number of timeseries in the set because `oldSum()` is O(N·M) while `sum()` is O(M log M) regardless of N.

### Pairwise arithmetic (`addSquare`, `subtractSquare`, etc.)

These methods call `_arithmeticPrep`, which computes a union of all interval breakpoints and samples both timeseries at those points via `sampleSquare`. This is correct for two-timeseries operations provided neither timeseries contains zero-duration intervals (now guaranteed by the constructor).

Do not use repeated `addSquare` calls to sum N timeseries — use `TimeseriesSet.sum()` instead, which is both faster (O(M log M) vs O(N·M)) and avoids accumulation of floating-point error.

### Implicit zeros

`TimeseriesRLE` represents zero implicitly — any time not covered by a row is zero. Methods that need explicit zeros (e.g. `periodicAverage`, `zeroPeriods`) materialize them internally. Do not store explicit zero-value rows in a `TimeseriesRLE` unless required; use `filterZeros=True` at construction time or `removeZerosNonDestructive()` to clean them up.

### Column name aliasing

All three column names (`startTimeColName`, `endTimeColName`, `valueColName`) can be overridden at construction. Arithmetic operations preserve the column names of `self`. Mixed-column-name arithmetic is not supported.

---

## Design Notes

### Zero-duration interval enforcement (2026-03-10)

`TimeseriesRLE.__init__` now raises `MalformedTimeseriesError` if any interval has `endTime <= startTime`. This check was added after the root cause of the `oldSum()` over-count bug was traced to zero-duration intervals in the input data.

**Root cause of the bug:** `sampleSquare` (the shared kernel for all pairwise arithmetic) unconditionally assigned each interval's value at its start time, regardless of duration. A zero-duration interval `[t, t)` therefore got its value written at breakpoint `t`, and the subsequent `fromCollections` call promoted that point into a finite-width interval in the accumulated result. This produced phantom non-zero values in `oldSum()` that did not correspond to any real interval overlap. `TimeseriesSet.sum()` was immune — opposing `+v`/`−v` events at the same time cancel exactly in the `groupby().sum()` step — but all pairwise methods (`addSquare`, `subtractSquare`, `multiplySquare`, `divideSquare`) were affected.

Enforcing the constraint at construction time prevents the issue from arising anywhere in the pipeline. Random data generators that produce durations by integer-casting a float must clamp to ≥ 1:

```python
durations = np.maximum(1, (rng.random(n) * deltas).astype(np.int64))
```

### `sampleSquare` rewrite — single-pass binary search (2026-03-10)

The original `sampleSquare` used a two-pass algorithm: (1) write each interval's value at its start-time breakpoint unconditionally, then (2) overwrite with the correct value for interior breakpoints found via `endTimes.searchsorted`. The two passes were implemented with pandas `.loc` index assignment on a constructed DataFrame, which is both slow and fragile — the first pass was the source of the zero-duration phantom bug described above.

The replacement is a single-pass numpy binary search:

```python
def sampleSquare(self, bpList):
    startTimes = self.df[self.startTimeColName].values
    endTimes   = self.df[self.endTimeColName].values
    values     = self.df[self.valueColName].values
    bpArr  = np.asarray(bpList)
    idx    = np.searchsorted(startTimes, bpArr, side='right') - 1
    valid  = idx >= 0
    vi     = idx[valid]
    vt     = bpArr[valid]
    result = np.zeros(len(bpArr))
    inside = vt < endTimes[vi]
    result[np.where(valid)[0][inside]] = values[vi[inside]]
    return pd.Series(result, index=bpArr)
```

`np.searchsorted(startTimes, bpArr, side='right') - 1` finds, for each breakpoint `t`, the index of the last interval whose `startTime <= t`. The half-open check `t < endTime` then determines whether `t` falls inside that interval or in a gap. No two-pass logic, no DataFrame allocation, no `.loc` assignment.

The rewrite passes all 94 unit tests unchanged. Because zero-duration intervals are now forbidden by the constructor, there is no longer any case where the old first-pass assignment and the new single-pass approach would differ — but the new implementation is correct by construction even without that invariant.
