"""Tests for the array-native summation fast path and its PDF-cascade call sites (issue #121).

Covers, in order:
  - TimeseriesSet.sum() fast path vs the retained legacy implementation (_sumLegacy), on
    randomized interval sets and on the edge cases the legacy code encodes (shared endpoints,
    the 1e-10 residual clip — including its deliberate drop of POSITIVE near-zero intervals,
    empty sets, single-timeseries gaps);
  - subclass exclusion: TimeseriesCategorical-style subclasses must never take the fast path;
  - TimeseriesRLE.fromValidatedArrays: attribute contract vs the validating __init__;
  - _coalesceAdjacentEqual / _roundedCacheArrays: lossless-by-construction merging (per-value
    duration mass preserved exactly);
  - _buildMCRunTimeseries: equivalence with a hand-built legacy sum on synthetic InstEmissions
    rows, and preservation of the boundary validation (overlap -> MalformedTimeseriesError).
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import unittest
from unittest import mock

import numpy as np
import pandas as pd

import Timeseries as ts
import Units as u
from Summaries2 import _buildMCRunTimeseries, _coalesceAdjacentEqual, _roundedCacheArrays
from Summaries2 import (_validateCacheBoundary, calculatePDFSummaryFromCache,
                        _calculatePDFSummaryFromCacheLegacy)


def makeRLE(starts, ends, values):
    """Build a TimeseriesRLE through the normal validating path, with the default colnames the
    PDF cascade's legacy code produced (fromCollections defaults)."""
    return ts.TimeseriesRLE.fromCollections(np.asarray(starts, dtype=float),
                                            np.asarray(ends, dtype=float),
                                            np.asarray(values, dtype=float))


def makeRandomRLE(rng, tMax=1000.0, maxIntervals=8):
    """Random non-overlapping interval set: sorted unique breakpoints carved into alternating
    kept/skipped intervals, values well away from the 1e-10 residual-clip threshold."""
    n = int(rng.integers(1, maxIntervals + 1))
    breakpoints = np.sort(rng.choice(np.arange(1, int(tMax)), size=2 * n, replace=False)).astype(float)
    starts = breakpoints[0::2]
    ends = breakpoints[1::2]
    values = rng.uniform(0.5, 50.0, size=n)
    return makeRLE(starts, ends, values)


def assertSummedEqual(testCase, fastTS, legacyTS):
    """Assert two summed RLEs describe the same step function: identical interval edges and
    values equal to within float-reassociation noise (~1e-16 relative; see the kernel's note —
    addition ORDER within a shared time bucket may differ from pandas' groupby order)."""
    fastDF, legacyDF = fastTS.df, legacyTS.df
    testCase.assertEqual(len(fastDF), len(legacyDF))
    if len(fastDF) == 0:
        return
    np.testing.assert_array_equal(fastDF[fastTS.startTimeColName].to_numpy(),
                                  legacyDF[legacyTS.startTimeColName].to_numpy())
    np.testing.assert_array_equal(fastDF[fastTS.endTimeColName].to_numpy(),
                                  legacyDF[legacyTS.endTimeColName].to_numpy())
    np.testing.assert_allclose(fastDF[fastTS.valueColName].to_numpy(),
                               legacyDF[legacyTS.valueColName].to_numpy(),
                               rtol=1e-12, atol=1e-13)


class TestSumFastPathEquivalence(unittest.TestCase):
    """The fast path must reproduce the legacy frame-based sum exactly (modulo FP
    reassociation far below every downstream threshold)."""

    def test_randomized_sets_match_legacy(self):
        rng = np.random.default_rng(20260708)
        for trial in range(50):
            memberCount = int(rng.integers(1, 6))
            members = []
            for _ in range(memberCount):
                members.append(makeRandomRLE(rng))
            fast = ts.TimeseriesSet(members).sum()
            legacy = ts.TimeseriesSet(members)._sumLegacy()
            assertSummedEqual(self, fast, legacy)

    def test_shared_endpoints_hand_case(self):
        """Two members sharing an exact endpoint: the +/- events land in one time bucket and
        must net, not create a zero-width interval. Hand-computed expectation."""
        a = makeRLE([0.0, 10.0], [5.0, 20.0], [1.5, 2.5])
        b = makeRLE([5.0], [10.0], [4.0])   # b starts exactly where a's first interval ends
        summed = ts.TimeseriesSet([a, b]).sum()
        legacy = ts.TimeseriesSet([a, b])._sumLegacy()
        assertSummedEqual(self, summed, legacy)
        np.testing.assert_array_equal(summed.df[summed.startTimeColName].to_numpy(),
                                      np.array([0.0, 5.0, 10.0]))
        np.testing.assert_array_equal(summed.df[summed.endTimeColName].to_numpy(),
                                      np.array([5.0, 10.0, 20.0]))
        np.testing.assert_allclose(summed.df[summed.valueColName].to_numpy(),
                                   np.array([1.5, 4.0, 2.5]))

    def test_exact_cancellation_clipped_to_empty(self):
        """A member and its exact negation cancel to 0.0 everywhere; the residual clip drops
        every interval and the result is the legacy empty frame (default column names)."""
        a = makeRLE([0.0], [10.0], [3.0])
        b = makeRLE([0.0], [10.0], [-3.0])
        summed = ts.TimeseriesSet([a, b]).sum()
        legacy = ts.TimeseriesSet([a, b])._sumLegacy()
        self.assertTrue(summed.isempty())
        self.assertTrue(legacy.isempty())
        self.assertEqual(list(summed.df.columns), ['timestamp', 'nextTS', 'tsValue'])

    def test_positive_near_zero_interval_dropped(self):
        """Documents the deliberate legacy quirk the kernel preserves: the clip drops ALL
        near-zero intervals, positive real values included — a 5e-11 emission interval
        vanishes from the sum on BOTH paths."""
        tiny = makeRLE([0.0], [10.0], [5e-11])
        summed = ts.TimeseriesSet([tiny]).sum()
        legacy = ts.TimeseriesSet([tiny])._sumLegacy()
        self.assertTrue(summed.isempty())
        self.assertTrue(legacy.isempty())

    def test_empty_set(self):
        summed = ts.TimeseriesSet([]).sum()
        self.assertTrue(summed.isempty())
        self.assertEqual(list(summed.df.columns), ['timestamp', 'nextTS', 'tsValue'])

    def test_single_member_with_gap(self):
        """One member with a gap: the sweep sees a zero-valued gap interval, the clip removes
        it, and the output equals the input's own intervals — on both paths."""
        a = makeRLE([0.0, 10.0], [5.0, 20.0], [2.0, 3.0])
        summed = ts.TimeseriesSet([a]).sum()
        legacy = ts.TimeseriesSet([a])._sumLegacy()
        assertSummedEqual(self, summed, legacy)
        np.testing.assert_array_equal(summed.df[summed.startTimeColName].to_numpy(),
                                      np.array([0.0, 10.0]))
        np.testing.assert_allclose(summed.df[summed.valueColName].to_numpy(),
                                   np.array([2.0, 3.0]))


class TestFastPathGating(unittest.TestCase):
    """Only sets whose EVERY member is a plain TimeseriesRLE may take the fast path;
    subclasses (e.g. TimeseriesCategorical state timelines, whose values are category codes)
    must always fall back to the legacy implementation."""

    def test_subclass_member_forces_legacy_path(self):
        class SubclassedRLE(ts.TimeseriesRLE):
            pass

        a = makeRLE([0.0], [10.0], [2.0])
        subclassed = SubclassedRLE.fromCollections(np.array([5.0]), np.array([15.0]),
                                                   np.array([1.0]))
        # If the fast path ran, it would call sumEventArrays — assert it never does.
        with mock.patch.object(ts, 'sumEventArrays',
                               side_effect=AssertionError("fast path must not run")):
            summed = ts.TimeseriesSet([a, subclassed]).sum()
        np.testing.assert_allclose(summed.df[summed.valueColName].to_numpy(),
                                   np.array([2.0, 3.0, 1.0]))

    def test_plain_members_use_fast_path(self):
        a = makeRLE([0.0], [10.0], [2.0])
        b = makeRLE([5.0], [15.0], [1.0])
        with mock.patch.object(ts, 'sumEventArrays',
                               wraps=ts.sumEventArrays) as kernelSpy:
            ts.TimeseriesSet([a, b]).sum()
        self.assertEqual(kernelSpy.call_count, 1)


class TestFromValidatedArrays(unittest.TestCase):
    """The trusted constructor must set everything the validating __init__ sets, and its
    results must be full citizens (usable as members of further sums)."""

    def test_attribute_contract(self):
        starts = np.array([0.0, 5.0])
        ends = np.array([5.0, 9.0])
        values = np.array([1.0, 2.0])
        rle = ts.TimeseriesRLE.fromValidatedArrays(starts, ends, values)
        self.assertIsInstance(rle, ts.TimeseriesRLE)
        self.assertEqual(rle.colList, [rle.startTimeColName, rle.endTimeColName, rle.valueColName])
        self.assertTrue(rle.sorted)
        self.assertIsNone(rle.name)
        self.assertIsNone(rle.units)
        self.assertFalse(rle.isempty())
        np.testing.assert_array_equal(rle.df[rle.startTimeColName].to_numpy(), starts)
        np.testing.assert_array_equal(rle.df[rle.endTimeColName].to_numpy(), ends)
        np.testing.assert_array_equal(rle.df[rle.valueColName].to_numpy(), values)

    def test_usable_in_subsequent_sum(self):
        first = ts.TimeseriesSet([makeRLE([0.0], [10.0], [2.0]),
                                  makeRLE([5.0], [15.0], [1.0])]).sum()
        second = ts.TimeseriesSet([first, makeRLE([0.0], [15.0], [1.0])]).sum()
        np.testing.assert_allclose(second.df[second.valueColName].to_numpy(),
                                   np.array([3.0, 4.0, 2.0]))


class TestSumEventArraysKernel(unittest.TestCase):
    """Direct kernel tests, independent of the Timeseries object layer."""

    def test_empty_inputs(self):
        starts, ends, values = ts.sumEventArrays([np.array([])], [np.array([])], [np.array([])])
        self.assertEqual(len(values), 0)

    def test_hand_case(self):
        starts, ends, values = ts.sumEventArrays(
            [np.array([0.0, 10.0]), np.array([2.0])],
            [np.array([5.0, 20.0]), np.array([12.0])],
            [np.array([1.5, 2.5]), np.array([4.0])])
        np.testing.assert_array_equal(starts, np.array([0.0, 2.0, 5.0, 10.0, 12.0]))
        np.testing.assert_array_equal(ends, np.array([2.0, 5.0, 10.0, 12.0, 20.0]))
        np.testing.assert_allclose(values, np.array([1.5, 5.5, 4.0, 6.5, 2.5]))

    def test_output_invariants_random(self):
        """The trusted-constructor contract: strictly increasing starts, contiguous positive-
        duration intervals — must hold on randomized input."""
        rng = np.random.default_rng(42)
        for trial in range(25):
            startsList, endsList, valsList = [], [], []
            for _ in range(int(rng.integers(1, 5))):
                member = makeRandomRLE(rng)
                startsList.append(member.df[member.startTimeColName].to_numpy())
                endsList.append(member.df[member.endTimeColName].to_numpy())
                valsList.append(member.df[member.valueColName].to_numpy())
            starts, ends, values = ts.sumEventArrays(startsList, endsList, valsList)
            if len(values) == 0:
                continue
            self.assertTrue(np.all(np.diff(starts) > 0))
            self.assertTrue(np.all(ends > starts))


class TestCoalesceAdjacentEqual(unittest.TestCase):
    """Round-then-recoalesce support: merging adjacent equal-value intervals must preserve the
    per-value duration mass exactly (that is the losslessness argument for the PDF)."""

    def test_merges_equal_runs(self):
        starts = np.array([0.0, 5.0, 10.0, 20.0])
        ends = np.array([5.0, 10.0, 20.0, 30.0])
        values = np.array([1.0, 1.0, 2.0, 2.0])
        s, e, v = _coalesceAdjacentEqual(starts, ends, values)
        np.testing.assert_array_equal(s, np.array([0.0, 10.0]))
        np.testing.assert_array_equal(e, np.array([10.0, 30.0]))
        np.testing.assert_array_equal(v, np.array([1.0, 2.0]))

    def test_no_merge_when_values_differ(self):
        starts = np.array([0.0, 5.0])
        ends = np.array([5.0, 10.0])
        values = np.array([1.0, 2.0])
        s, e, v = _coalesceAdjacentEqual(starts, ends, values)
        self.assertEqual(len(v), 2)

    def test_empty(self):
        empty = np.array([])
        s, e, v = _coalesceAdjacentEqual(empty, empty, empty)
        self.assertEqual(len(v), 0)

    def test_duration_mass_preserved_random(self):
        """For random already-rounded step functions: sum of durations per distinct value must
        be identical before and after coalescing — the exact property the duration-weighted
        PDF depends on."""
        rng = np.random.default_rng(7)
        for trial in range(25):
            n = int(rng.integers(2, 40))
            edges = np.cumsum(rng.uniform(1.0, 10.0, size=n + 1))
            starts = edges[:-1]
            ends = edges[1:]
            values = rng.choice(np.array([0.1, 0.2, 0.3]), size=n)
            s, e, v = _coalesceAdjacentEqual(starts, ends, values)
            for distinctValue in np.unique(values):
                before = float(np.sum((ends - starts)[values == distinctValue]))
                after = float(np.sum((e - s)[v == distinctValue]))
                self.assertAlmostEqual(before, after, places=9)

    def test_rounded_cache_arrays_composition(self):
        """_roundedCacheArrays: rounding collapses near-equal rates, then coalescing merges the
        now-equal neighbours; with coalesce off every interval survives (legacy behaviour)."""
        starts = np.array([0.0, 5.0, 10.0])
        ends = np.array([5.0, 10.0, 15.0])
        values = np.array([1.0000001, 0.9999999, 2.0])
        sOff, eOff, vOff = _roundedCacheArrays(starts, ends, values, 1e-3, False)
        self.assertEqual(len(vOff), 3)
        np.testing.assert_allclose(vOff[:2], np.array([1.0, 1.0]))
        sOn, eOn, vOn = _roundedCacheArrays(starts, ends, values, 1e-3, True)
        np.testing.assert_array_equal(sOn, np.array([0.0, 10.0]))
        np.testing.assert_array_equal(eOn, np.array([10.0, 15.0]))
        np.testing.assert_allclose(vOn, np.array([1.0, 2.0]))


class TestBuildMCRunTimeseries(unittest.TestCase):
    """The InstEmissions-boundary call site: array path must match a hand-built legacy sum and
    must preserve the boundary validation the TimeseriesRLE constructor used to provide."""

    @staticmethod
    def makeInstEmissions(rowsList):
        """rowsList: (emitterID, timestamp_s, duration_s, emission_kgPerS) tuples."""
        records = []
        for emitterID, timestamp, duration, rate in rowsList:
            records.append({'emitterID': emitterID, 'timestamp_s': timestamp,
                            'duration_s': duration, 'emission_kgPerS': rate,
                            'site': 'TestSite', 'mcRun': 0})
        return pd.DataFrame.from_records(records)

    def test_matches_legacy_sum(self):
        mcRunDF = self.makeInstEmissions([
            ('em1', 0.0, 5.0, 1.0),
            ('em1', 10.0, 10.0, 2.0),
            ('em2', 2.0, 10.0, 4.0),
        ])
        result = _buildMCRunTimeseries(mcRunDF)
        # Hand-built legacy equivalent: per-emitter RLEs summed via the retained legacy path.
        legacyMembers = [
            makeRLE([0.0, 10.0], [5.0, 20.0], [1.0 * u.SECONDS_PER_HOUR, 2.0 * u.SECONDS_PER_HOUR]),
            makeRLE([2.0], [12.0], [4.0 * u.SECONDS_PER_HOUR]),
        ]
        legacy = ts.TimeseriesSet(legacyMembers)._sumLegacy()
        assertSummedEqual(self, result, legacy)

    def test_zero_duration_rows_filtered(self):
        mcRunDF = self.makeInstEmissions([
            ('em1', 0.0, 5.0, 1.0),
            ('em1', 5.0, 0.0, 9.9),   # zero-duration: filtered with a warning, not an error
        ])
        result = _buildMCRunTimeseries(mcRunDF)
        self.assertEqual(len(result.df), 1)
        np.testing.assert_allclose(result.df[result.valueColName].to_numpy(),
                                   np.array([1.0 * u.SECONDS_PER_HOUR]))

    def test_overlap_raises_malformed(self):
        """Overlapping rows for one emitter (as ordered) are malformed input — the same
        MalformedTimeseriesError the TimeseriesRLE constructor raised on this data."""
        mcRunDF = self.makeInstEmissions([
            ('em1', 0.0, 10.0, 1.0),
            ('em1', 5.0, 10.0, 2.0),   # starts before the previous row ends
        ])
        with self.assertRaises(ts.MalformedTimeseriesError):
            _buildMCRunTimeseries(mcRunDF)

    def test_empty_after_filter_returns_empty(self):
        mcRunDF = self.makeInstEmissions([('em1', 0.0, 0.0, 1.0)])
        result = _buildMCRunTimeseries(mcRunDF)
        self.assertTrue(result.isempty())


class TestDurationWeightedDistribution(unittest.TestCase):
    """The round-2 kernel must reproduce pandas' durations.groupby(values).sum() exactly."""

    def test_matches_pandas_groupby_random(self):
        rng = np.random.default_rng(1234)
        for trial in range(30):
            pieces = int(rng.integers(1, 5))
            valsList = []
            dursList = []
            for _ in range(pieces):
                n = int(rng.integers(1, 30))
                # Draw from a small value pool so cross-piece collisions actually occur.
                valsList.append(rng.choice(np.array([0.1, 0.5, 1.0, 2.5, 7.0]), size=n))
                dursList.append(rng.uniform(1.0, 100.0, size=n))
            values, durations = ts.durationWeightedDistribution(valsList, dursList)
            refSeries = pd.Series(np.concatenate(dursList)).groupby(
                pd.Series(np.concatenate(valsList))).sum()
            np.testing.assert_array_equal(values, refSeries.index.to_numpy())
            np.testing.assert_allclose(durations, refSeries.to_numpy(), rtol=1e-12)

    def test_empty(self):
        values, durations = ts.durationWeightedDistribution([], [])
        self.assertEqual(len(values), 0)
        values, durations = ts.durationWeightedDistribution([np.array([])], [np.array([])])
        self.assertEqual(len(values), 0)

    def test_values_strictly_ascending(self):
        values, _ = ts.durationWeightedDistribution([np.array([3.0, 1.0, 3.0, 2.0])],
                                                    [np.array([1.0, 1.0, 1.0, 1.0])])
        self.assertTrue(np.all(np.diff(values) > 0))


def makeSyntheticCache(rng, mcRuns=3, fugitiveMode='mixed'):
    """Build a small synthetic PDF-cache frame with fine (per-facility) and coarse
    (NaN-facility) levels, non-overlapping intervals per partition, and configurable FUGITIVE
    presence: 'mixed' (VENTED+FUGITIVE), 'none' (VENTED only), 'only' (FUGITIVE only)."""
    if fugitiveMode == 'mixed':
        cats = ['VENTED', 'FUGITIVE']
    elif fugitiveMode == 'none':
        cats = ['VENTED']
    else:
        cats = ['FUGITIVE']
    records = []
    for mcRun in range(mcRuns):
        for emCat in cats:
            for level, facility in (('modelReadableName', 'F1'), ('modelReadableName', 'F2'),
                                    ('siteTotals', None)):
                n = int(rng.integers(1, 6))
                edges = np.cumsum(rng.uniform(1.0, 50.0, size=n + 1))
                for i in range(n):
                    records.append({'facilityID': facility, 'site': 'S1', 'species': 'METHANE',
                                    'operator': 'OP', 'psno': 'P1',
                                    'METype': 'Sep' if level == 'modelReadableName' else '',
                                    'unitID': 'U1' if level == 'modelReadableName' else '',
                                    'modelReadableName': 'M1' if level == 'modelReadableName' else '',
                                    'modelEmissionCategory': emCat, 'mcRun': mcRun,
                                    'startTime_s': float(edges[i]), 'endTime_s': float(edges[i + 1]),
                                    'emission_kgPerH': float(rng.uniform(0.2, 9.0)),
                                    'cacheLevel': level})
    return pd.DataFrame.from_records(records)


SYNTH_GROUPINGS = [('siteTotals', ['site', 'species', 'operator', 'psno']),
                   ('modelReadableName', ['site', 'species', 'operator', 'psno',
                                          'METype', 'unitID', 'modelReadableName'])]


def canonPDF(df):
    """Canonical row order + fresh index for frame comparison (legacy concat carries
    duplicate per-group indexes; content is what must match)."""
    if df.empty:
        return df.reset_index(drop=True)
    return df.sort_values(list(df.columns)).reset_index(drop=True)


class TestPDFFromCacheEquivalence(unittest.TestCase):
    """The array-native calculatePDFSummaryFromCache must reproduce the retained legacy
    implementation row-for-row: values, probabilities, cumulative probabilities, columns."""

    def assertFramesMatch(self, newDF, oldDF):
        self.assertEqual(list(newDF.columns), list(oldDF.columns))
        self.assertEqual(len(newDF), len(oldDF))
        new = canonPDF(newDF)
        old = canonPDF(oldDF)
        for col in newDF.columns:
            if new[col].dtype.kind in ('f',):
                np.testing.assert_allclose(new[col].to_numpy(), old[col].to_numpy(),
                                           rtol=1e-12, atol=1e-15)
            else:
                # Union-filled identity columns legitimately hold NaN (a level that lacks the
                # column) on BOTH sides; object-array NaN != NaN, so compare via sentinel fill.
                np.testing.assert_array_equal(new[col].fillna('__nan__').to_numpy(),
                                              old[col].fillna('__nan__').to_numpy())

    def runEquivalence(self, fugitiveMode, seed):
        rng = np.random.default_rng(seed)
        cacheDF = makeSyntheticCache(rng, fugitiveMode=fugitiveMode)
        totalSimSecs = 1.0e4
        newFull, newNoFug, newStats = calculatePDFSummaryFromCache(
            cacheDF, totalSimSecs, groupings=SYNTH_GROUPINGS)
        oldFull, oldNoFug, oldStats = _calculatePDFSummaryFromCacheLegacy(
            cacheDF, totalSimSecs, groupings=SYNTH_GROUPINGS)
        self.assertFramesMatch(newFull, oldFull)
        self.assertFramesMatch(newNoFug, oldNoFug)
        # Stats: same groups and mcRunCounts (buildSeconds legitimately differs).
        newS = newStats.drop(columns=['buildSeconds'])
        oldS = oldStats.drop(columns=['buildSeconds'])
        self.assertTrue(canonPDF(newS).equals(canonPDF(oldS)))

    def test_mixed_categories(self):
        self.runEquivalence('mixed', 11)

    def test_no_fugitive_group(self):
        """Tier 1b: without FUGITIVE, the noFugitive output equals the full output."""
        self.runEquivalence('none', 22)
        rng = np.random.default_rng(22)
        cacheDF = makeSyntheticCache(rng, fugitiveMode='none')
        full, noFug, _ = calculatePDFSummaryFromCache(cacheDF, 1.0e4, groupings=SYNTH_GROUPINGS)
        self.assertTrue(canonPDF(full).equals(canonPDF(noFug)))

    def test_fugitive_only_group(self):
        """A FUGITIVE-only group contributes to full but NOT to noFugitive — on both paths."""
        self.runEquivalence('only', 33)
        rng = np.random.default_rng(33)
        cacheDF = makeSyntheticCache(rng, fugitiveMode='only')
        full, noFug, _ = calculatePDFSummaryFromCache(cacheDF, 1.0e4, groupings=SYNTH_GROUPINGS)
        self.assertFalse(full.empty)
        self.assertTrue(noFug.empty)

    def test_probability_normalizes_to_active_fraction(self):
        """The new path must preserve the probability semantics the legacy regression test
        guards for _makePDFRows: probabilities sum to activeSecs/totalSimSecs, not 1.0."""
        cacheDF = pd.DataFrame.from_records([
            {'facilityID': 'F1', 'site': 'S1', 'species': 'METHANE', 'operator': 'OP',
             'psno': 'P1', 'METype': '', 'unitID': '', 'modelReadableName': '',
             'modelEmissionCategory': 'VENTED', 'mcRun': 0,
             'startTime_s': 0.0, 'endTime_s': 10.0, 'emission_kgPerH': 5.0,
             'cacheLevel': 'siteTotals'}])
        full, _, _ = calculatePDFSummaryFromCache(
            cacheDF, 100.0, groupings=[('siteTotals', ['site', 'species', 'operator', 'psno'])])
        self.assertAlmostEqual(float(full['probability'].sum()), 10.0 / 100.0, places=12)
        self.assertAlmostEqual(float(full['cumulativeProbability'].iloc[-1]), 1.0, places=12)


class TestValidateCacheBoundary(unittest.TestCase):
    """The single-pass validation must catch what the per-sub-group RLE constructors caught."""

    @staticmethod
    def validCache():
        rng = np.random.default_rng(5)
        return makeSyntheticCache(rng)

    def test_valid_cache_passes(self):
        _validateCacheBoundary(self.validCache())

    def test_zero_duration_raises(self):
        cacheDF = self.validCache()
        cacheDF.loc[cacheDF.index[0], 'endTime_s'] = cacheDF.loc[cacheDF.index[0], 'startTime_s']
        with self.assertRaises(ts.MalformedTimeseriesError):
            _validateCacheBoundary(cacheDF)

    def test_overlap_within_partition_raises(self):
        cacheDF = self.validCache()
        # Make row 0 of one partition overlap the NEXT row of the same partition.
        firstPartition = cacheDF[(cacheDF['cacheLevel'] == 'modelReadableName')
                                 & (cacheDF['mcRun'] == 0)
                                 & (cacheDF['modelEmissionCategory'] == cacheDF['modelEmissionCategory'].iloc[0])
                                 & (cacheDF['facilityID'] == 'F1')]
        if len(firstPartition) < 2:
            self.skipTest("synthetic partition too small to overlap")
        idx0 = firstPartition.index[0]
        idx1 = firstPartition.index[1]
        cacheDF.loc[idx0, 'endTime_s'] = cacheDF.loc[idx1, 'startTime_s'] + 1.0
        with self.assertRaises(ts.MalformedTimeseriesError):
            _validateCacheBoundary(cacheDF)

    def test_adjacent_partitions_do_not_false_positive(self):
        """Rows of DIFFERENT partitions may 'overlap' in time freely (they are different
        distributions); validation must group correctly, including NaN facilityID coarse rows."""
        cacheDF = self.validCache()
        _validateCacheBoundary(cacheDF)   # mixed fine (F1/F2) + coarse (NaN) rows share times


if __name__ == '__main__':
    unittest.main()
