"""
Tests for the SimSummary mean denominator fix (GitHub issue #33, Bug 2).

The bug: _filterAndPivot() computed mean = sum(readings) / len(readings).
For low-prevalence sources that fire in only k of N MC runs, len(readings) == k
rather than N, inflating the mean by N/k.

The fix: mean = sum(readings) / mcIterations, treating non-firing runs as zero.

Each test constructs a minimal DataFrame matching the structure that
calculateEmissionSummary() produces, then asserts that the mean in the
_filterAndPivot() output equals total / mcIterations.
"""

import pandas as pd
import numpy as np
import pytest

from Summaries2 import _filterAndPivot

MC_ITERATIONS = 100
SPECIES = 'CH4'
UNITS = 'mt/year'
CI_CATEGORY = 'instantEmissionsByModelReadableName'
PIVOT_FIELD = 'modelReadableName'


def _make_row(model_name, readings, include_fugitive=True):
    """Build one summary row as _filterAndPivot expects it."""
    return {
        'species': SPECIES,
        'units': UNITS,
        'includeFugitive': include_fugitive,
        'CICategory': CI_CATEGORY,
        'modelReadableName': model_name,
        'readings': readings,
    }


def _run(rows):
    df = pd.DataFrame(rows)
    result = _filterAndPivot(df, CI_CATEGORY, MC_ITERATIONS, pivotField=PIVOT_FIELD)
    return result.set_index('modelReadableName')


# ---------------------------------------------------------------------------
# Test 1 — Always-firing source (sanity check; fix must not change this)
# ---------------------------------------------------------------------------

def test_always_firing_source_mean_unchanged():
    """A source that fires in every MC run should have mean = total / N, same as before."""
    readings = [2.0] * MC_ITERATIONS  # fires once per run, same value each time
    result = _run([_make_row('SourceA', readings)])

    expected_mean = sum(readings) / MC_ITERATIONS  # 2.0
    actual_mean = result.loc['SourceA', 'mean']

    assert abs(actual_mean - expected_mean) < 1e-9, (
        f"Always-firing source: expected mean {expected_mean}, got {actual_mean}"
    )


# ---------------------------------------------------------------------------
# Test 2 — Single-firing source (the core bug case)
# ---------------------------------------------------------------------------

def test_single_firing_source_mean_divided_by_mc_iterations():
    """A source that fires in exactly 1 of N runs must have mean = value / N, not value."""
    single_reading = 262.4
    result = _run([_make_row('LargeEmitter', [single_reading])])

    expected_mean = single_reading / MC_ITERATIONS   # ~2.624
    inflated_mean = single_reading                   # what the old code produced
    actual_mean = result.loc['LargeEmitter', 'mean']

    assert abs(actual_mean - expected_mean) < 1e-9, (
        f"Single-firing source: expected mean {expected_mean} (total/N), "
        f"got {actual_mean} (old inflated value would be {inflated_mean})"
    )


# ---------------------------------------------------------------------------
# Test 3 — Partial-firing source (fires in k of N runs)
# ---------------------------------------------------------------------------

def test_partial_firing_source_mean_divided_by_mc_iterations():
    """A source firing in k of N runs must have mean = sum(readings) / N, not sum / k."""
    k = 10
    readings = [5.0] * k  # fires in 10 of 100 runs
    result = _run([_make_row('PartialSource', readings)])

    total = sum(readings)
    expected_mean = total / MC_ITERATIONS   # 0.5  (5.0 * 10 / 100)
    old_mean = total / k                   # 5.0  (what old code would give)
    actual_mean = result.loc['PartialSource', 'mean']

    assert abs(actual_mean - expected_mean) < 1e-9, (
        f"Partial-firing source (k={k}): expected mean {expected_mean}, "
        f"got {actual_mean} (old code would give {old_mean})"
    )


# ---------------------------------------------------------------------------
# Test 4 — Multiple sources: always-firing and rarely-firing in same DataFrame
# ---------------------------------------------------------------------------

def test_mixed_prevalence_sources_computed_independently():
    """
    Always-firing and rarely-firing sources in the same DataFrame.
    Each must use mcIterations as the denominator regardless of the other.
    """
    always_readings = [3.0] * MC_ITERATIONS
    rare_readings = [300.0]  # fires once

    result = _run([
        _make_row('AlwaysSource', always_readings),
        _make_row('RareSource', rare_readings),
    ])

    expected_always = sum(always_readings) / MC_ITERATIONS   # 3.0
    expected_rare = sum(rare_readings) / MC_ITERATIONS       # 3.0

    actual_always = result.loc['AlwaysSource', 'mean']
    actual_rare = result.loc['RareSource', 'mean']

    assert abs(actual_always - expected_always) < 1e-9, (
        f"AlwaysSource mean: expected {expected_always}, got {actual_always}"
    )
    assert abs(actual_rare - expected_rare) < 1e-9, (
        f"RareSource mean: expected {expected_rare}, got {actual_rare}"
    )


# ---------------------------------------------------------------------------
# Test 5 — Multi-site aggregation with a low-prevalence source
# ---------------------------------------------------------------------------

def test_multi_site_low_prevalence_mean():
    """
    Two sites contribute to the same source group.
    Site A fires in every run; Site B fires in only 1 run.
    The cross-site mean must still divide by mcIterations.
    """
    site_a_readings = [1.0] * MC_ITERATIONS   # fires every run
    site_b_readings = [99.0]                  # fires in 1 run only

    # After _filterAndPivot sums across sites per mcIdx:
    # - mcIdx 0: site_a[0] + site_b[0] = 1.0 + 99.0 = 100.0
    # - mcIdx 1..99: site_a[i] = 1.0 each (site_b has no entry)
    # runTotals = [100.0, 1.0, 1.0, ..., 1.0]  (100 entries)
    # expected mean = (100.0 + 99 * 1.0) / 100 = 199.0 / 100 = 1.99
    expected_total = 100.0 + 99 * 1.0
    expected_mean = expected_total / MC_ITERATIONS

    result = _run([
        _make_row('SharedSource', site_a_readings),
        _make_row('SharedSource', site_b_readings),
    ])

    actual_mean = result.loc['SharedSource', 'mean']

    assert abs(actual_mean - expected_mean) < 1e-9, (
        f"Multi-site low-prevalence: expected mean {expected_mean}, got {actual_mean}"
    )
