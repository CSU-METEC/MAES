"""
Tests for the _buildEventSummaryLevel zero-duration guard (GitHub issue #79).

The bug: when an event-emitter group accumulates a total duration of zero across
all MC iterations (e.g. instantaneous events, or transient events with realisations
of zero observed duration), the rate calculation in the storeArrays=True branch
divided 0/0 and raised ZeroDivisionError, killing the whole job at the final
summary roll-up — even though all MC runs themselves completed cleanly.

The fix: guard the rate computation symmetrically with the storeArrays=False
sibling 16 lines below, which already returned 0.0 in this case.
"""

import pandas as pd
import pytest

from Summaries2 import _buildEventSummaryLevel, EVENT_EMITTER_GROUP_COLS

MC_ITERATIONS = 5
GROUP_KEY = ('siteA', 'CH4', 'Op', 'PSN1', 'unit1', 'modelX')


def _acc_store_arrays(duration_s, emission_kgPerS, totalEmission_kg):
    """Shape matching _accumulateEventData output when storeArrays=True."""
    return {
        GROUP_KEY: {
            'duration_s': duration_s,
            'emission_kgPerS': emission_kgPerS,
            'totalEmission_kg': totalEmission_kg,
        }
    }


def _acc_slim(n, sum_duration_s, sum_emission_kg, sum_emission_kgPerS):
    """Shape matching _accumulateEventData output when storeArrays=False."""
    return {
        GROUP_KEY: {
            'n': n,
            'sum_duration_s': sum_duration_s,
            'sum_emission_kg': sum_emission_kg,
            'sum_emission_kgPerS': sum_emission_kgPerS,
        }
    }


# ---------------------------------------------------------------------------
# Zero-duration guards
# ---------------------------------------------------------------------------

def test_store_arrays_true_zero_duration_does_not_crash():
    """storeArrays=True path: a group with summed duration == 0 must return a
    DataFrame with meanEmissionRate == 0.0 rather than raising ZeroDivisionError."""
    acc = _acc_store_arrays(
        duration_s=[0.0],
        emission_kgPerS=[0.0],
        totalEmission_kg=[0.0],
    )

    result = _buildEventSummaryLevel(acc, EVENT_EMITTER_GROUP_COLS, MC_ITERATIONS, storeArrays=True)

    assert not result.empty, "expected one row per (kg/s, kg/h) variant, got empty DataFrame"
    assert (result['meanEmissionRate'] == 0.0).all(), (
        f"expected meanEmissionRate == 0.0 for both kg/s and kg/h rows, "
        f"got {result['meanEmissionRate'].tolist()}"
    )


def test_store_arrays_false_zero_duration_does_not_crash():
    """storeArrays=False path: pre-existing guard; this test pins the behavior
    so a future refactor cannot silently regress the lean path."""
    acc = _acc_slim(n=1, sum_duration_s=0.0, sum_emission_kg=0.0, sum_emission_kgPerS=0.0)

    result = _buildEventSummaryLevel(acc, EVENT_EMITTER_GROUP_COLS, MC_ITERATIONS, storeArrays=False)

    assert not result.empty
    assert (result['meanEmissionRate'] == 0.0).all()


# ---------------------------------------------------------------------------
# Non-zero duration sanity (fix must not change normal behavior)
# ---------------------------------------------------------------------------

def test_store_arrays_true_non_zero_duration_unchanged():
    """A normal group with non-zero duration must compute meanEmissionRate as
    totalEmission / totalDuration in kg/s and × SECONDS_PER_HOUR in kg/h."""
    acc = _acc_store_arrays(
        duration_s=[100.0, 200.0],          # totalDuration = 300 s
        emission_kgPerS=[0.5, 0.5],
        totalEmission_kg=[50.0, 100.0],      # totalEmission = 150 kg
    )

    result = _buildEventSummaryLevel(acc, EVENT_EMITTER_GROUP_COLS, MC_ITERATIONS, storeArrays=True)

    kg_per_s_row = result[result['emissionRateUnits'] == 'kg/s'].iloc[0]
    expected_rate_kgPerS = 150.0 / 300.0   # 0.5
    assert abs(kg_per_s_row['meanEmissionRate'] - expected_rate_kgPerS) < 1e-9, (
        f"expected meanEmissionRate {expected_rate_kgPerS} kg/s, "
        f"got {kg_per_s_row['meanEmissionRate']}"
    )
