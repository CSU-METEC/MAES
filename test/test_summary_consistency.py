"""
Tests for SummaryConsistency.py — the directory-level Summary/ consistency checker.

Each test builds minimal synthetic DataFrames for one check, asserts a clean
fixture yields no violations, and an injected fault is flagged at the right
severity. The cross-level rollup test mirrors the issue #77 triple-count; the
PDF-vs-summary test confirms the #74 discrepancy is a warning, not a violation.
"""

import os
import sys

import numpy as np
import pandas as pd
import pytest

# SummaryConsistency lives in src/Testing/, which conftest does not add to the path.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'Testing'))

import Units as u
import SummaryConsistency as sc

T_SECS = 365.0 * u.SECONDS_PER_DAY


def _by_check(results):
    return {(r.dataset, r.check): r for r in results}


# ---------------------------------------------------------------------------
# InstEmissions structural
# ---------------------------------------------------------------------------

def _inst(rows):
    return pd.DataFrame(rows, columns=['timestamp_s', 'duration_s', 'emission_kgPerS', 'totalEmission_kg'])


def test_instemissions_clean():
    df = _inst([
        (0.0, 3600.0, 0.001, 0.001 * 3600.0),
        (1000.0, 5000.0, 0.002, 0.002 * 5000.0),
    ])
    res = _by_check(sc.check_instemissions(df, T_SECS))
    assert all(r.ok for r in res.values()), res


def test_instemissions_overrun_flagged():
    start = T_SECS - 10 * u.SECONDS_PER_DAY
    df = _inst([
        (0.0, 3600.0, 0.001, 0.001 * 3600.0),                  # interior
        (start, 110 * u.SECONDS_PER_DAY, 0.001,                 # overruns by 100 days
         0.001 * 110 * u.SECONDS_PER_DAY),
    ])
    res = _by_check(sc.check_instemissions(df, T_SECS))
    assert res[('InstEmissions', 'event_end_within_window')].count == 1
    assert res[('InstEmissions', 'event_end_within_window')].severity == 'violation'


def test_instemissions_negative_and_inconsistent_flagged():
    df = _inst([
        (-5.0, 3600.0, 0.001, 0.001 * 3600.0),       # negative timestamp (total consistent)
        (0.0, -10.0, 0.001, 0.001 * -10.0),          # negative duration (total consistent)
        (0.0, 3600.0, -0.001, -0.001 * 3600.0),      # negative emission (total consistent)
        (0.0, 3600.0, 0.001, 999.0),                 # totalEmission inconsistent (isolated)
    ])
    res = _by_check(sc.check_instemissions(df, T_SECS))
    assert res[('InstEmissions', 'timestamp_nonneg')].count == 1
    assert res[('InstEmissions', 'duration_nonneg')].count == 1
    assert res[('InstEmissions', 'emission_nonneg')].count == 1
    assert res[('InstEmissions', 'totalEmission_consistent')].count == 1


def test_instemissions_missing_duration_skips_window_check():
    df = _inst([(0.0, 3600.0, 0.001, 0.001 * 3600.0)])
    res = _by_check(sc.check_instemissions(df, None))
    r = res[('InstEmissions', 'event_end_within_window')]
    assert r.severity == 'warning' and r.count == 0 and 'skipped' in r.detail


# ---------------------------------------------------------------------------
# Summary bounds
# ---------------------------------------------------------------------------

def _summary(rows):
    return pd.DataFrame(rows, columns=['species', 'mean', 'max', 'lowerCI', 'upperCI'])


def test_summary_bounds_clean():
    df = _summary([('METHANE', 5.0, 10.0, 3.0, 8.0)])
    res = _by_check(sc.check_summary_bounds(df, 'SimSummary'))
    assert all(r.ok for r in res.values())


def test_summary_mean_gt_max_is_violation():
    df = _summary([('METHANE', 12.0, 10.0, 3.0, 8.0)])
    res = _by_check(sc.check_summary_bounds(df, 'SimSummary'))
    assert res[('SimSummary', 'mean_le_max')].count == 1
    assert res[('SimSummary', 'mean_le_max')].severity == 'violation'


def test_summary_ulp_mean_over_max_not_flagged():
    """Constant-rate emitter: mean = sum/N can land ~1 ULP above max from float64
    rounding (all 388 real JennaBug2 mean>max rows were this). Must not be a violation."""
    val = 2815.516539
    mean_ulp = np.nextafter(val, np.inf)   # one ULP above the exact max
    df = _summary([('METHANE', mean_ulp, val, val, val)])
    res = _by_check(sc.check_summary_bounds(df, 'SiteSummary'))
    assert res[('SiteSummary', 'mean_le_max')].count == 0
    assert res[('SiteSummary', 'lowerCI_le_mean')].count == 0
    assert res[('SiteSummary', 'mean_le_upperCI')].count == 0


def test_summary_real_mean_over_max_still_flagged():
    """A genuine mean>max (well beyond ULP) is still a violation."""
    df = _summary([('METHANE', 10.1, 10.0, 3.0, 8.0)])
    res = _by_check(sc.check_summary_bounds(df, 'SiteSummary'))
    assert res[('SiteSummary', 'mean_le_max')].count == 1


def test_summary_ci_disorder_is_warning_and_c2c1_excluded():
    df = _summary([
        ('METHANE', 5.0, 10.0, 6.0, 4.0),       # lowerCI>mean and mean>upperCI
        ('C2/C1', 1.0, np.nan, np.nan, np.nan),  # excluded by design
    ])
    res = _by_check(sc.check_summary_bounds(df, 'SimSummary'))
    assert res[('SimSummary', 'lowerCI_le_mean')].count == 1
    assert res[('SimSummary', 'mean_le_upperCI')].count == 1
    assert res[('SimSummary', 'lowerCI_le_mean')].severity == 'warning'
    assert res[('SimSummary', 'mean_le_max')].count == 0  # C2/C1 NaN not counted


# ---------------------------------------------------------------------------
# PDF structural
# ---------------------------------------------------------------------------

def _pdf(rate, prob, cum, **idcols):
    df = pd.DataFrame({'emissionRate_kgPerH': rate, 'probability': prob, 'cumulativeProbability': cum})
    for k, v in idcols.items():
        df[k] = v
    return df


def test_pdf_structural_clean():
    df = _pdf([0.1, 0.2, 0.3], [0.2, 0.3, 0.5], [0.2, 0.5, 1.0], CICategory='siteTotals', site='S1')
    res = _by_check(sc.check_pdf_structural(df, 'PDF'))
    assert all(r.ok for r in res.values())


def test_pdf_nonmonotone_cdf_flagged():
    df = _pdf([0.1, 0.2, 0.3], [0.2, 0.3, 0.5], [0.2, 0.1, 1.0], CICategory='siteTotals', site='S1')
    res = _by_check(sc.check_pdf_structural(df, 'PDF'))
    assert res[('PDF', 'cdf_monotone')].count == 1


def test_pdf_negative_probability_flagged():
    df = _pdf([0.1, 0.2], [-0.1, 0.5], [0.0, 0.5], CICategory='siteTotals', site='S1')
    res = _by_check(sc.check_pdf_structural(df, 'PDF'))
    assert res[('PDF', 'probability_nonneg')].count == 1


# ---------------------------------------------------------------------------
# Cross-level: SimSummary rollup (issue #77)
# ---------------------------------------------------------------------------

def _simrow(cic, mean, mec=None):
    return {'species': 'METHANE', 'units': 'kg/year', 'includeFugitive': True,
            'CICategory': cic, 'mean': mean, 'modelEmissionCategory': mec}


def _simsummary(sim_total=100.0, level_total=100.0):
    """simulation == sim_total; every other level (and COMBINED) sums to level_total.
    Balanced when the two are equal; #77 is sim_total = 3 * level_total."""
    return pd.DataFrame([
        _simrow('simulation', sim_total),
        _simrow('modelReadableName', 0.6 * level_total), _simrow('modelReadableName', 0.4 * level_total),
        _simrow('METype', 0.7 * level_total), _simrow('METype', 0.3 * level_total),
        _simrow('unitID', 0.55 * level_total), _simrow('unitID', 0.45 * level_total),
        _simrow('modelEmissionCategory', level_total, mec='COMBINED'),
        _simrow('modelEmissionCategory', 0.6 * level_total, mec='VENTED'),
        _simrow('modelEmissionCategory', 0.4 * level_total, mec='FUGITIVE'),
    ])


def test_simsummary_rollup_balanced():
    res = _by_check(sc.check_simsummary_rollup(_simsummary()))
    assert all(r.ok for r in res.values()), res


def test_simsummary_rollup_triple_count_flagged():
    """Mirror issue #77: simulation is 3x the (correct) level totals."""
    df = _simsummary(sim_total=300.0, level_total=100.0)
    res = _by_check(sc.check_simsummary_rollup(df))
    assert res[('SimSummary', 'simulation_eq_modelReadableName')].count == 1
    assert res[('SimSummary', 'simulation_eq_METype')].count == 1
    assert res[('SimSummary', 'simulation_eq_unitID')].count == 1
    assert res[('SimSummary', 'simulation_eq_modelEmissionCategory')].count == 1
    assert all(r.severity == 'violation' for r in res.values())


# ---------------------------------------------------------------------------
# Cross-level: PDF vs summary mean (issue #74) — warning, not violation
# ---------------------------------------------------------------------------

def _pdf_sitetotals(mean_kgh, site='S1'):
    # single delta at rate=mean_kgh with probability 1 → PDF mean == mean_kgh
    return _pdf([mean_kgh], [1.0], [1.0], CICategory='siteTotals', site=site,
                species='METHANE', includeFugitive=True)


def _sitesummary_combined(mean_kgyr, site='S1'):
    return pd.DataFrame([{
        'site': site, 'species': 'METHANE', 'includeFugitive': True,
        'CICategory': 'modelEmissionCategory', 'modelEmissionCategory': 'COMBINED',
        'units': 'kg/year', 'mean': mean_kgyr,
    }])


def test_pdf_vs_summary_match():
    mean_kgh = 0.5
    pdf = _pdf_sitetotals(mean_kgh)
    site = _sitesummary_combined(mean_kgh * u.HOURS_PER_YEAR)
    res = _by_check(sc.check_pdf_vs_summary_mean(pdf, site))
    r = res[('PDF', 'pdf_mean_eq_summary_mean')]
    assert r.count == 0 and r.severity == 'warning'


def test_pdf_vs_summary_mismatch_is_warning_not_violation():
    pdf = _pdf_sitetotals(0.45)                                   # 10% below
    site = _sitesummary_combined(0.5 * u.HOURS_PER_YEAR)
    res = _by_check(sc.check_pdf_vs_summary_mean(pdf, site))
    r = res[('PDF', 'pdf_mean_eq_summary_mean')]
    assert r.count == 1
    assert r.severity == 'warning'   # #74 is open/unresolved → never a hard violation


# ---------------------------------------------------------------------------
# Orchestration switches
# ---------------------------------------------------------------------------

def test_switches_select_tiers():
    dfs = {'SimSummary': _simsummary(sim_total=300.0, level_total=100.0)}  # has a cross-level fault
    # structural only → rollup fault not examined
    sres = sc.run_checks(dfs, structural=True, cross_level=False)
    assert all(r.severity != 'violation' or r.ok for r in sres)
    # cross-level on → fault surfaces
    xres = sc.run_checks(dfs, structural=False, cross_level=True)
    assert any((not r.ok) and r.severity == 'violation' for r in xres)
