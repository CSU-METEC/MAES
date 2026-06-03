"""
Regression test for the simulation-level triple-count (GitHub issue #77).

The bug: summarizeSimulation built the 'simulation' CICategory rows with
    _filterAndPivot(nonRatioDF, 'modelEmissionCategory', mcIterations, pivotField='simulation')
But 'modelEmissionCategory' is the CICategory tag on THREE distinct full-total
aggregation levels emitted by calculateAnnualSummaries:
  1. the per-category detail (one row per modelEmissionCategory),
  2. the category-dropped rollup (modelEmissionCategory = NaN),
  3. the COMBINED total row (modelEmissionCategory = 'COMBINED').
Each level independently sums to the full per-site total. With
pivotField='simulation' the group key omits the category column, so none are
filtered out and all three are summed -> exactly 3x the true total.

Reported: sum(mean) for CICategory=='simulation' was 3x the sum for
CICategory=='modelReadableName' (212.79 vs 70.93) across 8..480 sites.

The fix: restrict to the COMBINED per-site totals before the simulation rollup.
This test reproduces calculateAnnualSummaries output for a multi-site,
multi-category run and asserts the simulation total equals the modelReadableName
total (i.e. the true total, counted once) on both includeFugitive paths.
"""

import numpy as np
import pandas as pd

from Summaries2 import calculateAnnualSummaries, _filterAndPivot, _aggregateEmittersByRun

MC_ITERATIONS = 5
SIM_DURATION_DAYS = 365.0  # annualization is identity, so totals are exact
UNITS = 'kg/year'

# emitterID, METype, unitID, modelReadableName, modelEmissionCategory, kg_per_run
EMITTER_DEFS = [
    ('e_comp', 'Compressor', 'U1', 'CompressorSeal', 'COMBUSTION', 10.0),
    ('e_leak', 'Tank',       'U2', 'TankLeak',       'FUGITIVE',    4.0),
]
SITES = ['S1', 'S2', 'S3']

# Per-site total = 14.0; fugitive component = 4.0, non-fugitive = 10.0.
TRUE_TOTAL_FULL = (10.0 + 4.0) * len(SITES)   # 42.0
TRUE_TOTAL_NOFUG = 10.0 * len(SITES)          # 30.0

AGG_FIELDS = {
    'total': ('emissions_kgPerYear', 'sum'),
    'count': ('emissions_kgPerYear', 'count'),
    'mean': ('emissions_kgPerYear', 'mean'),
    'min': ('emissions_kgPerYear', 'min'),
    'max': ('emissions_kgPerYear', 'max'),
    'lowerQuartile': ('emissions_kgPerYear', lambda x: np.percentile(x, 25)),
    'upperQuartile': ('emissions_kgPerYear', lambda x: np.percentile(x, 75)),
    'lowerCI': ('emissions_kgPerYear', lambda x: np.percentile(x, 2.5)),
    'upperCI': ('emissions_kgPerYear', lambda x: np.percentile(x, 97.5)),
    'readings': ('emissions_kgPerYear', list),
}


def _make_inst_emissions():
    rows = []
    for site in SITES:
        for mc in range(MC_ITERATIONS):
            for emitterID, metype, unit, mrn, mec, kg in EMITTER_DEFS:
                rows.append(dict(
                    site=site, mcRun=mc, species='METHANE',
                    emitterID=f'{site}_{emitterID}', operator='op', psno='ps',
                    METype=metype, unitID=unit, modelReadableName=mrn,
                    modelEmissionCategory=mec, totalEmission_kg=kg,
                ))
    return pd.DataFrame(rows)


def _site_summary():
    """Mirror summarizeSingleSite: run calculateAnnualSummaries on both the full
    and no-fugitive emissions and stamp includeFugitive, as the engine does."""
    inst = _make_inst_emissions()
    full = calculateAnnualSummaries(_aggregateEmittersByRun(inst, SIM_DURATION_DAYS), AGG_FIELDS, MC_ITERATIONS)
    full = full.assign(includeFugitive=True)
    nofug_inst = inst[inst['modelEmissionCategory'] != 'FUGITIVE']
    nofug = calculateAnnualSummaries(_aggregateEmittersByRun(nofug_inst, SIM_DURATION_DAYS), AGG_FIELDS, MC_ITERATIONS)
    nofug = nofug.assign(includeFugitive=False)
    ss = pd.concat([full, nofug]).assign(units=UNITS, species='METHANE')
    return ss[ss['species'] != 'C2/C1']


def _sum_mean(df, include_fugitive):
    mask = (df['units'] == UNITS) & (df['includeFugitive'] == include_fugitive)
    return df[mask]['mean'].sum()


def test_old_approach_triple_counts():
    """Documents the bug: the pre-fix simulation rollup (all modelEmissionCategory
    rows, pivot=simulation) sums three full-total layers -> 3x the true total."""
    nonRatioDF = _site_summary()
    buggy = (_filterAndPivot(nonRatioDF, 'modelEmissionCategory', MC_ITERATIONS, pivotField='simulation')
             .assign(CICategory='simulation'))
    assert abs(_sum_mean(buggy, True) - 3 * TRUE_TOTAL_FULL) < 1e-9
    assert abs(_sum_mean(buggy, False) - 3 * TRUE_TOTAL_NOFUG) < 1e-9


def test_simulation_level_counts_emissions_once():
    """The fixed simulation rollup (COMBINED only) equals the true total on both
    includeFugitive paths."""
    nonRatioDF = _site_summary()
    combined = nonRatioDF[nonRatioDF['modelEmissionCategory'] == 'COMBINED']
    sim = (_filterAndPivot(combined, 'modelEmissionCategory', MC_ITERATIONS, pivotField='simulation')
           .assign(CICategory='simulation'))
    assert abs(_sum_mean(sim, True) - TRUE_TOTAL_FULL) < 1e-9
    assert abs(_sum_mean(sim, False) - TRUE_TOTAL_NOFUG) < 1e-9


def test_simulation_matches_model_readable_name():
    """The reported symptom: simulation total must equal the modelReadableName
    total (they are two views of the same emissions, counted once)."""
    nonRatioDF = _site_summary()
    combined = nonRatioDF[nonRatioDF['modelEmissionCategory'] == 'COMBINED']
    sim = _filterAndPivot(combined, 'modelEmissionCategory', MC_ITERATIONS, pivotField='simulation')
    mrn = _filterAndPivot(nonRatioDF, 'modelReadableName', MC_ITERATIONS)
    for include_fugitive in (True, False):
        assert abs(_sum_mean(sim, include_fugitive) - _sum_mean(mrn, include_fugitive)) < 1e-9
