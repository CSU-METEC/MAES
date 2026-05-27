"""
SummaryConsistency.py — directory-level consistency checks for a MAES Summary/ output.

Given a `Summary/` parquet directory (the output of the `summarize` / `simSummary`
phases), run a battery of cheap invariant checks and report violations. Unlike
`SummaryTest.py` (which compares new-vs-legacy output and needs a scenario config),
this operates on a directory alone and is unit-testable on synthetic DataFrames.

Two tiers, independently switchable:

* structural — per-dataset invariants that must hold for any correct output:
    - InstEmissions: timestamps/durations non-negative, events do not overrun the
      simulation window (issue #87), emissions non-negative, totalEmission_kg
      consistent with rate x duration.
    - SiteSummary / SimSummary: mean <= max (violation); lowerCI <= mean <= upperCI
      (warning — can be valid for heavily right-skewed distributions, matching
      SummaryTest.checkSimSummaryConsistency).
    - PDF / SimPDF: probability non-negative, CDF monotone non-decreasing and
      bounded by 1.

* cross-level — relationships between aggregation levels:
    - SimSummary rollup: the `simulation` total equals the sum over each of the
      modelReadableName / METype / unitID levels, and the modelEmissionCategory
      COMBINED row (issue #77). A hard violation.
    - PDF-vs-summary mean: per-site `siteTotals` PDF mean vs the SiteSummary
      site-total mean (issue #74). Reported as a WARNING, not a violation: #74 is
      an open, unresolved discrepancy, so this identity is not yet guaranteed to
      hold even on otherwise-correct output.

CLI:
    python SummaryConsistency.py <Summary dir> [--structural/--no-structural]
        [--cross-level/--no-cross-level] [--rtol 1e-6] [--warn-rtol 0.05]
Exit status is non-zero if any *violation* (not warning) is found.
"""

import argparse
import logging
import os
import sys
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

import Units as u

logger = logging.getLogger(__name__)

# Datasets we know how to read from a Summary/ directory.
PARTITIONED = {'InstEmissions', 'SiteSummary', 'PDF'}        # hive-partitioned by site
UNPARTITIONED = {'SimSummary', 'SimPDF'}                     # single dataset
ALL_DATASETS = sorted(PARTITIONED | UNPARTITIONED)

RATIO_SPECIES = 'C2/C1'      # min/max/CI are NaN by design; excluded from bounds checks
EMISSION_SPECIES = ('METHANE', 'ETHANE')
EMISSION_UNITS = ('kg/year', 'mt/year', 'US tons/year')

# value columns of a PDF/CDF row — everything else identifies the distribution
PDF_VALUE_COLS = ('emissionRate_kgPerH', 'probability', 'cumulativeProbability')


@dataclass
class CheckResult:
    check: str
    dataset: str
    severity: str          # 'violation' | 'warning'
    count: int             # number of offending rows / groups
    total: int             # population examined
    detail: str = ''

    @property
    def ok(self):
        return self.count == 0


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def load_summary_dir(path):
    """Read whichever known datasets are present under `path`. Returns a dict
    {datasetName: DataFrame}; absent datasets are simply omitted."""
    dfs = {}
    for name in ALL_DATASETS:
        ds_path = os.path.join(path, name)
        if os.path.isdir(ds_path):
            dfs[name] = pd.read_parquet(ds_path)
    return dfs


def sim_duration_secs(dfs):
    """Pull simDurationSecs from the simDurationDays column of whichever summary
    dataset carries it. Returns None if unavailable."""
    for name in ('SiteSummary', 'SimSummary'):
        df = dfs.get(name)
        if df is not None and 'simDurationDays' in df.columns:
            days = df['simDurationDays'].dropna().unique()
            if len(days):
                return float(days[0]) * u.SECONDS_PER_DAY
    return None


# ---------------------------------------------------------------------------
# Structural checks
# ---------------------------------------------------------------------------

def check_instemissions(ieDF, simDurationSecs, rtol=1e-6):
    results = []
    n = len(ieDF)

    def _v(check, mask, detail=''):
        results.append(CheckResult(check, 'InstEmissions', 'violation', int(mask.sum()), n, detail))

    _v('timestamp_nonneg', ieDF['timestamp_s'] < 0)
    _v('duration_nonneg', ieDF['duration_s'] < 0)
    _v('emission_nonneg', ieDF['emission_kgPerS'] < 0)

    if simDurationSecs is not None:
        end = ieDF['timestamp_s'] + ieDF['duration_s']
        over = end > simDurationSecs + 1e-6
        worst = (end.max() - simDurationSecs) / u.SECONDS_PER_DAY if len(ieDF) else 0.0
        _v('event_end_within_window', over,
           f'simDurationSecs={simDurationSecs:.0f}; worst overrun={worst:.1f} days' if over.any() else '')
    else:
        results.append(CheckResult('event_end_within_window', 'InstEmissions', 'warning', 0, n,
                                   'skipped — simDurationSecs unavailable'))

    expected = ieDF['emission_kgPerS'] * ieDF['duration_s']
    inconsistent = ~np.isclose(ieDF['totalEmission_kg'], expected, rtol=rtol, atol=1e-12)
    _v('totalEmission_consistent', inconsistent)
    return results


def check_summary_bounds(df, dataset):
    """mean <= max is a hard invariant (mean is the average of the readings, bounded
    by their max). CI-bound ordering is a warning — valid for skewed distributions
    with extreme MC outliers (see SummaryTest.checkSimSummaryConsistency)."""
    required = ['species', 'mean', 'max', 'lowerCI', 'upperCI']
    missing = [c for c in required if c not in df.columns]
    if missing:
        return [CheckResult('summary_bounds', dataset, 'warning', 0, len(df),
                            f'skipped — missing columns {missing}')]
    checkDF = df[df['species'] != RATIO_SPECIES]
    n = len(checkDF)
    results = [
        CheckResult('mean_le_max', dataset, 'violation',
                    int((checkDF['mean'] > checkDF['max']).sum()), n),
        CheckResult('lowerCI_le_mean', dataset, 'warning',
                    int((checkDF['lowerCI'] > checkDF['mean']).sum()), n),
        CheckResult('mean_le_upperCI', dataset, 'warning',
                    int((checkDF['mean'] > checkDF['upperCI']).sum()), n),
    ]
    return results


def check_pdf_structural(df, dataset, tol=1e-9):
    """probability >= 0, CDF monotone non-decreasing, CDF bounded by 1. These hold
    for both the per-site PDF (whose probabilities sum to <1, the missing mass being
    P(rate=0)) and the SimPDF mixture, so sum-to-1 is intentionally not asserted here."""
    n = len(df)
    neg_prob = int((df['probability'] < -tol).sum())

    idCols = [c for c in df.columns if c not in PDF_VALUE_COLS]
    nonmonotone = 0
    overshoot = 0
    groups = 0
    for _, g in df.groupby(idCols, dropna=False):
        groups += 1
        g = g.sort_values('emissionRate_kgPerH')
        cp = g['cumulativeProbability'].values
        if np.any(np.diff(cp) < -tol):
            nonmonotone += 1
        if cp.size and cp.max() > 1.0 + 1e-6:
            overshoot += 1

    return [
        CheckResult('probability_nonneg', dataset, 'violation', neg_prob, n),
        CheckResult('cdf_monotone', dataset, 'violation', nonmonotone, groups),
        CheckResult('cdf_bounded_by_1', dataset, 'violation', overshoot, groups),
    ]


# ---------------------------------------------------------------------------
# Cross-level checks
# ---------------------------------------------------------------------------

def check_simsummary_rollup(simDF, rtol=1e-6):
    """The simulation-wide total must equal the sum over each aggregation level
    (issue #77). For each (species, units, includeFugitive) emission combination,
    compare sum(mean) at the simulation level against modelReadableName / METype /
    unitID rollups and the modelEmissionCategory COMBINED row."""
    results = []
    df = simDF[(simDF['species'].isin(EMISSION_SPECIES)) & (simDF['units'].isin(EMISSION_UNITS))]
    keys = ['species', 'units', 'includeFugitive']

    def level_total(sub, cic):
        return sub[sub['CICategory'] == cic]['mean'].sum()

    mismatches = {lvl: 0 for lvl in ('modelReadableName', 'METype', 'unitID', 'modelEmissionCategory')}
    examples = []
    combos = 0
    for keyvals, sub in df.groupby(keys, dropna=False):
        sim = level_total(sub, 'simulation')
        if sim == 0:
            continue
        combos += 1
        ref = max(abs(sim), 1e-12)
        for lvl in ('modelReadableName', 'METype', 'unitID'):
            lt = level_total(sub, lvl)
            if abs(lt - sim) > rtol * ref:
                mismatches[lvl] += 1
                if len(examples) < 5:
                    examples.append(f'{dict(zip(keys, keyvals if isinstance(keyvals, tuple) else (keyvals,)))}: '
                                    f'sim={sim:.4f} {lvl}={lt:.4f}')
        # modelEmissionCategory: the COMBINED row is the all-categories total
        mec = sub[(sub['CICategory'] == 'modelEmissionCategory') & (sub.get('modelEmissionCategory') == 'COMBINED')]
        combined = mec['mean'].sum()
        if abs(combined - sim) > rtol * ref:
            mismatches['modelEmissionCategory'] += 1

    for lvl, cnt in mismatches.items():
        results.append(CheckResult(f'simulation_eq_{lvl}', 'SimSummary', 'violation', cnt, combos,
                                   '; '.join(examples) if cnt and lvl == 'modelReadableName' else ''))
    return results


def check_pdf_vs_summary_mean(pdfDF, siteDF, warn_rtol=0.05):
    """Per-site siteTotals PDF mean vs SiteSummary site-total (COMBINED) mean
    (issue #74). WARNING only — #74 is an open, unresolved discrepancy, so this
    identity is not yet a guaranteed invariant."""
    if 'includeFugitive' not in pdfDF.columns or 'site' not in pdfDF.columns:
        return [CheckResult('pdf_mean_eq_summary_mean', 'PDF', 'warning', 0, 0,
                            'skipped — PDF lacks site/includeFugitive columns')]
    pdf = pdfDF[pdfDF['CICategory'] == 'siteTotals']
    keys = ['site', 'species', 'includeFugitive']
    mismatches = 0
    compared = 0
    examples = []
    for keyvals, g in pdf.groupby(keys, dropna=False):
        site, species, fug = keyvals
        pdf_mean = (g['emissionRate_kgPerH'] * g['probability']).sum()  # kg/h
        m = ((siteDF['site'] == site) & (siteDF['species'] == species)
             & (siteDF['includeFugitive'] == fug)
             & (siteDF['CICategory'] == 'modelEmissionCategory')
             & (siteDF['modelEmissionCategory'] == 'COMBINED')
             & (siteDF['units'] == 'kg/year'))
        summary_kgyr = siteDF[m]['mean'].sum()
        if summary_kgyr == 0:
            continue
        summary_kgh = summary_kgyr / u.HOURS_PER_YEAR
        compared += 1
        rel = abs(pdf_mean - summary_kgh) / max(abs(summary_kgh), 1e-12)
        if rel > warn_rtol:
            mismatches += 1
            if len(examples) < 5:
                examples.append(f'{site}/{species}/fug={fug}: pdf={pdf_mean:.4f} summary={summary_kgh:.4f} '
                                f'({100*rel:.1f}%)')
    return [CheckResult('pdf_mean_eq_summary_mean', 'PDF', 'warning', mismatches, compared,
                        '; '.join(examples))]


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_checks(dfs, structural=True, cross_level=True, rtol=1e-6, warn_rtol=0.05):
    results = []
    T = sim_duration_secs(dfs)

    if structural:
        if 'InstEmissions' in dfs:
            results += check_instemissions(dfs['InstEmissions'], T, rtol=rtol)
        for name in ('SiteSummary', 'SimSummary'):
            if name in dfs:
                results += check_summary_bounds(dfs[name], name)
        for name in ('PDF', 'SimPDF'):
            if name in dfs:
                results += check_pdf_structural(dfs[name], name)

    if cross_level:
        if 'SimSummary' in dfs:
            results += check_simsummary_rollup(dfs['SimSummary'], rtol=rtol)
        if 'PDF' in dfs and 'SiteSummary' in dfs:
            results += check_pdf_vs_summary_mean(dfs['PDF'], dfs['SiteSummary'], warn_rtol=warn_rtol)

    return results


def check_summary_dir(path, **kwargs):
    return run_checks(load_summary_dir(path), **kwargs)


def format_results(results):
    lines = []
    for r in results:
        status = 'OK' if r.ok else ('VIOLATION' if r.severity == 'violation' else 'WARNING')
        line = f'  [{status:9}] {r.dataset}/{r.check}: {r.count}/{r.total}'
        if r.detail:
            line += f'  ({r.detail})'
        lines.append(line)
    return '\n'.join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description='Consistency checks for a MAES Summary/ parquet directory.')
    parser.add_argument('path', help='Path to the Summary/ directory')
    parser.add_argument('--structural', action=argparse.BooleanOptionalAction, default=True,
                        help='Run per-dataset structural invariant checks (default: on)')
    parser.add_argument('--cross-level', dest='cross_level', action=argparse.BooleanOptionalAction, default=True,
                        help='Run cross-level rollup / PDF-vs-summary checks (default: on)')
    parser.add_argument('--rtol', type=float, default=1e-6, help='Relative tolerance for hard invariants')
    parser.add_argument('--warn-rtol', type=float, default=0.05,
                        help='Relative tolerance for the #74 PDF-vs-summary warning')
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format='%(message)s')
    results = check_summary_dir(args.path, structural=args.structural, cross_level=args.cross_level,
                                rtol=args.rtol, warn_rtol=args.warn_rtol)
    if not results:
        logging.info(f'No known datasets found under {args.path}')
        return 0

    logging.info(f'Consistency checks for {args.path}:')
    logging.info(format_results(results))

    violations = sum(r.count for r in results if r.severity == 'violation')
    warnings = sum(r.count for r in results if r.severity == 'warning')
    logging.info(f'\n{violations} violation(s), {warnings} warning(s).')
    return 1 if violations else 0


if __name__ == '__main__':
    sys.exit(main())
