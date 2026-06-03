"""
Tests for GitHub issue #101 — simulation-level summary must aggregate ALL sites
and be written to a deterministic, site-independent job-level location.

The bug: the single ``simSummary`` workitem inherited whatever site's config the
config manager was left on after the file loop (the last study). As a result
``summarizeSimulation`` / ``createSimPDF`` read only that one site's
``SiteSummary`` / ``PDF`` (``SimPDF mixture: 1 sites``) and wrote the sim-level
datasets under that arbitrary site's directory.

The fix threads every site's per-site dataset directory onto the workitem
(``allSiteSummaryDirs`` / ``allSitePDFDirs``) and points the SimSummary/SimPDF
write paths at a job-level ``simulationParquetDir``.
"""

import json
import os

import numpy as np
import pandas as pd

from Summaries2 import (
    _readSummaryAcrossSites,
    summarizeSimulation,
    createSimPDF,
)

MC_ITERATIONS = 2
CONFIG_PATH = os.path.join(os.path.dirname(__file__), '..', 'config', 'defaultConfig.json')


# ---------------------------------------------------------------------------
# Fixture builders — write per-site, hive-partitioned (by site) parquet dirs.
# ---------------------------------------------------------------------------

def _write_site_summary(base_dir, site, reading):
    """One SiteSummary 'COMBINED' modelEmissionCategory row for a site, with a
    constant per-MC-run ``reading``. Returns the directory written."""
    df = pd.DataFrame([{
        'species': 'METHANE',
        'units': 'kg/year',
        'includeFugitive': False,
        'CICategory': 'modelEmissionCategory',
        'modelEmissionCategory': 'COMBINED',
        'modelReadableName': 'src',
        'unitID': 'u1',
        'METype': 'mt1',
        'pneumatic': 'none',
        'readings': [float(reading)] * MC_ITERATIONS,
        'mean': float(reading),
        'site': site,
    }])
    out = os.path.join(base_dir, site, 'SiteSummary')
    df.to_parquet(out, partition_cols=['site'], index=False)
    return out


def _write_site_pdf(base_dir, site, rate):
    """One siteTotals PDF row (one component) for a site at emission ``rate``."""
    df = pd.DataFrame([{
        'species': 'METHANE',
        'includeFugitive': False,
        'CICategory': 'siteTotals',
        'site': site,
        'operator': 'op',
        'psno': 'ps',
        'emissionRate_kgPerH': float(rate),
        'probability': 1.0,
    }])
    out = os.path.join(base_dir, site, 'PDF')
    df.to_parquet(out, partition_cols=['site'], index=False)
    return out


# ---------------------------------------------------------------------------
# 1. The read helper aggregates across every site and preserves `site`.
# ---------------------------------------------------------------------------

def test_read_summary_across_sites_aggregates_all_sites(tmp_path):
    base = str(tmp_path / 'out')
    dir_a = _write_site_summary(base, 'siteA', 2.0)
    dir_b = _write_site_summary(base, 'siteB', 3.0)

    config = {'allSiteSummaryDirs': [dir_a, dir_b]}
    result = _readSummaryAcrossSites(config, 'allSiteSummaryDirs', 'parquetNewSummary')

    assert set(result['site']) == {'siteA', 'siteB'}, "both sites must be read"
    assert len(result) == 2


def test_read_summary_across_sites_falls_back_to_single_path(tmp_path):
    """With no threaded list, the helper reads the single configured path
    (single-study runs / direct callers)."""
    base = str(tmp_path / 'out')
    dir_b = _write_site_summary(base, 'siteB', 3.0)

    config = {'parquetNewSummary': dir_b}  # no allSiteSummaryDirs
    result = _readSummaryAcrossSites(config, 'allSiteSummaryDirs', 'parquetNewSummary')

    assert set(result['site']) == {'siteB'}


# ---------------------------------------------------------------------------
# 2. summarizeSimulation: the 'simulation' total sums across ALL sites.
# ---------------------------------------------------------------------------

def test_summarize_simulation_totals_span_all_sites(tmp_path):
    base = str(tmp_path / 'out')
    summary_a = _write_site_summary(base, 'siteA', 2.0)
    summary_b = _write_site_summary(base, 'siteB', 3.0)
    pdf_a = _write_site_pdf(base, 'siteA', 10.0)
    pdf_b = _write_site_pdf(base, 'siteB', 20.0)

    sim_dir = str(tmp_path / 'job' / 'SimSummary')
    config = {
        'allSiteSummaryDirs': [summary_a, summary_b],
        'allSitePDFDirs': [pdf_a, pdf_b],
        # fallback single paths point at the LAST site, mirroring the old bug:
        'parquetNewSummary': summary_b,
        'parquetNewPDF': pdf_b,
        'parquetNewSimSummary': sim_dir,
        'parquetNewSimPDF': str(tmp_path / 'job' / 'SimPDF'),
        'monteCarloIterations': MC_ITERATIONS,
        'simDurationDays': 365,
    }

    summarizeSimulation(config)

    sim = pd.read_parquet(sim_dir)
    simRows = sim[sim['CICategory'] == 'simulation']
    assert not simRows.empty, "simulation rollup rows must be written"

    # siteA=2 + siteB=3 per run → cross-site run total 5; mean = 5 (sum/N).
    # The pre-fix single-(last-)site value would have been 3.
    total = simRows['total'].sum()
    mean = simRows['mean'].sum()
    assert abs(mean - 5.0) < 1e-9, f"simulation mean must span both sites (got {mean}, last-site-only would be 3.0)"
    assert abs(total - 10.0) < 1e-9, f"simulation total must span both sites (got {total})"


# ---------------------------------------------------------------------------
# 3. createSimPDF mixes every site (was 'SimPDF mixture: 1 sites').
# ---------------------------------------------------------------------------

def test_create_simpdf_mixes_all_sites(tmp_path):
    base = str(tmp_path / 'out')
    pdf_a = _write_site_pdf(base, 'siteA', 10.0)
    pdf_b = _write_site_pdf(base, 'siteB', 20.0)

    sim_pdf_dir = str(tmp_path / 'job' / 'SimPDF')
    config = {
        'allSitePDFDirs': [pdf_a, pdf_b],
        'parquetNewPDF': pdf_b,           # last-site fallback (old bug source)
        'parquetNewSimPDF': sim_pdf_dir,
    }

    createSimPDF(config)

    simpdf = pd.read_parquet(sim_pdf_dir)
    rates = set(simpdf['emissionRate_kgPerH'].tolist())
    assert rates == {10.0, 20.0}, f"SimPDF must mix both sites' rates, got {rates}"
    # Two equal components → each scaled to 0.5; CDF reaches 1.0.
    assert abs(simpdf['probability'].sum() - 1.0) < 1e-9
    assert abs(simpdf['cumulativeProbability'].max() - 1.0) < 1e-9


# ---------------------------------------------------------------------------
# 4. Write location is deterministic & job-level (no per-site component).
# ---------------------------------------------------------------------------

def test_simsummary_write_path_is_job_level():
    with open(CONFIG_PATH) as f:
        cfg = json.load(f)
    sim_phase = cfg['phaseValues']['simulation']

    assert sim_phase['parquetNewSimSummary'] == '{simulationParquetDir}/Summary/SimSummary'
    assert sim_phase['parquetNewSimPDF'] == '{simulationParquetDir}/Summary/SimPDF'

    # The job-level dir resolves from job-wide vars ONLY — referencing the
    # per-site {parquetDir}/{studyName}/{site} would make format_map raise here.
    resolved = sim_phase['simulationParquetDir'].format_map({
        'outputRoot': '/out',
        'scenarioTimestamp': 'TS',
    })
    assert resolved == '/out/MC_TS/parquet'

    # SiteSummary is job-level by design since the -dr Summary consolidation
    # (#103): every parquetNew* summary key resolves through summaryParquetDir.
    assert sim_phase['parquetNewSummary'] == '{summaryParquetDir}/Summary/SiteSummary'
