"""Tests for GitHub issue CSU-METEC/MAES#114 — SimSummary/SimPDF over-count by site count.

The bug: ``generateWorkitems`` builds ``allSiteSummaryDirs`` / ``allSitePDFDirs`` by
appending ``parquetNewSummary`` / ``parquetNewPDF`` once **per site**. Those paths carry
no ``{site}`` component (``parquetDir`` is keyed by ``studyName`` + ``scenarioTimestamp``,
constant across the multi-site loop) — the per-site rows live inside one shared dataset,
hive-partitioned by ``site``. So each list ends up as N **identical** paths, and
``_readSummaryAcrossSites`` reads the one shared all-sites dataset N times and concatenates
→ every ``SimSummary`` / ``SimPDF`` level is inflated by exactly the site count.

Reproduced (5-site run): ``SimSummary`` modelReadableName total = 5.0x the true cross-site
total; ``SimPDF`` mixture probability sums to 5.0 instead of 1.0.

Why the #101 tests missed it: their fixtures wrote each site to a *distinct* directory
(``base/<site>/SiteSummary``) and built the dir lists from those, a layout the engine never
produces. These tests drive the real ``generateWorkitems`` construction against the real
``defaultConfig`` path templates instead.

The fix: in ``generateWorkitems``, add each distinct dataset directory to the list exactly
once (the shared dataset is read once; ``site`` is recovered from the hive partition).
"""

import json
import os

import pytest

import SiteMain2
from ConfigManager import ConfigManager
import pandas as pd
from Summaries2 import summarizeSimulation

CONFIG_PATH = os.path.join(os.path.dirname(__file__), '..', 'config', 'defaultConfig.json')


def _setupConfigManager(tmpOutputRoot):
    """Initialize the ConfigManager singleton from the real defaultConfig and expand
    phases in the same dependency order ``AppUtils.getConfig`` uses, so that
    ``parquetNewSummary`` / ``parquetNewPDF`` resolve through the real templates to the
    single shared (studyName + scenarioTimestamp keyed) dataset path the engine emits."""
    with open(CONFIG_PATH) as cf:
        config = json.load(cf)
    ConfigManager._initializeSingleton(config)
    ConfigManager.expandPhase('defaultValues')
    ConfigManager.expandPhase('arguments', studyName='run', studyDefinitionFile='seed.xlsx',
                              monteCarloIterations=1, outputRoot=str(tmpOutputRoot))
    ConfigManager.expandPhase('start', site='run', scenarioTimestamp='TS')
    ConfigManager.expandPhase('simulation')
    return ConfigManager


def _simSummaryWorkitem(cm, monkeypatch, siteNames):
    """Drive the real generateWorkitems over ``siteNames`` (stubbing only the on-disk
    study discovery in getFileList) and return the single simSummary workitem."""
    fileList = list(map(lambda s: (f'{s}.xlsx', f'{s}.xlsx', s), siteNames))
    monkeypatch.setattr(SiteMain2, 'getFileList', lambda _cm: iter(fileList))
    workitemGroups = SiteMain2.generateWorkitems(cm, phasesToInclude=['simSummary'])
    return workitemGroups[0][0]


def test_site_summary_dirs_have_no_per_site_duplicates(tmp_path, monkeypatch):
    cm = _setupConfigManager(tmp_path)
    wi = _simSummaryWorkitem(cm, monkeypatch, ['siteA', 'siteB', 'siteC'])

    dirs = wi['allSiteSummaryDirs']
    assert dirs, "simSummary workitem must carry the site summary dirs"
    assert dirs == list(dict.fromkeys(dirs)), (
        f"allSiteSummaryDirs must add each distinct dataset dir once; got {len(dirs)} "
        f"entries with {len(set(dirs))} unique -> the shared dataset would be read "
        f"{len(dirs)}x (={len(dirs)}x-site inflation): {dirs}")


def test_site_pdf_dirs_have_no_per_site_duplicates(tmp_path, monkeypatch):
    cm = _setupConfigManager(tmp_path)
    wi = _simSummaryWorkitem(cm, monkeypatch, ['siteA', 'siteB', 'siteC'])

    dirs = wi['allSitePDFDirs']
    assert dirs, "simSummary workitem must carry the site PDF dirs"
    assert dirs == list(dict.fromkeys(dirs)), (
        f"allSitePDFDirs must add each distinct dataset dir once; got {len(dirs)} "
        f"entries with {len(set(dirs))} unique -> SimPDF mixture inflated "
        f"{len(dirs)}x: {dirs}")


# ---------------------------------------------------------------------------
# End-to-end invariants (#27): the simulation-level value is the cross-site sum.
# Fixtures use the REAL shared-dir layout (ONE directory, hive-partitioned by
# site) that the engine actually emits -- not the per-site distinct dirs the
# #101 tests used -- so an N-identical-paths regression inflates them N-fold.
# ---------------------------------------------------------------------------

MC_ITERATIONS = 2


def _writeSharedSiteSummary(summaryDir, siteReadings, modelReadableName='src1'):
    """Write ONE shared SiteSummary dataset (single dir, hive-partitioned by site)
    with a COMBINED modelEmissionCategory + modelReadableName row per site, each
    carrying a per-MC-run ``readings`` list. Mirrors the engine's on-disk layout."""
    rows = list(map(lambda kv: {
        'species': 'METHANE', 'units': 'mt/year', 'includeFugitive': False,
        'CICategory': 'modelReadableName', 'modelEmissionCategory': 'COMBINED',
        'modelReadableName': modelReadableName, 'unitID': 'u1', 'METype': 'mt1',
        'pneumatic': 'none', 'readings': list(map(float, kv[1])),
        'mean': float(sum(kv[1])) / MC_ITERATIONS, 'site': kv[0],
    }, siteReadings.items()))
    pd.DataFrame(rows).to_parquet(summaryDir, partition_cols=['site'], index=False)


def _writeSharedSitePDF(pdfDir, siteRates):
    """Write ONE shared site-PDF dataset (single dir, partitioned by site); one
    siteTotals component per site so nComponents == number of sites."""
    rows = list(map(lambda kv: {
        'species': 'METHANE', 'includeFugitive': False, 'CICategory': 'siteTotals',
        'site': kv[0], 'operator': 'op', 'psno': kv[0],
        'emissionRate_kgPerH': float(kv[1]), 'probability': 1.0,
    }, siteRates.items()))
    pd.DataFrame(rows).to_parquet(pdfDir, partition_cols=['site'], index=False)


def _runSimSummary(tmp_path, monkeypatch, siteReadings, siteRates):
    """Drive the real generateWorkitems construction + summarizeSimulation against
    shared-dir fixtures written to the engine-resolved paths. Returns the workitem."""
    cm = _setupConfigManager(tmp_path)
    _writeSharedSiteSummary(cm.getConfigVar('parquetNewSummary'), siteReadings)
    _writeSharedSitePDF(cm.getConfigVar('parquetNewPDF'), siteRates)
    wi = _simSummaryWorkitem(cm, monkeypatch, list(siteReadings))
    wi['monteCarloIterations'] = MC_ITERATIONS
    summarizeSimulation(wi)  # writes SimSummary, then createSimPDF
    return wi


def test_simsummary_total_equals_sum_over_sites(tmp_path, monkeypatch):
    siteReadings = {'siteA': [2.0, 2.0], 'siteB': [3.0, 3.0], 'siteC': [5.0, 5.0]}
    siteRates = {'siteA': 10.0, 'siteB': 20.0, 'siteC': 30.0}
    wi = _runSimSummary(tmp_path, monkeypatch, siteReadings, siteRates)

    sim = pd.read_parquet(wi['parquetNewSimSummary'])
    row = sim[(sim['CICategory'] == 'modelReadableName')
              & (sim['modelReadableName'] == 'src1') & (sim['species'] == 'METHANE')]
    simTotal = row['total'].sum()
    expected = sum(map(sum, siteReadings.values()))  # 4 + 6 + 10 = 20

    assert abs(simTotal - expected) < 1e-9, (
        f"SimSummary modelReadableName total must equal the sum over sites (#27): "
        f"got {simTotal}, expected {expected} "
        f"({len(siteReadings)}x-site inflation would give {expected * len(siteReadings)})")


def test_simpdf_mixture_mass_not_inflated(tmp_path, monkeypatch):
    siteReadings = {'siteA': [2.0, 2.0], 'siteB': [3.0, 3.0], 'siteC': [5.0, 5.0]}
    siteRates = {'siteA': 10.0, 'siteB': 20.0, 'siteC': 30.0}
    wi = _runSimSummary(tmp_path, monkeypatch, siteReadings, siteRates)

    simpdf = pd.read_parquet(wi['parquetNewSimPDF'])
    assert not simpdf.empty, "SimPDF must be produced"
    # A mixture distribution's probability mass is at most 1.0 per identity group.
    # (Per #94 it may sum to <1.0 for transient sources -- a separate open bug --
    # so we assert <= 1.0 rather than == 1.0; the N-identical-paths bug makes it
    # equal the site count, here 3.0, which this catches.)
    massPerGroup = simpdf.groupby(['species', 'includeFugitive'], observed=True)['probability'].sum()
    assert massPerGroup.max() <= 1.0 + 1e-9, (
        f"SimPDF mixture mass must not exceed 1.0; got max {massPerGroup.max()} "
        f"({len(siteReadings)}x-site inflation would give {len(siteReadings)}.0)")
