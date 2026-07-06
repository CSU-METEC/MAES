"""Tests for GitHub issue CSU-METEC/MAES#113 — the ``--noPDF`` flag.

``--noPDF`` skips PDF generation while keeping every other summary. It touches two
places, both covered here:

- ``SiteMain2.generateWorkitems`` drops the per-site ``createPDFCache`` phase from the
  workitem plan (and must not mutate the shared ``ALL_PHASES`` default it filters).
- ``Summaries2.summarizeSimulation`` skips the simulation-level ``createSimPDF``.

The flag was added as a debugging / performance aid for #113: PDF generation is the
serial phase that dominates wall clock on large runs (see #70 / #71), so being able to
turn it off isolates the rest of the pipeline.

These tests drive the real ``generateWorkitems`` / ``summarizeSimulation`` against the
real ``defaultConfig`` templates (stubbing only on-disk study discovery), threading
``noPDF`` through the ``arguments`` phase exactly as ``AppUtils.getConfig`` does for a
parsed ``--noPDF`` flag — so they exercise the actual flag path, not a reimplementation.
"""

import json
import os

import pandas as pd
import pytest

import SiteMain2
import Summaries2
from ConfigManager import ConfigManager
from Summaries2 import summarizeSimulation

CONFIG_PATH = os.path.join(os.path.dirname(__file__), '..', 'config', 'defaultConfig.json')
SITES = ['siteA', 'siteB', 'siteC']
MC_ITERATIONS = 2


def _setupConfigManager(tmpOutputRoot, noPDF):
    """Initialize the ConfigManager singleton from the real defaultConfig with ``noPDF``
    supplied to the ``arguments`` phase, the same way a parsed ``--noPDF`` flag reaches
    config in ``AppUtils.getConfig``."""
    with open(CONFIG_PATH) as cf:
        config = json.load(cf)
    ConfigManager._initializeSingleton(config)
    ConfigManager.expandPhase('defaultValues')
    ConfigManager.expandPhase('arguments', studyName='run', studyDefinitionFile='seed.xlsx',
                              monteCarloIterations=1, outputRoot=str(tmpOutputRoot), noPDF=noPDF)
    ConfigManager.expandPhase('start', site='run', scenarioTimestamp='TS')
    ConfigManager.expandPhase('simulation')
    return ConfigManager


def _workitemGroups(cm, monkeypatch, siteNames, phasesToInclude=None):
    """Drive the real generateWorkitems over ``siteNames``, stubbing only the on-disk
    study discovery in getFileList. Returns the list of per-phase workitem groups."""
    fileList = list(map(lambda s: (f'{s}.xlsx', f'{s}.xlsx', s), siteNames))
    monkeypatch.setattr(SiteMain2, 'getFileList', lambda _cm: iter(fileList))
    if phasesToInclude is None:
        ret = SiteMain2.generateWorkitems(cm)
    else:
        ret = SiteMain2.generateWorkitems(cm, phasesToInclude=phasesToInclude)
    return ret


def _worktypes(groups):
    """Flatten the per-phase workitem groups to the set of ``workType`` values present."""
    allItems = []
    for group in groups:
        allItems.extend(group)
    ret = set(map(lambda wi: wi['workType'], allItems))
    return ret


def test_nopdf_drops_createpdfcache_phase(tmp_path, monkeypatch):
    cm = _setupConfigManager(tmp_path, noPDF=True)
    groups = _workitemGroups(cm, monkeypatch, SITES)
    worktypes = _worktypes(groups)
    assert 'createPDFCache' not in worktypes, (
        f"--noPDF must drop the createPDFCache phase; got worktypes {sorted(worktypes)}")
    assert {'initialization', 'simulation', 'parquet', 'summarize', 'simSummary'} <= worktypes, (
        f"--noPDF must keep every non-PDF phase; got {sorted(worktypes)}")


def test_pdf_on_includes_createpdfcache_phase(tmp_path, monkeypatch):
    cm = _setupConfigManager(tmp_path, noPDF=False)
    groups = _workitemGroups(cm, monkeypatch, SITES)
    allItems = []
    for group in groups:
        allItems.extend(group)
    createPdfItems = list(filter(lambda wi: wi['workType'] == 'createPDFCache', allItems))
    assert len(createPdfItems) == len(SITES), (
        f"without --noPDF the createPDFCache phase must be planned once per site; "
        f"got {len(createPdfItems)} for {len(SITES)} sites")


def test_nopdf_does_not_mutate_all_phases_default(tmp_path, monkeypatch):
    before = list(SiteMain2.ALL_PHASES)
    cm = _setupConfigManager(tmp_path, noPDF=True)
    _workitemGroups(cm, monkeypatch, SITES)
    assert SiteMain2.ALL_PHASES == before, (
        "generateWorkitems must not mutate the shared ALL_PHASES default when --noPDF "
        f"filters createPDFCache; was {before}, now {SiteMain2.ALL_PHASES}")
    assert 'createPDFCache' in SiteMain2.ALL_PHASES


def _writeSharedSiteSummary(summaryDir):
    """Write a minimal single-site SiteSummary dataset (hive-partitioned by site) so
    summarizeSimulation can build the SimSummary before reaching the createSimPDF branch."""
    rows = [{
        'species': 'METHANE', 'units': 'mt/year', 'includeFugitive': False,
        'CICategory': 'modelReadableName', 'modelEmissionCategory': 'COMBINED',
        'modelReadableName': 'src1', 'unitID': 'u1', 'METype': 'mt1',
        'pneumatic': 'none', 'readings': [2.0, 2.0], 'mean': 2.0, 'site': 'siteA',
    }]
    pd.DataFrame(rows).to_parquet(summaryDir, partition_cols=['site'], index=False)


def _driveSimSummary(cm, monkeypatch):
    """Run the real summarizeSimulation with createSimPDF replaced by a recorder, so the
    test observes whether the PDF step is invoked without needing PDF fixtures. Returns the
    list of createSimPDF calls."""
    _writeSharedSiteSummary(cm.getConfigVar('parquetNewSummary'))
    calls = []
    monkeypatch.setattr(Summaries2, 'createSimPDF', lambda config: calls.append(config))
    groups = _workitemGroups(cm, monkeypatch, ['siteA'], phasesToInclude=['simSummary'])
    wi = groups[0][0]
    wi['monteCarloIterations'] = MC_ITERATIONS
    summarizeSimulation(wi)
    return calls


def test_nopdf_skips_createsimpdf(tmp_path, monkeypatch):
    cm = _setupConfigManager(tmp_path, noPDF=True)
    calls = _driveSimSummary(cm, monkeypatch)
    assert calls == [], "--noPDF must skip createSimPDF in summarizeSimulation"


def test_pdf_on_calls_createsimpdf(tmp_path, monkeypatch):
    cm = _setupConfigManager(tmp_path, noPDF=False)
    calls = _driveSimSummary(cm, monkeypatch)
    assert len(calls) == 1, "without --noPDF, summarizeSimulation must call createSimPDF"
