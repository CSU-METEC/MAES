"""
Tests that a directory (`-dr`) run consolidates its Summary parquet datasets into a
single job-level location, matching the bundle layout.

The engine routes every `parquetNew*` summary key (site-level *and* sim-level) through
`summaryParquetDir`. Bundle mode overrides that to a job-level dir; the non-bundle path
uses the config default. This change flips that default from the per-study `{parquetDir}`
to a job-level `{outputRoot}/MC_{scenarioTimestamp}/parquet`, so `-dr` consolidates too.

Invariant under test: the Summary datasets resolve to a path that is **independent of
the study/site**, while the per-site raw (non-summary) parquet stays study-specific.
"""

import json
import os

from ConfigManager import ConfigManager

CONFIG_PATH = os.path.join(os.path.dirname(__file__), '..', 'config', 'defaultConfig.json')

SUMMARY_KEYS = ['summaryParquetDir', 'parquetNewSummary', 'parquetNewInstEmissions',
                'parquetNewEventSummary', 'parquetNewPDF', 'parquetNewPDFCache',
                'parquetNewSimSummary', 'parquetNewSimPDF', 'parquetSummaryDS']
PER_SITE_KEYS = ['parquetDir', 'parquetEventDS', 'parquetTimeseriesDS']


def _resolve_for_study(study_name, output_root='/out', ts='TS'):
    """Drive ConfigManager through the phases a `-dr` run uses for one study and
    return the resolved path config vars."""
    with open(CONFIG_PATH) as f:
        config = json.load(f)
    ConfigManager._initializeSingleton(config)
    cm = ConfigManager
    cm.expandPhase('defaultValues', outputRoot=output_root)
    cm.expandPhase('arguments', studyDefinitionFile=f'{study_name}.xlsx', studyName=study_name)
    cm.expandPhase('start', site=study_name, scenarioTimestamp=ts)
    cm.expandPhase('simulation')
    return {k: cm.getConfigVar(k) for k in SUMMARY_KEYS + PER_SITE_KEYS}


def test_summary_paths_are_study_independent():
    """Two different studies in the same job resolve every Summary dataset to the
    SAME (job-level) location — that is what makes the layout consolidated."""
    a = _resolve_for_study('siteA')
    b = _resolve_for_study('siteB')
    for k in SUMMARY_KEYS:
        assert a[k] == b[k], f"{k} must be study-independent, got {a[k]!r} vs {b[k]!r}"
        assert 'siteA' not in a[k] and 'siteB' not in b[k], f"{k} still embeds the study name: {a[k]!r}"


def test_per_site_raw_paths_stay_study_specific():
    """Non-summary (raw) parquet remains per-study — only the Summary tree consolidates."""
    a = _resolve_for_study('siteA')
    b = _resolve_for_study('siteB')
    for k in PER_SITE_KEYS:
        assert a[k] != b[k], f"{k} should stay per-study, but both resolved to {a[k]!r}"
        assert 'siteA' in a[k] and 'siteB' in b[k]


def test_summary_dir_is_job_level():
    """summaryParquetDir resolves from job-wide vars only (outputRoot + scenarioTimestamp)."""
    a = _resolve_for_study('siteA', output_root='/out', ts='TS')
    assert a['summaryParquetDir'] == '/out/MC_TS/parquet'
    assert a['parquetNewSummary'] == '/out/MC_TS/parquet/Summary/SiteSummary'
    assert a['parquetNewSimSummary'] == '/out/MC_TS/parquet/Summary/SimSummary'
    # per-site parquet is the per-study sibling, NOT the job-level dir
    assert a['parquetDir'] == '/out/siteA/MC_TS/parquet'
