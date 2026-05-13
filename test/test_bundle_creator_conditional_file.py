"""
Tests for BundleCreator conditional-file handling (GitHub issue #81).

The bug: BundleCreator hardcoded `_CONDITIONAL_FILE_PARAMS = {'gasFractionDistFileName'}`
as the parallel-to-runtime exception set for "this file ref may also be numeric."
`crankcaseDistrib` was missing from that set even though `getCrankcaseDist` accepts
numeric values at runtime — bundling any xlsx with `CCEE Ratio Distribution = 0.144`
warned "File reference not found" and dropped the value.

The fix moves the source of truth into the model definition JSONs via a new
`"Conditional File": true` key, propagated through BuildKwargTable.py into
ModelDefinitionMap.csv as an `isConditionalFile` column, then consumed by
BundleCreator._buildConditionalFileIndex(modelDefDf).
"""

import logging
from pathlib import Path

import pandas as pd
import pytest

from BundleCreator import (
    _buildConditionalFileIndex,
    _collectXlsxFileRefs,
    _buildFileRefIndex,
)

MAES_ROOT = Path(__file__).resolve().parent.parent
MODEL_DEF_MAP_PATH = MAES_ROOT / 'src' / 'utilities' / 'SiteDefinitionValidation' / 'ModelDefinitionMap.csv'


def _siteData(tab_name, columns):
    """Build a minimal siteData dict for _collectXlsxFileRefs."""
    df = pd.DataFrame(columns)
    return {
        'masterEquipment': [{'Tab': tab_name}],
        'tabs': {tab_name: df},
    }


def _fileRefIndex():
    """Lookup matching how _buildFileRefIndex maps column-name valKeys to python params."""
    return {
        'ccee_ratio_distribution': 'crankcaseDistrib',
        'fraction_of_flash_released': 'gasFractionDistFileName',
    }


# ---------------------------------------------------------------------------
# Builder: _buildConditionalFileIndex
# ---------------------------------------------------------------------------

def test_build_conditional_file_index_extracts_flagged_params():
    """Synthetic modelDefDf: only rows with isConditionalFile=True appear in the set."""
    df = pd.DataFrame([
        {'pythonParameter': 'crankcaseDistrib',       'isConditionalFile': True},
        {'pythonParameter': 'gasFractionDistFileName', 'isConditionalFile': True},
        {'pythonParameter': 'loadCondition',          'isConditionalFile': False},
        {'pythonParameter': 'productionGC',           'isConditionalFile': False},
    ])
    result = _buildConditionalFileIndex(df)
    assert result == {'crankcaseDistrib', 'gasFractionDistFileName'}


def test_build_conditional_file_index_loud_failure_on_missing_column():
    """Stale ModelDefinitionMap.csv without isConditionalFile column → loud KeyError."""
    df = pd.DataFrame([
        {'pythonParameter': 'crankcaseDistrib', 'isFileRef': True},
    ])
    with pytest.raises(KeyError):
        _buildConditionalFileIndex(df)


def test_build_conditional_file_index_against_committed_csv():
    """Sanity check: the real committed ModelDefinitionMap.csv contains the
    two expected pythonParameters as conditional-file rows."""
    df = pd.read_csv(MODEL_DEF_MAP_PATH)
    result = _buildConditionalFileIndex(df)
    assert 'crankcaseDistrib' in result, (
        f"crankcaseDistrib missing from conditional-file set; got {sorted(result)}"
    )
    assert 'gasFractionDistFileName' in result, (
        f"gasFractionDistFileName missing from conditional-file set; got {sorted(result)}"
    )


# ---------------------------------------------------------------------------
# Behavior: _collectXlsxFileRefs with conditional-file params
# ---------------------------------------------------------------------------

def test_crankcase_numeric_short_circuits(tmp_path, caplog):
    """Issue #81 red→green: numeric CCEE Ratio Distribution must not warn and
    must not produce any file ref."""
    siteData = _siteData('Compressors', {'CCEE Ratio Distribution': ['0.144']})
    conditional = {'crankcaseDistrib', 'gasFractionDistFileName'}

    with caplog.at_level(logging.WARNING, logger='BundleCreator'):
        refs = _collectXlsxFileRefs(
            siteData, _fileRefIndex(), conditional,
            cwd=tmp_path, emitterProfileDir=tmp_path,
        )

    assert refs == {}
    assert not any('File reference not found' in r.message for r in caplog.records), (
        f"unexpected file-ref warning: {[r.message for r in caplog.records]}"
    )


def test_gas_fraction_numeric_short_circuits(tmp_path, caplog):
    """Pre-existing conditional-file behavior must continue to work
    (regression pin against future changes to the builder/use site)."""
    siteData = _siteData('Separators', {'Fraction of Flash Released': ['1.0']})
    conditional = {'crankcaseDistrib', 'gasFractionDistFileName'}

    with caplog.at_level(logging.WARNING, logger='BundleCreator'):
        refs = _collectXlsxFileRefs(
            siteData, _fileRefIndex(), conditional,
            cwd=tmp_path, emitterProfileDir=tmp_path,
        )

    assert refs == {}
    assert not any('File reference not found' in r.message for r in caplog.records)


def test_crankcase_real_file_path_resolved(tmp_path):
    """A non-numeric value pointing at an existing file under emitterProfileDir
    must still be collected as a file ref (the conditional case is *only* numeric)."""
    fileName = 'crank.csv'
    (tmp_path / fileName).write_text('emitterModelFactorTag,factorTag\nx,y\n')

    siteData = _siteData('Compressors', {'CCEE Ratio Distribution': [fileName]})
    conditional = {'crankcaseDistrib', 'gasFractionDistFileName'}

    refs = _collectXlsxFileRefs(
        siteData, _fileRefIndex(), conditional,
        cwd=tmp_path, emitterProfileDir=tmp_path,
    )

    assert len(refs) == 1, f"expected one file ref, got {refs}"
    [(destPath, srcPath)] = refs.items()
    assert destPath.endswith(fileName)
    assert srcPath == (tmp_path / fileName).resolve()


def test_crankcase_non_numeric_missing_path_still_warns(tmp_path, caplog):
    """A non-numeric value that *looks* like a path but doesn't exist should
    still emit the not-found warning. The fix only suppresses for numeric values."""
    siteData = _siteData('Compressors', {'CCEE Ratio Distribution': ['does_not_exist.csv']})
    conditional = {'crankcaseDistrib', 'gasFractionDistFileName'}

    with caplog.at_level(logging.WARNING, logger='BundleCreator'):
        refs = _collectXlsxFileRefs(
            siteData, _fileRefIndex(), conditional,
            cwd=tmp_path, emitterProfileDir=tmp_path,
        )

    assert refs == {}
    assert any('File reference not found' in r.message for r in caplog.records), (
        f"expected file-ref-not-found warning, got: {[r.message for r in caplog.records]}"
    )
