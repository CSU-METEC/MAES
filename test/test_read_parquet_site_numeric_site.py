"""
Tests for _read_parquet_site partition-type robustness (GitHub issue #80).

The bug: when a parquet dataset is hive-partitioned on `site` and every site value
parses as an integer, pyarrow's default HivePartitioning.discover() promotes the
site column to int32, then `_ds.field('site') == str(site_name)` raises:

    ArrowNotImplementedError: Function 'equal' has no kernel matching input types
    (int32, string)

The fix: probe to learn the partition column names, then re-open with an explicit
HivePartitioning(schema) where every partition field is pinned to pa.string().
"""

import pandas as pd
import pyarrow as pa
import pytest

from Summaries2 import _read_parquet_site


def _write_partitioned(tmp_path, df, partition_cols):
    path = tmp_path / 'ds'
    df.to_parquet(path, partition_cols=partition_cols)
    return path


# ---------------------------------------------------------------------------
# Numeric-only site IDs: the crash case from the issue
# ---------------------------------------------------------------------------

def test_numeric_only_site_ids_do_not_crash(tmp_path):
    """Every site ID parses as int. Pre-fix: pyarrow infers int32 for the site
    partition column and the str-filter raises ArrowNotImplementedError.
    Post-fix: the read returns just the rows for the requested site."""
    df = pd.DataFrame({
        'site': ['52843', '52843', '51661', '51661'],
        'value': [10.0, 20.0, 30.0, 40.0],
    })
    path = _write_partitioned(tmp_path, df, partition_cols=['site'])

    result = _read_parquet_site(path, '52843')

    assert not result.empty, "expected rows for site 52843"
    assert set(result['value']) == {10.0, 20.0}, f"unexpected values: {result['value'].tolist()}"
    assert (result['site'].astype(str) == '52843').all(), (
        f"site column should contain only the filtered value, got: {result['site'].unique()}"
    )


# ---------------------------------------------------------------------------
# Mixed numeric / non-numeric site IDs (pin existing-good behavior)
# ---------------------------------------------------------------------------

def test_mixed_site_ids_still_work(tmp_path):
    """At least one non-numeric site -> pyarrow infers string under default
    discovery, so this case worked pre-fix. Pin it so the fix doesn't regress."""
    df = pd.DataFrame({
        'site': ['52843', 'XPlant_A', '52843', 'XPlant_A'],
        'value': [10.0, 11.0, 20.0, 21.0],
    })
    path = _write_partitioned(tmp_path, df, partition_cols=['site'])

    result_numeric = _read_parquet_site(path, '52843')
    result_named = _read_parquet_site(path, 'XPlant_A')

    assert set(result_numeric['value']) == {10.0, 20.0}
    assert set(result_named['value']) == {11.0, 21.0}


# ---------------------------------------------------------------------------
# Multi-partition (site, mcRun): mcRun must remain a column post-fix
# ---------------------------------------------------------------------------

def test_multi_partition_preserves_mcRun_column(tmp_path):
    """Datasets partitioned on ['site', 'mcRun'] (e.g. InstEmissions) must
    still expose mcRun as a column after the fix. Guards against the silent
    regression mode where pinning only `site` would drop `mcRun` recognition."""
    df = pd.DataFrame({
        'site':  ['52843', '52843', '52843', '51661'],
        'mcRun': ['0',     '1',     '2',     '0'],
        'value': [1.0, 2.0, 3.0, 4.0],
    })
    path = _write_partitioned(tmp_path, df, partition_cols=['site', 'mcRun'])

    result = _read_parquet_site(path, '52843')

    assert 'mcRun' in result.columns, (
        f"mcRun must be present as a column; got: {list(result.columns)}"
    )
    assert set(result['mcRun'].astype(str)) == {'0', '1', '2'}, (
        f"expected all 3 mcRun partitions for site 52843, got: {result['mcRun'].unique()}"
    )
    assert set(result['value']) == {1.0, 2.0, 3.0}
