"""Unit tests for the Output Equivalence oracle (src/Testing/OutputEquivalence.py).

These exercise the comparators on synthetic DataFrames — no engine run required — plus a
round-trip through two on-disk parquet trees. See CSU-METEC/MAES #95.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import numpy as np
import pandas as pd

from Testing import OutputEquivalence as oe


# --------------------------------------------------------------------------- helpers

def _summary_df(rows):
    """SiteSummary-like table: identity cols + numeric value cols."""
    return pd.DataFrame(rows, columns=[
        "site", "species", "operator", "psno", "METype",  # identity
        "mean", "max", "lowerCI", "upperCI",               # values
    ])


_BASE_SUMMARY = [
    ["S1", "METHANE", "OP", "PS1", "Tank", 10.0, 20.0, 8.0, 12.0],
    ["S1", "METHANE", "OP", "PS1", "Well", 5.0, 9.0, 4.0, 6.0],
]


def _pdf_df(groups):
    """PDF-like distribution table for one or more identity groups.

    groups: list of (modelReadableName, rates, cdf) tuples.
    """
    recs = []
    for name, rates, cdf in groups:
        for r, c in zip(rates, cdf):
            recs.append(["S1", "METHANE", "OP", "PS1", "siteTotals", name,
                         r, np.nan, c])
    return pd.DataFrame(recs, columns=[
        "site", "species", "operator", "psno", "CICategory", "modelReadableName",
        "emissionRate_kgPerH", "probability", "cumulativeProbability",
    ])


_BASE_PDF = [("Blowdown", [1.0, 2.0, 3.0], [0.5, 0.8, 1.0])]


# --------------------------------------------------------------------------- tabular

def test_tabular_identical_is_equivalent():
    df = _summary_df(_BASE_SUMMARY)
    assert compare_ok(oe.compare_tabular(df.copy(), df.copy()))


def test_tabular_within_tolerance_is_equivalent():
    a = _summary_df(_BASE_SUMMARY)
    b = a.copy()
    b.loc[0, "mean"] = 10.0 + 5e-3  # abs 0.005 < ABS_EPSILON; also rel < REL_EPSILON
    assert compare_ok(oe.compare_tabular(a, b))


def test_tabular_abs_small_but_rel_large_is_equivalent():
    # Tiny magnitudes: abs delta below ABS_EPSILON even though rel delta is huge.
    a = _summary_df([["S1", "METHANE", "OP", "PS1", "Tank", 1e-4, 1.0, 0.0, 1.0]])
    b = a.copy()
    b.loc[0, "mean"] = 2e-4  # rel=100% but abs=1e-4 < ABS_EPSILON → not a mismatch
    assert compare_ok(oe.compare_tabular(a, b))


def test_tabular_beyond_both_tolerances_flags_value():
    a = _summary_df(_BASE_SUMMARY)
    b = a.copy()
    b.loc[0, "mean"] = 15.0  # abs 5 and rel 33% → both exceeded
    disc = oe.compare_tabular(a, b)
    assert any(d.kind == "value" and "mean" in d.detail for d in disc)


def test_tabular_missing_row_flagged_both_directions():
    a = _summary_df(_BASE_SUMMARY)
    b = _summary_df(_BASE_SUMMARY[:1])  # drop the Well row
    disc = oe.compare_tabular(a, b)
    assert any(d.kind == "missing_in_b" for d in disc)

    disc2 = oe.compare_tabular(b, a)
    assert any(d.kind == "missing_in_a" for d in disc2)


def test_tabular_schema_mismatch():
    a = _summary_df(_BASE_SUMMARY)
    b = a.drop(columns=["max"])
    disc = oe.compare_tabular(a, b)
    assert len(disc) == 1 and disc[0].kind == "schema"


def test_tabular_nan_equal_but_nan_vs_value_flags():
    a = _summary_df([["S1", "METHANE", "OP", "PS1", "Tank", np.nan, 20.0, 8.0, 12.0]])
    b = a.copy()
    assert compare_ok(oe.compare_tabular(a, b))   # NaN == NaN
    b.loc[0, "mean"] = 10.0
    assert not compare_ok(oe.compare_tabular(a, b))  # NaN vs value → mismatch


# --------------------------------------------------------------------------- distribution

def test_distribution_identical_is_equivalent():
    df = _pdf_df(_BASE_PDF)
    assert compare_ok(oe.compare_distribution(df.copy(), df.copy()))


def test_distribution_small_shift_within_ks_is_equivalent():
    a = _pdf_df([("Blowdown", [1.0, 2.0, 3.0], [0.50, 0.80, 1.00])])
    b = _pdf_df([("Blowdown", [1.0, 2.0, 3.0], [0.52, 0.82, 1.00])])  # max ΔCDF 0.02 < 0.05
    assert compare_ok(oe.compare_distribution(a, b))


def test_distribution_large_shift_beyond_ks_flags():
    a = _pdf_df([("Blowdown", [1.0, 2.0, 3.0], [0.50, 0.80, 1.00])])
    b = _pdf_df([("Blowdown", [1.0, 2.0, 3.0], [0.20, 0.50, 1.00])])  # max ΔCDF 0.30 > 0.05
    disc = oe.compare_distribution(a, b)
    assert any(d.kind == "ks" for d in disc)


def test_distribution_missing_group_flagged():
    a = _pdf_df([("Blowdown", [1.0, 2.0], [0.5, 1.0]),
                 ("Startup", [1.0, 2.0], [0.5, 1.0])])
    b = _pdf_df([("Blowdown", [1.0, 2.0], [0.5, 1.0])])
    disc = oe.compare_distribution(a, b)
    assert any(d.kind == "missing_in_b" and d.key.get("modelReadableName") == "Startup"
               for d in disc)


# --------------------------------------------------------------------------- end-to-end (parquet)

def test_compare_outputs_roundtrip_equivalent(tmp_path):
    a_root, b_root = _write_tree(tmp_path / "a"), _write_tree(tmp_path / "b")
    report = oe.compare_outputs(a_root, b_root)
    assert report.equivalent, report.summary()
    assert "SiteSummary" in report.datasets_compared
    assert "PDF" in report.datasets_compared


def test_compare_outputs_roundtrip_detects_diff(tmp_path):
    a_root = _write_tree(tmp_path / "a")
    b_root = _write_tree(tmp_path / "b", mean_bump=15.0)
    report = oe.compare_outputs(a_root, b_root)
    assert not report.equivalent
    assert any(d.dataset == "SiteSummary" and d.kind == "value" for d in report.discrepancies)


def test_assert_output_equivalent_raises_on_diff(tmp_path):
    a_root = _write_tree(tmp_path / "a")
    b_root = _write_tree(tmp_path / "b", mean_bump=15.0)
    import pytest
    with pytest.raises(AssertionError):
        oe.assert_output_equivalent(a_root, b_root)


def test_dataset_absent_in_one_tree_flagged(tmp_path):
    a_root = _write_tree(tmp_path / "a")
    b_root = _write_tree(tmp_path / "b")
    # Remove SimPDF from B only.
    import shutil
    shutil.rmtree(b_root / "Summary" / "SimPDF")
    report = oe.compare_outputs(a_root, b_root, datasets=["SimPDF"])
    assert any(d.kind == "missing_in_b" for d in report.discrepancies)


# --------------------------------------------------------------------------- local helpers

def compare_ok(discrepancies) -> bool:
    return len(discrepancies) == 0


def _write_tree(root, *, mean_bump=None):
    """Write a minimal output tree with SiteSummary / SimPDF / PDF parquet datasets."""
    root = root
    summ = _summary_df(_BASE_SUMMARY)
    if mean_bump is not None:
        summ.loc[0, "mean"] = mean_bump
    pdf = _pdf_df(_BASE_PDF)
    simpdf = pdf.drop(columns=["site"]).assign()  # SimPDF is flat (no site)

    (root / "Summary" / "SiteSummary").mkdir(parents=True, exist_ok=True)
    (root / "Summary" / "PDF").mkdir(parents=True, exist_ok=True)
    (root / "Summary" / "SimPDF").mkdir(parents=True, exist_ok=True)
    summ.to_parquet(root / "Summary" / "SiteSummary" / "part.parquet", index=False)
    pdf.to_parquet(root / "Summary" / "PDF" / "part.parquet", index=False)
    simpdf.to_parquet(root / "Summary" / "SimPDF" / "part.parquet", index=False)
    return root
