"""Output Equivalence (OE) oracle — see CSU-METEC/MAES #95.

Two engine runs are *output-equivalent* iff, run under the same configuration and a
fixed RNG seed, their persisted output datasets are equal under a defined comparison:

  - same identity-keyed row set (no missing/extra groups);
  - **tabular** datasets (summaries): for each numeric value column, a row is a mismatch
    only when BOTH the absolute delta exceeds ABS_EPSILON AND the relative delta exceeds
    REL_EPSILON (the criterion used by ``SummaryTest.py``);
  - **distribution** datasets (PDF / SimPDF): per identity group, the Kolmogorov–Smirnov
    distance between the two CDFs (max |ΔCDF| over the union of rate bins) must be
    <= KS_EPSILON.

The core comparators operate on DataFrames, so this module is dependency-light
(pandas / numpy only) and unit-testable without running the engine. ``compare_outputs``
reads the persisted parquet datasets from two output trees and compares each.

Tolerances and the KS approach mirror ``src/Testing/SummaryTest.py`` (the prior art named
in #95); they are duplicated here rather than imported to keep this module free of the
engine's heavy dependencies.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import pandas as pd

# Tolerances — mirror src/Testing/SummaryTest.py.
ABS_EPSILON = 0.01   # absolute tolerance on a value column
REL_EPSILON = 0.01   # relative tolerance; a mismatch requires BOTH abs and rel exceeded
KS_EPSILON = 0.05    # max CDF deviation (KS statistic) tolerated for distribution datasets

# Column roles. Identity columns are derived as "everything that is not a value or an
# ignored column", so the comparators stay robust to schema variation across datasets.
DISTRIBUTION_VALUE_COLS = {"emissionRate_kgPerH", "probability", "cumulativeProbability"}
TABULAR_NUMERIC_COLS = {
    "mean", "max", "min", "count",
    "lowerCI", "upperCI", "lowerQuartile", "upperQuartile",
    "rawMean", "rawCount",
}
IGNORE_COLS = {"readings"}  # list/object-valued; not meaningfully comparable

# CDF axes for distribution comparison.
CDF_X = "emissionRate_kgPerH"
CDF_Y = "cumulativeProbability"


@dataclass(frozen=True)
class Dataset:
    relpath: str          # path relative to an output tree's root
    mode: str             # "tabular" | "distribution"


# Headline persisted deliverables compared by OE (per #95). Intermediate/granular datasets
# (InstEmissions, PDFCache) are intentionally excluded.
DATASETS: dict[str, Dataset] = {
    "SiteSummary": Dataset("Summary/SiteSummary", "tabular"),
    "SimSummary":  Dataset("Summary/SimSummary", "tabular"),
    "EventSummary": Dataset("Summary/EventSummary", "tabular"),
    "PDF":    Dataset("Summary/PDF", "distribution"),
    "SimPDF": Dataset("Summary/SimPDF", "distribution"),
}


@dataclass
class Discrepancy:
    dataset: str
    kind: str            # "schema" | "missing_in_a" | "missing_in_b" | "value" | "ks"
    key: dict            # identity of the offending row/group ({} for schema-level)
    detail: str
    magnitude: float = float("nan")

    def __str__(self) -> str:
        keystr = ", ".join(f"{k}={v}" for k, v in self.key.items()) if self.key else "-"
        mag = "" if np.isnan(self.magnitude) else f" (Δ={self.magnitude:.6g})"
        return f"[{self.dataset}/{self.kind}] {keystr}: {self.detail}{mag}"


@dataclass
class OEReport:
    discrepancies: list[Discrepancy] = field(default_factory=list)
    datasets_compared: list[str] = field(default_factory=list)

    @property
    def equivalent(self) -> bool:
        return not self.discrepancies

    def summary(self) -> str:
        if self.equivalent:
            return f"OUTPUT-EQUIVALENT across {len(self.datasets_compared)} dataset(s): " \
                   f"{', '.join(self.datasets_compared)}"
        lines = [f"NOT OUTPUT-EQUIVALENT — {len(self.discrepancies)} discrepancy(ies):"]
        lines += [f"  {d}" for d in self.discrepancies]
        return "\n".join(lines)


def _identity_cols(columns: Iterable[str], value_cols: set[str]) -> list[str]:
    """Identity = all columns that are neither value columns nor ignored."""
    return [c for c in columns if c not in value_cols and c not in IGNORE_COLS]


def _values_match(a: float, b: float, abs_eps: float, rel_eps: float) -> tuple[bool, float]:
    """A pair matches unless BOTH abs and rel deltas are exceeded. Returns (match, absΔ)."""
    a_nan, b_nan = pd.isna(a), pd.isna(b)
    if a_nan and b_nan:
        return True, 0.0
    if a_nan or b_nan:
        return False, float("nan")
    abs_delta = abs(float(a) - float(b))
    rel_delta = abs_delta / max(abs(float(a)), abs(float(b)), 1e-9)
    match = not (abs_delta > abs_eps and rel_delta > rel_eps)
    return match, abs_delta


def compare_tabular(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    *,
    dataset: str = "tabular",
    abs_eps: float = ABS_EPSILON,
    rel_eps: float = REL_EPSILON,
) -> list[Discrepancy]:
    """Compare two summary-style tables on identity keys + numeric value columns."""
    out: list[Discrepancy] = []

    if set(df_a.columns) != set(df_b.columns):
        only_a = sorted(set(df_a.columns) - set(df_b.columns))
        only_b = sorted(set(df_b.columns) - set(df_a.columns))
        out.append(Discrepancy(dataset, "schema", {},
                               f"column mismatch: only_in_a={only_a} only_in_b={only_b}"))
        return out

    value_cols = sorted(TABULAR_NUMERIC_COLS & set(df_a.columns))
    id_cols = _identity_cols(df_a.columns, set(value_cols))
    if not id_cols:
        out.append(Discrepancy(dataset, "schema", {}, "no identity columns found"))
        return out

    merged = df_a.merge(df_b, on=id_cols, how="outer", suffixes=("_a", "_b"), indicator=True)

    for _, row in merged[merged["_merge"] == "left_only"].iterrows():
        out.append(Discrepancy(dataset, "missing_in_b",
                               {c: row[c] for c in id_cols}, "row present in A only"))
    for _, row in merged[merged["_merge"] == "right_only"].iterrows():
        out.append(Discrepancy(dataset, "missing_in_a",
                               {c: row[c] for c in id_cols}, "row present in B only"))

    both = merged[merged["_merge"] == "both"]
    for _, row in both.iterrows():
        for vc in value_cols:
            match, absd = _values_match(row[f"{vc}_a"], row[f"{vc}_b"], abs_eps, rel_eps)
            if not match:
                out.append(Discrepancy(
                    dataset, "value", {c: row[c] for c in id_cols},
                    f"{vc}: a={row[f'{vc}_a']!r} b={row[f'{vc}_b']!r}", absd))
    return out


def _ks_distance(g_a: pd.DataFrame, g_b: pd.DataFrame) -> float:
    """Max |ΔCDF| over the union of rate bins, via interpolation of the two CDFs."""
    a = g_a.sort_values(CDF_X)
    b = g_b.sort_values(CDF_X)
    ax, ay = a[CDF_X].to_numpy(float), a[CDF_Y].to_numpy(float)
    bx, by = b[CDF_X].to_numpy(float), b[CDF_Y].to_numpy(float)
    grid = np.union1d(ax, bx)
    ai = np.interp(grid, ax, ay)
    bi = np.interp(grid, bx, by)
    return float(np.abs(ai - bi).max()) if grid.size else 0.0


def compare_distribution(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    *,
    dataset: str = "distribution",
    ks_eps: float = KS_EPSILON,
) -> list[Discrepancy]:
    """Compare two PDF/SimPDF datasets group-by-group via KS distance between CDFs."""
    out: list[Discrepancy] = []

    if set(df_a.columns) != set(df_b.columns):
        only_a = sorted(set(df_a.columns) - set(df_b.columns))
        only_b = sorted(set(df_b.columns) - set(df_a.columns))
        out.append(Discrepancy(dataset, "schema", {},
                               f"column mismatch: only_in_a={only_a} only_in_b={only_b}"))
        return out

    id_cols = _identity_cols(df_a.columns, DISTRIBUTION_VALUE_COLS)
    if not id_cols:
        out.append(Discrepancy(dataset, "schema", {}, "no identity columns found"))
        return out

    groups_a = {k: g for k, g in df_a.groupby(id_cols, dropna=False)}
    groups_b = {k: g for k, g in df_b.groupby(id_cols, dropna=False)}

    def _keydict(k) -> dict:
        kt = k if isinstance(k, tuple) else (k,)
        return dict(zip(id_cols, kt))

    for k in groups_a.keys() - groups_b.keys():
        out.append(Discrepancy(dataset, "missing_in_b", _keydict(k), "group present in A only"))
    for k in groups_b.keys() - groups_a.keys():
        out.append(Discrepancy(dataset, "missing_in_a", _keydict(k), "group present in B only"))

    for k in groups_a.keys() & groups_b.keys():
        ks = _ks_distance(groups_a[k], groups_b[k])
        if ks > ks_eps:
            out.append(Discrepancy(dataset, "ks", _keydict(k),
                                   f"KS distance {ks:.4f} > {ks_eps}", ks))
    return out


def compare_dataset(name: str, df_a: pd.DataFrame, df_b: pd.DataFrame) -> list[Discrepancy]:
    """Dispatch to the right comparator for a registered dataset name."""
    mode = DATASETS[name].mode
    if mode == "tabular":
        return compare_tabular(df_a, df_b, dataset=name)
    if mode == "distribution":
        return compare_distribution(df_a, df_b, dataset=name)
    raise ValueError(f"unknown mode {mode!r} for dataset {name!r}")


def _read_dataset(root: Path, relpath: str) -> Optional[pd.DataFrame]:
    """Read a (possibly hive-partitioned) parquet dataset; None if absent."""
    path = root / relpath
    if not path.exists():
        return None
    return pd.read_parquet(path)


def compare_outputs(
    dir_a,
    dir_b,
    datasets: Optional[Iterable[str]] = None,
) -> OEReport:
    """Compare two engine output trees. Each tree root contains ``Summary/<dataset>``."""
    root_a, root_b = Path(dir_a), Path(dir_b)
    names = list(datasets) if datasets is not None else list(DATASETS)
    report = OEReport()

    for name in names:
        ds = DATASETS[name]
        df_a = _read_dataset(root_a, ds.relpath)
        df_b = _read_dataset(root_b, ds.relpath)
        if df_a is None and df_b is None:
            continue  # neither run produced it — nothing to compare
        if df_a is None or df_b is None:
            missing = "a" if df_a is None else "b"
            report.discrepancies.append(
                Discrepancy(name, f"missing_in_{missing}", {}, "dataset absent in one tree"))
            report.datasets_compared.append(name)
            continue
        report.discrepancies.extend(compare_dataset(name, df_a, df_b))
        report.datasets_compared.append(name)

    return report


def assert_output_equivalent(dir_a, dir_b, datasets: Optional[Iterable[str]] = None) -> None:
    """Raise AssertionError with a readable diff if the two output trees are not OE."""
    report = compare_outputs(dir_a, dir_b, datasets=datasets)
    if not report.equivalent:
        raise AssertionError(report.summary())
