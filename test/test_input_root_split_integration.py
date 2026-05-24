"""Input-root split — slow integration suite (real engine + Output Equivalence) — CSU-METEC/MAES #93.

Separated from test_input_root_split.py (the fast unit/getConfig suite) so the engine-driven
cases can be excluded from routine CI via `pytest -m 'not slow'` and run on a nightly/RC cadence.

Currently holds the behavior-neutrality case; shadowing (D-S), GC/factors end-to-end, immutability
(F5), and the error cases (E1–E5) from MAESTestPlan §9.5 land here as they are built.
"""

import os
import sys
import shutil
import pathlib
import subprocess

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

MAES_ROOT = pathlib.Path(__file__).parent.parent
STUDY = "C3/C3_Prototypical_Sites/P1_1stage_noflare.xlsx"
OE_DATASETS = ["SiteSummary", "SimSummary", "PDF", "SimPDF"]

pytestmark = pytest.mark.slow


def _stage_study(root: pathlib.Path) -> pathlib.Path:
    """Copy the curated study into root/Studies/<STUDY> so an overlay can serve it."""
    dst = root / "Studies" / STUDY
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(MAES_ROOT / "input" / "Studies" / STUDY, dst)
    return dst


def _run(out_dir: pathlib.Path, study_input_root=None) -> pathlib.Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, "src/SiteMain2.py", "-mc", "3", "-t", "7", "-rs", "42",
           "-s", STUDY, "-or", str(out_dir)]
    if study_input_root is not None:
        cmd += ["-sir", str(study_input_root)]
    r = subprocess.run(cmd, cwd=MAES_ROOT, capture_output=True, text=True, timeout=900)
    assert r.returncode == 0, f"engine failed:\nSTDOUT:\n{r.stdout}\nSTDERR:\n{r.stderr}"
    candidates = [d for d in out_dir.rglob("Summary") if d.is_dir() and d.parent.name == "parquet"]
    assert candidates, f"no parquet/Summary tree under {out_dir}"
    return candidates[0].parent


def test_overlay_identical_content_is_output_equivalent(tmp_path):
    """A studyInputRoot serving an identical copy of the study yields output equivalent to the
    single-root run — the overlay path is exercised end-to-end and is behavior-neutral."""
    from Testing import OutputEquivalence as oe
    sir = tmp_path / "sir"
    _stage_study(sir)
    plain = _run(tmp_path / "plain")
    overlay = _run(tmp_path / "overlay", study_input_root=sir)
    rep = oe.compare_outputs(plain, overlay, datasets=OE_DATASETS)
    assert rep.datasets_compared, "OE compared no datasets — output layout may have changed"
    assert rep.equivalent, f"overlay (identical content) should match single-root:\n{rep.summary()}"
