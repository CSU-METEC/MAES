"""Input-root split — slow integration suite (real engine + Output Equivalence) — CSU-METEC/MAES #93.

Separated from test_input_root_split.py (the fast unit/getConfig suite) so the engine-driven
cases can be excluded from routine CI via `pytest -m 'not slow'` and run on a nightly/RC cadence.

Covers the realizable MAESTestPlan §9.5 integration cases against the P1 prototypical study:
  - behavior neutrality: an overlay serving an identical study copy ≡ single-root,
  - GC shadowing (D-S2 / C2): an overlaid GC composition is consumed end-to-end and changes the
    output (the studyInputRoot copy wins),
  - curatedRoot immutability (F5),
  - a malformed overlay study errors out (E1).

Scope notes:
  - The "studyInputRoot wins for the study *file*" property is covered at the resolution level by
    test_input_root_split.test_getconfig_overlay_picks_studyinputroot and end-to-end by the
    neutrality case here. A dedicated different-content study-file shadowing case is omitted: no
    simple study-parameter change (component count, GOR, production rate) perturbs the OE-compared
    datasets at this scale/seed, and substituting a different prototypical site fails on its own
    GC references — so GC shadowing is the reliable "overlay content drives output" case.
  - Factors-override shadowing is out of scope (the P1 study sets no `Activity / Emission Factor
    Name`, so there is no override to shadow). Bundle parity / concurrency (F6/F9) and directory
    mode (CSU-METEC/MAES #97) are tracked elsewhere.
"""

import os
import sys
import csv
import shutil
import pathlib
import subprocess

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

MAES_ROOT = pathlib.Path(__file__).parent.parent
STUDY = "C3/C3_Prototypical_Sites/P1_1stage_noflare.xlsx"
# The 1-stage GC composition the P1 study references (facility productionGC + well flowGC),
# expressed relative to curatedRoot — the path the overlay rebases onto studyInputRoot.
GC_REL = "Studies/C3/C3_Prototypical_Sites/CompositionsC3_1stage.csv"
OE_DATASETS = ["SiteSummary", "SimSummary", "PDF", "SimPDF"]

pytestmark = pytest.mark.slow


def _run(out_dir: pathlib.Path, study_input_root=None) -> pathlib.Path:
    """Run one engine simulation (mc=3, 7 days, seed 42); return its parquet root."""
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


def _stage_study(root: pathlib.Path) -> pathlib.Path:
    """Copy the curated study into root/Studies/<STUDY> so an overlay can serve it."""
    dst = root / "Studies" / STUDY
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(MAES_ROOT / "input" / "Studies" / STUDY, dst)
    return dst


def _stage_modified_gc(root: pathlib.Path) -> pathlib.Path:
    """Copy the curated GC composition into the overlay with the Methane column of its data table
    doubled — same structure (FluidFlow names/units preserved, so conversions still resolve) but a
    deliberately different composition, so the output must change if the overlay copy is consumed."""
    dst = root / GC_REL
    dst.parent.mkdir(parents=True, exist_ok=True)
    rows = list(csv.reader(open(MAES_ROOT / "input" / GC_REL)))
    in_table = False
    methane_idx = None
    for row in rows:
        if row and row[0].startswith("%%%ENDOFMETADATA%%%"):
            in_table = True
            continue
        if in_table and methane_idx is None and "Methane" in row:
            methane_idx = row.index("Methane")
            continue
        if in_table and methane_idx is not None and len(row) > methane_idx:
            try:
                row[methane_idx] = str(float(row[methane_idx]) * 2.0)
            except ValueError:
                pass
    with open(dst, "w", newline="") as fh:
        csv.writer(fh).writerows(rows)
    return dst


def _manifest(root: pathlib.Path) -> dict:
    """Map every file under root to (size, mtime_ns) for an immutability snapshot."""
    out = {}
    for p in sorted(pathlib.Path(root).rglob("*")):
        if p.is_file():
            st = p.stat()
            out[str(p)] = (st.st_size, st.st_mtime_ns)
    return out


@pytest.fixture(scope="session")
def curated_baseline(tmp_path_factory):
    """A single curated (no-overlay) P1 run, shared by the equivalence/shadowing comparisons."""
    return _run(tmp_path_factory.mktemp("curated_baseline"))


def test_overlay_identical_content_is_output_equivalent(curated_baseline, tmp_path):
    """An overlay serving an identical study copy is output-equivalent to the single-root run."""
    from Testing import OutputEquivalence as oe
    sir = tmp_path / "sir"
    _stage_study(sir)
    overlay = _run(tmp_path / "overlay", study_input_root=sir)
    rep = oe.compare_outputs(curated_baseline, overlay, datasets=OE_DATASETS)
    assert rep.datasets_compared, "OE compared no datasets — output layout may have changed"
    assert rep.equivalent, f"overlay (identical content) should match single-root:\n{rep.summary()}"


def test_gc_overlay_is_consumed_and_changes_output(curated_baseline, tmp_path):
    """An overlaid GC composition (Methane doubled) is consumed end-to-end and changes the output,
    proving the studyInputRoot copy wins over the curatedRoot one (D-S2 / C2)."""
    from Testing import OutputEquivalence as oe
    sir = tmp_path / "sir"
    _stage_modified_gc(sir)
    overlay = _run(tmp_path / "overlay", study_input_root=sir)
    rep = oe.compare_outputs(curated_baseline, overlay, datasets=OE_DATASETS)
    assert not rep.equivalent, "overlaid GC composition should change output (GC overlay not applied?)"


def test_curatedroot_immutable_after_run(tmp_path):
    """A run must not write anything under curatedRoot (input/) — F5."""
    curated = MAES_ROOT / "input"
    before = _manifest(curated)
    _run(tmp_path / "immut")
    after = _manifest(curated)
    assert before == after, "curatedRoot (input/) changed during a run — it must be immutable"


def test_malformed_study_in_overlay_errors(tmp_path):
    """A malformed study served from studyInputRoot fails with a nonzero exit (E1)."""
    sir = tmp_path / "sir"
    dst = sir / "Studies" / STUDY
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_bytes(b"this is not a valid xlsx file")
    out = tmp_path / "out"
    out.mkdir()
    cmd = [sys.executable, "src/SiteMain2.py", "-mc", "1", "-t", "1", "-rs", "42",
           "-s", STUDY, "-or", str(out), "-sir", str(sir)]
    r = subprocess.run(cmd, cwd=MAES_ROOT, capture_output=True, text=True, timeout=300)
    assert r.returncode != 0, f"malformed overlay study should fail, got exit 0\nSTDOUT:\n{r.stdout}"
