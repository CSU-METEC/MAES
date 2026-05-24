"""Input-root split tests (curatedRoot / studyInputRoot overlay) — CSU-METEC/MAES #93.

Fast layer (no engine): the resolveInputRef resolution matrix and the getConfig study-file
overlay + root-alias precedence. Slow layer: one Output-Equivalence integration test asserting
that an overlay whose content is identical to curated produces the same output as a single-root
run (the overlay machinery is behavior-neutral when content matches).
"""

import os
import sys
import shutil
import pathlib
import subprocess

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import AppUtils as au

MAES_ROOT = pathlib.Path(__file__).parent.parent
STUDY = "C3/C3_Prototypical_Sites/P1_1stage_noflare.xlsx"
OE_DATASETS = ["SiteSummary", "SimSummary", "PDF", "SimPDF"]


def _stage_study(root: pathlib.Path) -> pathlib.Path:
    """Copy the curated study into root/Studies/<STUDY> so an overlay can serve it."""
    dst = root / "Studies" / STUDY
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(MAES_ROOT / "input" / "Studies" / STUDY, dst)
    return dst


# --------------------------------------------------------------------------- resolveInputRef unit

def test_resolve_noop_when_roots_equal():
    assert str(au.resolveInputRef("input/Studies/x.xlsx", "input", "input")) == "input/Studies/x.xlsx"


def test_resolve_absolute_path_bypasses():
    assert str(au.resolveInputRef("/etc/hostname", "input", "/overlay")) == "/etc/hostname"


def test_resolve_bare_ref_overlay_present(tmp_path):
    (tmp_path / "gc").mkdir()
    (tmp_path / "gc" / "x.csv").write_text("a")
    assert str(au.resolveInputRef("gc/x.csv", "input", str(tmp_path))) == str(tmp_path / "gc" / "x.csv")


def test_resolve_bare_ref_overlay_absent_falls_through(tmp_path):
    assert str(au.resolveInputRef("gc/missing.csv", "input", str(tmp_path))) == "gc/missing.csv"


def test_resolve_templated_ref_overlay_present(tmp_path):
    (tmp_path / "Studies").mkdir()
    (tmp_path / "Studies" / "s.xlsx").write_text("a")
    got = au.resolveInputRef("input/Studies/s.xlsx", "input", str(tmp_path))
    assert str(got) == str(tmp_path / "Studies" / "s.xlsx")


def test_resolve_templated_ref_overlay_absent_keeps_curated(tmp_path):
    assert str(au.resolveInputRef("input/Studies/none.xlsx", "input", str(tmp_path))) == "input/Studies/none.xlsx"


# --------------------------------------------------------------------------- getConfig overlay (fast)

@pytest.fixture
def at_maes_root(monkeypatch):
    monkeypatch.chdir(MAES_ROOT)


def test_getconfig_overlay_picks_studyinputroot(at_maes_root, tmp_path):
    _stage_study(tmp_path)
    cm, _ = au.getConfig(commandArgs=["-s", STUDY, "-sir", str(tmp_path)])
    assert cm.getConfigVar("studyFilename").startswith(str(tmp_path))


def test_getconfig_overlay_absent_falls_back_to_curated(at_maes_root, tmp_path):
    cm, _ = au.getConfig(commandArgs=["-s", STUDY, "-sir", str(tmp_path)])
    assert cm.getConfigVar("studyFilename") == f"input/Studies/{STUDY}"


def test_getconfig_default_is_single_root(at_maes_root):
    cm, _ = au.getConfig(commandArgs=["-s", STUDY])
    assert cm.getConfigVar("curatedRoot") == "input"
    assert cm.getConfigVar("studyInputRoot") == "input"
    assert cm.getConfigVar("studyFilename") == f"input/Studies/{STUDY}"


def test_getconfig_inputroot_alias_seeds_both(at_maes_root, tmp_path):
    _stage_study(tmp_path)
    cm, _ = au.getConfig(commandArgs=["-s", STUDY, "-i", str(tmp_path)])
    assert cm.getConfigVar("curatedRoot") == str(tmp_path)
    assert cm.getConfigVar("studyInputRoot") == str(tmp_path)


def test_getconfig_explicit_curated_and_studyinput(at_maes_root, tmp_path):
    cm, _ = au.getConfig(commandArgs=["-s", STUDY, "-cr", "input", "-sir", str(tmp_path)])
    assert cm.getConfigVar("curatedRoot") == "input"
    assert cm.getConfigVar("studyInputRoot") == str(tmp_path)


# --------------------------------------------------------------------------- integration (slow)

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


@pytest.mark.slow
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
