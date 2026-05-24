"""Seeding-contract integration tests (I1–I6) — CSU-METEC/MAES #96.

Replaces the original single same/different-seed test. Each engine configuration runs
exactly once (session-scoped fixtures) and pairs are compared with the Output Equivalence
oracle.

Contract:
  --randomSeed N  -> deterministic: identical output for any worker count and across runs.
  no seed         -> entropy-based: differs every run (serial or parallel); effective seed logged.

I1–I3 (seeded determinism) hold today. I4–I6 (entropy default + replay) encode the
not-yet-implemented behavior and are marked xfail(strict) against #96 — they will flip to
XPASS (forcing removal of the marker) once entropy-by-default + seed logging land.

Slow: runs several small simulations. Requires the engine's runtime deps (run in the MAES
conda env).
"""

import os
import re
import sys
import pathlib
import subprocess
from dataclasses import dataclass

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from Testing import OutputEquivalence as oe

MAES_ROOT = pathlib.Path(__file__).parent.parent
STUDY = "C3/C3_Prototypical_Sites/P1_1stage_noflare.xlsx"
MC_ITERATIONS = 3
SIM_DAYS = 7
# Datasets with well-defined OE semantics (summary stats + distributions). EventSummary is
# excluded: its value columns aren't in the oracle's tabular value set yet.
OE_DATASETS = ["SiteSummary", "SimSummary", "PDF", "SimPDF"]

PENDING_96 = "CSU-METEC/MAES#96: entropy-by-default + seed logging not yet implemented"

pytestmark = pytest.mark.slow


@dataclass
class Run:
    root: pathlib.Path
    stdout: str
    stderr: str


def _run(out_dir: pathlib.Path, seed=None, workers: int = 1) -> Run:
    """Run one engine simulation; omit -rs entirely when seed is None (entropy/default path)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable, "src/SiteMain2.py",
        "-mc", str(MC_ITERATIONS),
        "-t", str(SIM_DAYS),
        "-s", STUDY,
        "-or", str(out_dir),
        "-w", str(workers),
    ]
    if seed is not None:
        cmd += ["-rs", str(seed)]
    r = subprocess.run(cmd, cwd=MAES_ROOT, capture_output=True, text=True, timeout=900)
    assert r.returncode == 0, (
        f"simulation failed (seed={seed}, workers={workers}).\n"
        f"STDOUT:\n{r.stdout}\nSTDERR:\n{r.stderr}"
    )
    return Run(_parquet_root(out_dir), r.stdout, r.stderr)


def _parquet_root(out_dir: pathlib.Path) -> pathlib.Path:
    """Locate the `.../parquet` dir (the OE tree root, parent of `Summary/`)."""
    candidates = [d for d in out_dir.rglob("Summary")
                  if d.is_dir() and d.parent.name == "parquet"]
    assert candidates, f"no parquet/Summary tree found under {out_dir}"
    return candidates[0].parent


def _compare(a: Run, b: Run):
    rep = oe.compare_outputs(a.root, b.root, datasets=OE_DATASETS)
    assert rep.datasets_compared, "OE compared no datasets — engine output layout may have changed"
    return rep


# --------------------------------------------------------------------------- run-once fixtures
# Each engine configuration is executed exactly once per session; tests below read the
# cached output roots. Keeps the suite at one simulation per distinct configuration.

@pytest.fixture(scope="session")
def seed42_a(tmp_path_factory):
    return _run(tmp_path_factory.mktemp("seed42_a"), seed=42, workers=1)


@pytest.fixture(scope="session")
def seed42_b(tmp_path_factory):
    return _run(tmp_path_factory.mktemp("seed42_b"), seed=42, workers=1)


@pytest.fixture(scope="session")
def seed42_w4(tmp_path_factory):
    return _run(tmp_path_factory.mktemp("seed42_w4"), seed=42, workers=4)


@pytest.fixture(scope="session")
def seed43_a(tmp_path_factory):
    return _run(tmp_path_factory.mktemp("seed43_a"), seed=43, workers=1)


@pytest.fixture(scope="session")
def noseed_a(tmp_path_factory):
    return _run(tmp_path_factory.mktemp("noseed_a"), seed=None, workers=1)


@pytest.fixture(scope="session")
def noseed_b(tmp_path_factory):
    return _run(tmp_path_factory.mktemp("noseed_b"), seed=None, workers=1)


@pytest.fixture(scope="session")
def noseed_w4_a(tmp_path_factory):
    return _run(tmp_path_factory.mktemp("noseed_w4_a"), seed=None, workers=4)


@pytest.fixture(scope="session")
def noseed_w4_b(tmp_path_factory):
    return _run(tmp_path_factory.mktemp("noseed_w4_b"), seed=None, workers=4)


# --------------------------------------------------------------------------- I1–I3: seeded (green)

def test_I1_same_seed_serial_equivalent(seed42_a, seed42_b):
    """Two serial runs with --randomSeed 42 are output-equivalent."""
    rep = _compare(seed42_a, seed42_b)
    assert rep.equivalent, f"same seed, serial, should be equivalent:\n{rep.summary()}"


def test_I2_worker_independence(seed42_a, seed42_w4):
    """--randomSeed 42 gives the same result regardless of worker count (-w 1 vs -w 4)."""
    rep = _compare(seed42_a, seed42_w4)
    assert rep.equivalent, f"same seed across worker counts should be equivalent:\n{rep.summary()}"


def test_I3_seed_sensitivity(seed42_a, seed43_a):
    """Different seeds (42 vs 43) produce non-equivalent output (the seed actually matters)."""
    rep = _compare(seed42_a, seed43_a)
    assert not rep.equivalent, "runs with --randomSeed 42 vs 43 were equivalent — seed had no effect"


# --------------------------------------------------------------------------- I4–I6: entropy (red)

# Matches a future "effective (random) seed: NNN" log line emitted by the entropy-default path.
_SEED_LOG_RE = re.compile(r"(?:effective|base)\s+(?:random\s+)?seed\s*[:=]\s*(\d+)", re.I)


@pytest.mark.xfail(reason=PENDING_96, strict=True)
def test_I4_entropy_default_serial_differs(noseed_a, noseed_b):
    """With no seed, two serial runs of the same simulation must differ."""
    rep = _compare(noseed_a, noseed_b)
    assert not rep.equivalent, "two no-seed serial runs were equivalent — default is not entropy-based"


@pytest.mark.xfail(reason=PENDING_96, strict=True)
def test_I5_entropy_default_parallel_differs(noseed_w4_a, noseed_w4_b):
    """With no seed, two parallel runs of the same simulation must differ."""
    rep = _compare(noseed_w4_a, noseed_w4_b)
    assert not rep.equivalent, "two no-seed parallel runs were equivalent — default is not entropy-based"


@pytest.mark.xfail(reason=PENDING_96, strict=True)
def test_I6_logged_seed_replay(noseed_a, tmp_path_factory):
    """A no-seed run logs its effective seed; replaying that seed reproduces the run."""
    m = _SEED_LOG_RE.search(noseed_a.stdout) or _SEED_LOG_RE.search(noseed_a.stderr)
    assert m, "no effective seed was logged by a no-seed run"
    replay = _run(tmp_path_factory.mktemp("replay"), seed=int(m.group(1)), workers=1)
    rep = _compare(noseed_a, replay)
    assert rep.equivalent, f"replaying the logged seed did not reproduce the run:\n{rep.summary()}"
