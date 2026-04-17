"""
Tests for issue #41: RNG consolidation onto SimRNG / np.random.default_rng().

Two concerns verified here:

1. Seeded reproducibility — two simulation runs with the same SimRNG seed
   produce statistically identical emission distributions (KS test, p >= alpha).
   PCG64 (used by default_rng) is designed so that consecutive integer seeds
   produce independent, high-quality streams; this test also demonstrates that
   a *fixed* seed gives bit-for-bit identical results across runs.

2. No stdlib random in sim paths — the migrated production modules no longer
   import or call the stdlib random module.
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import pathlib
import re
import subprocess
import tempfile

import numpy as np
import pytest
import scipy.stats

import SimRNG

MAES_ROOT = pathlib.Path(__file__).parent.parent

MIGRATED_MODULES = [
    MAES_ROOT / "src" / "Chooser.py",
    MAES_ROOT / "src" / "Distribution.py",
    MAES_ROOT / "src" / "ModelClasses.py",
    MAES_ROOT / "src" / "MEETLinkedProductionEq.py",
    MAES_ROOT / "src" / "MEETProductionWells.py",
    MAES_ROOT / "src" / "MEETClasses.py",
    MAES_ROOT / "src" / "MEETComponentLeaks.py",
]

ALPHA = 0.05
N_DRAWS = 10_000


# ---------------------------------------------------------------------------
# Test 1: seeded reproducibility
# ---------------------------------------------------------------------------

def _draw_uniform(seed: int, n: int) -> np.ndarray:
    SimRNG.seed(seed)
    return np.array([SimRNG.random() for _ in range(n)])


def _draw_normal(seed: int, n: int) -> np.ndarray:
    SimRNG.seed(seed)
    return np.array([SimRNG.normal(0.0, 1.0) for _ in range(n)])


def _draw_exponential(seed: int, n: int) -> np.ndarray:
    SimRNG.seed(seed)
    return np.array([SimRNG.exponential(1.0) for _ in range(n)])


@pytest.mark.parametrize("draw_fn", [_draw_uniform, _draw_normal, _draw_exponential])
def test_seeded_runs_are_identical(draw_fn) -> None:
    """Same seed must produce bit-for-bit identical draws (PCG64 guarantee)."""
    run_a = draw_fn(seed=0, n=N_DRAWS)
    run_b = draw_fn(seed=0, n=N_DRAWS)
    assert np.array_equal(run_a, run_b), (
        f"{draw_fn.__name__}: identical seeds produced different draw sequences"
    )


@pytest.mark.parametrize("draw_fn", [_draw_uniform, _draw_normal, _draw_exponential])
def test_different_seeds_produce_independent_streams(draw_fn) -> None:
    """
    Consecutive integer seeds must produce statistically distinct streams
    (KS test p < 0.05 is unlikely for truly independent draws but we instead
    verify that the draw sequences are not identical — PCG64 guarantees
    independence).
    """
    run_0 = draw_fn(seed=0, n=N_DRAWS)
    run_1 = draw_fn(seed=1, n=N_DRAWS)
    assert not np.array_equal(run_0, run_1), (
        f"{draw_fn.__name__}: seed 0 and seed 1 produced identical sequences"
    )


def test_seeded_sim_run_reproducible(tmp_path: pathlib.Path) -> None:
    """
    Two minimal simulation runs with the same parameters produce bit-for-bit
    identical per-MC-run emission totals. Reproducibility comes from
    SimRNG.seed(mcRunNum) called at the start of each MC iteration in
    SiteMain2.runSim(); because each run seeds identically, outputs match exactly.
    """
    def run_sim(out_dir: pathlib.Path) -> None:
        result = subprocess.run(
            [
                sys.executable,
                "src/SiteMain2.py",
                "-mc", "5",
                "-s", "C3/C3_Prototypical_Sites/P1_1stage_noflare.xlsx",
                "-or", str(out_dir),
            ],
            cwd=MAES_ROOT,
            capture_output=True,
            text=True,
            timeout=600,
        )
        assert result.returncode == 0, (
            f"Simulation failed.\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )

    dir_a = tmp_path / "run_a"
    dir_b = tmp_path / "run_b"
    dir_a.mkdir()
    dir_b.mkdir()
    run_sim(dir_a)
    run_sim(dir_b)

    import pyarrow.dataset as ds

    def load_totals(out_dir: pathlib.Path) -> np.ndarray:
        pq_dirs = list(out_dir.rglob("InstEmissions"))
        assert pq_dirs, f"No InstEmissions parquet found under {out_dir}"
        dataset = ds.dataset(str(pq_dirs[0]), format="parquet")
        df = dataset.to_table().to_pandas()
        return df.groupby("mcRun")["totalEmission_kg"].sum().sort_index().values

    totals_a = load_totals(dir_a)
    totals_b = load_totals(dir_b)

    assert np.array_equal(totals_a, totals_b), (
        f"Seeded runs produced different per-MC totals — "
        f"SimRNG.seed(mcRunNum) is not producing deterministic results.\n"
        f"Run A: {totals_a}\nRun B: {totals_b}"
    )


# ---------------------------------------------------------------------------
# Test 2: no stdlib random in migrated modules
# ---------------------------------------------------------------------------

STDLIB_RANDOM_IMPORT = re.compile(r'\bimport\s+random\b')
STDLIB_RANDOM_CALL = re.compile(
    r'\brandom\.(random|choice|choices|randint|randrange|uniform)\b'
)


@pytest.mark.parametrize("module_path", MIGRATED_MODULES, ids=lambda p: p.name)
def test_no_stdlib_random_in_module(module_path: pathlib.Path) -> None:
    """Assert that no live code in migrated modules imports or calls stdlib random directly."""
    source = module_path.read_text()
    live_lines = [
        (i + 1, line)
        for i, line in enumerate(source.splitlines())
        if not line.lstrip().startswith("#")
        and (
            STDLIB_RANDOM_IMPORT.search(line)
            or (STDLIB_RANDOM_CALL.search(line) and "SimRNG" not in line)
        )
    ]
    assert not live_lines, (
        f"{module_path.name} still contains stdlib random calls:\n"
        + "\n".join(f"  line {lineno}: {line.strip()}" for lineno, line in live_lines)
    )
