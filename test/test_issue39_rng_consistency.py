"""
Regression test for GitHub issue #39 — RNG mismatch in MEETComponentLeaks.

MEETComponentLeaks.pickFromMTTR and calcLeakList must draw from np.random,
not Python's stdlib random module. These tests verify determinism by seeding
np.random and confirming that two runs with the same seed produce identical
results. If the functions use stdlib random (which is not seeded), results
will differ across runs.

RED:   before fix — functions use random.random(); np.random seeding has no
       effect; results differ across identically-seeded runs.
GREEN: after fix  — functions use np.random.random(); seeding np.random
       produces identical results across runs.
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import numpy as np
import pytest
import MEETComponentLeaks as mcl


SEED = 42
N_DRAWS = 200
SCALE = float(3600 * 24 * 30)   # 30 days in seconds — representative MTTR/MTBF scale


class _Dummy:
    """Minimal stand-in for self; pickFromMTTR and calcLeakList access no instance state."""

    def pickFromMTTR(self, num: float) -> int:
        """Forward to the real implementation."""
        ret = mcl.ComponentLeaks.pickFromMTTR(self, num)
        return ret


def _pickFromMTTR(scale: float) -> int:
    """Invoke ComponentLeaks.pickFromMTTR with a dummy self."""
    ret = mcl.ComponentLeaks.pickFromMTTR(_Dummy(), scale)
    return ret


def _drawN(n: int, seed: int) -> list:
    """Seed np.random then draw n values via pickFromMTTR."""
    np.random.seed(seed)
    ret = list(map(lambda _: _pickFromMTTR(SCALE), range(n)))
    return ret


def _calcLeakListTrials(n: int, seed: int) -> list:
    """Seed np.random then run calcLeakList n times, returning leak counts."""
    PPLEAK = 0.1
    MTBF_HOURS = 24.0 * 30
    MTTR_HOURS = 24.0 * 7
    TMAX_S = 365 * 24 * 3600
    np.random.seed(seed)
    ret = list(map(
        lambda _: len(mcl.ComponentLeaks.calcLeakList(
            _Dummy(),
            tMax=TMAX_S,
            pLeak=PPLEAK,
            MTBF_hours=MTBF_HOURS,
            MTTR_hours=MTTR_HOURS,
        )),
        range(n)
    ))
    return ret


def test_pickFromMTTR_deterministic_under_numpy_seed() -> None:
    """
    Two runs of pickFromMTTR with the same np.random seed must produce
    identical results. Fails if the implementation uses stdlib random,
    which is unaffected by np.random.seed().
    """
    run_a = _drawN(N_DRAWS, SEED)
    run_b = _drawN(N_DRAWS, SEED)
    assert run_a == run_b, (
        "pickFromMTTR is not deterministic under np.random seeding — "
        "implementation is using stdlib random instead of np.random.random()"
    )


def test_calcLeakList_deterministic_under_numpy_seed() -> None:
    """
    Two sets of calcLeakList calls with the same np.random seed must produce
    identical per-trial leak counts. Fails if the implementation uses stdlib
    random, which is unaffected by np.random.seed().
    """
    run_a = _calcLeakListTrials(N_DRAWS, SEED)
    run_b = _calcLeakListTrials(N_DRAWS, SEED)
    assert run_a == run_b, (
        "calcLeakList is not deterministic under np.random seeding — "
        "implementation is using stdlib random instead of np.random.random()"
    )
