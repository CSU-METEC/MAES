"""
SimRNG: single simulation-wide random number generator.

All simulation code draws from this module so that:
  - Only one RNG is in play at any time (no np.random / random.stdlib split)
  - Each MC run can be seeded independently via seed(mcRunNum)
  - Consecutive integer seeds are safe: PCG64 is designed so that nearby seeds
    produce statistically independent streams, unlike the legacy Mersenne Twister
    where correlated streams were a known hazard.

Usage:
    import SimRNG
    SimRNG.seed(mcRunNum)   # call once at the top of each MC iteration
    x = SimRNG.random()     # scalar uniform draw on [0, 1)
"""
import numpy as np

_rng: np.random.Generator = np.random.default_rng()


def seed(s=None) -> None:
    global _rng
    _rng = np.random.default_rng(s)


def random() -> float:
    return float(_rng.random())


def uniform(low: float, high: float) -> float:
    return float(_rng.uniform(low, high))


def normal(loc: float, scale: float) -> float:
    return float(_rng.normal(loc, scale))


def lognormal(mean: float, sigma: float) -> float:
    return float(_rng.lognormal(mean, sigma))


def triangular(left: float, mode: float, right: float) -> float:
    return float(_rng.triangular(left, mode, right))


def exponential(scale: float) -> float:
    return float(_rng.exponential(scale))


def choice(seq):
    idx = int(_rng.integers(0, len(seq)))
    return seq[idx]


def randint(low: int, high: int) -> int:
    """Return a random integer N such that low <= N <= high (inclusive)."""
    return int(_rng.integers(low, high + 1))


def randrange(start: int, stop: int) -> int:
    """Return a random integer N such that start <= N < stop."""
    return int(_rng.integers(start, stop))


def choices(population, weights=None, k=1):
    """Weighted random sample with replacement; mirrors stdlib random.choices."""
    if weights is not None:
        total = sum(weights)
        probs = [w / total for w in weights]
    else:
        probs = None
    indices = _rng.choice(len(population), size=k, replace=True, p=probs)
    return [population[i] for i in indices]
