"""
Smoke tests that verify MAES is installed correctly.
"""
import importlib
import importlib.metadata
import pathlib
import re
import subprocess
import sys

import pytest

MAES_ROOT = pathlib.Path(__file__).parent.parent

MAES_MODULES = [
    "SiteMain2",
    "AppUtils",
    "SimDataManager",
    "ConfigManager",
    "Distribution",
    "Timeseries",
]


def parseRequirements() -> list[str]:
    """Return package names from requirements.txt with version specifiers stripped."""
    lines = (MAES_ROOT / "requirements.txt").read_text().splitlines()
    nonEmpty = filter(lambda l: l.strip() and not l.strip().startswith("#"), lines)
    return list(map(lambda l: re.split(r"[><=!~]", l)[0].strip(), nonEmpty))


REQUIRED_PACKAGES = parseRequirements()


@pytest.mark.parametrize("package", REQUIRED_PACKAGES)
def test_dependency_installed(package: str) -> None:
    """Verify that each package in requirements.txt is installed."""
    version = importlib.metadata.version(package)
    assert version is not None


@pytest.mark.parametrize("module", MAES_MODULES)
def test_maes_module_importable(module: str) -> None:
    """Verify that each key MAES source module can be imported."""
    mod = importlib.import_module(module)
    assert mod is not None


def test_minimal_run(tmp_path: pathlib.Path) -> None:
    """Run a single-iteration simulation and verify it exits cleanly."""
    result = subprocess.run(
        [
            sys.executable,
            "src/SiteMain2.py",
            "-mc", "1",
            "-s", "C3/C3_Prototypical_Sites/P1_1stage_noflare.xlsx",
            "-or", str(tmp_path),
        ],
        cwd=MAES_ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, (
        f"MAES simulation failed.\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )
