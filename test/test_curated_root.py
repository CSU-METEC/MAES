"""curatedRoot validity checks — CSU-METEC/MAES #95 (criterion 2: per-file validity).

These assert that the curated/reference tree is well-formed without running the engine:
every ModelFormulation definition parses and carries its required keys, the reference
Factors file is structurally sound, and every curated CSV is parseable (a corruption /
truncation guard).

Scope note: "completeness" (which curated artifacts are *required* for a given study) is
data-driven and is deferred to the baseline-reproducibility check (#95 criterion 3, the
golden-master run), per decision. This file covers validity only.

Required keys / columns below were derived by inspecting the actual tree (all 20 JSONs
share these four keys; Factors.csv has these columns), not assumed.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import json
from pathlib import Path

import pandas as pd
import pytest

# The curated emission/activity files are not plain CSVs: they carry a metadata block
# (key,value rows) terminated by %%%ENDOFMETADATA%%%, then the data table. The engine
# reads them via EmitterProfile.readRawDistributionFile — so we validate the same way
# rather than with a naive pd.read_csv (which mis-parses the metadata section).
from EmitterProfile import readRawDistributionFile, METADATA_END_TOKEN

MAES_ROOT = Path(__file__).parent.parent
CURATED_ROOT = MAES_ROOT / "input"           # rename target of #93's curatedRoot
MODEL_FORMULATION_DIR = CURATED_ROOT / "ModelFormulation"
CURATED_DATA_DIR = CURATED_ROOT / "CuratedData"
FACTORS_CSV = CURATED_DATA_DIR / "FactorsFileReference" / "Factors.csv"

# Keys present in ALL 20 ModelFormulation JSONs (verified against the tree).
MODEL_REQUIRED_KEYS = {"Model Parameters", "Python Category", "Python Class", "Readable Name"}

# Core columns the factor-lookup path relies on (verified against Factors.csv).
FACTORS_REQUIRED_COLS = {
    "MajorEquipment", "emitterModelFactorTag", "factorTag",
    "Emitter", "activityDistribution", "emissionDriver",
}

_MODEL_JSONS = sorted(MODEL_FORMULATION_DIR.glob("*.json"))
_CURATED_CSVS = sorted(CURATED_DATA_DIR.rglob("*.csv"))


# --------------------------------------------------------------------------- A: ModelFormulation

def test_model_formulation_dir_nonempty():
    assert _MODEL_JSONS, f"no ModelFormulation JSONs under {MODEL_FORMULATION_DIR}"


@pytest.mark.parametrize("json_path", _MODEL_JSONS, ids=lambda p: p.name)
def test_model_formulation_json_parses_and_has_required_keys(json_path):
    """Each model definition parses and carries the four universally-required keys."""
    try:
        data = json.loads(json_path.read_text())
    except json.JSONDecodeError as e:
        pytest.fail(f"{json_path.name} is not valid JSON: {e}")
    assert isinstance(data, dict), f"{json_path.name} is not a JSON object"
    missing = MODEL_REQUIRED_KEYS - set(data)
    assert not missing, f"{json_path.name} missing required keys: {sorted(missing)}"
    assert str(data["Python Class"]).strip(), f"{json_path.name} has empty 'Python Class'"


# --------------------------------------------------------------------------- B: Factors file

def test_factors_file_exists_and_valid():
    assert FACTORS_CSV.exists(), f"missing reference factors file: {FACTORS_CSV}"
    df = pd.read_csv(FACTORS_CSV)
    assert len(df) > 0, "Factors.csv parsed but has no rows"
    missing = FACTORS_REQUIRED_COLS - set(df.columns)
    assert not missing, f"Factors.csv missing required columns: {sorted(missing)}"


# --------------------------------------------------------------------------- C: CuratedData CSV integrity

def test_curated_data_has_csvs():
    assert _CURATED_CSVS, f"no CuratedData CSVs found under {CURATED_DATA_DIR}"


def _has_metadata_token(path: Path) -> bool:
    with open(path, "r") as fh:
        return any(line.startswith(METADATA_END_TOKEN) for line in fh)


def test_all_curated_csvs_parse():
    """Every curated CSV must read the way the engine reads it (corruption/truncation guard).

    Files carrying the %%%ENDOFMETADATA%%% sentinel are validated via the engine's
    readRawDistributionFile (metadata key,value block + data table); the rare plain CSVs
    are validated with pd.read_csv. Reported as one test that collects all failures so a
    bad file names itself. ~1150 small files; runs in a few seconds.
    """
    failures = []
    for csv in _CURATED_CSVS:
        try:
            if _has_metadata_token(csv):
                md, df = readRawDistributionFile(csv)
                if not md:
                    failures.append((csv, "no metadata rows before %%%ENDOFMETADATA%%%"))
                elif df.shape[1] == 0:
                    failures.append((csv, "metadata parsed but data section has no columns"))
            else:
                df = pd.read_csv(csv)
                if df.shape[1] == 0:
                    failures.append((csv, "parsed but has no columns"))
        except Exception as e:  # ParserError, IndexError (ragged metadata), EmptyDataError, ...
            failures.append((csv, f"{type(e).__name__}: {e}"))
    assert not failures, (
        f"{len(failures)} invalid curated CSV(s) (showing up to 50):\n"
        + "\n".join(f"  {c.relative_to(CURATED_ROOT)}: {msg}" for c, msg in failures[:50])
    )
