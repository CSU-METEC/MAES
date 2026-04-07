import sys
import inspect
import json
import logging
import argparse
from pathlib import Path
from typing import Any

import pandas as pd

VERSION = "0.1.0"

LOG_PREFIX_FMT = "%(asctime)s %(process)d %(thread)d"
LOG_DATEFMT = "%Y-%m-%dT%H:%M:%S%z"

SCRIPT_DIR = Path(__file__).resolve().parent
MAES_ROOT = SCRIPT_DIR.parents[2]
MAES_SRC = MAES_ROOT / "src"
MODEL_FORMULATION_DIR = MAES_ROOT / "input" / "ModelFormulation"

sys.path.insert(0, str(MAES_SRC))

import GitVersion as gv
import EquipmentTable as et
import MEETClasses
import MEETProductionWells
import MEETLinkedProductionEq
import MEETComponentLeaks
import MEETIntermittentPneumatic
import MEETSamples
import MEETTestEquipment
import MEETFFClasses
import ModelClasses   # also pulls in MEET_1_Compatability and MEETFluidFlow transitively


def getKwargsForClass(cls: type) -> dict[str, dict[str, Any]]:
    """Walk the MRO of cls and return a flat dict of all accepted kwargs.

    Each entry maps kwarg name to a dict with keys hasDefault, default, and
    inheritedFrom. Child class definitions take precedence over ancestors;
    *args and **kwargs sentinels are excluded.
    """
    seen: dict[str, dict[str, Any]] = {}
    for ancestor in cls.__mro__:
        if ancestor is object:
            continue
        if '__init__' not in ancestor.__dict__:
            continue
        try:
            sig = inspect.signature(ancestor.__init__)
        except (ValueError, TypeError):
            logging.warning(f"Could not inspect {ancestor.__module__}.{ancestor.__name__}.__init__")
            continue
        for name, param in sig.parameters.items():
            if name == 'self':
                continue
            if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
                continue
            if name in seen:
                continue
            hasDefault = param.default is not inspect.Parameter.empty
            if hasDefault:
                default = repr(param.default)
            else:
                default = ''
            if ancestor is cls:
                inheritedFrom = ''
            else:
                inheritedFrom = f"{ancestor.__module__}.{ancestor.__name__}"
            seen[name] = {
                'hasDefault': hasDefault,
                'default': default,
                'inheritedFrom': inheritedFrom
            }
    ret = seen
    return ret


def buildAllClassMap() -> dict[str, dict[str, Any]]:
    """Build a combined classmap across all four MAES equipment categories.

    Merges the subclass maps for Facility, MajorEquipment, Emitter, and
    MEETService into a single dict keyed by class name. Requires all relevant
    MAES modules to have been imported first so __subclasses__() is populated.
    """
    allClasses: dict[str, dict[str, Any]] = {}
    allClasses.update(et.buildSubclassMap(et.Facility))
    allClasses.update(et.buildSubclassMap(et.MajorEquipment))
    allClasses.update(et.buildSubclassMap(et.Emitter))
    allClasses.update(et.buildSubclassMap(et.MEETService))
    ret = allClasses
    return ret


def buildKwargTable(allClasses: dict[str, dict[str, Any]], kwargTablePath: Path) -> pd.DataFrame:
    """Produce KwargTable.csv — one row per (class, kwarg) across all classes.

    Columns: sourceClass, className, kwargName, hasDefault, default, inheritedFrom.
    sourceClass is the module-qualified name of the leaf class being documented.
    inheritedFrom is the module-qualified ancestor that defines the kwarg, or
    empty if the kwarg is defined directly on the leaf class.
    """
    rows = []
    for className, classInfo in allClasses.items():
        cls = classInfo['class']
        sourceClass = f"{cls.__module__}.{cls.__name__}"
        kwargs = getKwargsForClass(cls)
        for kwargName, kwargInfo in kwargs.items():
            rows.append({
                'sourceClass': sourceClass,
                'className': className,
                'kwargName': kwargName,
                'hasDefault': kwargInfo['hasDefault'],
                'default': kwargInfo['default'],
                'inheritedFrom': kwargInfo['inheritedFrom']
            })
    df = pd.DataFrame(rows)
    df.to_csv(kwargTablePath, index=False)
    logging.info(f"KwargTable: {len(df)} rows → {kwargTablePath}")
    ret = df
    return ret


def extractParamRows(
    jsonData: dict,
    jsonFile: str,
    context: str,
    kwargTableSet: set[tuple[str, str]]
) -> list[dict]:
    """Extract one row per Model Parameter entry from a JSON object.

    Works for both top-level equipment definitions and nested emitter
    definitions. context is the Readable Name of the equipment or emitter,
    used to identify which definition a parameter belongs to in the output.
    inKwargTable is True if the pythonParameter exists in the kwarg table
    for the declared pythonClass.
    """
    rows = []
    pythonClass = jsonData.get('Python Class', '')
    for param in jsonData.get('Model Parameters', []):
        pythonParam = param.get('Python Parameter', '')
        paramType = param.get('Parameter Type', '')
        modelParam = param.get('Model Parameter', '')
        optional = param.get('Optional', 'False') == 'True'
        inKwargTable = (pythonClass, pythonParam) in kwargTableSet
        rows.append({
            'jsonFile': jsonFile,
            'pythonClass': pythonClass,
            'context': context,
            'modelParameter': modelParam,
            'pythonParameter': pythonParam,
            'parameterType': paramType,
            'optional': optional,
            'inKwargTable': inKwargTable
        })
    ret = rows
    return ret


def buildModelDefinitionMap(kwargDf: pd.DataFrame, modelDefMapPath: Path) -> pd.DataFrame:
    """Produce ModelDefinitionMap.csv — one row per model definition parameter.

    Reads all JSON files in MODEL_FORMULATION_DIR and processes both top-level
    equipment parameters and nested emitter parameters. The inKwargTable column
    flags any pythonParameter that does not exist in KwargTable.csv for the
    declared pythonClass, which is the basis for Pass A1 validation.
    """
    kwargTableSet = set(zip(kwargDf['className'], kwargDf['kwargName']))

    jsonFiles = sorted(MODEL_FORMULATION_DIR.glob('*.json'))
    allRows = []

    for jsonPath in jsonFiles:
        jsonFile = jsonPath.name
        with open(jsonPath) as f:
            jsonData = json.load(f)

        topContext = jsonData.get('Readable Name', jsonFile)
        allRows.extend(extractParamRows(jsonData, jsonFile, topContext, kwargTableSet))

        for emitter in jsonData.get('Emitters', []):
            emitterContext = emitter.get('Readable Name', 'Unknown Emitter')
            allRows.extend(extractParamRows(emitter, jsonFile, emitterContext, kwargTableSet))

    df = pd.DataFrame(allRows)
    df.to_csv(modelDefMapPath, index=False)
    logging.info(f"ModelDefinitionMap: {len(df)} rows → {modelDefMapPath}")
    ret = df
    return ret


def buildUnmappedKwargs(kwargDf: pd.DataFrame, modelDefDf: pd.DataFrame, unmappedKwargsPath: Path) -> pd.DataFrame:
    """Produce UnmappedKwargs.csv — required kwargs with no model definition entry.

    Joins KwargTable and ModelDefinitionMap on (className, kwargName) =
    (pythonClass, pythonParameter). Rows in KwargTable where hasDefault is False
    and no model definition entry exists represent required Python kwargs that
    cannot be satisfied from any site file, and are reported as Pass A2 warnings
    by runPassA().
    """
    mappedPairs = set(zip(modelDefDf['pythonClass'], modelDefDf['pythonParameter']))
    requiredKwargs = kwargDf[kwargDf['hasDefault'] == False].copy()
    unmapped = requiredKwargs[
        requiredKwargs.apply(lambda row: (row['className'], row['kwargName']) not in mappedPairs, axis=1)
    ]
    df = unmapped.reset_index(drop=True)
    df.to_csv(unmappedKwargsPath, index=False)
    logging.info(f"UnmappedKwargs: {len(df)} rows → {unmappedKwargsPath}")
    ret = df
    return ret


def runPassA(modelDefDf: pd.DataFrame, unmappedDf: pd.DataFrame) -> bool:
    """Run Pass A consistency checks between the static tables and report findings.

    Pass A1: model definition parameters whose Python kwarg does not exist in
    the kwarg table (inKwargTable == False) → logged as errors.
    Pass A2: required Python kwargs with no model definition entry
    (from UnmappedKwargs.csv) → logged as warnings.

    Returns True if any A1 errors were found.
    """
    a1Failures = modelDefDf[modelDefDf['inKwargTable'] == False]
    for _, row in a1Failures.iterrows():
        logging.error(f"[A1] {row['jsonFile']}: Python parameter '{row['pythonParameter']}' not found in {row['pythonClass']} kwargs")

    for _, row in unmappedDf.iterrows():
        logging.warning(f"[A2] {row['className']}: required kwarg '{row['kwargName']}' has no model definition entry")

    ret = len(a1Failures) > 0
    return ret


def buildBuildMetadata(buildMetadataPath: Path) -> pd.DataFrame:
    """Write BuildMetadata.csv capturing the MAES version and git state at build time.

    Columns: maesVersion, gitDescribe, gitBranch, gitCommit, builtAt.
    git fields are 'unknown' if git is not available or the repo has no tags.
    """
    from datetime import datetime
    rows = [{
        'maesVersion': gv.MAES_VERSION,
        'gitDescribe': gv.GIT_DESCRIBE,
        'gitBranch': gv.GIT_BRANCH,
        'gitCommit': gv.GIT_COMMIT,
        'builtAt': datetime.now().astimezone().isoformat()
    }]
    df = pd.DataFrame(rows)
    df.to_csv(buildMetadataPath, index=False)
    logging.info(f"BuildMetadata: written to {buildMetadataPath}")
    ret = df
    return ret


def main() -> int:
    """Build KwargTable.csv, ModelDefinitionMap.csv, and UnmappedKwargs.csv; run Pass A checks.

    Returns 1 if Pass A1 errors were found, 0 otherwise.
    """
    logging.basicConfig(
        level=logging.INFO,
        format=f"{LOG_PREFIX_FMT} %(levelname)s %(message)s",
        datefmt=LOG_DATEFMT
    )
    logging.info(f"BuildKwargTable v{VERSION}")

    parser = argparse.ArgumentParser(description=f"MAES kwarg table builder v{VERSION}")
    parser.add_argument(
        "--table-dir", type=Path, default=SCRIPT_DIR,
        help="Directory for CSV output files (default: script directory)"
    )
    args = parser.parse_args()

    tableDir = args.table_dir
    tableDir.mkdir(parents=True, exist_ok=True)
    kwargTablePath = tableDir / "KwargTable.csv"
    modelDefMapPath = tableDir / "ModelDefinitionMap.csv"
    unmappedKwargsPath = tableDir / "UnmappedKwargs.csv"
    buildMetadataPath = tableDir / "BuildMetadata.csv"

    allClasses = buildAllClassMap()
    logging.info(f"Found {len(allClasses)} classes in classmap")

    kwargDf = buildKwargTable(allClasses, kwargTablePath)
    modelDefDf = buildModelDefinitionMap(kwargDf, modelDefMapPath)
    unmappedDf = buildUnmappedKwargs(kwargDf, modelDefDf, unmappedKwargsPath)
    buildBuildMetadata(buildMetadataPath)

    hasErrors = runPassA(modelDefDf, unmappedDf)
    logging.info("Done.")

    if hasErrors:
        ret = 1
    else:
        ret = 0
    return ret


if __name__ == "__main__":
    sys.exit(main())
