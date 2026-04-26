import json
import logging
import zipfile
from datetime import datetime
from pathlib import Path, PurePosixPath

import pandas as pd

import AppUtils as au
import BundleFormat as bf
import GitVersion as gv
from SiteMain2 import getFileList
from ParamUtils import toParamKey
from utilities.SiteDefinitionValidation.ValidateSite import (
    loadSiteXlsx,
    loadStaticTables,
    loadBuildMetadata,
    runPassB,
    runPassC,
    runPassM,
    runPassF,
)

logger = logging.getLogger(__name__)

_FACTORS_DATA_COLS = ('activityDistribution', 'emissionDriver')

# File-reference parameters resolved relative to CWD
_CWD_RELATIVE_PARAMS = {'productionGCFilename', 'flowGasComposition'}

# File-reference parameters resolved relative to emitterProfileDir
_EMITTER_PROFILE_RELATIVE_PARAMS = {'loadCondition', 'gasFractionDistFileName', 'crankcaseDistrib'}

# Parameters whose xlsx value is a file path only when the value is a non-numeric string
_CONDITIONAL_FILE_PARAMS = {'gasFractionDistFileName'}

# File extensions that suggest a value is a path (used by the runtime heuristic)
_PATH_EXTENSIONS = {'.csv', '.json', '.xlsx', '.txt'}


def _toPosix(val: str) -> str:
    return str(PurePosixPath(Path(val)))


def _isNumeric(val: str) -> bool:
    try:
        float(val)
        return True
    except ValueError:
        return False


def _buildFileRefIndex(modelDefDf: pd.DataFrame) -> dict[str, str]:
    """Return {colValKey: pythonParameter} for all isFileRef Sheet parameters."""
    mask = (
        (modelDefDf['isFileRef'] == True) &
        (modelDefDf['parameterType'] == 'Sheet') &
        (modelDefDf['modelParameter'].notna()) &
        (modelDefDf['modelParameter'] != '')
    )
    fileRefRows = modelDefDf[mask].drop_duplicates(subset=['modelParameter'])
    index = {}
    for _, row in fileRefRows.iterrows():
        try:
            key = toParamKey(row['modelParameter'])['valKey']
            index[key] = row['pythonParameter']
        except Exception:
            pass
    return index


def _collectXlsxFileRefs(
    siteData: dict,
    fileRefIndex: dict[str, str],
    cwd: Path,
    emitterProfileDir: Path,
) -> dict[str, Path]:
    """Scan a loaded study xlsx for file-reference column values.

    Returns {zip_dest_path: source_Path}.
    Emits a warning for any untagged column whose value looks like a file path
    (runtime heuristic to catch missing File Reference tags).
    """
    refs: dict[str, Path] = {}
    warnedUntagged: set[tuple[str, str, str]] = set()

    for tabRow in siteData['masterEquipment']:
        tabName = tabRow['Tab']
        tabDf = siteData['tabs'][tabName]

        for colName in tabDf.columns:
            try:
                colKey = toParamKey(colName)['valKey']
            except Exception:
                continue

            pythonParam = fileRefIndex.get(colKey)
            isTagged = pythonParam is not None

            for val in tabDf[colName].dropna():
                val = str(val).strip()
                if not val:
                    continue

                if not isTagged:
                    p = Path(val)
                    if p.suffix in _PATH_EXTENSIONS and ('/' in val or '\\' in val):
                        key = (tabName, colName, val)
                        if key not in warnedUntagged:
                            logger.warning(
                                f"Possible untagged file reference in tab '{tabName}', "
                                f"column '{colName}', value '{val}' — check model definition "
                                f"for missing \"File Reference\": true"
                            )
                            warnedUntagged.add(key)
                    continue

                if pythonParam in _CONDITIONAL_FILE_PARAMS and _isNumeric(val):
                    continue

                if pythonParam in _CWD_RELATIVE_PARAMS:
                    srcPath = (cwd / val).resolve()
                    destPath = f"{bf.GC_FILES_DIR}/{_toPosix(val)}"
                elif pythonParam in _EMITTER_PROFILE_RELATIVE_PARAMS:
                    srcPath = (emitterProfileDir / val).resolve()
                    destPath = f"{bf.FACTORS_DIR}/{_toPosix(val)}"
                elif pythonParam == 'stateMachineFile':
                    srcPath = Path(val).resolve() if Path(val).is_absolute() else (cwd / val).resolve()
                    destPath = f"{bf.STATE_MACHINES_DIR}/{Path(val).name}"
                else:
                    logger.warning(f"Unhandled file reference parameter '{pythonParam}', value '{val}'")
                    continue

                if not srcPath.exists():
                    logger.warning(f"File reference not found: {srcPath} (tab '{tabName}', column '{colName}')")
                    continue

                if srcPath.is_dir():
                    for child in sorted(srcPath.rglob('*')):
                        if child.is_file():
                            childRel = child.relative_to(srcPath)
                            refs[f"{destPath}/{_toPosix(str(childRel))}"] = child
                else:
                    refs[destPath] = srcPath

    return refs


# Directories that model classes reference via hard-coded fallback paths
# (not declared in model definition JSONs, so not picked up by _collectXlsxFileRefs)
_IMPLICIT_EMITTER_DIRS = {
    'Common/EnginesfuelConsumpEq',              # MEETCompressor.getLoadConditions default
    'Common/CompressorDestructionEfficiencies',  # GasComposition3.DestructionGC.getDestEfficiencies
}


def _collectImplicitRefs(emitterProfileDir: Path) -> dict[str, Path]:
    """Return {zip_dest_path: source_Path} for hard-coded emitter profile directories.

    These are directories that model classes access by fixed path when no explicit
    Sheet parameter is provided. They must be bundled unconditionally.
    """
    refs: dict[str, Path] = {}
    for relPath in _IMPLICIT_EMITTER_DIRS:
        srcDir = (emitterProfileDir / relPath).resolve()
        if not srcDir.exists():
            logger.warning(f"Implicit emitter dir not found: {srcDir}")
            continue
        for child in sorted(srcDir.rglob('*')):
            if child.is_file():
                childRel = child.relative_to(emitterProfileDir)
                refs[f"{bf.FACTORS_DIR}/{_toPosix(str(childRel))}"] = child
    return refs


def _collectFactorDataFiles(
    factorsCsv: Path, emitterProfileDir: Path, usedTags: set[str]
) -> dict[str, Path]:
    """Return {zip_dest_path: source_Path} for factor data files needed by the bundled studies.

    Only rows whose factorTag is in usedTags are considered; all others are silently skipped.
    """
    df = pd.read_csv(factorsCsv).dropna(how='all')
    if 'factorTag' in df.columns and usedTags:
        df = df[df['factorTag'].isin(usedTags)]
    refs: dict[str, Path] = {}
    warnedMissing: set[Path] = set()
    for col in _FACTORS_DATA_COLS:
        if col not in df.columns:
            continue
        for val in df[col].dropna():
            val = str(val).strip().replace('\\', '/')
            if not val or _isNumeric(val):
                continue
            srcPath = (emitterProfileDir / val).resolve()
            if not srcPath.exists():
                if srcPath not in warnedMissing:
                    logger.warning(f"Factor data file not found: {srcPath}")
                    warnedMissing.add(srcPath)
                continue
            refs[f"{bf.FACTORS_DIR}/{_toPosix(val)}"] = srcPath
    return refs


def _validateStudies(
    studyFiles: list[tuple[Path, str]],
    modelDefDf: pd.DataFrame,
    buildMeta: dict,
) -> tuple[bool, set[str]]:
    """Run ValidateSite passes B, C, M on each study.

    Returns (allValid, usedFactorTags) where usedFactorTags is the union of all
    'Factor Tag' column values seen across every equipment tab in every study.
    """
    allValid = True
    usedFactorTags: set[str] = set()
    for xlsxPath, studyName in studyFiles:
        logger.info(f"Validating {xlsxPath.name}...")
        siteData = loadSiteXlsx(xlsxPath)

        passMWarnings = runPassM(siteData['globalSimParams'], buildMeta)
        passBErrors, passBWarnings = runPassB(siteData, modelDefDf)
        passCErrors = runPassC(siteData, modelDefDf)

        for w in passMWarnings:
            logger.warning(f"  [Pass M] {xlsxPath.name}: {w['message']}")
        for w in passBWarnings:
            logger.warning(f"  [Pass B] {xlsxPath.name}: {w['message']}")
        for e in passBErrors:
            logger.error(f"  [Pass B] {xlsxPath.name}: {e['message']}")
        for e in passCErrors:
            logger.error(f"  [Pass C] {xlsxPath.name}: {e['message']}")

        if passBErrors or passCErrors:
            logger.error(f"Validation FAILED: {xlsxPath.name}")
            allValid = False
        else:
            logger.info(f"Validation passed: {xlsxPath.name}")

        for tabDf in siteData['tabs'].values():
            if 'Factor Tag' in tabDf.columns:
                usedFactorTags.update(
                    str(v).strip() for v in tabDf['Factor Tag'].dropna() if str(v).strip()
                )

    return allValid, usedFactorTags


def createBundle(cm, outputZipPath: Path) -> Path:
    """Create a simulation bundle zip from a ConfigManager.

    Validates all study files against model definitions, resolves all file
    references, and packages everything into a self-contained zip bundle.

    Raises ValueError if any study fails validation.
    Returns the path to the created zip file.
    """
    logging.basicConfig(level=logging.INFO, format=au.LOG_FORMAT)

    config = cm.asDict()
    cwd = Path.cwd()
    emitterProfileDir = Path(config['emitterProfileDir']).resolve()
    factorsCsv = Path(au.expandFilename(config['factorName'], config, readonly=True)).resolve()
    modelFormulationDir = Path(__file__).parent.parent / 'input' / 'ModelFormulation'

    validationDir = Path(__file__).parent / 'utilities' / 'SiteDefinitionValidation'
    modelDefDf = loadStaticTables(validationDir)
    buildMeta = loadBuildMetadata(validationDir)

    studyFiles = [
        (Path(fullFilename).resolve(), studyName)
        for fullFilename, _, studyName in getFileList(cm)
    ]

    allValid, usedFactorTags = _validateStudies(studyFiles, modelDefDf, buildMeta)
    if not allValid:
        raise ValueError("Bundle creation aborted: validation errors in one or more study files")

    passFFindings = runPassF(factorsCsv, emitterProfileDir, usedTags=usedFactorTags)
    for f in passFFindings:
        logger.error(f"[Pass F] {f['message']}")
    if passFFindings:
        raise ValueError("Bundle creation aborted: factor data file validation failed (Pass F)")

    factorsDf = pd.read_csv(factorsCsv).dropna(how='all')
    if 'factorTag' in factorsDf.columns:
        knownTags = set(factorsDf['factorTag'].dropna().astype(str))
        for tag in sorted(usedFactorTags - knownTags):
            logger.warning(f"Factor tag '{tag}' is referenced in studies but not defined in Factors.csv")
    for col in _FACTORS_DATA_COLS:
        if col in factorsDf.columns:
            factorsDf[col] = factorsDf[col].apply(
                lambda v: str(v).replace('\\', '/') if pd.notna(v) else v
            )

    fileRefIndex = _buildFileRefIndex(modelDefDf)

    allXlsxRefs: dict[str, Path] = {}
    for xlsxPath, _ in studyFiles:
        siteData = loadSiteXlsx(xlsxPath)
        refs = _collectXlsxFileRefs(siteData, fileRefIndex, cwd, emitterProfileDir)
        allXlsxRefs.update(refs)

    factorDataRefs = _collectFactorDataFiles(factorsCsv, emitterProfileDir, usedFactorTags)
    implicitRefs = _collectImplicitRefs(emitterProfileDir)

    metadata = {
        'bundleFormatVersion': bf.BUNDLE_FORMAT_VERSION,
        'maesVersion': gv.MAES_VERSION,
        'gitDescribe': gv.GIT_DESCRIBE,
        'createdAt': datetime.now().astimezone().isoformat(),
    }
    simConfig = {
        'monteCarloIterations': config.get('monteCarloIterations'),
        'studyName': config.get('studyName'),
    }

    outputZipPath = Path(outputZipPath)
    outputZipPath.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(outputZipPath, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(bf.METADATA_FILE, json.dumps(metadata, indent=2))
        zf.writestr(bf.SIM_CONFIG_FILE, json.dumps(simConfig, indent=2))
        zf.writestr(bf.FACTORS_CSV_FILE, factorsDf.to_csv(index=False))

        for xlsxPath, _ in studyFiles:
            zf.write(xlsxPath, f"{bf.STUDIES_DIR}/{xlsxPath.name}")

        for destPath, srcPath in allXlsxRefs.items():
            zf.write(srcPath, destPath)

        for destPath, srcPath in factorDataRefs.items():
            zf.write(srcPath, destPath)

        for destPath, srcPath in implicitRefs.items():
            zf.write(srcPath, destPath)

        if modelFormulationDir.exists():
            for jsonFile in sorted(modelFormulationDir.glob('*.json')):
                zf.write(jsonFile, f"{bf.MODEL_DEFS_DIR}/{jsonFile.name}")

    logger.info(f"Bundle created: {outputZipPath}")
    return outputZipPath
