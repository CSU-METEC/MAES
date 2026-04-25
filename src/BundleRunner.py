import json
import logging
import os
import sys
import tempfile
import zipfile
from pathlib import Path

import AppUtils as au
import BundleFormat as bf

logger = logging.getLogger(__name__)

# Args that must not be forwarded to ConfigManager when running a bundle
_BUNDLE_SKIP_ARGS = {'bundle', 'configFile', 'createBundle'}


def _extractBundle(zf: zipfile.ZipFile, tempDir: Path) -> tuple[dict, dict]:
    """Extract bundle zip to tempDir, remapping paths to match the inputRoot layout.

    Path mapping:
      studies/<file>           -> <tempDir>/Studies/<file>
      factors/Factors.csv      -> <tempDir>/CuratedData/FactorsFileReference/Factors.csv
      factors/<other>          -> <tempDir>/CuratedData/<other>
      model_definitions/<file> -> <tempDir>/ModelFormulation/<file>
      gc_files/<path>          -> <tempDir>/<path>   (CWD-relative; resolved after chdir)
      state_machines/<file>    -> <tempDir>/state_machines/<file>

    Returns (metadata dict, simConfig dict).
    """
    metadata  = json.loads(zf.read(bf.METADATA_FILE))
    simConfig = json.loads(zf.read(bf.SIM_CONFIG_FILE))

    studiesPrefix      = bf.STUDIES_DIR      + '/'
    factorsCsvName     = bf.FACTORS_CSV_FILE           # 'factors/Factors.csv'
    factorsPrefix      = bf.FACTORS_DIR      + '/'
    modelDefsPrefix    = bf.MODEL_DEFS_DIR   + '/'
    gcFilesPrefix      = bf.GC_FILES_DIR     + '/'
    stateMachinesPrefix = bf.STATE_MACHINES_DIR + '/'

    for info in zf.infolist():
        name = info.filename
        if name.endswith('/'):
            continue

        if name.startswith(studiesPrefix):
            dest = tempDir / 'Studies' / name[len(studiesPrefix):]
        elif name == factorsCsvName:
            dest = tempDir / 'CuratedData' / 'FactorsFileReference' / 'Factors.csv'
        elif name.startswith(factorsPrefix):
            dest = tempDir / 'CuratedData' / name[len(factorsPrefix):]
        elif name.startswith(modelDefsPrefix):
            dest = tempDir / 'ModelFormulation' / name[len(modelDefsPrefix):]
        elif name.startswith(gcFilesPrefix):
            dest = tempDir / name[len(gcFilesPrefix):]
        elif name.startswith(stateMachinesPrefix):
            dest = tempDir / 'state_machines' / name[len(stateMachinesPrefix):]
            logger.warning(
                f"State machine '{name[len(stateMachinesPrefix):]}' extracted to temp dir; "
                "xlsx path references may not resolve if they use absolute paths"
            )
        else:
            continue  # metadata.json, sim_config.json — already read

        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(zf.read(name))

    return metadata, simConfig


def runBundle(zipPath: Path, args) -> None:
    """Extract a MAES bundle zip and run the simulation from it.

    Caller is responsible for ensuring either -s or -dr is set.
    Study iteration is handled by the existing getFileList() / generateWorkitems() machinery.
    Output is written relative to the original working directory.
    """
    import ConfigManager as cm_mod
    import SiteMain2

    zipPath = Path(zipPath).resolve()

    with zipfile.ZipFile(zipPath) as zf:
        if bf.METADATA_FILE not in zf.namelist():
            raise ValueError(f"Not a valid MAES bundle: missing {bf.METADATA_FILE}")

    originalCwd = Path.cwd().resolve()
    outputRoot  = getattr(args, 'outputRoot', None) or str(originalCwd / 'output')

    with open(args.configFile, 'r') as cf:
        config = json.load(cf)

    with tempfile.TemporaryDirectory(prefix='maes_bundle_') as tmpStr:
        tempDir = Path(tmpStr)
        logger.info(f"Extracting bundle: {zipPath.name}")

        with zipfile.ZipFile(zipPath) as zf:
            metadata, simConfig = _extractBundle(zf, tempDir)

        logger.info(
            f"Bundle: format={metadata.get('bundleFormatVersion')}, "
            f"maesVersion={metadata.get('maesVersion')}, "
            f"createdAt={metadata.get('createdAt')}"
        )

        cm_mod.ConfigManager._initializeSingleton(config)
        cMgr = cm_mod.ConfigManager
        cMgr.expandPhase('defaultValues')

        filteredArgs = {k: v for k, v in vars(args).items() if v and k not in _BUNDLE_SKIP_ARGS}
        filteredArgs['inputRoot']  = str(tempDir)
        filteredArgs['outputRoot'] = outputRoot
        cMgr.expandPhase('arguments', **filteredArgs)

        # Apply bundle MC count unless CLI overrides with -mc
        bundleMCIter = simConfig.get('monteCarloIterations')
        if bundleMCIter is not None and not getattr(args, 'monteCarloIterations', None):
            cMgr.expandPhase('arguments', monteCarloIterations=bundleMCIter)

        # Read siteDefinitionParams from a study file in the bundle
        studyFileForVars = _resolveStudyFileForVars(args, tempDir, cMgr)
        if studyFileForVars:
            studyVars = au.readVarsFromStudy(studyFileForVars, config['intakeSpreadsheetConfigParams'])
            filteredStudyVars = {k: v for k, v in studyVars.items() if v and k not in filteredArgs}
            cMgr.expandPhase('siteDefinitionParams', **filteredStudyVars)

        if cMgr.getConfigVar('studyName') is None and studyFileForVars:
            cMgr.expandPhase('arguments', studyName=Path(studyFileForVars).stem)

        cMgr.expandPhase('start')
        cMgr.expandPhase('simulation')

        # chdir to tempDir so CWD-relative file paths in xlsx (e.g. flowGasComposition)
        # resolve against the extracted bundle content
        os.chdir(tempDir)
        try:
            SiteMain2.main(cMgr)
        finally:
            os.chdir(originalCwd)


def _resolveStudyFileForVars(args, tempDir: Path, cMgr) -> str | None:
    """Return a study file path to use for siteDefinitionParams.

    In -s mode: use the config-expanded studyFilename (points into tempDir/Studies/).
    In -dr mode: use the first xlsx found in the target Studies/ subdirectory.
    """
    dir = getattr(args, 'directory', None)
    if dir is not None:
        studiesPath = tempDir / 'Studies' / dir
        first = next(iter(sorted(studiesPath.glob('*.xlsx'))), None)
        return str(first) if first else None
    return cMgr.getConfigVar('studyFilename')
