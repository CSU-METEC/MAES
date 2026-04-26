import json
import logging
import os
import tempfile
import zipfile
from pathlib import Path

import AppUtils as au
import BundleFormat as bf

logger = logging.getLogger(__name__)

# Args that must not be forwarded to ConfigManager when running a bundle —
# either handled specially here or meaningless in bundle mode.
_BUNDLE_SKIP_ARGS = {
    'bundle', 'configFile', 'createBundle',
    'study',                  # handled below
    'directory',              # we control study iteration ourselves
    'studyDefinitionFile',    # derived from --study or directory scan
}


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

    studiesPrefix       = bf.STUDIES_DIR      + '/'
    factorsCsvName      = bf.FACTORS_CSV_FILE           # 'factors/Factors.csv'
    factorsPrefix       = bf.FACTORS_DIR      + '/'
    modelDefsPrefix     = bf.MODEL_DEFS_DIR   + '/'
    gcFilesPrefix       = bf.GC_FILES_DIR     + '/'
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

    By default runs all studies in the bundle's Studies/ directory.
    Pass --study <name> to run a single study by filename stem.
    Global simulation parameters (monteCarloIterations, etc.) are read from
    sim_config.json in the bundle; CLI flags override them where applicable.
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

        studyName = getattr(args, 'study', None)
        studiesDir = tempDir / 'Studies'

        if studyName is not None:
            # Single-study mode: resolve to the named xlsx
            stem = Path(studyName).stem  # strip .xlsx if the user included it
            xlsxPath = studiesDir / f'{stem}.xlsx'
            if not xlsxPath.exists():
                raise ValueError(f"Study not found in bundle: {xlsxPath.name}")
            filteredArgs['studyDefinitionFile'] = xlsxPath.name
            xlsxForVars = xlsxPath
        else:
            # Multi-study mode: scan Studies/ root
            filteredArgs['directory'] = ''
            allXlsx = sorted(studiesDir.glob('*.xlsx'))
            xlsxForVars = allXlsx[0] if allXlsx else None
            if xlsxForVars:
                # satisfies the studyFilename template expansion; overridden per-study in generateWorkitems
                filteredArgs['studyDefinitionFile'] = xlsxForVars.name

        cMgr.expandPhase('arguments', **filteredArgs)

        # Apply bundle global params unless the CLI overrides them
        bundleMCIter = simConfig.get('monteCarloIterations')
        if bundleMCIter is not None and not getattr(args, 'monteCarloIterations', None):
            cMgr.expandPhase('arguments', monteCarloIterations=bundleMCIter)

        # siteDefinitionParams from a representative study file
        if xlsxForVars:
            studyVars = au.readVarsFromStudy(str(xlsxForVars), config['intakeSpreadsheetConfigParams'])
            filteredStudyVars = {k: v for k, v in studyVars.items() if v and k not in filteredArgs}
            cMgr.expandPhase('siteDefinitionParams', **filteredStudyVars)

        if cMgr.getConfigVar('studyName') is None and studyName is not None:
            cMgr.expandPhase('arguments', studyName=Path(studyName).stem)

        # chdir to tempDir so CWD-relative file paths in xlsx (e.g. flowGasComposition)
        # resolve against the extracted bundle content
        os.chdir(tempDir)
        try:
            SiteMain2.main(cMgr)
        finally:
            os.chdir(originalCwd)
