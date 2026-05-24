import json
import logging
import sys
from pathlib import Path

import AppUtils as au
import BundleCreator

logger = logging.getLogger(__name__)

_DEPRECATED_ARGS = {'testIntervalDays', 'abnormal'}
_CREATOR_ARGS    = {'output', 'anonymize'}


def getParser():
    parser = au.getParser(au.DEFAULT_CONFIG)
    parser.add_argument(
        '--output', '-out',
        required=True,
        metavar='ZIP_PATH',
        help="Output zip bundle path (e.g. my_simulation.zip)"
    )
    parser.add_argument(
        '--anonymize',
        action='store_true',
        default=False,
        help="Anonymize the bundle and produce a key file alongside the zip"
    )
    return parser


def preMain():
    logging.basicConfig(level=logging.INFO, format=au.LOG_FORMAT)

    parser = getParser()
    args = parser.parse_args()

    for argName in _DEPRECATED_ARGS:
        if getattr(args, argName, None) is not None:
            logger.warning(f"--{argName} is deprecated for bundle creation and is ignored")

    import ConfigManager as cm_mod

    with open(args.configFile, 'r') as cf:
        config = json.load(cf)

    cm_mod.ConfigManager._initializeSingleton(config)
    cm = cm_mod.ConfigManager
    cm.expandPhase('defaultValues')

    skipArgs = _CREATOR_ARGS | _DEPRECATED_ARGS | {'configFile'}
    filteredArgs = {k: v for k, v in vars(args).items() if v and k not in skipArgs}
    cm.expandPhase('arguments', **filteredArgs)

    if getattr(args, 'directory', None):
        curatedRoot = cm.getConfigVar('curatedRoot') or 'input'
        dirPath = Path(curatedRoot) / 'Studies' / args.directory
        candidates = sorted(dirPath.glob('*.xlsx'))
        studyFilename = str(candidates[0]) if candidates else cm.getConfigVar('studyFilename')
    else:
        studyFilename = cm.getConfigVar('studyFilename')

    studyVars = au.readVarsFromStudy(studyFilename, config['intakeSpreadsheetConfigParams'])
    filteredStudyVars = {k: v for k, v in studyVars.items() if v and k not in filteredArgs}
    cm.expandPhase('siteDefinitionParams', **filteredStudyVars)

    studyName = cm.getConfigVar('studyName')
    if studyName is None:
        studyName = Path(studyFilename).stem
        cm.expandPhase('arguments', studyName=studyName)

    cm.expandPhase('start')
    cm.expandPhase('simulation')

    outputPath = Path(args.output)
    BundleCreator.createBundle(cm, outputPath)

    if args.anonymize:
        import Anonymizer
        keyPath = outputPath.with_suffix(outputPath.suffix + '.key.json')
        Anonymizer.anonymizeBundle(outputPath, keyPath)


if __name__ == '__main__':
    preMain()
