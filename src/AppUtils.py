import argparse
import json
import sys
import logging
import os
from datetime import datetime
from pathlib import Path
import shutil
import Units as u
import pandas as pd
from ConfigManager import ConfigManager as cm
import MEETExceptions as me
import math
import re
from FileSystemManager import FileStorageManager as fsm

logger = logging.getLogger(__name__)

# Do not delete these imports -- even though pycharm might mark them as unused, they are important
# for instantiating the emitters in loadEmitterFile

# from Well import Well
# from Leak import Leak, CompressorLeak
# from Compressor import Compressor
# from Dehydrator import Dehydrator
# from Pneumatic import Pneumatic
# from IntermittentPneumatic import IntermittentPneumatic
# from ChemicalInjectionPump import ChemicalInjectionPump
# from TankFlash import TankFlash
# from Unloading import Unloading
# from Completion import Completion
# from TestEmitter import TestEmitter
# from LDAR import LDAR
# from OtherEquipment import Other
import inspect


DEFAULT_CONFIG = "config/defaultConfig.json"

LOGFORMATSTRING = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
DEFAULT_TS_FMT = "%y%m%d_%H%M%S"

def getParser(defaultConfig):
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("-s",  "--studyDefinitionFile", default="MEET2/ConstantSeparator.xlsx", help="Study definition file")
    parser.add_argument("-c",  "--configFile", default=defaultConfig, help="Set the configuration file")
    parser.add_argument("-i",  "--inputRoot", help="Input directory.  Read from config file by default")
    parser.add_argument("-o",  "--outputDir", help="Output directory.  Read from config file by default")
    parser.add_argument("-or", "--outputRoot", help="Output root directory, used to set the base of outputDir.  Read from config file by default")
    parser.add_argument("-t",  "--testIntervalDays", help="simulation / serialization duration, days", type=int)
    parser.add_argument('-mc', '--monteCarloIterations', help="number of MC iterations", type=int)
    parser.add_argument('-r',  '--runNumber', help="scenario number", type=int)

    parser.add_argument("-fst", "--fsType", help="File system type (local, s3, etc.)", type=str, default="local")

    parser.add_argument("-ts", "--scenarioTimestamp", help="simulation / serialization identifier (timestamp)")

    parser.add_argument('-w',  '--workers', type=int, help="Number of parallel python images (experimental)")

    parser.add_argument('-si', '--disableSimulation', help="Disable simulation in MEETMain", action='store_true', default=False)
    parser.add_argument('-gr', '--disableGraph', help="Disable graph creation in MEETMain", action='store_true', default=False)
    parser.add_argument('-su', '--disableSummary', help="Disable summary generation in MEETMain", action='store_true', default=False)

    parser.add_argument("-dr", "--directory", help="Study definition folder. Will run every study sheet in directory"),

    parser.add_argument("-sn", "--studyName", help="Name of study")

    parser.add_argument("-fs", "--fullSummaries", help="Generate all summaries (annual, instantaneous, PDFs, and Average Emission Rates and Durations)", default=False)
    parser.add_argument("-as", "--annualSummaries", help="Generate annual emissions summaries", default=False)
    parser.add_argument("-is", "--instantaneousSummaries", help="Generate instantaneous emissions summaries", default=False)
    parser.add_argument("-ps", "--pdfSummaries", help="Generate PDF emissions summaries", default=False)
    parser.add_argument("-ad", "--avgDurSummaries", help="Generate Average Emission Rates and Durations table summary", default=False)

    parser.add_argument("-ab", "--abnormal", help="include abnormal emittions or not", default=None)
    parser.add_argument("-me", "--METype", help="Choose a METype to calculate its pdf", default=None)
    parser.add_argument("-ui", "--unitID", help="Choose a unitID to calculate its pdf", default=None)

    return parser

def getArgs(defaultConfig=DEFAULT_CONFIG, argsToParse=sys.argv[1:]):

    parser = getParser(defaultConfig)

    args = parser.parse_args(argsToParse)
    return args

def readVarsFromStudy(studyFullName, configParamMap):
    modelDF = pd.read_excel(studyFullName, sheet_name="Global Simulation Parameters", header=None)
    sheetConfig = {}
    for sheetName, paramConfig in configParamMap.items():
        configVar = paramConfig['configVar']
        sheetRows = modelDF[modelDF[0] == sheetName]
        if sheetRows.empty:
            sheetConfig[configVar] = paramConfig.get('default', None)
            continue
        if pd.isna(sheetRows[1]).values[0]:
            sheetConfig[configVar] = paramConfig.get('default', None)
            continue
        sheetRow = sheetRows.iloc[-1]
        sheetVal = sheetRow[1]
        sheetConfig[paramConfig['configVar']] = sheetVal
    return sheetConfig

def getConfig(defaultConfig=DEFAULT_CONFIG, commandArgs=sys.argv[1:]):
    args = getArgs(defaultConfig=defaultConfig, argsToParse=commandArgs)
    configFile = args.configFile
    with open(configFile, "r") as cf:
        config = json.load(cf)

    cm._initializeSingleton(config)
    cm.expandPhase('defaultValues')

    # Process command line arguments first to get the study definition file

    filteredCommandLineArgs = dict(filter(lambda x: x[1], args.__dict__.items()))
    cm.expandPhase("arguments", **filteredCommandLineArgs)

    # Process arguments out of the study definition file

    studyFilename = cm.getConfigVar("studyFilename")
    studyVars = readVarsFromStudy(studyFilename, config['intakeSpreadsheetConfigParams'])
    filteredStudyVars = dict(filter(lambda x: x[1], studyVars.items()))
    cm.expandPhase("siteDefinitionParams", **filteredStudyVars)

    # even though we processed the command line arguments first, then the values out of the study definition file,
    # values specified as arguments will take precedence over values in the study definition file because of the
    # order defined in the defaultConfig.json file.

    studyName = cm.getConfigVar('studyName')  # could be set with -sn / studyName argument
    if studyName is None:
        studyPath = Path(studyFilename)
        studyName = studyPath.stem
        cm.expandPhase("arguments", studyName=studyName)

    cm.expandPhase("start")
    cm.expandPhase("simulation")

    return cm, args


def findMostRecentScenario(cm):
    outDir = cm.getConfigVar("studyRoot")
    outPath = Path(outDir)
    # path doesn't exist, check that the parent directory exists
    if not outPath.exists():
        msg = f"Unknown parent directory {outPath}, cannot determine MC scenario"
        logger.error(msg)
        raise me.IllegalArgumentError(msg)
    subdirs = filter(lambda x: x.is_dir(), outPath.iterdir())
    maxSubdir = max(subdirs, key=lambda x: x.stat().st_ctime).as_posix()
    maxSubdirStr = str(maxSubdir)
    maxSubdirPath = Path(maxSubdirStr)
    maxSubdirNameOnly = maxSubdirPath.stem

    # parse out config file params from subdir name using simulationRootRE
    m = re.match(cm.getConfigVar("simulationRootRE"), str(maxSubdirNameOnly))
    if m:
        cm.expandPhase("start", studyRoot=str(outPath), scenarioTimestamp=m['scenarioTimestamp'])
    cm.expandPhase("simulation", simulationRoot=maxSubdirStr)
    cm.expandPhase("MCIteration", MCIteration="")

def writeEmitter(file, emitter):
    def myconverter(o):
        if isinstance(o, datetime.datetime):
            return o.strftime('%Y-%m-%d')
        elif isinstance(o, datetime.time):
            return o.strftime('%H:%M:%S')
    str = json.dumps(emitter, default=myconverter)
    file.write(str)
    file.write("\n")

def ensureDirectory(filePath, takeParent=True):
    parentPath = Path(filePath)
    if takeParent:
        parentPath = parentPath.parent
    parentPath.mkdir(parents=True, exist_ok=True)

def globalInitializer(config, main=False):
    if main:
        logfile = "{}/log_main_{}.log".format(config['LogDir'], os.getpid())
        ensureDirectory(logfile)
        h = [logging.FileHandler(logfile), logging.StreamHandler()]
    else:
        logfile = "{}/log_{}.log".format(config['LogDir'], os.getpid())
        ensureDirectory(logfile)
        h = [logging.FileHandler(logfile)]
    logging.basicConfig(level=logging.DEBUG, format=LOGFORMATSTRING, handlers=h)

def expandTemplate(fStr, config):
    oPath = fStr.format(**config)
    return oPath

def expandFilename(fStr, config, readonly=False):
    oPath = expandTemplate(fStr, config)
    if not readonly:
        ensureDirectory(oPath)
    return oPath

def expandFilename2(fStr, config, readonly=False):
    oPath1 = expandTemplate(fStr, config)
    return expandFilename(oPath1, config, readonly)

def expandCopy(iStr, oStr, config):
    iPath = expandFilename(iStr, config, readonly=True)
    oPath = expandFilename(oStr, config)
    shutil.copy(iPath, oPath)

def clearDirectory(inPathName):
    pathHasElements = False
    inPath = Path(inPathName)
    if not inPath.exists():
        return
    logger.debug(f"Clearing directory {inPathName}")
    for singleSubpath in inPath.iterdir():
        if singleSubpath.is_dir():
            logger.debug(f"  removing tree {str(singleSubpath)}")
            shutil.rmtree(singleSubpath)
        else:
            logger.debug(f"  removing file {str(singleSubpath)}")
            singleSubpath.unlink()
        pathHasElements = True
    if pathHasElements:
        logger.info(f"Cleared {inPathName}")

def getSimDuration():
    config, _ = getConfig()
    simDuration = u.daysToSecs(config['testIntervalDays'] + 1)
    return simDuration

def readGCFingerprintFile(config):
    filename = expandFilename(config['gcFingerprintFile'], config, readonly=True)
    gcProfiles = {}
    with open(filename, "r") as iFile:
        for inLine in iFile:
            profileJson = json.loads(inLine)
            gcProfiles[profileJson['profile_id']] = profileJson
    return gcProfiles

def cleanKeys(df):
    cleanDF = df.assign(facilityID=df['facilityID'].replace({math.nan: ''}),
                        unitID=df['unitID'].replace({math.nan: ''}),
                        emitterID=df['emitterID'].replace({math.nan: ''})
                        )
    return cleanDF
def getEvents(config):
    emPath = config['eventFilename']
    eventLog = pd.read_csv(emPath, dtype={'facilityID': str, 'unitID': str, 'emitterID': str})
    eventLog = cleanKeys(eventLog)
    secondaryPath = config['secondaryInfoFilename']
    secondaryDF = pd.read_csv(secondaryPath)
    secondaryInfoWideDF = secondaryDF.pivot(index='eventID', columns='fieldName', values='fieldValue')
    ret = eventLog.merge(secondaryInfoWideDF, left_on='eventID', right_on='eventID', how='left')
    if 'mdGroup' not in ret.columns:
        ret = ret.assign(mdGroup='')
    return ret

def readCoreTables(config):

    mdPath = config['mdScenarioFilename']
    gcPath = config['gasCompositionLogFilename']
    tsPath = config['tsFilename']

    metadata = pd.read_csv(mdPath, dtype={'facilityID': str, 'unitID': str, 'emitterID': str})
    gascomp = pd.read_csv(gcPath)
    tstable = pd.read_csv(tsPath)

    eventDF = getEvents(config)
    metadata = cleanKeys(metadata)

    pqSite  = config['site']
    eventDF  = eventDF.assign(site=pqSite)
    gascomp  = gascomp.assign(site=pqSite)
    tstable  = tstable.assign(site=pqSite)
    metadata = metadata.assign(site=pqSite)

    return eventDF, tstable, gascomp, metadata
