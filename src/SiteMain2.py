import AppUtils as au
import ModelFormulation as mf
import logging
import json
import sys
from DESMain2 import main as DESMain
import SimDataManager as sdm
from Timer import Timer
import MEETClasses as mc
from pathlib import Path
import functools
import utilities.EmissionsCSVGenerator as eg
import Units as u
import ParquetLib as pl
import os
import pandas as pd
import datetime as dt
import Summaries2 as sum

ALL_PHASES = ['initialization', 'simulation', 'parquet', 'summarize', 'createPDFCache', 'computeSimSummary', 'createSimPDF']

logger = logging.getLogger(__name__)

# Shared workitem base set once per Pool worker via initWorker; avoids pickling the
# full config dict through the inter-process pipe for every task.
workerBase: dict = {}

def MCInit(simdm):
    studyFile = simdm.config['studyFullName']
    rawIntake = mf.parseIntakeSpreadsheet(studyFile)
    mf.instantiateIntake(simdm, rawIntake)
    simdm.dumpTemplates()
    # todo: copy emitter profile files to templatedir

def mpInit2(simdm, controller=False):
    # want to load everything that is not dependant on MC iterations -- model formulation & instantiated MF
    scenarioTemplateDir = au.expandFilename(simdm.config['MCTemplateDir'], simdm.config)
    logger.info(f"Template dir: {scenarioTemplateDir}")

def pick(simdm, mcRunNum):
    et = simdm.getEquipmentTable()
    eqCount = 0
    for singleEq in et.getTemplates():
        eqJson = singleEq.filteredClassDict()
        with Timer(f"    instantiating {eqJson['key']}", loglevel=logging.DEBUG) as t0:
            if isinstance(singleEq, mc.ActivityDistributionEnabled):
                newInstCount = singleEq.instantiateMultiple(simdm, **{**eqJson, 'mcRunNum': mcRunNum})
            else:
                newEq = singleEq.instantiateFromTemplate(simdm, **{**eqJson, 'mcRunNum': mcRunNum})
                newInstCount = 1
            eqCount += newInstCount
            t0.setCount(newInstCount)
    return eqCount

def initializeSim(config, simdm):
    with Timer("Initialize Simulation") as t0:
        studyFile = config['studyFilename']
        rawIntake = mf.parseIntakeSpreadsheet(studyFile)
        mf.instantiateIntake(simdm, rawIntake)
        simdm.dumpTemplates()
    eqCount = len(simdm.getEquipmentTable().getTemplates())
    logger.info(f"Equipment instances: {eqCount}")
    return t0.deltat.total_seconds()

def runSim(config, simdm):
    mcRunNum = config['MCScenario']
    with Timer(f"Run Simulation MC Iteration {mcRunNum}") as t0:
        with Timer("  Restore templates") as t1:
            simdm.restoreTemplates()
        with Timer("  Initializing MC Run") as t2:
            simdm.initMCRun(mcRunNum)
        with Timer(f"  Instantiating random variables for MCScenario {mcRunNum}") as t3:
            t3.setCount(pick(simdm, mcRunNum))
        with Timer("  Dump instantiated scenario") as t4:
            simdm.dumpInstantiatedScenario(mcRunNum)
        with Timer("  Run simulation") as t5:
            DESMain(simdm, mcRunNum=mcRunNum)
            # add exception handling here + log to screen or file
        with Timer("  Dump simulation results") as t6:
            simdm.dumpDESResults(mcRunNum)
    return t0.deltat.total_seconds()

def validateSim(config, simdm):
    return 0

def generateEmissions(config, simdm):
    with Timer("Validate and write emissions") as t0:
        mcRunNum = config['MCScenario']
        ieFile = au.expandFilename(config['eventTemplate'], config)
        iemFile = au.expandFilename(config['InstantaneousEmissions'], {**config, 'MCScenario': mcRunNum})
        eg.validateAndWriteEmissions({**config,
                                      'InstantaneousEvents': ieFile,
                                      'InstantaneousEmissions': iemFile,
                                      'runNumber': mcRunNum
                                      },
                                     mcRunNum)
    return t0.deltat.total_seconds()

def toParquet(config, simdm):
    with Timer("Validate and write emissions") as t0:
        mergedEmissionDF = pl.toParquet(config)
    partialDir = Path(config['parquetNewInstEmissions']).parent / '_partials' / str(config['siteName']) / str(config['MCScenario'])
    partial = sum.computeAndWritePartialAccumulator(config, mergedEmissionDF, partialDir)
    del mergedEmissionDF
    elapsed = t0.deltat.total_seconds()
    return elapsed, partial

def summarize(config, simdm):
    with Timer("Summarize") as t0:
        sum.summarize(config)
    return t0.deltat.total_seconds()

def finalizeSite(config, simdm):
    with Timer("Summarize") as t0:
        sum.finalizeAccumulatorsFromPaths(config, config.get('partialPaths', []))
    return t0.deltat.total_seconds()

def summarizeSimulation(config, simdm):
    with Timer("Summarize") as t0:
        sum.summarizeSimulation(config)
    return t0.deltat.total_seconds()

def computeSimSummary(config, simdm):
    with Timer("SimSummary") as t0:
        sum.computeSimSummary(config)
    return t0.deltat.total_seconds()

def createPDFCache(config, simdm):
    with Timer("Create PDF Cache") as t0:
        statsDF = sum.createPDFCache(config)
    return t0.deltat.total_seconds(), statsDF

def createSimPDF(config: dict, simdm: 'sdm.SimDataManager') -> float:
    """Run cross-simulation PDF/CDF generation; writes Summary/SimPDF parquet."""
    with Timer("Create Sim PDF") as t0:
        sum.createSimPDF(config)
    return t0.deltat.total_seconds()

def runWorkitem(workitem):
    with sdm.SimDataManager(workitem) as simdm:
        worktype = workitem['workType']
        logger.info(f"runWorkitem: {worktype}, file: {workitem['studyFilename']}, mcIter: {workitem['MCIteration']}, pid: {os.getpid()}")
        runtime = 0
        statsDF = pd.DataFrame()
        partialAcc = None
        if worktype == 'initialization':
            runtime = initializeSim(workitem, simdm)
        elif worktype == 'simulation':
            runtime = runSim(workitem, simdm)
        elif worktype == 'parquet':
            runtime, partialAcc = toParquet(workitem, simdm)
        elif worktype == 'summarize':
            if workitem.get('partialPaths'):
                runtime = finalizeSite(workitem, simdm)
            else:
                runtime = summarize(workitem, simdm)
        elif worktype == 'createPDFCache':
            runtime, statsDF = createPDFCache(workitem, simdm)
        elif worktype == 'simSummary':
            runtime = summarizeSimulation(workitem, simdm)
        elif worktype == 'computeSimSummary':
            runtime = computeSimSummary(workitem, simdm)
        elif worktype == 'createSimPDF':
            runtime = createSimPDF(workitem, simdm)
        else:
            logger.error(f"Unknown worktype: {worktype}")

    return {
        'worktype': worktype,
        'studyShortname': workitem['studyName'],
        'studyFilename': workitem['studyFilename'],
        'MCScenario': workitem['MCScenario'],
        'runtime': runtime,
        'statsDF': statsDF,
        'partialAcc': partialAcc,
        'pid': os.getpid()
    }

def generateSingleWorkitem(cm, workType):
    scenarioConfig = {
        'siteName': cm.getConfigVar('site'),
        'studyFilename': cm.getConfigVar('studyFilename'),
        'MCScenario': cm.getConfigVar('MCIteration'),
        'workType': workType,
        **cm.asDict()
    }

    return scenarioConfig

def getFileList(cm):
    dir = cm.getConfigVar("directory")
    if dir is not None:
        # dir='' means run everything in Studies/ root; non-empty means a subdirectory of Studies/
        inputRoot = cm.getConfigVar('inputRoot')
        dirPath = Path(inputRoot) / 'Studies' / dir
        if not cm.getConfigVar('scenarioTimestamp'):
            scenarioTimestampFormat = cm.getConfigVar('scenarioTimestampFormat') or ''
            sharedTimestamp = dt.datetime.now().strftime(scenarioTimestampFormat)
            cm.expandPhase('arguments', scenarioTimestamp=sharedTimestamp)
        for singleFile in sorted(dirPath.iterdir()):
            if not singleFile.is_file():
                continue
            studyDef = f"{dir}/{singleFile.name}" if dir else singleFile.name
            yield (str(singleFile), studyDef, singleFile.stem)
    else:
        yield (cm.getConfigVar("studyFilename"), cm.getConfigVar("studyDefinitionFile"), cm.getConfigVar('studyName'))

PDF_PHASES = {'createPDFCache', 'createSimPDF'}


def generateWorkitems(cm, phasesToInclude=None):
    if phasesToInclude is None:
        if cm.getConfigVar('noPDF'):
            phasesToInclude = list(filter(lambda p: p not in PDF_PHASES, ALL_PHASES))
        else:
            phasesToInclude = list(ALL_PHASES)
    initWorkitems = []
    simWorkitems = []
    parquetWorkitems = []
    summaryWorkitems = []
    createPDFCacheWorkitems = []
    simSummaryWorkitems = []
    computeSimSummaryWorkitems = []
    createSimPDFWorkitems = []

    fileList = getFileList(cm)
    for (fullFilename, studyFilename, studyName) in fileList:
        cm.expandPhase("arguments", studyDefinitionFile=studyFilename, studyName=studyName)
        # cm.expandPhase("siteDefinitionParams")
        cm.expandPhase("start", site=studyName, scenarioTimestamp=cm.getConfigVar("scenarioTimestamp"))
        bundleSummaryParquetDir = cm.getConfigVar('bundleSummaryParquetDir')
        if bundleSummaryParquetDir:
            cm.expandPhase("simulation", summaryParquetDir=bundleSummaryParquetDir)
        else:
            cm.expandPhase("simulation")
        cm.expandPhase("MCIteration", MCIteration=-1)
        initWorkitems.append(generateSingleWorkitem(cm, 'initialization'))
        # simulation & parquet workitems work on individual site & MC iterations
        numMCIters = int(cm.getConfigVar('monteCarloIterations'))
        for singleMCIter in range(numMCIters):
            cm.expandPhase("MCIteration", MCIteration=singleMCIter)
            simWorkitems.append(generateSingleWorkitem(cm, 'simulation'))
            parquetWorkitems.append(generateSingleWorkitem(cm, "parquet"))
        # summarization happens at the site level only
        summaryWI = generateSingleWorkitem(cm, 'summarize')
        summaryWorkitems.append(summaryWI)
        createPDFCacheWorkitems.append(generateSingleWorkitem(cm, 'createPDFCache'))
    # simulation-level summaries happen once per simulation
    simSummaryWorkitems.append(generateSingleWorkitem(cm, 'simSummary'))
    computeSimSummaryWorkitems.append(generateSingleWorkitem(cm, 'computeSimSummary'))
    createSimPDFWorkitems.append(generateSingleWorkitem(cm, 'createSimPDF'))

    retWorkitems = []
    if 'initialization' in phasesToInclude:
        retWorkitems.append(initWorkitems)
    if 'simulation' in phasesToInclude:
        retWorkitems.append(simWorkitems)
    if 'parquet' in phasesToInclude:
        retWorkitems.append(parquetWorkitems)
    if 'summarize' in phasesToInclude:
        retWorkitems.append(summaryWorkitems)
    if 'createPDFCache' in phasesToInclude:
        retWorkitems.append(createPDFCacheWorkitems)
    if 'simSummary' in phasesToInclude:
        retWorkitems.append(simSummaryWorkitems)
    if 'computeSimSummary' in phasesToInclude:
        retWorkitems.append(computeSimSummaryWorkitems)
    if 'createSimPDF' in phasesToInclude:
        retWorkitems.append(createSimPDFWorkitems)

    return retWorkitems

def generateSummaryWorkitems(cm):
    summaryWorkItems = []
    summaryWorkItems.append(generateSingleWorkitem(cm, 'summarize'))
    return [summaryWorkItems]

def configFromConfigMgr(cMgr):
    workItems = generateWorkitems(cMgr)
    config = workItems[3][0]
    return config

def initWorker(base: dict) -> None:
    """Set the shared workitem base dict in each Pool worker process."""
    global workerBase
    workerBase = base


def makeSlimWorkitem(base: dict, workitem: dict) -> dict:
    """Return a copy of workitem containing only fields that differ from base.

    Fields that cannot be compared (e.g. complex objects) are always included.
    """
    slim = {}
    for k, v in workitem.items():
        try:
            differs = (v != base.get(k))
        except Exception:
            differs = True
        if differs:
            slim[k] = v
    ret = slim
    return ret


def runWorkitemSlim(slim: dict) -> dict:
    """Merge slim per-iteration fields with the shared workerBase and run the workitem."""
    ret = runWorkitem({**workerBase, **slim})
    return ret


def runLocal(workQueue):
    t_start = dt.datetime.now()
    retList = []
    for singleWorkitem in workQueue:
        # try:
        res = runWorkitem(singleWorkitem)
        retList.append(res)
        # except Exception as e:
        #     msg = f'MC STOP ERROR: mcRun {singleWorkitem["MCScenario"]} did not exit cleanly, continuing with next MC'
        #     logging.error(f'{msg} Error: {e}')
        #     save this mc for review/debugging
    wallClock = (dt.datetime.now() - t_start).total_seconds()
    for r in retList:
        r['wallClockTime'] = wallClock
    return retList

def runMultiprocessing(workQueue, workers):
    import multiprocessing as mp
    workType = workQueue[0].get('workType', 'UNKNOWN') if workQueue else 'UNKNOWN'
    logger.info(f"multiprocessing w/ work type: {workType}, workers: {workers}")
    base = workQueue[0]
    slimQueue = list(map(lambda wi: makeSlimWorkitem(base, wi), workQueue))
    with Timer(f"{workType}") as t0:
        # maxtasksperchild=1 forces each worker process to exit after one mc run and be
        # replaced by a fresh process. CPython's arena allocator never returns freed pages
        # to the OS, so without this, worker RSS grows monotonically across sequential mc
        # runs within a process. For N5+ workloads this exhausts physical RAM + swap before
        # the pool closes. The fork cost (~50-100 ms) is negligible vs simulation run times.
        with mp.Pool(workers, initializer=initWorker, initargs=(base,),
                     maxtasksperchild=1) as p:
            res = list(p.imap_unordered(runWorkitemSlim, slimQueue))
        t0.setCount(len(res))
    for r in res:
        r['wallClockTime'] = t0.deltat.total_seconds()
    return res

def defineConvenienceConfigVars(cMgr):
    simDurationDays = cMgr.getConfigVar("simDurationDays")
    simDurationSeconds = u.daysToSecs(simDurationDays)
    cMgr.expandPhase("arguments", simDurationSeconds=simDurationSeconds)
    pass

def main(cm, workitemQueues=None):
    """Run the full MAES simulation pipeline."""
    logging.basicConfig(level=logging.INFO, format=au.LOG_FORMAT)
    defineConvenienceConfigVars(cm)
    if workitemQueues is None:
        listOfWorkitemQueues = generateWorkitems(cm)
        cm.freeze()
    else:
        listOfWorkitemQueues = workitemQueues
    resList = []
    workers = cm.getConfigVar("workers")
    parallel = workers and (workers > 1)
    pendingSitePartials = {}  # site -> [partialAcc, ...] collected from parquet phase
    # if parallel:
    #     db = initializeDask(cm)
    with Timer("Run simulations") as t0:
        for singleWorkitemQueue in listOfWorkitemQueues:
            workType = singleWorkitemQueue[0]['workType'] if singleWorkitemQueue else None
            effectiveWorkers = workers

            if workType == 'summarize' and pendingSitePartials:
                for wi in singleWorkitemQueue:
                    wi['partialPaths'] = [p['path'] for p in pendingSitePartials.pop(wi['siteName'], [])]

            if effectiveWorkers and effectiveWorkers > 1 and len(singleWorkitemQueue) > 1:
                # queueResults = runDask(singleWorkitemQueue, db)
                queueResults = runMultiprocessing(singleWorkitemQueue, effectiveWorkers)
            else:
                queueResults = runLocal(singleWorkitemQueue)
            resList.extend(queueResults)

            if workType == 'parquet':
                for r in queueResults:
                    partialAcc = r.get('partialAcc')
                    if partialAcc is not None:
                        site = partialAcc['site']
                        pendingSitePartials.setdefault(site, []).append(partialAcc)

        t0.count = len(resList)
    totalRuntime = functools.reduce(lambda cumulative, incr: cumulative + incr, map(lambda x: x['runtime'], resList))
    clocktime = t0.deltat.total_seconds()
    totalMCIterations = cm.getConfigVar('monteCarloIterations')
    logger.info(f"Total runtime: {totalRuntime} seconds, clock time: {clocktime}, MC Iterations: {totalMCIterations}, items: {len(resList)}")
    resultsDir = cm.getConfigVar('resultsDir')  # set by BundleRunner in bundle mode; None otherwise

    if not resultsDir:
        for worktype, prefix in [('createPDFCache', 'PDFCache')]:
            statsDFs = [r['statsDF'] for r in resList if r['worktype'] == worktype and not r['statsDF'].empty]
            if statsDFs:
                statsFilename = f"{prefix}_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                pd.concat(statsDFs, ignore_index=True).to_csv(statsFilename, index=False)
                logger.info(f"Wrote {statsFilename}")

    resDF = pd.DataFrame(resList).drop(columns=['statsDF', 'partialAcc'])
    resFileFormat = f"results_{cm.getConfigVar('scenarioTimestampFormat')}.csv"
    resFilename = dt.datetime.now().strftime(resFileFormat)
    resDF = resDF.assign(scenarioTimestamp=cm.getConfigVar('scenarioTimestamp'))
    resPath = Path(resultsDir) / resFilename if resultsDir else Path(resFilename)
    resDF.to_csv(resPath, index=False)
    logger.info(f"Wrote {resPath}")

# set this up as preMain so config does not get instantiated as a global variable

_BUNDLE_CREATE_SKIP_ARGS = {'bundle', 'createBundle', 'anonymize', 'configFile', 'study'}


def preMain():
    parser = au.getParser(au.DEFAULT_CONFIG)
    parser.add_argument(
        '--bundle', '-bun',
        metavar='ZIP_PATH',
        default=None,
        help="Run a simulation from a bundle zip (created with --createBundle or BundleCreatorMain)"
    )
    parser.add_argument(
        '--createBundle', '-cb',
        metavar='ZIP_PATH',
        default=None,
        help="Create a simulation bundle zip and exit instead of running the simulation"
    )
    parser.add_argument(
        '--anonymize',
        action='store_true',
        default=False,
        help="Anonymize the bundle after creation (with --createBundle); writes a key file alongside the zip"
    )
    parser.add_argument(
        '--study',
        metavar='STUDY_NAME',
        default=None,
        help="Run a single study from the bundle by name (stem or filename); default runs all studies"
    )
    parser.add_argument(
        '--keepRaw',
        action='store_true',
        default=False,
        help="Preserve per-site raw CSV output in the bundle output tree (default: discarded after parquet conversion)"
    )
    args = parser.parse_args()

    if args.bundle:
        import BundleRunner

        logging.basicConfig(level=logging.INFO, format=au.LOG_FORMAT)
        BundleRunner.runBundle(Path(args.bundle), args)

    elif args.createBundle:
        import BundleCreator
        import ConfigManager as cm_mod

        logging.basicConfig(level=logging.INFO, format=au.LOG_FORMAT)
        with open(args.configFile, 'r') as cf:
            config = json.load(cf)

        cm_mod.ConfigManager._initializeSingleton(config)
        cMgr = cm_mod.ConfigManager
        cMgr.expandPhase('defaultValues')

        filteredArgs = {k: v for k, v in vars(args).items() if v and k not in _BUNDLE_CREATE_SKIP_ARGS}
        cMgr.expandPhase('arguments', **filteredArgs)

        if getattr(args, 'directory', None):
            inputRoot = cMgr.getConfigVar('inputRoot') or 'input'
            dirPath = Path(inputRoot) / 'Studies' / args.directory
            candidates = sorted(dirPath.glob('*.xlsx'))
            studyFilename = str(candidates[0]) if candidates else cMgr.getConfigVar('studyFilename')
        else:
            studyFilename = cMgr.getConfigVar('studyFilename')

        studyVars = au.readVarsFromStudy(studyFilename, config['intakeSpreadsheetConfigParams'])
        filteredStudyVars = {k: v for k, v in studyVars.items() if v and k not in filteredArgs}
        cMgr.expandPhase('siteDefinitionParams', **filteredStudyVars)

        studyName = cMgr.getConfigVar('studyName')
        if studyName is None:
            studyName = Path(studyFilename).stem
            cMgr.expandPhase('arguments', studyName=studyName)

        cMgr.expandPhase('start')
        cMgr.expandPhase('simulation')

        outputPath = Path(args.createBundle)
        BundleCreator.createBundle(cMgr, outputPath)

        if getattr(args, 'anonymize', False):
            import Anonymizer
            keyPath = outputPath.with_suffix(outputPath.suffix + '.key.json')
            Anonymizer.anonymizeBundle(outputPath, keyPath)

    else:
        cMgr, args = au.getConfig()
        main(cMgr, parquetWorkers=args.parquetWorkers)


if __name__ == "__main__":
    preMain()