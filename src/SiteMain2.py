import AppUtils as au
import ModelFormulation as mf
import logging
from DESMain2 import main as DESMain
import SimDataManager as sdm
import SimRNG
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

ALL_PHASES = ['initialization', 'simulation', 'parquet', 'summarize', 'createPDFCache', 'simSummary']

logger = logging.getLogger(__name__)

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
    return t0.deltat.total_seconds()

def runSim(config, simdm):
    mcRunNum = config['MCScenario']
    # Seed = [baseSeed?, crc32(siteName), mcRunNum] (SimRNG.composeSeed): site identity
    # keeps identical site definitions from replaying the same stream within a run
    # (issue #106); mcRunNum keeps MC iterations distinct (#69); the optional
    # --randomSeed base keeps the whole simulation reproducible on demand (#96).
    SimRNG.seed(SimRNG.composeSeed(mcRunNum,
                                   siteName=config.get('siteName'),
                                   baseSeed=config.get('randomSeed')))
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
        pl.toParquet(config)  # Don't summarize

    return t0.deltat.total_seconds()

def summarize(config, simdm):
    with Timer("Summarize") as t0:
        sum.summarize(config)
    return t0.deltat.total_seconds()

def summarizeSimulation(config, simdm):
    with Timer("Summarize") as t0:
        sum.summarizeSimulation(config)
    return t0.deltat.total_seconds()

def createPDFCache(config, simdm):
    with Timer("Create PDF Cache") as t0:
        statsDF = sum.createPDFCache(config)
    return t0.deltat.total_seconds(), statsDF

def runWorkitem(workitem):
    with sdm.SimDataManager(workitem) as simdm:
        worktype = workitem['workType']
        logger.info(f"runWorkitem: {worktype}, file: {workitem['studyFilename']}, mcIter: {workitem['MCIteration']}, pid: {os.getpid()}")
        runtime = 0
        statsDF = pd.DataFrame()
        if worktype == 'initialization':
            runtime = initializeSim(workitem, simdm)
        elif worktype == 'simulation':
            runtime = runSim(workitem, simdm)
        elif worktype == 'parquet':
            runtime = toParquet(workitem, simdm)
        elif worktype == 'summarize':
            runtime = summarize(workitem, simdm)
        elif worktype == 'createPDFCache':
            runtime, statsDF = createPDFCache(workitem, simdm)
        elif worktype == 'simSummary':
            runtime = summarizeSimulation(workitem, simdm)
        else:
            logger.error(f"Unknown worktype: {worktype}")

    return {
        'worktype': worktype,
        'studyShortname': workitem['studyName'],
        'studyFilename': workitem['studyFilename'],
        'MCScenario': workitem['MCScenario'],
        'runtime': runtime,
        'statsDF': statsDF,
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
    if dir is not None: # todo: fix this in argument parsing code
        directoryRoot = cm.expandDynamicTemplate('directoryRootTemplate')
        cm.expandPhase('start', directoryRoot=directoryRoot, scenarioTimestamp=cm.getConfigVar("scenarioTimestamp"))
        dirPath = Path(directoryRoot)
        for singleFile in dirPath.iterdir():
            if not singleFile.is_file():
                continue
            gennedStudyDefinitionFile = cm.expandDynamicTemplate('relativeStudyFileTemplate', studyFilename=singleFile.name)

            yield (str(singleFile), gennedStudyDefinitionFile, singleFile.stem)
    else:
        yield (cm.getConfigVar("studyFilename"), cm.getConfigVar("studyDefinitionFile"), cm.getConfigVar('studyName'))

def generateWorkitems(cm, phasesToInclude=ALL_PHASES):
    initWorkitems = []
    simWorkitems = []
    parquetWorkitems = []
    summaryWorkitems = []
    createPDFCacheWorkitems = []
    simSummaryWorkitems = []

    # The SiteSummary/PDF datasets are single shared, hive-partitioned-by-site
    # directories (their config paths carry no {site} component), so every site in
    # the loop resolves the SAME path. Add each distinct directory exactly once; the
    # simSummary workitem then reads each shared dataset once and recovers `site` from
    # the partition. Appending per-site would read the whole dataset N times and inflate
    # every SimSummary/SimPDF level by the site count (issue #114).
    allSiteSummaryDirs = []
    allSitePDFDirs = []

    fileList = getFileList(cm)
    for (fullFilename, studyFilename, studyName) in fileList:
        cm.expandPhase("arguments", studyDefinitionFile=studyFilename)
        # cm.expandPhase("siteDefinitionParams")
        cm.expandPhase("start", site=studyName, scenarioTimestamp=cm.getConfigVar("scenarioTimestamp"))
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
        summaryDir = cm.getConfigVar('parquetNewSummary')
        if summaryDir not in allSiteSummaryDirs:
            allSiteSummaryDirs.append(summaryDir)
        pdfDir = cm.getConfigVar('parquetNewPDF')
        if pdfDir not in allSitePDFDirs:
            allSitePDFDirs.append(pdfDir)
    # simSummary happens once per simulation, aggregating every site's summaries
    simSummaryWI = generateSingleWorkitem(cm, 'simSummary')
    simSummaryWI['allSiteSummaryDirs'] = allSiteSummaryDirs
    simSummaryWI['allSitePDFDirs'] = allSitePDFDirs
    simSummaryWorkitems.append(simSummaryWI)

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

    return retWorkitems

def generateSummaryWorkitems(cm):
    summaryWorkItems = []
    summaryWorkItems.append(generateSingleWorkitem(cm, 'summarize'))
    return [summaryWorkItems]

def configFromConfigMgr(cMgr):
    workItems = generateWorkitems(cMgr)
    config = workItems[3][0]
    return config

def runLocal(workQueue):
    retList = []
    for singleWorkitem in workQueue:
        # try:
        res = runWorkitem(singleWorkitem)
        retList.append(res)
        # except Exception as e:
        #     msg = f'MC STOP ERROR: mcRun {singleWorkitem["MCScenario"]} did not exit cleanly, continuing with next MC'
        #     logging.error(f'{msg} Error: {e}')
        #     save this mc for review/debugging
    return retList

def runMultiprocessing(workQueue, workers):
    import multiprocessing as mp
    workType = 'UNKNOWN'
    if len(workQueue) > 0:
        workType = workQueue[0].get('workType', 'UNKNOWN')
    logger.info(f"multiprocessing w/ work type: {workType}, workers: {workers}")
    with Timer(f"{workType}") as t0:
        with mp.Pool(workers) as p:
            res = list(p.imap_unordered(runWorkitem, workQueue))
        t0.setCount(len(res))
        return res
    pass

def defineConvenienceConfigVars(cMgr):
    simDurationDays = cMgr.getConfigVar("simDurationDays")
    simDurationSeconds = u.daysToSecs(simDurationDays)
    cMgr.expandPhase("start", simDurationSeconds=simDurationSeconds)
    pass

def main(cm, workitemQueues=None):
    logging.basicConfig(level=logging.INFO, format=au.LOG_FORMAT)
    defineConvenienceConfigVars(cm)
    if workitemQueues is None:
        listOfWorkitemQueues = generateWorkitems(cm)
    else:
        listOfWorkitemQueues = workitemQueues
    resList = []
    workers = cm.getConfigVar("workers")
    parallel = workers and (workers > 0)
    # if parallel:
    #     db = initializeDask(cm)
    with Timer("Run simulations") as t0:
        for singleWorkitemQueue in listOfWorkitemQueues:
            if parallel:
                # queueResults = runDask(singleWorkitemQueue, db)
                queueResults = runMultiprocessing(singleWorkitemQueue, workers)
            else:
                queueResults = runLocal(singleWorkitemQueue)
            resList.extend(queueResults)
        t0.count = len(resList)
    totalRuntime = functools.reduce(lambda cumulative, incr: cumulative + incr, map(lambda x: x['runtime'], resList))
    clocktime = t0.deltat.total_seconds()
    totalMCIterations = cm.getConfigVar('monteCarloIterations')
    logger.info(f"Total runtime: {totalRuntime} seconds, clock time: {clocktime}, MC Iterations: {totalMCIterations}, items: {len(resList)}")
    for worktype, prefix in [('createPDFCache', 'PDFCache')]:
        statsDFs = [r['statsDF'] for r in resList if r['worktype'] == worktype and not r['statsDF'].empty]
        if statsDFs:
            statsFilename = f"{prefix}_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            pd.concat(statsDFs, ignore_index=True).to_csv(statsFilename, index=False)
            logger.info(f"Wrote {statsFilename}")

    resDF = pd.DataFrame(resList).drop(columns=['statsDF'])
    resFileFormat = f"results_{cm.getConfigVar('scenarioTimestampFormat')}.csv"
    resFilename = dt.datetime.now().strftime(resFileFormat)
    resDF = resDF.assign(scenarioTimestamp=cm.getConfigVar('scenarioTimestamp'))
    resDF.to_csv(resFilename, index=False)
    logger.info(f"Wrote {resFilename}")

# set this up as preMain so config does not get instantiated as a global variable

def preMain():
    cm, args = au.getConfig()
    main(cm)

if __name__ == "__main__":
    preMain()