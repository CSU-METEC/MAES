import os
# Cap BLAS / OpenMP / pyarrow threads BEFORE pandas/numpy/pyarrow are imported.
# On Linux multiprocessing defaults to fork, which inherits the parent's BLAS
# thread pool — and BLAS reads these env vars at library load time. Setting them
# in the pool initializer is too late. This block makes sure the parent (and
# therefore every forked worker) starts with thread counts of 1. Override by
# setting the env var explicitly (e.g. OMP_NUM_THREADS=4) before launching.
for _var in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
             "BLIS_NUM_THREADS", "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ.setdefault(_var, "1")

import AppUtils as au
import ModelFormulation as mf
import logging
from DESMain2 import main as DESMain
import SimDataManager as sdm
from Timer import Timer
import MEETClasses as mc
from pathlib import Path
import functools
import utilities.EmissionsCSVGenerator as eg
import Units as u
import ParquetLib as pl

import pandas as pd
import datetime as dt

ALL_PHASES = ['initialization', 'simulation', 'parquet', 'summarize']

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
        # todo: this will reread the parquet files for all MC iterations.  Is it possible to do this iteration-by-iteration?
        pl.postprocess(config)
    return t0.deltat.total_seconds()

def runWorkitem(workitem):
    with sdm.SimDataManager(workitem) as simdm:
        worktype = workitem['workType']
        logger.info(f"runWorkitem: {worktype}, file: {workitem['studyFilename']}, mcIter: {workitem['MCIteration']}, pid: {os.getpid()}")
        runtime = 0
        if worktype == 'initialization':
            runtime = initializeSim(workitem, simdm)
        elif worktype == 'simulation':
            runtime = runSim(workitem, simdm)
        elif worktype == 'parquet':
            runtime = toParquet(workitem, simdm)
        elif worktype == 'summarize':
            runtime = summarize(workitem, simdm)
        else:
            logger.error(f"Unknown worktype: {worktype}")

    return {
        'worktype': worktype,
        'studyShortname': workitem['studyName'],
        'studyFilename': workitem['studyFilename'],
        'MCScenario': workitem['MCScenario'],
        'runtime': runtime,
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

    retWorkitems = []
    if 'initialization' in phasesToInclude:
        retWorkitems.append(initWorkitems)
    if 'simulation' in phasesToInclude:
        retWorkitems.append(simWorkitems)
    if 'parquet' in phasesToInclude:
        retWorkitems.append(parquetWorkitems)
    if 'summarize' in phasesToInclude:
        retWorkitems.append(summaryWorkitems)

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

def _workerInitializer():
    # Each worker handles one workitem at a time; without these caps BLAS / pyarrow
    # spawn N threads per worker and a 64-worker pool oversubscribes the host badly.
    for var in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
                "BLIS_NUM_THREADS", "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
        os.environ.setdefault(var, "1")
    try:
        import pyarrow as pa
        pa.set_cpu_count(1)
        pa.set_io_thread_count(1)
    except Exception:
        pass

def runMultiprocessing(workQueue, workers):
    import multiprocessing as mp
    workType = 'UNKNOWN'
    nItems = len(workQueue)
    if nItems > 0:
        workType = workQueue[0].get('workType', 'UNKNOWN')
    # Don't spin up more workers than there are workitems (matters for single-item phases).
    effectiveWorkers = max(1, min(workers, nItems)) if nItems > 0 else workers
    # ~4 chunks per worker keeps load balancing while cutting IPC overhead on large queues.
    chunksize = max(1, nItems // (effectiveWorkers * 4)) if nItems > 0 else 1
    logger.info(f"multiprocessing w/ work type: {workType}, workers: {effectiveWorkers}, items: {nItems}, chunksize: {chunksize}")
    with Timer(f"{workType}") as t0:
        with mp.Pool(effectiveWorkers, initializer=_workerInitializer) as p:
            res = list(p.imap_unordered(runWorkitem, workQueue, chunksize=chunksize))
        t0.setCount(len(res))
        return res
    pass

def defineConvenienceConfigVars(cMgr):
    simDurationDays = cMgr.getConfigVar("simDurationDays")
    simDurationSeconds = u.daysToSecs(simDurationDays)
    cMgr.expandPhase("start", simDurationSeconds=simDurationSeconds)
    pass

def main(cm, workitemQueues=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s  %(message)s")
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
    resDF = pd.DataFrame(resList)
    resFileFormat = f"results_{cm.getConfigVar('scenarioTimestampFormat')}.csv"
    resFilename = dt.datetime.now().strftime(resFileFormat)
    resDF.to_csv(resFilename, index=False)
    logger.info(f"Wrote {resFilename}")


# set this up as preMain so config does not get instantiated as a global variable

def preMain():
    cm, args = au.getConfig()
    main(cm)

if __name__ == "__main__":
    preMain()