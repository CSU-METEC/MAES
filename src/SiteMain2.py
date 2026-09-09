import os
for _envVar in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
                "BLIS_NUM_THREADS", "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ.setdefault(_envVar, "1")

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
import pandas as pd
import datetime as dt
import Summaries2 as sum

ALL_PHASES = ['initialization', 'simulation', 'parquet', 'summarizeMCRun', 'createPDFCache', 'simSummary']

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
                                   siteName=config.get('seedSiteName') or config.get('siteName'),
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

def createPDFCacheMCRun(config, simdm):
    with Timer("Create PDF Cache Slice", loglevel=logging.DEBUG) as t0:
        cacheDF, groupCount = sum._buildCacheSliceForMCRun(config, config['MCScenario'])
    return t0.deltat.total_seconds(), cacheDF, groupCount

def summarizeMCRun(config, simdm):
    with Timer("Summarize MC run", loglevel=logging.DEBUG) as t0:
        pieces = sum.summarizeMCRunPieces(config, config['MCScenario'])
    return t0.deltat.total_seconds(), pieces

def runWorkitem(workitem):
    with sdm.SimDataManager(workitem) as simdm:
        worktype = workitem['workType']
        logger.info(f"runWorkitem: {worktype}, file: {workitem['studyFilename']}, mcIter: {workitem['MCIteration']}, pid: {os.getpid()}")
        runtime = 0
        statsDF = pd.DataFrame()
        cacheDF = pd.DataFrame()
        groupCount = 0
        summaryPieces = None
        if worktype == 'initialization':
            runtime = initializeSim(workitem, simdm)
        elif worktype == 'simulation':
            runtime = runSim(workitem, simdm)
        elif worktype == 'parquet':
            runtime = toParquet(workitem, simdm)
        elif worktype == 'summarize':
            runtime = summarize(workitem, simdm)
        elif worktype == 'summarizeMCRun':
            runtime, summaryPieces = summarizeMCRun(workitem, simdm)
        elif worktype == 'createPDFCache':
            runtime, statsDF = createPDFCache(workitem, simdm)
        elif worktype == 'createPDFCacheMCRun':
            runtime, cacheDF, groupCount = createPDFCacheMCRun(workitem, simdm)
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
        'pid': os.getpid(),
        # Only meaningfully populated for 'createPDFCacheMCRun'/'summarizeMCRun' -- consumed
        # by finalizeAllPDFCaches()/runCreatePDFCacheIncremental() and
        # finalizeAllSummaries()/runSummarizeIncremental(), dropped before results_*.csv is
        # written (see main()).
        'siteName': workitem.get('siteName'),
        'config': workitem,
        'cacheDF': cacheDF,
        'groupCount': groupCount,
        'summaryPieces': summaryPieces,
    }

def generateSingleWorkitem(cm, workType):
    # 'siteName' stays equal to 'site' unconditionally -- summarize()/parquet-read code uses
    # config['siteName'] as the literal on-disk partition key (must match whatever 'site' was
    # when the data was written), not just an RNG input. 'seedSiteName' (set via -ssn) is passed
    # through separately and consumed only by composeSeed() in runSim(), below, so an explicit
    # RNG seed identity never has to match the on-disk partition name.
    scenarioConfig = {
        'siteName': cm.getConfigVar('site'),
        'seedSiteName': cm.getConfigVar('seedSiteName'),
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
    # --noPDF drops the per-site PDF cache phase here; summarizeSimulation skips SimPDF
    # for the same flag. Build a local copy so the ALL_PHASES default is never mutated.
    if cm.getConfigVar('noPDF'):
        phasesToInclude = list(filter(lambda phase: phase != 'createPDFCache', phasesToInclude))
    initWorkitems = []
    simWorkitems = []
    parquetWorkitems = []
    summaryWorkitems = []
    summarizeMCRunWorkitems = []
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
            # One createPDFCacheMCRun item per (site, mcRun) instead of one createPDFCache
            # item per site -- lets the outer per-work-type dispatch parallelize the PDF
            # cache build across real worker processes the same way simulation/parquet
            # already are, instead of one process looping over every MC run sequentially.
            createPDFCacheWorkitems.append(generateSingleWorkitem(cm, 'createPDFCacheMCRun'))
            # One summarize work item per (site, mcRun) instead of one per site, same
            # reasoning as createPDFCache above -- see summarizeMCRun()/finalizeAllSummaries()/
            # runSummarizeIncremental(). Kept alongside (not replacing) the old per-site
            # 'summarize' work item below: generateSummaryWorkitems() and any caller that
            # explicitly asks for phasesToInclude=['summarize', ...] still gets the original
            # monolithic path unchanged.
            summarizeMCRunWorkitems.append(generateSingleWorkitem(cm, 'summarizeMCRun'))
        # summarization happens at the site level only
        summaryWI = generateSingleWorkitem(cm, 'summarize')
        summaryWorkitems.append(summaryWI)
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
    if 'summarizeMCRun' in phasesToInclude:
        retWorkitems.append(summarizeMCRunWorkitems)
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
        with mp.Pool(workers, maxtasksperchild=1) as p:
            res = list(p.imap_unordered(runWorkitem, workQueue))
        t0.setCount(len(res))
        return res
    pass

def _finalizePDFCacheForSite(siteName, config, sliceResults):
    with Timer("Create PDF Cache") as t1:
        statsDF = sum.finalizePDFCache(config, sliceResults)
    return {
        'worktype': 'createPDFCache',
        'studyShortname': siteName,
        'studyFilename': config.get('studyFilename'),
        'MCScenario': -1,
        'runtime': t1.deltat.total_seconds(),
        'statsDF': statsDF,
        'pid': os.getpid(),
    }

def finalizeAllPDFCaches(rawResults):
    """Collect-everything-then-finalize-by-site variant of runCreatePDFCacheIncremental
    below, used by the non-parallel (-w<=1) path. Groups every site's (cacheDF, groupCount)
    slices together -- critical for -dr multi-site runs, where results from every site's MC
    runs land in one combined list, and combining across sites would silently corrupt each
    site's own PDF/PDFCache -- then finalizes one site at a time. Not memory-bounded the way
    the incremental parallel path is; acceptable here since nothing at multi-hundred-site
    scale runs without -w in the first place."""
    resList = []
    sliceResultsBySite = {}
    configBySite = {}
    for res in rawResults:
        if res['worktype'] != 'createPDFCacheMCRun':
            resList.append(res)
            continue
        # siteName, not studyShortname: studyShortname (workitem['studyName']) is the -sn
        # value, which stays fixed at the placeholder study for the whole -dr scan and
        # never varies per site -- siteName (workitem['siteName'], i.e. cm.getConfigVar
        # ('site')) is what actually changes per site in the directory-scan loop.
        siteName = res['siteName']
        configBySite.setdefault(siteName, res['config'])
        sliceResultsBySite.setdefault(siteName, []).append((res['cacheDF'], res['groupCount']))
    for siteName, sliceResults in sliceResultsBySite.items():
        resList.append(_finalizePDFCacheForSite(siteName, configBySite[siteName], sliceResults))
    return resList

def runCreatePDFCacheIncremental(workQueue, workers):
    """createPDFCacheMCRun-specific dispatch: consumes the imap_unordered iterator directly
    (no list(...) wrapper) and finalizes each site's PDFCache/PDF the moment its own MC set
    completes, instead of collecting every site's results first. Bounds memory to roughly
    pool-size-plus-a-few-sites'-in-flight rather than the full cross-site total -- a real
    risk for a -dr batch spanning many sites, where every site's raw cache slices would
    otherwise sit in memory simultaneously until the last site's last MC run finishes.

    Each MC run's cache slice is now written straight to its own small parquet file as soon
    as it arrives (sum._writeCacheSlice) instead of being held in a per-site list for a later
    pd.concat -- only a per-site slice counter and running groupCount are kept in memory, not
    the DataFrames themselves. Returns (lightResults, finalizeResults): lightResults drops
    each raw result's cacheDF (already on disk, no longer needed) so the long-lived result
    list doesn't hold every slice's data for the rest of the run; finalizeResults is the
    small number of per-site finalize summaries. Caller extends its own list with both."""
    import multiprocessing as mp
    logger.info(f"multiprocessing w/ work type: createPDFCacheMCRun, workers: {workers}")
    expectedCountBySite = {}
    for item in workQueue:
        expectedCountBySite[item['siteName']] = expectedCountBySite.get(item['siteName'], 0) + 1

    sliceCountBySite = {}
    groupCountBySite = {}
    configBySite = {}
    lightResults = []
    finalizeResults = []
    with Timer("createPDFCacheMCRun") as t0:
        with mp.Pool(workers, maxtasksperchild=1) as p:
            for res in p.imap_unordered(runWorkitem, workQueue):
                # siteName, not studyShortname -- see the comment in finalizeAllPDFCaches.
                siteName = res['siteName']
                configBySite.setdefault(siteName, res['config'])
                lightResults.append({k: v for k, v in res.items() if k != 'cacheDF'})
                cacheDF = res['cacheDF']
                if cacheDF is not None and not cacheDF.empty:
                    sliceIndex = sliceCountBySite.get(siteName, 0)
                    sum._writeCacheSlice(configBySite[siteName], cacheDF, sliceIndex)
                    sliceCountBySite[siteName] = sliceIndex + 1
                    groupCountBySite[siteName] = groupCountBySite.get(siteName, 0) + res['groupCount']
                expectedCountBySite[siteName] -= 1
                if expectedCountBySite[siteName] <= 0:
                    with Timer("Create PDF Cache") as t1:
                        statsDF = sum.finalizePDFCacheFromDisk(configBySite[siteName],
                                                                groupCountBySite.get(siteName, 0))
                    finalizeResults.append({
                        'worktype': 'createPDFCache',
                        'studyShortname': siteName,
                        'studyFilename': configBySite[siteName].get('studyFilename'),
                        'MCScenario': -1,
                        'runtime': t1.deltat.total_seconds(),
                        'statsDF': statsDF,
                        'pid': os.getpid(),
                    })
                    del configBySite[siteName]
                    del sliceCountBySite[siteName]
                    del groupCountBySite[siteName]
        t0.setCount(len(lightResults))
    return lightResults, finalizeResults

def _finalizeSummaryForSite(siteName, config, orderedPieces):
    with Timer("Summarize") as t1:
        sum.finalizeSummariesForSite(config, orderedPieces)
    return {
        'worktype': 'summarize',
        'studyShortname': siteName,
        'studyFilename': config.get('studyFilename'),
        'MCScenario': -1,
        'runtime': t1.deltat.total_seconds(),
        'statsDF': pd.DataFrame(),
        'pid': os.getpid(),
    }

def finalizeAllSummaries(rawResults):
    """Collect-everything-then-finalize-by-site variant of runSummarizeIncremental below,
    used by the non-parallel (-w<=1) path. Groups summarizeMCRun's per-(site, mcRun) pieces
    by siteName (same reasoning as finalizeAllPDFCaches -- a -dr scan's combined work queue
    spans multiple sites, and mixing pieces across sites would corrupt each site's summary).

    Pieces are sorted by mcRun before finalizing: summary 'readings' lists are positionally
    consumed downstream (the cross-site rollup and the methane/ethane ratio zip both assume
    position i means "the same mcRun" across every group), so pieces must be handed to
    finalizeSummariesForSite in a deterministic mcRun order rather than whatever order they
    happen to appear in this list."""
    resList = []
    piecesBySite = {}
    configBySite = {}
    for res in rawResults:
        if res['worktype'] != 'summarizeMCRun':
            resList.append(res)
            continue
        siteName = res['siteName']
        configBySite.setdefault(siteName, res['config'])
        piecesBySite.setdefault(siteName, []).append((res['MCScenario'], res['summaryPieces']))
    for siteName, pieces in piecesBySite.items():
        orderedPieces = [p for _, p in sorted(pieces, key=lambda x: x[0])]
        resList.append(_finalizeSummaryForSite(siteName, configBySite[siteName], orderedPieces))
    return resList

def runSummarizeIncremental(workQueue, workers):
    """summarizeMCRun-specific dispatch: consumes the imap_unordered iterator directly (no
    list(...) wrapper) and finalizes each site's summary the moment its own MC set completes,
    same incremental pattern as runCreatePDFCacheIncremental above. Each result's accumulator
    pieces are small per-group-per-mcRun DataFrames (not raw event data), so unlike PDFCache's
    cacheDF they're cheap to hold in memory per site until that site's mcRun set is complete,
    rather than needing to stream straight to disk.

    Pieces are sorted by mcRun before finalizing -- see finalizeAllSummaries's docstring for
    why: a real /compare run against this code caught SimSummary values scrambled by
    imap_unordered's non-deterministic completion order (SiteSummary/EventSummary themselves
    stayed exact, since per-group stats are order-independent; only the cross-group positional
    consumers were affected). Returns (lightResults, finalizeResults): lightResults drops each
    raw result's summaryPieces (already folded into a site's ordered pieces list) so the
    long-lived result list doesn't hold every mcRun's pieces for the rest of the run;
    finalizeResults is the small number of per-site finalize summaries. Caller extends its own
    list with both."""
    import multiprocessing as mp
    logger.info(f"multiprocessing w/ work type: summarizeMCRun, workers: {workers}")
    expectedCountBySite = {}
    for item in workQueue:
        expectedCountBySite[item['siteName']] = expectedCountBySite.get(item['siteName'], 0) + 1

    piecesBySite = {}
    configBySite = {}
    lightResults = []
    finalizeResults = []
    with Timer("summarizeMCRun") as t0:
        with mp.Pool(workers, maxtasksperchild=1) as p:
            for res in p.imap_unordered(runWorkitem, workQueue):
                # siteName, not studyShortname -- see the comment in finalizeAllPDFCaches.
                siteName = res['siteName']
                configBySite.setdefault(siteName, res['config'])
                piecesBySite.setdefault(siteName, []).append((res['MCScenario'], res['summaryPieces']))
                lightResults.append({k: v for k, v in res.items() if k != 'summaryPieces'})
                expectedCountBySite[siteName] -= 1
                if expectedCountBySite[siteName] <= 0:
                    orderedPieces = [p for _, p in sorted(piecesBySite[siteName], key=lambda x: x[0])]
                    finalizeResults.append(_finalizeSummaryForSite(siteName, configBySite[siteName], orderedPieces))
                    del piecesBySite[siteName]
                    del configBySite[siteName]
        t0.setCount(len(lightResults))
    return lightResults, finalizeResults

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
            workType = singleWorkitemQueue[0]['workType'] if singleWorkitemQueue else None
            if workType == 'createPDFCacheMCRun':
                if parallel:
                    queueResults, finalizeResults = runCreatePDFCacheIncremental(singleWorkitemQueue, workers)
                    resList.extend(queueResults)
                    resList.extend(finalizeResults)
                    continue
                else:
                    queueResults = finalizeAllPDFCaches(runLocal(singleWorkitemQueue))
            elif workType == 'summarizeMCRun':
                if parallel:
                    queueResults, finalizeResults = runSummarizeIncremental(singleWorkitemQueue, workers)
                    resList.extend(queueResults)
                    resList.extend(finalizeResults)
                    continue
                else:
                    queueResults = finalizeAllSummaries(runLocal(singleWorkitemQueue))
            elif parallel:
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

    resDF = pd.DataFrame(resList).drop(columns=['statsDF', 'siteName', 'config', 'cacheDF', 'groupCount', 'summaryPieces'])
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