import os
import sys
import time
import json
import traceback
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
    # Writes its own slice straight to disk from inside the worker, keyed by mcRun (already
    # unique per work item, no need for a main-process-side counter), instead of returning
    # cacheDF through the Pool's IPC pipe -- a single mcRun's cache slice for a dense fixture
    # can run into the GB range, and with -w N workers that many can be in flight through IPC
    # simultaneously even though none of it was ever retained long-term on the receiving end.
    with Timer("Create PDF Cache Slice", loglevel=logging.DEBUG) as t0:
        cacheDF, groupCount = sum._buildCacheSliceForMCRun(config, config['MCScenario'])
        if cacheDF is not None and not cacheDF.empty:
            sum._writeCacheSlice(config, cacheDF, config['MCScenario'])
    return t0.deltat.total_seconds(), groupCount

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
        failed = False
        failureReason = None
        try:
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
                runtime, groupCount = createPDFCacheMCRun(workitem, simdm)
            elif worktype == 'simSummary':
                runtime = summarizeSimulation(workitem, simdm)
            else:
                logger.error(f"Unknown worktype: {worktype}")
        except Exception as e:
            # A worker-task exception escaping this function propagates raw through the
            # Pool's imap_unordered and unwinds the caller's `with mp.Pool(...)` block via
            # __exit__ -> terminate() -- but other workers can still genuinely be mid-task
            # at that moment (this is one item among many dispatched to the same pool), and
            # terminate()'s SIGTERM can catch one of them mid-write of its own unrelated
            # result, leaving the result-handler thread blocked forever waiting for bytes
            # that will never arrive (confirmed live: a real MalformedTimeseriesError from
            # one mcRun's data hung an otherwise-healthy 20-worker pool permanently, twice,
            # under two different Pool topologies -- the topology was never the problem).
            # Catching here instead means the pool always tears down only after every task
            # has genuinely returned, never mid-flight. runtime/statsDF/cacheDF/groupCount/
            # summaryPieces keep their sane empty defaults above; siteName/config below still
            # come from workitem itself, so callers that key off them are unaffected.
            #
            # This must never be mistaken for "the error doesn't matter" -- catching it here
            # only prevents the *pool* from deadlocking; it does not mean the run actually
            # succeeded. main()'s own final check inspects every result's 'failed' flag and
            # exits non-zero (job status FAILED, not DONE) if any task failed, with this
            # exact failureReason surfaced in the summary -- a real FileNotFoundError for a
            # required curated-data file once completed a whole -dr batch as "done" with over
            # half its sites silently producing zero data, which is far worse than a loud
            # crash would have been.
            failureType = type(e).__name__
            failureReason = f"{failureType}: {e}"
            logger.error(f"runWorkitem: {worktype} failed for file {workitem['studyFilename']}, "
                         f"mcIter {workitem['MCIteration']}, pid {os.getpid()}", exc_info=True)
            failed = True

    return {
        'worktype': worktype,
        'studyShortname': workitem['studyName'],
        'studyFilename': workitem['studyFilename'],
        'MCScenario': workitem['MCScenario'],
        'runtime': runtime,
        'statsDF': statsDF,
        'pid': os.getpid(),
        'failureReason': failureReason,
        # Exception class alone (no message) -- used to GROUP the job-level failure
        # summary. failureReason's full message differs per site/mcIter even for the
        # exact same underlying cause (e.g. every 'parquet' cascade failure names a
        # different site's own metadata.csv path), so grouping on the full string
        # never collapses those into one line; grouping on the type does.
        'failureType': failureType if failed else None,
        # Only meaningfully populated for 'createPDFCacheMCRun'/'summarizeMCRun' -- consumed
        # by finalizeAllPDFCaches()/runCreatePDFCacheIncremental() and
        # finalizeAllSummaries()/runSummarizeIncremental(), dropped before results_*.csv is
        # written (see main()).
        'siteName': workitem.get('siteName'),
        'config': workitem,
        'cacheDF': cacheDF,
        'groupCount': groupCount,
        'summaryPieces': summaryPieces,
        'failed': failed,
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

    Each MC run's cache slice is written straight to its own small parquet file by the worker
    itself (createPDFCacheMCRun, keyed by mcRun) before this function ever sees a result --
    cacheDF never travels back through the Pool's IPC pipe at all, only the small groupCount
    does, since a single dense fixture's slice can run into the GB range and -w workers'
    worth of those in flight at once added up to real memory pressure even though none of it
    was ever retained here. Returns (lightResults, finalizeResults): finalizeResults is the
    small number of per-site finalize summaries; caller extends its own list with both."""
    import multiprocessing as mp
    logger.info(f"multiprocessing w/ work type: createPDFCacheMCRun, workers: {workers}")
    expectedCountBySite = {}
    for item in workQueue:
        expectedCountBySite[item['siteName']] = expectedCountBySite.get(item['siteName'], 0) + 1

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
    # Per-phase wall-clock breakdown (see runDurations.json below). One entry per outer-loop
    # iteration, keyed by that phase's canonical name (createPDFCacheMCRun's/summarizeMCRun's
    # wall-clock is attributed to 'createPDFCache'/'summarize', matching ALL_PHASES -- their
    # own CPU-time still shows up as separate rows below since that comes from resList's own
    # worktype tags, not this dict).
    phaseWallClock = {}
    # if parallel:
    #     db = initializeDask(cm)
    with Timer("Run simulations") as t0:
        for singleWorkitemQueue in listOfWorkitemQueues:
            # Checked before dispatch (not on queueResults) since an empty queue would
            # otherwise silently skip the finalize step below.
            isPDFCacheMCRunQueue = bool(singleWorkitemQueue) and singleWorkitemQueue[0]['workType'] == 'createPDFCacheMCRun'
            isSummarizeMCRunQueue = bool(singleWorkitemQueue) and singleWorkitemQueue[0]['workType'] == 'summarizeMCRun'
            phaseName = singleWorkitemQueue[0]['workType'] if singleWorkitemQueue else None
            if phaseName == 'createPDFCacheMCRun':
                phaseName = 'createPDFCache'
            elif phaseName == 'summarizeMCRun':
                phaseName = 'summarize'
            phaseStart = time.time()
            if parallel and isPDFCacheMCRunQueue:
                queueResults, finalizeResults = runCreatePDFCacheIncremental(singleWorkitemQueue, workers)
                resList.extend(queueResults)
                resList.extend(finalizeResults)
            elif parallel and isSummarizeMCRunQueue:
                queueResults, finalizeResults = runSummarizeIncremental(singleWorkitemQueue, workers)
                resList.extend(queueResults)
                resList.extend(finalizeResults)
            elif parallel:
                # queueResults = runDask(singleWorkitemQueue, db)
                queueResults = runMultiprocessing(singleWorkitemQueue, workers)
                resList.extend(queueResults)
            else:
                queueResults = runLocal(singleWorkitemQueue)
                resList.extend(queueResults)
                if isPDFCacheMCRunQueue:
                    resList.extend(finalizeAllPDFCaches(queueResults))
                elif isSummarizeMCRunQueue:
                    resList.extend(finalizeAllSummaries(queueResults))
            if phaseName is not None:
                phaseWallClock[phaseName] = phaseWallClock.get(phaseName, 0.0) + (time.time() - phaseStart)
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

    # Per-phase CPU-time breakdown (runtime summed across every workitem of a given
    # worktype -- single-core-equivalent cost) plus the wall-clock timing captured above,
    # written alongside the existing summary fields. The per-mcRun dispatch phases
    # ('createPDFCacheMCRun'/'summarizeMCRun') and their per-site finalize steps
    # ('createPDFCache'/'summarize') show as separate CPU-time rows here, since they're
    # tracked independently in resList -- wall-clock for the MCRun rows isn't independently
    # meaningful (the two steps overlap for -dr multi-site runs under the incremental
    # finalizer), so they're deliberately left out of phaseWallClock.
    phaseDurations = {}
    for r in resList:
        phaseDurations[r['worktype']] = phaseDurations.get(r['worktype'], 0.0) + r['runtime']
    simulationRoot = cm.getConfigVar('simulationRoot')
    if simulationRoot:
        durationsPath = Path(simulationRoot) / "runDurations.json"
        durationsPath.parent.mkdir(parents=True, exist_ok=True)
        durationsPath.write_text(json.dumps({
            'clockTimeSeconds': clocktime,
            'totalRuntimeSeconds': totalRuntime,
            'monteCarloIterations': totalMCIterations,
            'phaseDurationsSeconds': phaseDurations,
            'phaseWallClockSeconds': phaseWallClock,
        }))
        logger.info(f"Wrote {durationsPath}")

    resDF = pd.DataFrame(resList).drop(columns=['statsDF', 'siteName', 'config', 'cacheDF', 'groupCount', 'summaryPieces'])
    resFileFormat = f"results_{cm.getConfigVar('scenarioTimestampFormat')}.csv"
    resFilename = dt.datetime.now().strftime(resFileFormat)
    resDF = resDF.assign(scenarioTimestamp=cm.getConfigVar('scenarioTimestamp'))
    resDF.to_csv(resFilename, index=False)
    logger.info(f"Wrote {resFilename}")

    # runWorkitem's own try/except (see its docstring) exists only to stop one task's
    # exception from deadlocking the Pool it shares with other still-running tasks -- it
    # was never meant to make a real failure look like success. A missing required
    # curated-data file once let a whole -dr batch finish as "done" with over half its
    # sites silently producing zero data; nothing about that run's own status or log
    # tail said so unless someone happened to grep for it. Check every result here,
    # after every output file above has already been written (so a failed run still
    # leaves runDurations.json/results_*.csv behind for whoever investigates), and exit
    # non-zero if anything failed -- MaesQueueWorker.py maps a non-zero returncode
    # straight to job status FAILED, so this is what actually makes the queue/web UI
    # show the run as failed instead of done.
    failedItems = [r for r in resList if r.get('failed')]
    if failedItems:
        logger.error(f"{len(failedItems)} of {len(resList)} work items failed -- this run's "
                     f"output is incomplete and must not be treated as a successful result.")
        logger.error("Per-item detail:")
        for r in failedItems:
            logger.error(f"  {r['worktype']} failed for {r['studyFilename']} "
                         f"(mcIter {r['MCScenario']}): {r.get('failureReason')}")
        # A single root cause (e.g. one missing curated-data file) commonly fails many
        # work items identically, plus a second wave of *different* cascade failures
        # once dependent worktypes (e.g. 'parquet') can't find output that an earlier
        # failed worktype (e.g. 'simulation') never wrote -- confirmed live: a missing
        # HeaterOperating.csv produced both a direct FileNotFoundError from 'simulation'
        # AND a distinct downstream FileNotFoundError from 'parquet' for the same sites.
        # Grouping by (worktype, reason) surfaces every distinct underlying cause with
        # its count. Printed LAST, after the per-item dump above, not before it -- with
        # hundreds of items the per-item list can run to hundreds of lines, and anyone
        # checking a failed run's log first looks at its tail (confirmed: that's exactly
        # how 'main's own crash traceback reads, since it dies right after printing its
        # one real exception). Putting this summary first meant that same tail-check
        # landed on an arbitrary per-item cascade line instead of the actual root cause.
        # Grouped by (worktype, exception TYPE) -- not the full message -- specifically
        # because the full message never collapses: a real live run had 224 'parquet'
        # cascade failures, each naming a different site's own metadata.csv path, so
        # grouping on the full string still printed 224 near-identical lines and buried
        # the 2 lines that actually mattered (222x/2x HeaterOperating.csv/HeaterMalfunction.csv
        # from 'simulation', the real root cause). Grouping on type alone collapses all
        # 224 into one line; one representative example (the first one seen) is kept
        # per group so the specific file/path is still visible without re-reading the
        # per-item dump above.
        groups: dict[tuple[str, str | None], dict] = {}
        for r in failedItems:
            key = (r['worktype'], r.get('failureType'))
            group = groups.setdefault(key, {'count': 0, 'example': r.get('failureReason')})
            group['count'] += 1
        logger.error("Distinct failure reasons (see this section first):")
        for (worktype, failureType), group in sorted(groups.items(), key=lambda kv: -kv[1]['count']):
            logger.error(f"  {worktype}: {group['count']}x {failureType} (e.g. {group['example']})")
        sys.exit(1)

# set this up as preMain so config does not get instantiated as a global variable

def preMain():
    cm, args = au.getConfig()
    main(cm)

if __name__ == "__main__":
    preMain()