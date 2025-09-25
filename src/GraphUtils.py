import logging
import AppUtils as au
from EventLogger import StreamingEventLogger
import pandas as pd
from pathlib import Path
import MEETExceptions as me
import json
import numpy as np
from Timer import Timer
import itertools

pd.options.mode.chained_assignment = None  # default='warn'

logger = logging.getLogger(__name__)

FIXED_EVENT_COLUMNS = [
    'eventID', 'facilityID', 'unitID', 'emitterID', 'mcRun',
    'timestamp', 'command', 'event', 'duration', 'nextTS',
    'gcKey', 'flowID', 'tsKey',
    # 'tsUnits'
]

def readCoreTables(config, studyName, runNum):
    config = au.updateMCScenarioDir(config, studyName, runNum)
    newConfig = {**config, 'MCScenario': runNum}
    logger.info(f"Reading simulation results from {config['outputDir']}, mcRun:  {runNum}")
    mdPath = au.expandFilename(config['mdScenarioTemplate'], newConfig, readonly=True)
    gcPath = au.expandFilename(config['GasComposition'],  newConfig, readonly=True)
    tsPath = au.expandFilename(config['tsTemplate'],  newConfig, readonly=True)

    metadata = pd.read_csv(mdPath)
    gascomp = pd.read_csv(gcPath, dtype={'gcKey': float})
    tstable = pd.read_csv(tsPath, dtype={'tsKey': float})

    eventDF = getEvents(config, config['runNumber'])
    cleanKeys(metadata)

    return eventDF, tstable, gascomp, metadata, newConfig

def mergeEmissionRecords(eventDF, tsTable, gascomp, debugEventList=False):
    emDF = eventDF[eventDF['command'] == 'EMISSION']
    emTsDF = emDF.merge(tsTable, on=['tsKey', 'mcRun', 'site'], how='left')
    emEmTsDF = emTsDF.merge(gascomp, on=['gcKey', 'mcRun', 'site'], how='left')
    # Convert from categorical to int -- otherwise we get an instance of the pivot_table per category
    emEmTsDF = emEmTsDF.assign(emission=emEmTsDF['tsValue']*emEmTsDF['gcValue'],
                               mcRun=emEmTsDF['mcRun'].astype(int),
                               emissionUnits='kg/s',
                               site=emEmTsDF['site'].astype(str))
    
    return emEmTsDF

def readCompleteEvents(config):
    eventDF, tsTable, gascomp, metadata, config = readCoreTables(config, config['studyName'], config['runNumber'])

    # filter out zero-length emission events
    eventDF = eventDF[eventDF['duration'].astype(float) != 0.0]
    eventDF = coalescePseudoEvents(eventDF)

    emissionEvents = mergeEmissionRecords(eventDF, tsTable, gascomp)
    nonEmissionEvents = eventDF[eventDF['command'] != 'EMISSION']
    ret = (pd.concat([nonEmissionEvents, emissionEvents])
           )

    # ret = coalesceFluidFlows(eventDF, tsTable, gascomp)

    eventTSDF = ret.merge(metadata, on=['facilityID', 'unitID', 'emitterID'], how='left')
    # coalesceEventTSDF = coalescePseudoEvents(eventTSDF)

    #return eventTSDF.sort_values('eventID'), metadata, config
    return eventTSDF, config

def coalesceEmissionEvents(eventDF):
    emissionEventMask = (eventDF['command'] == 'EMISSION')
    emissionEvents = eventDF[emissionEventMask]
    nonEmissionEvents = eventDF[~emissionEventMask]

    for (facilityID, unitID, emitterID, mcRun), emitterGrp in emissionEvents.groupby(['facilityID', 'unitID', 'emitterID', 'mcRun']):
        emitterGrp = emitterGrp.assign(shiftedTS=emitterGrp['timestamp'].shift(-1))
        emitterGrp = emitterGrp.assign(tsDiff=emitterGrp['nextTS']-emitterGrp['shiftedTS'])
        pass

    retDF = pd.concat([emissionEvents, nonEmissionEvents]).sort_values('timestamp')
    return retDF

def coalescePseudoEvents(eventTSDF):
    with Timer("  Start") as t00:
        nonStateEvents = eventTSDF[eventTSDF.command != "STATE_TRANSITION"]
    with Timer("  Start") as t000:
        # stateEvents = eventTSDF[eventTSDF.command == "STATE_TRANSITION"]
        stateEvents = eventTSDF[~eventTSDF.index.isin(nonStateEvents.index)]

    with Timer("  Filter zero len") as t01:

        # Filter out zero length events
        stateEvents = stateEvents[stateEvents['duration'] > 0]

    with Timer("  Create summary lists") as t02:

        unitSummaries = [] # keep the non state events for inclusion later
        coalescedEventIDs = [pd.DataFrame(columns=['eventID', 'events', 'mcRun', 'site'])]

    # with Timer("empty groupby") as t0:
    #     numGrps = 0
    #     for unitID, filtTable in stateEvents.groupby('unitID', sort=False):
    #         numGrps += 1
    #     t0.setCount(numGrps)
    #
    # with Timer("iterate over unique unitIDs") as t0:
    #     unitIDList = stateEvents['unitID'].unique()
    #     numGrps = 0
    #     for singleUnitID in unitIDList:
    #         filtTable = stateEvents[stateEvents['unitID'] == singleUnitID]
    #         numGrps += 1
    #     t0.setCount(numGrps)

    with Timer("  Filter events") as t0:
        debugTable = []
        for unitID, filtTable in stateEvents.groupby('unitID', sort=False):
            debugTable.append(filtTable)
            filtTable = filtTable.assign(stateCode=pd.Categorical(filtTable.state).codes)
            scd = filtTable.stateCode.diff(1)
            dupStates = (scd == 0)
            dupList = zip(dupStates.index, dupStates)
            for k, g in itertools.groupby(dupList, lambda x: x[1]):
                g = list(g)
                idxList = list(map(lambda x: x[0], g))
                if k: # True means there are multiple events that need to be coasleced
                    eventTable = filtTable.loc[idxList]
                    ts = eventTable.iloc[0].timestamp
                    nextTS = eventTable.iloc[-1].nextTS
                    dur = nextTS - ts
                    calcDur = eventTable['duration'].sum()
                    if calcDur != dur:
                        raise AssertionError(f"Duration {dur} does not equial calculated duration {calcDur} in coalescePseudoEvents")
                    unitSummarizedTable = filtTable.loc[idxList[0:1]].assign(
                        timestamp=ts,
                        nextTS=nextTS,
                        duration=dur
                    )
                    coalesceDF = pd.DataFrame(data={'eventID': eventTable.iloc[0].eventID,
                                                    'events': idxList,
                                                    'mcRun': eventTable.iloc[0].mcRun,
                                                    'site': eventTable.iloc[0].site})
                else:  # False means no events to coalesce -- bring them over directly
                    unitSummarizedTable = filtTable.loc[idxList]
                    coalesceDF = pd.DataFrame(data={'eventID': idxList,
                                                    'events': idxList,
                                                    'mcRun': unitSummarizedTable.iloc[0].mcRun,
                                                    'site': unitSummarizedTable.iloc[0].site})

                unitSummaries.append(unitSummarizedTable)
                coalescedEventIDs.append(coalesceDF)

    with Timer("  Concat events") as t1:
        coalescedStateEventDF = pd.concat(unitSummaries)
        completeEventDF = pd.concat([nonStateEvents, coalescedStateEventDF])

    with Timer("  Sort coalesced events") as t2:
        completeEventDF = completeEventDF.sort_values(['eventID', 'timestamp'])

    with Timer("  Finish") as t3:
        columnOrder = list(eventTSDF.columns)
        coalescedEventDF = completeEventDF[columnOrder]
        coalescedEventListDF = pd.concat(coalescedEventIDs)
    return coalescedEventDF, coalescedEventListDF

def createSummaryDF(eventDF):
    summaryDF = eventDF[(eventDF['command'] == 'SIM-START') | (eventDF['command'] == 'SIM-STOP')]
    return summaryDF

def readEquipmentFile(config, mcRun=None):
    logger.info(f"Reading equipment file from {config['MCScenarioDir']}")
    mdPath = au.expandFilename(config['equipmentTemplate'], {**config}, readonly=True)
    eqDict = {}
    with open(mdPath, "r") as iFile:
        for singleEqtStr in iFile:
            singleEqt = json.loads(singleEqtStr)
            singleEqtKey = tuple(map(lambda x: x if x is not None else '', singleEqt['key']))
            eqDict[singleEqtKey] = singleEqt

    return eqDict

#
# check that every key referenced in table is defined in table2.  By convention, the key columns are named the same
# in both table and table1, and are named by the key parameter.
#

def validateTable(sourceTable, referenceTable, keyColumnName):
    srcKeys = set(sourceTable[keyColumnName].astype(int))
    refKeys = set(referenceTable[keyColumnName].astype(int))
    # are there any keys in the source set that are not in the reference set?
    undefinedKeys = sorted(list(srcKeys - refKeys))
    return undefinedKeys

def validateEmissions(config, studyName, runNum, eventCommand='EMISSION'):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s  %(message)s")
    eventLog, tstable, gascomp = readCoreTables(config, studyName, runNum)
    emissionEvents = eventLog[eventLog['command'] == eventCommand][FIXED_EVENT_COLUMNS]
    if emissionEvents.empty:
        return emissionEvents, tstable, gascomp # it's ok to have an event log with no emission events

    # validate that all emissionEvents have valid tsKeys
    validationsFailed = False
    errorCheck = validateTable(emissionEvents, tstable, 'tsKey')
    if len(errorCheck) > 0:
        msg = f"Unmatched entries in timeseriesTable, key: 'tsKey': {errorCheck}, path: {tsPath}"
        logger.error(msg)
        raise me.IllegalELementError(msg)
    errorCheck = validateTable(emissionEvents, gascomp, 'gcKey')
    if len(errorCheck) > 0:
        msg = f"Unmatched entries in gas composition table, key: 'gcKey': {errorCheck}, path: {gcPath}"
        logger.error(msg)
        raise me.IllegalElementError(msg)

    return emissionEvents, tstable, gascomp

#
# Utilities for reading and merging InstantaneousEmissions file
#

def secondaryInfoPath(infile):
    infilePath = Path(infile)
    secondaryEventInfoPath = infilePath.with_name('secondaryEventInfo.csv')
    return secondaryEventInfoPath

def cleanKeys(df):
    df['facilityID'].replace({np.nan: ''}, inplace=True)
    df['unitID'].replace({np.nan: ''}, inplace=True)
    df['emitterID'].replace({np.nan: ''}, inplace=True)

def getEvents(config, runNum):
    emPath = au.expandFilename(config['eventTemplate'], {**config, 'MCScenario': runNum}, readonly=True)
    eventLog = pd.read_csv(emPath)
    cleanKeys(eventLog)
    secondaryPath = secondaryInfoPath(emPath)
    secondaryDF = pd.read_csv(secondaryPath)
    secondaryInfoWideDF = secondaryDF.pivot(index='eventID', columns='fieldName', values='fieldValue')
    ret = eventLog.merge(secondaryInfoWideDF, left_on='eventID', right_on='eventID', how='left')
    if 'mdGroup' not in ret.columns:
        ret = ret.assign(mdGroup='')
    return ret

def getMetadata(config, runNum):
    mdPath = au.expandFilename(config['InstantaneousEvents'], {**config, 'MCScenario': runNum}, readonly=True)
    mdDF = pd.read_csv(mdPath)
    mdDF['facilityID'].replace({np.nan: ''}, inplace=True)
    mdDF['unitID'].replace({np.nan: ''}, inplace=True)
    mdDF['emitterID'].replace({np.nan: ''}, inplace=True)
    return mdDF


def calculateStateTiming(eventDF):
    simDuration = eventDF[eventDF['command'] == 'SIM-STOP'].iloc[0]['timestamp']
    stateTransitionDF = eventDF[
        (eventDF['command'] == 'STATE_TRANSITION')
        & (eventDF['event'] == 'START')
    ]

    # break event list into those that span the full duration vs. those that do not
    fullEventMask = (stateTransitionDF['timestamp'] == 0) & (stateTransitionDF['duration'] >= simDuration)
    fullEventsDF = stateTransitionDF[fullEventMask]
    partialEventsDF = stateTransitionDF[~fullEventMask]

    # Initial state transition durations are randomly chosen to have equipment running before simulation
    # Final state transition events can extend past the end of the simulation
    # Both of these throw off the timing summaries of state transitions, so filter them out

    if not partialEventsDF.empty:
        partialEventsDF = partialEventsDF[partialEventsDF['timestamp'] != 0]
        partialEventsDF = partialEventsDF[(partialEventsDF.nextTS < simDuration)]

    stateTransitionDF = pd.concat([fullEventsDF, partialEventsDF])

    stateTransitionPT = (stateTransitionDF.pivot_table(values=['duration', 'command'],
                                                       index=['facilityID', 'unitID', 'emitterID', 'state', 'mcRun'],
                                                       aggfunc={'duration': ['sum', 'min', 'max', 'mean'],
                                                                'command': 'count'})
                         .reset_index()
                         )
    stateTransitionPT.columns = ['facilityID', 'unitID', 'emitterID', 'state', 'mcRun',
                                 'count',
                                 'maxDuration', 'meanDuration', 'minDuration', 'duration']
    stateTotalPT = (stateTransitionPT.pivot_table(values=['duration', 'count'],
                                                  index=['facilityID', 'unitID', 'emitterID'],
                                                  aggfunc='sum')
                    .reset_index())
    stateTotalPT = stateTotalPT.rename(columns={'duration': 'totalDuration', 'count': 'totalCount'})

    stateTransitionPT = stateTransitionPT.merge(stateTotalPT,
                                                on=['facilityID', 'unitID', 'emitterID'])


    stateTransitionPT = stateTransitionPT.assign(
        durationRatio=stateTransitionPT['duration'] / stateTransitionPT['totalDuration'],
        countRatio=stateTransitionPT['count'] / stateTransitionPT['totalCount'],
    )

    return stateTransitionPT

def calculateEmissions(eventDF, pivotCol='modelEmissionCategory'):
    MD_FIELDS = {'equipmentType': 'first',
                 'latitude': 'first',
                 'longitude': 'first',
                 'modelID': 'first',
                 'modelCategory': 'first',
                 'modelSubcategory': 'first',
                 'modelEmissionCategory': 'first',
                 'modelReadableName': 'first',
                 'massUnits': 'first'}
    emissionDF = eventDF[eventDF['command'] == 'EMISSION']
    if emissionDF.empty:
        return emissionDF, emissionDF
    emissionDF = emissionDF.assign(totalMass=emissionDF['emission']*emissionDF['duration'], massUnits='kg')
    emitterPT = (emissionDF.pivot_table(index=['facilityID', 'unitID', 'emitterID', 'mcRun', 'species'],
                                        values=['totalMass',
                                                *MD_FIELDS.keys()],
                                        aggfunc={'totalMass': 'sum', **MD_FIELDS})
                        .fillna(0.0)
                        .reset_index()
                  )
    emitterPT = emitterPT.assign(modelSubcategory=emitterPT['modelSubcategory'].replace({0.0: ''}))

    emissionPT = (emissionDF.pivot_table(index=['species', 'mcRun'], values='totalMass', columns=pivotCol, aggfunc='sum')
                  .fillna(0.0)
                  .reset_index()
                  )
    return emitterPT, emissionPT

def calculateFluidFlows(eventDF):
    ffDF = eventDF[eventDF['command'] == 'FLUID-FLOW']
    if ffDF.empty:
        return ffDF, ffDF
    ffDF = ffDF.assign(driverRate=ffDF['driverRate'].astype(float),
                       mdGroup=ffDF['mdGroup'].fillna(''))
    ffDF = ffDF.assign(totalVolume=ffDF['driverRate']*ffDF['duration'])
    ffPT = (ffDF.pivot_table(index=['facilityID', 'unitID', 'emitterID', 'mcRun'],
                                   values=['totalVolume', 'driverUnits', 'mdGroup'],
                                   aggfunc={'totalVolume': 'sum', 'driverUnits': 'first', 'mdGroup': 'first'})
                  .fillna(0.0)
                  .reset_index()
                  )
    ffRollupPT = (ffPT.pivot_table(index=['mdGroup', 'mcRun'],
                                   values=['totalVolume', 'driverUnits'],
                                   aggfunc={'totalVolume': 'sum', 'driverUnits': 'first'})
                  .reset_index())
    return ffPT, ffRollupPT
