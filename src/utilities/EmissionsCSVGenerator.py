import logging
import math
import sys
import AppUtils as au
import pandas as pd
import logging
from EventLogger import StreamingEventLogger
from pathlib import Path

FIXED_EVENT_COLUMNS = [
    'eventID', 'facilityID', 'unitID', 'emitterID', 'mcRun',
    'timestamp', 'command', 'event', 'duration', 'nextTS',
    'gcKey', 'flowID', 'tsKey',
    # 'tsUnits'
]


def secondaryInfoPath(infile):
    infilePath = Path(infile)
    secondaryEventInfoPath = infilePath.with_name('secondaryEventInfo.csv')
    return secondaryEventInfoPath

def validateEmissions(config, studyName, runNum, eventCommand='EMISSION'):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s  %(message)s")
    config = au.updateMCScenarioDir(config, studyName, runNum)
    emPath = au.expandFilename(config['InstantaneousEvents'], {**config, 'MCScenario': runNum}, readonly=True)
    gcPath = au.expandFilename(config['GasComposition'],  {**config, 'MCScenario': runNum}, readonly=True)
    tsPath = au.expandFilename(config['tsTemplate'],  {**config, 'MCScenario': runNum}, readonly=True)
    # get tstable and gc table
    # eventLog = pd.read_csv(emPath)
    eventLog = StreamingEventLogger.restore(emPath)
    gascomp = pd.read_csv(gcPath)
    tstable = pd.read_csv(tsPath)
    emissionEvents = eventLog[eventLog['command'] == eventCommand][FIXED_EVENT_COLUMNS]
    if emissionEvents.empty:
        return emissionEvents, tstable, gascomp # it's ok to have an event log with no emission events

    # validate that all emissionEvents have valid tsKeys
    validationsFailed = False
    errorCheck = validateTable(emissionEvents, tstable, 'tsKey')
    if len(errorCheck) > 0:
        logging.error(f"Unmatched entries in timeseriesTable, key: 'tsKey': {errorCheck}, path: {tsPath}")
        validationsFailed = True
    errorCheck = validateTable(emissionEvents, gascomp, 'gcKey')
    if len(errorCheck) > 0:
        logging.error(f"Unmatched entries in gas composition table, key: 'gcKey': {errorCheck}, path: {gcPath}")
        logging.error(errorCheck)
        validationsFailed = True
    if validationsFailed:
        return False

    return emissionEvents, tstable, gascomp

def getInstEmissions(emissionEvents, tstable, gascomp):

    # Merge the emission events with the timeseries table, which will add the timeseries data
    emTSDF = emissionEvents.merge(tstable, left_on='tsKey', right_on='tsKey')
    # We need to convert the GC table from "long" format to "wide" format to merge it into the events table
    gascomp['speciesPerUnit'] = gascomp['species'].astype(str) + ' / gcUnit'
    gascomp['speciesPerkg'] = gascomp['species'].astype(str) + ' (kg/s)'
    emTSGCDF = emTSDF.merge(gascomp, left_on='gcKey', right_on='gcKey')
    emTSGCDF['kgValue'] = emTSGCDF['gcValue'] * emTSGCDF['tsValue']

    gcCoreDF = pd.pivot_table(emTSGCDF[['gcKey', 'gcUnits']], index='gcKey', aggfunc='first')
    gcPerUnitDF = pd.pivot_table(emTSGCDF[['gcKey', 'tsKey', 'species', 'gcValue', 'speciesPerUnit']],
                                 index=['gcKey', 'tsKey'], columns='speciesPerUnit', values='gcValue', aggfunc='first')
    gcDF = gcPerUnitDF
    gcPerkgDF = pd.pivot_table(emTSGCDF[['gcKey', 'tsKey', 'species', 'kgValue', 'speciesPerkg']],
                               index=['gcKey', 'tsKey'], columns='speciesPerkg', values='kgValue', aggfunc='first')
    gcDF = gcDF.merge(gcPerkgDF, left_index=True, right_index=True)
    gcDF.reset_index(inplace=True)

    gcDF = gcCoreDF.merge(gcDF, left_index=True, right_on='gcKey')

    # Now, merge in the gc data.
    mergedDF = emTSDF.merge(gcDF, left_on=['gcKey', 'tsKey'], right_on=['gcKey', 'tsKey'], how='left')
    return mergedDF

def getEvents(config, runNum):
    emPath = au.expandFilename(config['InstantaneousEvents'], {**config, 'MCScenario': runNum}, readonly=True)
    eventLog = pd.read_csv(emPath)
    secondaryPath = secondaryInfoPath(emPath)
    secondaryDF = pd.read_csv(secondaryPath)
    secondaryInfoWideDF = secondaryDF.pivot(index='eventID', columns='fieldName', values='fieldValue')
    ret = eventLog.merge(secondaryInfoWideDF, left_on='eventID', right_on='eventID', how='left')
    return ret


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

def validateAndWriteEmissions(config, studyName, runNumber):
    veRet = validateEmissions(config, studyName, runNumber)
    if not veRet:
        return False

    emissionEvents, tsTable, gascomp, _ = veRet
    iEmissionPath = au.expandFilename(config['InstantaneousEmissions'], {**config, 'MCScenario': config['runNumber']})
    instE = getInstEmissions(emissionEvents, tsTable, gascomp)
    instE.to_csv(iEmissionPath, index=False)

def preMain():
    config, _ = au.getConfig()
    veRet = validateAndWriteEmissions(config, config['studyName'], config['runNumber'])
    if not veRet:
        sys.exit(1)

if __name__ == "__main__":
    preMain()
