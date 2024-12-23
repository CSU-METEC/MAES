import pandas as pd
import AppUtils as au
from Timer import Timer
import pyarrow.parquet as pq
import GraphUtils as gu
import logging
import Units as u
import numpy as np
from pathlib import Path
import re

logger = logging.getLogger(__name__)

EVENT_DS = 'events'
TIMESERIES_DS = 'timeseries'
GC_DS = 'gascomposition'
METADATA_DS = 'metadata'
SUMMARY_DS = 'simsummary'

SUMMARY_STATE_DS = 'summaryState'
SUMMARY_FLUIDFLOW_DS = 'summaryFluidFlow'
SUMMARY_FLUIDFLOWROLLUP_DS = 'summaryFluidFlowRollup'
SUMMARY_EMITTER_DS = 'summaryEmitter'
SUMMARY_EMISSION_DS = 'summaryEmission'

def toBaseParquet(config, df, dsName, partition_cols=['site', 'mcRun']):
    pqBase = au.expandFilename(config['parquetBaseTemplate'], {**config, 'dataset': dsName})
    df.to_parquet(pqBase, partition_cols=partition_cols,
                  basename_template=f"{dsName}-{{i}}.parquet",
                  existing_data_behavior='overwrite_or_ignore')

def toBaseParquetFullConfig(config, df, dsName, partition_cols=['site', 'mcRun']):
    pqBase = config[dsName]
    au.ensureDirectory(pqBase)
    df.to_parquet(pqBase, partition_cols=partition_cols,
                  basename_template=f"{dsName}-{{i}}.parquet",
                  existing_data_behavior='overwrite_or_ignore',
                  engine='auto',
                  index=False
                  )

# clean up the equipment type field as per Matlab postprocessing code.
# for metadata entries of implCategory==MajorEquipment, replace the equipmentType field
#      with the contents of the model category

def cleanupEquipmentType(metadata):
    majorEquipmentDF = metadata[metadata['implCategory'] == 'MajorEquipment']
    majorEquipmentDF = majorEquipmentDF.assign(METype=majorEquipmentDF['modelCategory'])
    metadata = metadata.merge(majorEquipmentDF[['facilityID', 'unitID', 'METype']], on=['facilityID', 'unitID'], how='left')
    return metadata

def dumpSummary(df, config, tagName):
    outFile = config[tagName]
    df.to_csv(outFile, index=False)

def toParquet(config):
    with Timer("Read event log") as t0:
        eventDF, tsTable, gascomp, metadata = au.readCoreTables(config)
        t0.setCount(len(eventDF))

    metadata = metadata.assign(mcRun=config['MCIteration'])
    metadata = cleanupEquipmentType(metadata)

    with Timer("Coalesce") as t0:
        coalescedEventDF, eventListDF = gu.coalescePseudoEvents(eventDF)

    summaryDF = gu.createSummaryDF(eventDF)

    with Timer("Write") as t0:
        toBaseParquetFullConfig(config, coalescedEventDF, 'parquetEventDS')
        toBaseParquetFullConfig(config, eventListDF,      'parquetEventListDS')
        toBaseParquetFullConfig(config, tsTable,          'parquetTimeseriesDS')
        toBaseParquetFullConfig(config, gascomp,          'parquetGasCompositionDS')
        toBaseParquetFullConfig(config, metadata,         'parquetMetadataDS')
        toBaseParquetFullConfig(config, summaryDF,        'parquetSummaryDS')



def toFilter(varName, var):
    if var is not None:
        if isinstance(var, list):
            return [(varName, 'in', var)]
        else:
            return [(varName, '==', var)]
    return []


def baseReadParquet(config, dsName, mcRun=None, species=None, sort_by=None, additionalFilters=None):
    if additionalFilters is not None:
        filter = additionalFilters
    else:
        filter = []
    filter.extend(toFilter('mcRun', mcRun))
    filter.extend(toFilter('species', species))
    if filter:
        filter = {'filters': filter}
    else:
        filter = {}

    pqBase = au.expandFilename(config['parquetBaseTemplate'], {**config, 'dataset': dsName})
    try:
        pqTable = pq.read_table(pqBase,
                                **filter)
        if sort_by:
            pqTable = pqTable.sort_by(sort_by)

        pqDF = pqTable.to_pandas()
    except Exception as e:
        i = 10

    return pqDF

def _extendDSName(ds, site, mcRun):
    if site is None:
        if mcRun is None:
            return ds
        else:
            raise ValueError(f"Parquet dataset: {ds} must specify site filter if mcRun is set as a filter ({mcRun})")
    else:
        dsPath = Path(ds)
        if mcRun is None:
            return dsPath / f'site={site}'
        else:
            return dsPath / f'site={site}/mcRun={mcRun}'

def baseReadParquetFullConfig(config, dsName, site=None, mcRun=None, species=None, sort_by=None, additionalFilters=None):
    if additionalFilters is not None:
        filter = additionalFilters
    else:
        filter = []

    # filter.extend(toFilter('site', site))
    # filter.extend(toFilter('mcRun', mcRun))
    filter.extend(toFilter('species', species))
    if filter:
        filter = {'filters': filter}
    else:
        filter = {}

    pqBase = config[dsName]
    pqBase = _extendDSName(pqBase, site, mcRun)
    try:
        pqTable = pq.read_table(pqBase,
                                **filter)
    except FileNotFoundError as e:
        return None

    if sort_by:
        pqTable = pqTable.sort_by(sort_by)

    pqDF = pqTable.to_pandas()

    if site is not None:
        pqDF = pqDF.assign(site=site)
    if mcRun is not None:
        pqDF = pqDF.assign(mcRun=mcRun)


    return pqDF

def readParquetRawEvents(config, site=None, mcRun=None, additionalFilters=None):
    # return baseReadParquet(config, EVENT_DS,
    #                        mcRun=mcRun,
    #                        sort_by=[('timestamp', 'ascending'), ('eventID', 'ascending')],
    #                        additionalFilters=additionalFilters
    #                        )
    return baseReadParquetFullConfig(config, 'parquetEventDS',
                                     site=site,
                                     mcRun=mcRun,
                                     sort_by=[('timestamp', 'ascending'), ('eventID', 'ascending')],
                                     additionalFilters=additionalFilters
                                     )

def readParquetTimeseries(config, site=None, mcRun=None):
    return baseReadParquetFullConfig(config, 'parquetTimeseriesDS', site=site, mcRun=mcRun)

def readParquetGasComposition(config, site=None, mcRun=None, species=None):
    return baseReadParquetFullConfig(config, 'parquetGasCompositionDS', site=site, mcRun=mcRun, species=species)

def readParquetMetadata(config, site=None, mcRun=None, additionalMDFilters=None):
    return baseReadParquetFullConfig(config, 'parquetMetadataDS', site=site, mcRun=mcRun, additionalFilters=additionalMDFilters)

def readParquetSummary(config, site=None, mcRun=None):
    return baseReadParquetFullConfig(config, 'parquetSummaryDS', site=site, mcRun=mcRun)

def readParquetEvents(config, site=None, mcRun=None, mergeGC=False, species=None, additionalEventFilters=None):
    eventDF = readParquetRawEvents(config, site=site, mcRun=mcRun, additionalFilters=additionalEventFilters)
    tsDF = readParquetTimeseries(config, site=site, mcRun=mcRun)
    mdDF = readParquetMetadata(config, site=site, mcRun=mcRun)

    retDF = eventDF
    # todo: this conditional is nasty -- rewrite
    if mergeGC:
        gcDF = readParquetGasComposition(config, site=site, mcRun=mcRun, species=species)
        if gcDF is not None:
            emissionMask = eventDF['command'] == 'EMISSION'
            emissionDF = eventDF[emissionMask]
            nonEmissionDF = eventDF[~emissionMask]
            fullEmissionDF = gu.mergeEmissionRecords(emissionDF, tsDF, gcDF)
            retDF = pd.concat([fullEmissionDF, nonEmissionDF]).sort_values('eventID')
    else:
        if tsDF is not None:
            retDF = eventDF.merge(tsDF, how='left', on=['tsKey', 'mcRun', 'site'])

    mdDF['site'] = mdDF['site'].astype('str')
    retDF['site'] = retDF['site'].astype('str')
    retDF = retDF.merge(mdDF, how='left', on=['facilityID', 'unitID', 'emitterID', 'mcRun', 'site'])

    return retDF

def toParquetNestedOrAnds(listOfAndClauses):
    orList = list(listOfAndClauses)
    return orList

def readParquetEventsByMetadata(config, mcRun=None, mergeGC=False, species=None, additionalEventFilters=None, mdFilters=None):
    if mcRun is not None:
        mcRunClause = [('mcRun', '=', mcRun)]
    else:
        mcRunClause = []

    def toEmitterFilter(x):
        ret = [
            *mcRunClause,
            *additionalEventFilters,
            ('facilityID', '=', x.facilityID),
            ('unitID', '=', x.unitID),
            ('emitterID', '=', x.emitterID)
        ]
        return ret
    mdDF = readParquetMetadata(config, mcRun=mcRun, additionalMDFilters=mdFilters)
    if mdDF.empty:
        return pd.DataFrame({'mcRun': [], 'timestamp': [], 'nextTS': [], 'tsValue': [],
                             'facilityID': [], 'unitID': [], 'emitterID': []})
    additionalFiltersForEmitters = toParquetNestedOrAnds(map(toEmitterFilter, mdDF.itertuples()))
    return readParquetEvents(config, mcRun=None, mergeGC=mergeGC, species=species,
                             additionalEventFilters=additionalFiltersForEmitters)

def coalescePseudoEvents(eventTSDF):
    nonStateEvents = eventTSDF[eventTSDF.command != "STATE_TRANSITION"]
    stateEvents = eventTSDF[eventTSDF.command == "STATE_TRANSITION"]

    # Filter out zero length events
    stateEvents = stateEvents[stateEvents['duration'] > 0]

    listUnitIDs = stateEvents['unitID'].unique()
    coalesceEventTSDF = pd.DataFrame()
    for unit in listUnitIDs:
        filtTable = stateEvents[stateEvents.unitID == unit]
        session = (filtTable.state != filtTable.state.shift()).cumsum()
        unitSummarizedTable = filtTable.groupby(['state', session], as_index=False, sort=False).agg({
                                                                              'eventID': 'unique',
                                                                              'facilityID': 'first',
                                                                              'unitID': 'first',
                                                                              'emitterID': 'first',
                                                                              'mcRun': 'first',
                                                                              'timestamp': 'first',
                                                                              'command': 'first',
                                                                              'event': 'first',
                                                                              'duration': 'sum',
                                                                              'nextTS': lambda x: x.tail(1)
                                                                              })
        unitSummarizedTable = unitSummarizedTable.assign(eventList=unitSummarizedTable['eventID'],
                                                         eventID=unitSummarizedTable['eventID'].apply(lambda x: x[0]))

        coalesceEventTSDF = pd.concat([coalesceEventTSDF, unitSummarizedTable])

    coalesceEventTSDF = pd.concat([coalesceEventTSDF, nonStateEvents])
    # todo: Why are we dropping the index??
    coalesceEventTSDF = coalesceEventTSDF.sort_values(['eventID', 'timestamp']).reset_index()

    columnOrder = list(eventTSDF.columns)
    columnOrder.append('eventList')
    ret = coalesceEventTSDF[columnOrder]
    return ret


def processEmissionsCat(df):
    df = df.groupby(['facilityID', 'site', 'mcRun', 'modelEmissionCategory'])['emissions_USTonsPerYear'].sum().reset_index()

    # Sum of emissions for each mcRun
    sumDF = df.groupby(['facilityID', 'site', 'mcRun'], as_index=False)['emissions_USTonsPerYear'].sum()

    # Creating 'TOTAL' rows for each mcRun
    totalDF = pd.DataFrame({'facilityID': sumDF['facilityID'], 'site': sumDF['site'], 'mcRun': sumDF['mcRun'],
                             'modelEmissionCategory': 'TOTAL', 'emissions_USTonsPerYear': sumDF['emissions_USTonsPerYear']})
    emissCatDF = pd.concat([df, totalDF], ignore_index=True).sort_values(['facilityID', 'site', 'mcRun'])
    return emissCatDF


def processEquipEmissions(df):
    df = df.groupby(['site', 'facilityID', 'mcRun', 'METype', 'unitID', 'modelReadableName', 'modelEmissionCategory'])[
        'emissions_USTonsPerYear'].sum().reset_index()
    return df


def processInstantEquipEmissions(df):
    df['emissions_kgPerH'] = df['emission']*3600
    df = df[['site', 'facilityID', 'mcRun', 'METype', 'unitID', 'modelReadableName', 'modelEmissionCategory',
             'timestamp', 'duration', 'emission', 'emissions_kgPerH']]
    newColumnNames = {'emission': 'emissions_kgPerS', 'duration': 'duration_s', 'timestamp': 'timestamp_s'}
    df.rename(columns=newColumnNames, inplace=True)
    df = df[df['emissions_kgPerS'] > 0]
    return df


def calcFiveNumberSummary(emissCatDF):
    facilityID = emissCatDF['facilityID'].unique().tolist()
    summary = [facilityID]
    categories = ['FUGITIVE', 'VENTED', 'COMBUSTION', 'TOTAL']
    header = []

    for cat in categories:
        headStr = cat.capitalize()[:6]
        header.append([headStr + 'Min', headStr + 'Lower', headStr + 'Mean', headStr + 'Upper', headStr + 'Max'])
        catEmissions = emissCatDF[emissCatDF['modelEmissionCategory'] == cat]['emissions_USTonsPerYear'].tolist()
        if len(catEmissions) > 0:
            minList = min(catEmissions)
            maxList = max(catEmissions)
            mean = sum(catEmissions) / len(catEmissions)
            lower = np.percentile(catEmissions, 25)
            upper = np.percentile(catEmissions, 75)
            summary.append([minList, lower, mean, upper, maxList])
        else:
            summary.append([0, 0, 0, 0, 0])

    summary = [item for sublist in summary for item in sublist]
    headers = ['facilityID'] + [item for sublist in header for item in sublist]
    summaryDF = pd.DataFrame([summary], columns=headers)
    return summaryDF

def dumpEmissions(summaryDF, config):
    facID = summaryDF['facilityID'].unique().tolist()[0]
    # todo: this expandFilename is wrong
    # outFile = au.expandFilename(config[tagName], config) + str(facID) + '.csv'
    # todo: would it be better to put all the facility summaries into a single .csv file?
    outFile = au.expandFilename(config['siteEmissions'], {**config, 'facilityID': facID})
    summaryDF.to_csv(outFile, index=False)
    logger.info(f"Wrote {outFile}")


def postProcessParquetResults(config, df):
    simDuration = config['simDurationDays']
    df['emissions_USTonsPerYear'] = (df['emission'] * df['duration'] * u.KG_TO_SHORT_TONS) * u.DAYS_PER_YEAR / simDuration

    avg_duration = df[df['command'] == 'EMISSION']['duration'].mean()
    total_events = len(df[df['command'] == 'EMISSION'])
    total_years = simDuration / u.DAYS_PER_YEAR  # Assuming u.DAYS_PER_YEAR is defined in Units module
    avg_frequency = total_events / total_years

    # Get DFs for emissions by category (fugitive, vented, combustion) and by major equipment
    logging.info("Creating Parquet Files for Emission by Categories...")
    emissCatDF = processEmissionsCat(df)
    logging.info("Creating Parquet Files for Emission by Equipment...")
    emissEquipDF = processEquipEmissions(df)
    logging.info("Creating Parquet Files for Instantaneous Emissions by Equipment...")
    emissInstEquipDF = processInstantEquipEmissions(df)

    # Get 5 number summary for emission categories (vented, fugitives, combusted)
    summaryDF = calcFiveNumberSummary(emissCatDF)

    # Dump summaries
    dumpEmissions(summaryDF, config)
    toBaseParquet(config, emissCatDF, 'siteEmissionsbyCat', partition_cols=['facilityID'])
    toBaseParquet(config, emissEquipDF, 'siteEmissionsByEquip', partition_cols=['facilityID'])
    toBaseParquet(config, emissInstEquipDF, 'siteInstantEmissionsByEquip', partition_cols=['facilityID'])

    avg_data = pd.DataFrame({
        'facilityID': df['facilityID'].unique(),
        'average_duration_days': avg_duration,
        'average_annual_frequency': avg_frequency
    })
    toBaseParquet(config, avg_data, 'averageEmissionMetrics', partition_cols=['facilityID'])


def postprocess(config):
    with Timer("Read events") as t0:
        logging.info("Read Parquet Files")
        eventDF2 = readParquetEvents(config,
                                     mergeGC=True,
                                     species=['METHANE'],
                                     additionalEventFilters=[('command', '=', 'EMISSION')])
        t0.setCount(len(eventDF2))

    with Timer("Process events") as t1:
        for fac, df in eventDF2.groupby('facilityID'):
            postProcessParquetResults(config, df)

def getParquetMetadata(parquetDir):
    PARQUET_RE = re.compile(r'events\/site=(?P<site>.*)\/mcRun=(?P<mcRun>.*)')

    fakeConfig = {
        "parquetEventDS":                       f"{parquetDir}/events",
        "parquetTimeseriesDS":                  f"{parquetDir}/timeseries",
        "parquetGasCompositionDS":              f"{parquetDir}/gascomposition",
        "parquetMetadataDS":                    f"{parquetDir}/metadata",
        "parquetSummaryDS":                     f"{parquetDir}/simsummary",
        "parquetEventListDS":                   f"{parquetDir}/eventList",
        "parquetFilteredEventSummaryDS":        f"{parquetDir}/filteredEventSummary",
        "parquetSiteInstantaneousEmissionsDS":  f"{parquetDir}/siteInstantaneousEmissions",
        "parquetSiteEmissionsByEquipmentDS":    f"{parquetDir}/siteEmissionsByEquipment",
        "parquetSiteEmissionsByCategoryDS":     f"{parquetDir}/siteEmissionsByCategory",

    }

    pqBasePath = Path(parquetDir)
    matchList = []
    for singleFile in pqBasePath.glob('events/site=*/mcRun=*'):
        pqDSName = singleFile.relative_to(pqBasePath).as_posix()
        match = re.match(PARQUET_RE, pqDSName)
        matchDict = match.groupdict()
        matchList.append(matchDict)
    pqMetadataDF = pd.DataFrame(matchList)
    pqMetadataDF = pqMetadataDF.assign(mcRun=pqMetadataDF['mcRun'].astype(int))
    return pqMetadataDF, fakeConfig