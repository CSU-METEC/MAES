import pandas as pd
import AppUtils as au
from Timer import Timer
import pyarrow.parquet as pq
import pyarrow.lib as pl
import GraphUtils as gu
import logging
import Units as u
from pathlib import Path
import re
import urllib.parse as up
import Summaries as sm
import json
import pyarrow as pa

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
SECONDSINHOUR = 3600
US_TO_PER_METRIC_TON = 1.10231
US_TO_PER_HOUR_TO_KG_PER_HOUR = 0.1035

def toBaseParquet(config, df, dsName, partition_cols=['site', 'mcRun'], baseName=None):
    pqBase = au.expandFilename(config['parquetBaseTemplate'], {**config, 'dataset': dsName})
    toParquetkwArgs = {
        'partition_cols': partition_cols,
        'index': False
    }
    if baseName is not None:
        toParquetkwArgs = {**toParquetkwArgs, 'basename_template': f"{baseName}-{{i}}.parquet"}

    df.to_parquet(pqBase, **toParquetkwArgs)

def toBaseParquetFullConfig(config, df, dsName, partition_cols=['site', 'mcRun'], basename=None):
    # ── Skip any empty write ──────────────────────
    if df is None or df.empty:
        site_hint = None
        if df is not None and 'site' in df.columns and df['site'].nunique() == 1:
            site_hint = df['site'].iloc[0]
        msg = f"Skip {dsName}: empty DataFrame"
        if site_hint:
            msg += f" (site = {site_hint})"
        logger.info(msg)
        return

    if basename is None:
        basename_template = f"{dsName}-{{i}}.parquet"
    else:
        basename_template = f"{basename}-{{i}}.parquet"

    pqBase = config[dsName]
    au.ensureDirectory(pqBase)
    df.to_parquet(pqBase, partition_cols=partition_cols,
                  basename_template=basename_template,
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

def safe_read_parquet_summary(config, *, site=None, mcRun=None):
    """
    Robust reader for summary parquet partitions.

    Order of attempts
    -----------------
    1.  ParquetLib.readParquetSummary  – the fast path.
    2.  pyarrow.parquet.ParquetDataset.read() – still fast, but may raise the
        ArrowNotImplementedError if schemas clash.
    3.  Manual per-file read + pandas.concat – always works, a bit slower.

    Returns
    -------
    pandas.DataFrame
    """
    # ─────────────────────────────────────────────────────────────────────────
    # Build the on-disk path for the requested slice
    # ─────────────────────────────────────────────────────────────────────────
    base_dir = getattr(config, "parquetSummaryDS", None) or config["parquetSummaryDS"]
    base_path = Path(base_dir)

    if site:
        base_path = base_path / f"site={up.quote(site)}"
    if mcRun is not None:
        base_path = base_path / f"mcRun={mcRun}"

    # ─────────────────────────────────────────────────────────────────────────
    # 1. Try the library helper (fastest)
    # ─────────────────────────────────────────────────────────────────────────
    try:
        import ParquetLib as Pl  # local import avoids circularity at import-time
        return Pl.readParquetSummary(config, site=site, mcRun=mcRun)
    except Exception:
        # Anything thrown here will be retried with the slower fallbacks
        pass

    # ─────────────────────────────────────────────────────────────────────────
    # 2. Try pyarrow’s dataset reader (still vectorised & fast)
    # ─────────────────────────────────────────────────────────────────────────
    try:
        ds = pq.ParquetDataset(str(base_path), use_legacy_dataset=False)
        return ds.read().to_pandas()
    except Exception:
        # On ArrowNotImplementedError or other merge issues we fall through
        pass

    # ─────────────────────────────────────────────────────────────────────────
    # 3. Last-resort: read each physical file separately and concat
    # ─────────────────────────────────────────────────────────────────────────
    parquet_files = list(base_path.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(str(base_path))

    frames = []
    for fp in parquet_files:
        table = pq.read_table(fp)  # each individual file has a valid schema
        frames.append(table.to_pandas())  # convert here to sidestep Arrow merging

    df = pd.concat(frames, ignore_index=True, sort=False)

    # Ensure partition columns are present for downstream code
    if site and "site" not in df.columns:
        df["site"] = site
    if mcRun is not None and "mcRun" not in df.columns:
        df["mcRun"] = mcRun

    return df

def continueReadPQFile(pqBase, **kwargs):
    try:
        ds = pq.ParquetDataset(str(pqBase), use_legacy_dataset=False, **kwargs)
        return ds.read().to_pandas()
    except Exception as e:
        # On ArrowNotImplementedError or other merge issues we fall through
        pass

    # ─────────────────────────────────────────────────────────────────────────
    # 3. Last-resort: read each physical file separately and concat
    # ─────────────────────────────────────────────────────────────────────────
    parquet_files = list(pqBase.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(str(pqBase))

    frames = []
    for fp in parquet_files:
        table = pq.read_table(fp)  # each individual file has a valid schema
        frames.append(table.to_pandas())  # convert here to sidestep Arrow merging

    df = pd.concat(frames, ignore_index=True, sort=False)

    # Ensure partition columns are present for downstream code
    if site and "site" not in df.columns:
        df["site"] = site
    if mcRun is not None and "mcRun" not in df.columns:
        df["mcRun"] = mcRun

    return df


def baseReadParquetFullConfig(config, dsName, site=None, mcRun=None, species=None, sort_by=None, additionalFilters=None):
    if additionalFilters is not None:
        filter = additionalFilters
    else:
        filter = []

    expSite = up.quote(site) if site else None  # pandas to_parquet urlencodes filter strings
    # filter.extend(toFilter('site', expSite))   # don't include filters for site / mcrun.  Instead, they are directly used to get the parquet file path in _extendDSName below
    # filter.extend(toFilter('mcRun', mcRun))
    filter.extend(toFilter('species', species))
    if filter:
        filter = {'filters': filter}
    else:
        filter = {}

    pqBase = config[dsName]
    pqBase = _extendDSName(pqBase, expSite, mcRun)
    try:
        pqTable = pq.read_table(pqBase,
                                **filter)
    except FileNotFoundError as e:
        return e

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

def _safeReadParquetSummary(config, dsName, site=None, mcRun=None):
    try:
        return baseReadParquetFullConfig(config, 'parquetSummaryDS', site=site, mcRun=mcRun)
    except pl.ArrowNotImplementedError:
        logger.warning(f"Got ArrowNotImplementedError, {dsName=}, {site=}, {mcRun=}")
    pqBase = config[dsName]
    pqPath = Path(pqBase)
    parquet_files = list(pqPath.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(str(pqPath))

    frames = []
    for fp in parquet_files:
        try:
            table = pq.read_table(fp)  # each individual file has a valid schema
            frames.append(table.to_pandas())  # convert here to sidestep Arrow merging
        except Exception as e:
            logging.warning(f"fp: {str(fp)}, exception: {e}")

    df = pd.concat(frames, ignore_index=True, sort=False)
    return df[['eventID', 'timestamp', 'command', 'mcRun']]

def readParquetSummary(config, site=None, mcRun=None):
    # return baseReadParquetFullConfig(config, 'parquetSummaryDS', site=site, mcRun=mcRun)
    return _safeReadParquetSummary(config, 'parquetSummaryDS', site=site, mcRun=mcRun)

def readParquetEvents(config, site=None, mcRun=None, mergeGC=False, species=None, additionalEventFilters=[('command', '=', 'EMISSION')]):
    eventDF = readParquetRawEvents(config, site=site, mcRun=mcRun, additionalFilters=additionalEventFilters)
    if eventDF.empty:
        logging.warning(f"No emissions recorded for site {site} at MC run {mcRun}")
        return None
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
    # add psno and operator name in emissions
    operatorInfo = mdDF[mdDF['equipmentType'] == 'MEETFacility'][['facilityID', 'operator', 'psno']].drop_duplicates()
    # newRetDF = retDF.drop(columns=['psno','operator']).merge(operatorInfo, on='facilityID', how='left')
    psno_map = operatorInfo.set_index('facilityID')['psno']
    op_map   = operatorInfo.set_index('facilityID')['operator']
    retDF['psno'] = retDF['psno'].fillna(retDF['facilityID'].map(psno_map))
    retDF['operator'] = retDF['operator'].fillna(retDF['facilityID'].map(op_map))

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
    df = df.groupby(['facilityID', 'site', 'mcRun', 'species', 'modelEmissionCategory'])['emissions_USTonsPerYear'].sum().reset_index()

    # Sum of emissions for each mcRun
    sumDF = df.groupby(['facilityID', 'site', 'mcRun', 'species'], as_index=False)['emissions_USTonsPerYear'].sum()

    # Creating 'TOTAL' rows for each mcRun
    totalDF = pd.DataFrame({'facilityID': sumDF['facilityID'], 'site': sumDF['site'], 'mcRun': sumDF['mcRun'],
                            'species': sumDF['species'], 'modelEmissionCategory': 'TOTAL',
                            'emissions_USTonsPerYear': sumDF['emissions_USTonsPerYear']})
    emissCatDF = pd.concat([df, totalDF], ignore_index=True).sort_values(['facilityID', 'site', 'mcRun'])
    return emissCatDF


def processEquipEmissions(df):
    df = df.groupby(['site', 'facilityID', 'mcRun', 'METype', 'unitID', 'modelReadableName', 'modelEmissionCategory',
                     'species', 'emitterID'])['emissions_USTonsPerYear'].sum().reset_index()
    return df

def processInstantEquipEmissions(df):
    df['emissions_kgPerH'] = df['emission'] * SECONDSINHOUR
    df = df[['site', 'facilityID', 'mcRun', 'METype', 'unitID', 'modelReadableName', 'modelEmissionCategory',
             'timestamp', 'duration', 'species', 'emission', 'emissions_kgPerH', 'emitterID']]
    newColumnNames = {'emission': 'emissions_kgPerS', 'duration': 'duration_s', 'timestamp': 'timestamp_s'}
    df.rename(columns=newColumnNames, inplace=True)
    df = df[df['emissions_kgPerS'] > 0]
    return df

def filterAbnormalEmissions(df):
    valid_emitter_ids = df[df['modelEmissionCategory'] != 'FUGITIVE']['emitterID']
    df = df[df['emitterID'].isin(valid_emitter_ids)]
    return df

def addPsnoOperatorToParquets(retDF, psnoMap, operatorMap):

    retDF['psno'] = retDF['psno'].fillna(retDF['facilityID'].map(psnoMap))
    retDF['operator'] = retDF['operator'].fillna(retDF['facilityID'].map(operatorMap))
    return retDF

def postProcessParquetResults(config, df, site):
    simDuration = config['simDurationDays']
    df['emissions_USTonsPerYear'] = (df['emission'] * df['duration'] * u.KG_TO_SHORT_TONS) * u.DAYS_PER_YEAR / simDuration

    avg_duration = df[df['command'] == 'EMISSION']['duration'].mean()
    total_events = len(df[df['command'] == 'EMISSION'])
    total_years = simDuration / u.DAYS_PER_YEAR  # Assuming u.DAYS_PER_YEAR is defined in Units module
    avg_frequency = total_events / total_years

    operatorInfo = df[['facilityID', 'operator', 'psno']].drop_duplicates()

    # Get DFs for emissions for the parquet files
    logging.info("Creating Parquet Files for Emission by Categories for...")
    emissCatDFParq = processEmissionsCat(df)
    emissCatDFParq = emissCatDFParq.merge(operatorInfo, on='facilityID')
    logging.info("Creating Parquet Files for Emission by Equipment...")
    emissEquipDFParq = processEquipEmissions(df)
    emissEquipDFParq = emissEquipDFParq.merge(operatorInfo, on='facilityID')
    logging.info("Creating Parquet Files for Instantaneous Emissions by Equipment...")
    emissInstEquipDFParq = processInstantEquipEmissions(df)
    emissInstEquipDFParq = emissInstEquipDFParq.merge(operatorInfo, on='facilityID')
    

    toBaseParquet(config, emissCatDFParq, 'siteEmissionsbyCat', partition_cols=['facilityID'])
    toBaseParquet(config, emissEquipDFParq, 'siteEmissionsByEquip', partition_cols=['facilityID'])
    toBaseParquet(config, emissInstEquipDFParq, 'siteInstantEmissionsByEquip', partition_cols=['facilityID'])
    avg_data = pd.DataFrame({
        'facilityID': df['facilityID'].unique(),
        'average_duration_days': avg_duration,
        'average_annual_frequency': avg_frequency
    })
    toBaseParquet(config, avg_data, 'averageEmissionMetrics', partition_cols=['facilityID'])

    #Check for abnormal condition
    if not config['abnormal']:
        sm.generatedCsvSummaries(config, df, site, abnormal="ON")
        dfAbnormalOFF = filterAbnormalEmissions(df)
        if dfAbnormalOFF.empty:
            logger.info(f"No non-fugitive emissions where found for site {site}")
        else:    
            sm.generatedCsvSummaries(config, dfAbnormalOFF, site, abnormal="OFF")
       
    elif config['abnormal'].upper() == "OFF":
        dfAbnormalOFF = filterAbnormalEmissions(df)
        if dfAbnormalOFF.empty:
            logger.info(f"No non-fugitive emissions where found for site {site}")
        else:    
            sm.generatedCsvSummaries(config, dfAbnormalOFF, site, abnormal="OFF")
    elif config['abnormal'].upper() == "ON":
        sm.generatedCsvSummaries(config, df, site, abnormal="ON")

    else:
        raise(ValueError("abnormal value should be on or off"))


    return None  # to aggregate stats across sites


def postprocess(config):
    with Timer("Read events") as t0:
        logging.info("Read Parquet Files")
        eventDF2 = readParquetEvents(config,
                                     site=config['siteName'],
                                     mergeGC=True,
                                     species=['METHANE', 'ETHANE'],
                                     additionalEventFilters=[('command', '=', 'EMISSION')])
        if eventDF2 is None:
            return
        t0.setCount(len(eventDF2))

    with Timer("Process events") as t1:
        for fac, df in eventDF2.groupby('facilityID'):
            site = df['site'].unique()[0]
            postProcessParquetResults(config, df, site)


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