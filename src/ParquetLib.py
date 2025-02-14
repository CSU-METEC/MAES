import pandas as pd
import AppUtils as au
from Timer import Timer
import pyarrow.parquet as pq
import GraphUtils as gu
import logging
import Units as u
import numpy as np
from pathlib import Path
import scipy.stats as st
import Timeseries as ts
import re
import urllib.parse as up

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

def readParquetEvents(config, site=None, mcRun=None, mergeGC=False, species=None, additionalEventFilters=[('command', '=', 'EMISSION')]):
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
             'timestamp', 'duration', 'species', 'emission', 'emissions_kgPerH']]
    newColumnNames = {'emission': 'emissions_kgPerS', 'duration': 'duration_s', 'timestamp': 'timestamp_s'}
    df.rename(columns=newColumnNames, inplace=True)
    df = df[df['emissions_kgPerS'] > 0]
    return df

def generate_intervals(series, mean_header,convert):
    modelNameValues = series / US_TO_PER_METRIC_TON if convert else series
    confidence = 0.95
    alpha = 1 - confidence
    ci_lower = np.percentile(modelNameValues, alpha / 2 * 100)
    ci_upper = np.percentile(modelNameValues, (1 - alpha / 2) * 100)
    mean_value = np.mean(modelNameValues)
    return {"lower95CI": ci_lower, "upper95CI":ci_upper, mean_header: mean_value}

def process_unit_summary(mType, unitId, emissions, lower_cis, upper_cis,headers):
    return pd.DataFrame([[
        mType, unitId, "Summed_modelReadableName",
        np.sum(emissions),
        np.sum(lower_cis),
        np.sum(upper_cis)
    ]], columns=headers)


def process_mtype_summary(mType, summary_df, headers, mean_header):
    grouped_summaries = []
    for (grouped_mType, mName), df in summary_df.groupby(["METype", "modelReadableName"]):
        if grouped_mType == mType:
            grouped_summaries.append([
                mType, "Summed_unitIds", mName,
                np.sum(df[mean_header]),
                np.sum(df["lower95CI"]),
                np.sum(df["upper95CI"])
            ])
    return pd.DataFrame(grouped_summaries, columns=headers)

def calc_detailed_emissions_summary(emissionsDf, emissions_colmn, converted_emission_colmn = None):
    if converted_emission_colmn:
        mean_header = "MeanCH4Emission_MetricTonsPerYear"
        convert = True
    else:
        mean_header = "MeanCH4Emission_kg/h"
        convert = False
    headers = ["METype", "unitID", "modelReadableName", mean_header, "lower95CI", "upper95CI"]
    summary = []
    
    old_unit_id = None
    old_mType = None
    mean_emission_values = []
    lower_ci_values = []
    upper_ci_values = []

    for (mType, unitId, mName), df in emissionsDf.groupby(["METype", "unitID", "modelReadableName"]):

        if old_mType is None:
            old_mType = mType

        if old_unit_id is None:
            old_unit_id = unitId

        if unitId != old_unit_id:
            summary.append(process_unit_summary(old_mType, old_unit_id, mean_emission_values, lower_ci_values, upper_ci_values,headers))
            mean_emission_values = []
            lower_ci_values = []
            upper_ci_values = []

            if mType != old_mType:
                summary.append(process_mtype_summary(old_mType, pd.concat(summary), headers, mean_header))
                old_mType = mType

            old_unit_id = unitId

        intervals = generate_intervals(df[emissions_colmn], mean_header,convert=convert)
        mean_emission_values.append(intervals[mean_header])
        lower_ci_values.append(intervals["lower95CI"])
        upper_ci_values.append(intervals["upper95CI"])

        summary.append(pd.DataFrame([[
            mType, unitId, mName,
            intervals[mean_header],
            intervals["lower95CI"],
            intervals["upper95CI"]
        ]], columns=headers))

    if mean_emission_values:
        summary.append(process_unit_summary(old_mType, old_unit_id, mean_emission_values, lower_ci_values, upper_ci_values,headers))
    summary.append(process_mtype_summary(old_mType, pd.concat(summary), headers, mean_header))

    final_summary = pd.concat(summary, ignore_index=True)
    return final_summary.sort_values(by=["METype", "unitID"], ascending=True)




def calcFiveNumberSummary(emissCatDF, species, confidence_level=0.95):
    emissCatDF = emissCatDF[emissCatDF.species == species]
    facilityID = emissCatDF['facilityID'].unique().tolist()
    summary = [facilityID,  [species], ['mt/year']]
    categories = ['FUGITIVE', 'VENTED', 'COMBUSTION', 'TOTAL']
    header = []
    alpha = 1 - confidence_level

    for cat in categories:
        headStr = cat.capitalize()[:6]
        header.append([headStr + 'Min', headStr + 'Lower', headStr + 'Mean', headStr + 'Upper', headStr + 'Max', headStr + 'lower95CI', headStr + '95CI'])
        catEmissions = emissCatDF[emissCatDF['modelEmissionCategory'] == cat]['emissions_USTonsPerYear'] * 0.9071847  # convert from US tons to metric tons
        catEmissions = catEmissions.tolist()
        if len(catEmissions) > 0:
            minList = min(catEmissions)
            maxList = max(catEmissions)
            mean = sum(catEmissions) / len(catEmissions)
            lower = np.percentile(catEmissions, 25)
            upper = np.percentile(catEmissions, 75)
            ci_lower = np.percentile(catEmissions, alpha/2*100)
            ci_upper = np.percentile(catEmissions, (1 - alpha / 2) * 100)
            summary.append([minList, lower, mean, upper, maxList, ci_lower, ci_upper])
        else:
            summary.append([0, 0, 0, 0, 0, None, None])


    summary = [item for sublist in summary for item in sublist]
    headers = ['facilityID', 'species', 'units'] + [item for sublist in header for item in sublist]
    summaryDF = pd.DataFrame([summary], columns=headers)
    return summaryDF

def calcEmissSummaryByMEType(emissEquipDF, species, confidence_level=0.95):
    nRuns = emissEquipDF['mcRun'].max() + 1
    emissEquipDF = emissEquipDF[emissEquipDF.species == species]   # Get only methane data for the summary
    facilityID = emissEquipDF['facilityID'].unique().tolist()
    summary = [facilityID,  [species], ['mt/year']]
    METype = ['Compressor', 'Tank', 'Separator', 'Heater', 'Flare', 'Well', 'Dehydrator', 'Misc']
    header = []
    alpha = 1 - confidence_level

    for equip in METype:
        headStr = equip
        header.append([headStr + 'Min', headStr + 'Lower', headStr + 'Mean', headStr + 'Upper', headStr + 'Max', headStr + 'lower95CI', headStr + '95CI'])
        equipEmissions = emissEquipDF[emissEquipDF['METype'] == equip]
        equipEmissions = equipEmissions.groupby(['mcRun'])['emissions_USTonsPerYear'].sum().tolist()
        if equipEmissions:
            minList = min(equipEmissions)
            maxList = max(equipEmissions)
            mean = sum(equipEmissions) / nRuns
            lower = np.percentile(equipEmissions, 25)
            upper = np.percentile(equipEmissions, 75)
            ci_lower = np.percentile(equipEmissions, alpha/2*100)
            ci_upper = np.percentile(equipEmissions, (1 - alpha / 2) * 100)
            summary.append([minList, lower, mean, upper, maxList, ci_lower, ci_upper])
        else:
            summary.append([0, 0, 0, 0, 0, None, None])

    summary = [item for sublist in summary for item in sublist]
    headers = ['facilityID', 'species', 'units'] + [item for sublist in header for item in sublist]
    equipEmissSummaryDF = pd.DataFrame([summary], columns=headers)
    return equipEmissSummaryDF


def dumpEmissions(summaryDF, config, summaryType, facID=None):
    abnormal = config['abnormal'].lower()

    match summaryType:
        case "facility":
            extension = "_Fac_Level"

        case "equipment":
            extension = f"_Equip_Level_abnormal_{abnormal}"

        case "unit_level":
            extension = f"_unit_level_abnormal_{abnormal}"

        case "equip_group_level":
            extension = f"_equip_group_level_abnormal_{abnormal}"

        case "fac_pdf":
            extension = f"_Fac_PDF_abnormal_{abnormal}"

        case "pdf_site_aggregate":
            extension = f"_Site_PDF_abnormal_{abnormal}"

        case "detailed_annualEmissions_summary":
            extension = "_detailed_annualEmissions_summary"

        case "detailed_instantEmissions_summary":
            extension = "_detailed_instantEmissions_summary"

        case _:
            extension = None


    if facID is None:
        facID = summaryDF['facilityID'].unique().tolist()[0]
    # todo: would it be better to put all the facility summaries into a single .csv file?
    outFile = au.expandFilename(config['siteEmissions'], {**config, 'facilityID': 'summaries/' + facID + extension})
    summaryDF.to_csv(outFile, index=False)
    logger.info(f"Wrote {outFile}")

def aggrSet(input_df, value_column, group_options=None):
    """Aggregates a DataFrame by specified options, creating Timeseries objects."""
    timeseries_set = []
    if group_options:
        input_df = input_df[input_df[group_options[0]] == group_options[1]]
    
    grouping_cols = ['facilityID', 'METype'] if value_column == "state" else ['facilityID', 'unitID', 'emitterID']
    TimeseriesClass = ts.TimeseriesCategorical if value_column == "state" else ts.TimeseriesRLE

    for _, subset_df in input_df.groupby(grouping_cols):
        timeseries_set.append(TimeseriesClass(subset_df, valueColName=value_column))

    if not timeseries_set:
        raise ValueError("Group options do not match input data")
    return timeseries_set

def grouping(dfToGroup, siteEndSimDF, valueColName, groupOptions=None):
    AllMcRuns = {}
    for mcRun, mcRunDF in dfToGroup.groupby('mcRun'):
        EndSimDF = siteEndSimDF[siteEndSimDF['mcRun'] == mcRun]
        simDuration = EndSimDF.loc[EndSimDF['command'] == 'SIM-STOP', 'timestamp'].values[0]
        totalTimeseriesSet = ts.TimeseriesSet(aggrSet(input_df=mcRunDF.sort_values(by=['nextTS'], ascending=[True]), value_column=valueColName, group_options=groupOptions))

        if valueColName == "emission":
            tdf = totalTimeseriesSet.sum()
            tdf.df = tdf.df[tdf.df['nextTS'] <= simDuration]
            tdf.df.loc[:, 'tsValue'] = tdf.df['tsValue'] * SECONDSINHOUR
            AllMcRuns[mcRun] = tdf
        else:
            for tscat in totalTimeseriesSet.tsSetList:
                tscat.df = tscat.df[tscat.df["nextTS"] <= simDuration]

            AllMcRuns[mcRun] = totalTimeseriesSet.tsSetList

    return AllMcRuns

def calcProbabilitiesAllMCs(tss):
    combined_ts_df = pd.concat([t.df for t in tss], ignore_index=True)
    combined_ts = ts.TimeseriesRLE(combined_ts_df.sort_values(by=['nextTS'], ascending=[True]), filterZeros=True)
    pdf = combined_ts.toPDF()
    return pdf.data

def postProcessParquetResults(config, df):
    abnormal = config['abnormal'].upper()
    simDuration = config['simDurationDays']
    meType = config['METype']
    unitID = config['unitID']

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

    #Check for abnormal condition
    if abnormal == "OFF":
        valid_emitter_ids = df[df['modelEmissionCategory'] != 'FUGITIVE']['emitterID']
        df = df[df['emitterID'].isin(valid_emitter_ids)]

    # Get PDF at Facility Level for CH4 Emissions
    facilityDF = df[df['species'] == 'METHANE']
    facEndSimDF = readParquetSummary(config)
    facilityGrouped = grouping(dfToGroup=facilityDF, siteEndSimDF=facEndSimDF, valueColName="emission")
    facilityPDF = calcProbabilitiesAllMCs(facilityGrouped.values())
    facilityPDF['FacilityCH4Emission_kg/h'] = facilityPDF['value']
    facilityPDF.drop(columns=['value', 'count'], inplace=True)
    # remove the value & count columns
    dumpEmissions(facilityPDF, config, "fac_pdf", facID=df['facilityID'].unique().tolist()[0])

    # Get PDF at Site Level for CH4 Emissions
    for site, Sdf in facilityDF.groupby('site'):
        siteDF = Sdf[Sdf['site'] == site]
        siteEndSimDF = readParquetSummary(config, site=site)
        allMCruns = grouping(dfToGroup=siteDF, siteEndSimDF=siteEndSimDF, valueColName="emission")
        pdf = calcProbabilitiesAllMCs(allMCruns.values())
        pdf['siteCH4Emission_kg/h'] = pdf['value']
        pdf.drop(columns=['value', 'count'], inplace=True)

        dumpEmissions(pdf, config, "pdf_site_aggregate", facID=site)

        if meType:
            meTypeAllMCruns = grouping(dfToGroup=siteDF, siteEndSimDF=siteEndSimDF, valueColName="emission", groupOptions=("METype", meType))
            meTypepdf = calcProbabilitiesAllMCs(meTypeAllMCruns.values())
            meTypepdf['equipCH4Emission_kg/h'] = meTypepdf['value']
            meTypepdf.drop(columns=['value', 'count'], inplace=True)
            dumpEmissions(meTypepdf, config, "equip_group_level", facID=f"{site}_{meType}")
        else:
            for siMeType, meTyDF in siteDF.groupby('METype'):
                meTypeAllMCruns = grouping(dfToGroup=meTyDF, siteEndSimDF=siteEndSimDF, valueColName="emission")
                meTypepdf = calcProbabilitiesAllMCs(meTypeAllMCruns.values())
                meTypepdf['equipCH4Emission_kg/h'] = meTypepdf['value']
                meTypepdf.drop(columns=['value', 'count'], inplace=True)
                dumpEmissions(meTypepdf, config, "equip_group_level", facID=f"{site}_{siMeType}")

        
        if unitID:
            unitAllMCruns = grouping(dfToGroup=siteDF, siteEndSimDF=siteEndSimDF, valueColName="emission", groupOptions=("unitID", unitID))
            unitPDF = calcProbabilitiesAllMCs(unitAllMCruns.values())
            unitPDF['unitCH4Emission_kg/h'] = unitPDF['value']
            unitPDF.drop(columns=['value', 'count'], inplace=True)
            dumpEmissions(unitPDF, config, "unit_level", facID=f"{site}_{unitID}")
        else:
            for unitID, unitIDDF in siteDF.groupby('unitID'):
                unitAllMCruns = grouping(dfToGroup=unitIDDF, siteEndSimDF=siteEndSimDF, valueColName="emission")
                unitPDF = calcProbabilitiesAllMCs(unitAllMCruns.values())
                unitPDF['unitCH4Emission_kg/h'] = unitPDF['value']
                unitPDF.drop(columns=['value', 'count'], inplace=True)
                dumpEmissions(unitPDF, config, "unit_level", facID=f"{site}_{unitID}")

        

    # Get 5 number summary for emission categories (vented, fugitives, combusted, total)
    summaryDF = calcFiveNumberSummary(emissCatDF, species='METHANE', confidence_level=0.95)
    summaryDF = pd.concat([summaryDF, calcFiveNumberSummary(emissCatDF, species='ETHANE', confidence_level=0.95)])  # add ethane summary
    detailed_emissionsDF = calc_detailed_emissions_summary(emissEquipDF, emissions_colmn="emissions_USTonsPerYear", converted_emission_colmn="emissions_MetricTonsPerYear")
    detailed_inst_emissionsDF = calc_detailed_emissions_summary(emissInstEquipDF, emissions_colmn="emissions_kgPerH")

    # Get emissions summary by METype
    if abnormal == "OFF":
        valid_emitter_ids = emissEquipDF[emissEquipDF['modelEmissionCategory'] != 'FUGITIVE']['emitterID']
        emissEquipDF = emissEquipDF[emissEquipDF['emitterID'].isin(valid_emitter_ids)]

    equipEmissSummaryDF = calcEmissSummaryByMEType(emissEquipDF, species='METHANE', confidence_level=0.95)
    equipEmissSummaryDF = pd.concat([equipEmissSummaryDF, calcEmissSummaryByMEType(emissEquipDF, species='ETHANE', confidence_level=0.95)])  # add ethane summary

    # Dump summaries
    dumpEmissions(detailed_emissionsDF, config, "detailed_annualEmissions_summary", facID=summaryDF['facilityID'].unique().tolist()[0])
    dumpEmissions(detailed_inst_emissionsDF, config, "detailed_instantEmissions_summary", facID=summaryDF['facilityID'].unique().tolist()[0])
    dumpEmissions(summaryDF, config, "facility")
    dumpEmissions(equipEmissSummaryDF , config, "equipment")
    toBaseParquet(config, emissCatDF, 'siteEmissionsbyCat', partition_cols=['facilityID'])
    toBaseParquet(config, emissEquipDF, 'siteEmissionsByEquip', partition_cols=['facilityID'])
    toBaseParquet(config, emissInstEquipDF, 'siteInstantEmissionsByEquip', partition_cols=['facilityID'])

    avg_data = pd.DataFrame({
        'facilityID': df['facilityID'].unique(),
        'average_duration_days': avg_duration,
        'average_annual_frequency': avg_frequency
    })
    toBaseParquet(config, avg_data, 'averageEmissionMetrics', partition_cols=['facilityID'])

    return summaryDF  # to aggregate stats across sites


def postprocess(config):
    with Timer("Read events") as t0:
        logging.info("Read Parquet Files")
        eventDF2 = readParquetEvents(config,
                                     site=config['siteName'],
                                     mergeGC=True,
                                     species=['METHANE', 'ETHANE'],
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