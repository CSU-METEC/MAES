import pandas as pd
import AppUtils as au
import os
import json
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
US_TO_PER_HOUR_TO_KG_PER_HOUR = 0.1035

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
    eq_df = df.groupby(['site', 'facilityID', 'mcRun', 'METype', 'unitID', 'modelReadableName', 'modelEmissionCategory',
                     'species', 'emitterID'])['emissions_USTonsPerYear'].sum().reset_index()
    complete_df = fillEmptyDataWithZero(full_df=df, eq_df=eq_df)
    return complete_df



def get_average_event_count_per_mcRun(df: pd.DataFrame, unitID_name: str, model_name: str, species_name: str) -> float:
    """
    Computes the average number of emission events per Monte Carlo (MC) run
    for a given modelReadableName, species, and unitID.
    """
    # Filter the DataFrame for the specified emission type, species, and unitID
    df_filtered = df[
        (df["modelReadableName"] == model_name) &
        (df["species"] == species_name) &
        (df["unitID"] == unitID_name)
        ].copy()

    # Get the total number of MC runs in the dataset (max mcRun + 1)
    total_mcRuns = int(df["mcRun"].max()) + 1  # Ensures all mcRuns are accounted for

    # Count occurrences per mcRun
    count_per_mcRun = df_filtered.groupby("mcRun").size()

    # Create a Series covering all mcRuns (0 to max mcRun), defaulting to 0
    all_mcRuns = pd.Series(0, index=range(total_mcRuns))

    # Merge actual counts, filling missing MC runs with zeroes
    count_per_mcRun = all_mcRuns.add(count_per_mcRun, fill_value=0)

    return count_per_mcRun.mean(), total_mcRuns


def get_average_rate_and_duration(df: pd.DataFrame, unitID_name: str, model_name: str, species_name: str):
    """
    Computes the average emission rate (kg/h) and average duration (s)
    for a given modelReadableName, species, and unitID.
    """
    # Filter the DataFrame for the specified emission type, species, and unitID
    df_filtered = df[
        (df["modelReadableName"] == model_name) &
        (df["species"] == species_name) &
        (df["unitID"] == unitID_name)
        ].copy()

    # If no matching records exist, return 0 for both values
    if df_filtered.empty:
        return 0.0, 0.0

    return df_filtered["emission"].mean(), df_filtered["duration"].mean()


def create_summary_table(df, species):
    """
    Creates a summary table (DataFrame) that contains, for each unique
    combination of unitID and modelReadableName, the average event count,
    average emission rate, and average emission duration.
    """
    # Get unique combinations of unitID and modelReadableName
    unique_combinations = df[['unitID', 'modelReadableName']].drop_duplicates()
    results = []

    # Loop over each combination and compute the metrics using the functions above
    for _, row in unique_combinations.iterrows():
        unitID = row['unitID']
        model = row['modelReadableName']

        avg_event_count, _ = get_average_event_count_per_mcRun(df, unitID, model, species)
        avg_rate, avg_duration = get_average_rate_and_duration(df, unitID, model, species)

        results.append({
            'unitID': unitID,
            'modelReadableName': model,
            'species': species,
            'avg_event_count': avg_event_count,
            'avg_emission_rate (kg/h)': avg_rate,
            'avg_emission_duration (s)': avg_duration
        })

    summary_df = pd.DataFrame(results)
    return summary_df


def processInstantEquipEmissions(df):
    # if emissionCol:
    df['emissions_kgPerH'] = df['emission'] * SECONDSINHOUR
    # else:
    #      df['emissions_kgPerH'] = df['emissions_mtPerYear'] / 8.76
    df = df[['site', 'facilityID', 'mcRun', 'METype', 'unitID', 'modelReadableName', 'modelEmissionCategory',
             'timestamp', 'duration', 'species', 'emission', 'emissions_kgPerH']]
    newColumnNames = {'emission': 'emissions_kgPerS', 'duration': 'duration_s', 'timestamp': 'timestamp_s'}
    df.rename(columns=newColumnNames, inplace=True)
    df = df[df['emissions_kgPerS'] > 0]
    return df

def generate_intervals(series, mean_header, convert):
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

 
def calcFiveNumberSummary(emissCatDF, species, confidence_level=0.95, instantEmissions=False):
    if instantEmissions:
        emissionsColumn = "emissions_kgPerH"
        emissCatDF[emissionsColumn] = emissCatDF['emissions_USTonsPerYear'] * US_TO_PER_HOUR_TO_KG_PER_HOUR
        mt = "kg/hour"
    else:
        emissionsColumn = "emissions_MetricTonsPerYear"
        emissCatDF[emissionsColumn] = emissCatDF['emissions_USTonsPerYear'] / US_TO_PER_METRIC_TON # convert from US tons to metric tons
        mt = "mt/year"

    emissCatDF = emissCatDF[emissCatDF.species == species]
    facilityID = emissCatDF['facilityID'].unique().tolist()
    summary = [facilityID,  [species], [mt]]
    header = []
    alpha = 1 - confidence_level

    for cat, df in emissCatDF.groupby('modelEmissionCategory'):
        headStr = cat.capitalize()[:6]
        header.append([headStr + 'Min', headStr + 'Lower', headStr + 'Mean', headStr + 'Upper', headStr + 'Max', headStr + 'lower95CI', headStr + '95CI'])
        catEmissions = df[emissionsColumn]

        minList = min(catEmissions)
        maxList = max(catEmissions)
        mean = np.mean(catEmissions)
        lower = np.percentile(catEmissions, 25)
        upper = np.percentile(catEmissions, 75)
        ci_lower = np.percentile(catEmissions, alpha / 2 * 100)
        ci_upper = np.percentile(catEmissions, (1 - alpha / 2) * 100)
        summary.append([minList, lower, mean, upper, maxList, ci_lower, ci_upper])
        

    summary = [item for sublist in summary for item in sublist]
    headers = ['facilityID', 'species', 'units'] + [item for sublist in header for item in sublist]
    summaryDF = pd.DataFrame([summary], columns=headers)
    return summaryDF

def calcEmissSummaryByMEType(emissEquipDF, species, confidence_level=0.95, instantEmissions = False):
    if instantEmissions:
        emissionsColumn = "emissions_kgPerH"
        mt = "kg/hour"
        emissEquipDF[emissionsColumn] = emissEquipDF['emissions_USTonsPerYear'] * US_TO_PER_HOUR_TO_KG_PER_HOUR
    else:
        emissionsColumn = "emissions_MetricTonsPerYear"
        emissEquipDF[emissionsColumn] = emissEquipDF['emissions_USTonsPerYear'] / US_TO_PER_METRIC_TON # convert from US tons to metric tons
        mt = "mt/year"

    emissEquipDF = emissEquipDF[emissEquipDF.species == species]   # Get only methane data for the summary
    facilityID = emissEquipDF['facilityID'].unique().tolist()
    summary = [facilityID,  [species], [mt]]
    header = []
    alpha = 1 - confidence_level

    for equip, df in emissEquipDF.groupby('METype'):
        header.append([equip + 'Min', equip + 'Lower', equip + 'Mean', equip + 'Upper', equip + 'Max', equip + 'lower95CI', equip + '95CI'])
        equipEmissions = df.groupby('mcRun')[emissionsColumn].sum()
  
        minList = min(equipEmissions)
        maxList = max(equipEmissions)
        mean = np.mean(equipEmissions)
        lower = np.percentile(equipEmissions, 25)
        upper = np.percentile(equipEmissions, 75)
        ci_lower = np.percentile(equipEmissions, alpha / 2 * 100)
        ci_upper = np.percentile(equipEmissions, (1 - alpha / 2) * 100)
        summary.append([minList, lower, mean, upper, maxList, ci_lower, ci_upper])


    summary = [item for sublist in summary for item in sublist]
    summary = [x for x in summary if str(x) != 'nan']
    headers = ['facilityID', 'species', 'units'] + [item for sublist in header for item in sublist]
    equipEmissSummaryDF = pd.DataFrame([summary], columns=headers)
    return equipEmissSummaryDF


def dumpEmissions(summaryDF, config, summaryType, facID=None, abnormal=None):
    abnormal = abnormal.lower()

    match summaryType:
        case "facility":
            extension = f"_annualEmissions_by_site_abnormal_{abnormal}"

        case "equipment":
            extension = f"_annualEmissions_by_METype_abnormal_{abnormal}"

        case "unit_level":
            extension = f"_abnormal_{abnormal}"

        case "equip_group_level":
            extension = f"_abnormal_{abnormal}"

        case "fac_pdf":
            extension = f"_Fac_PDF_abnormal_{abnormal}"

        case "pdf_site_aggregate":
            extension = f"_PDF_for_site_abnormal_{abnormal}"

        case "detailed_annualEmissions_summary":
            extension = f"_annualEmissions_by_modelReadableName_abnormal_{abnormal}"

        case "detailed_instantEmissions_summary":
            extension = f"_instantEmissions_by_modelReadableName_abnormal_{abnormal}"

        case "inst_site":
            extension = f"_instantEmissions_by_site_abnormal_{abnormal}"

        case "instMETypes":
            extension = f"_instantEmissions_by_METype_abnormal_{abnormal}"

        case "avgERandDur":
            extension = f"_avg_ER_and_duration_by_modelReadableName_abnormal_{abnormal}"

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

def generatePDFs(config, df, abnormal):
    facilityDF = df[df['species'] == 'METHANE']
    meType = config['METype']
    unitID = config['unitID']
    for site, Sdf in facilityDF.groupby('site'):
        siteDF = Sdf[Sdf['site'] == site]
        siteEndSimDF = readParquetSummary(config, site=site)
        allMCruns = grouping(dfToGroup=siteDF, siteEndSimDF=siteEndSimDF, valueColName="emission")
        pdf = calcProbabilitiesAllMCs(allMCruns.values())
        pdf['siteCH4Emission_kg/h'] = pdf['value']
        pdf.drop(columns=['value', 'count'], inplace=True)

        dumpEmissions(pdf, config, "pdf_site_aggregate", facID=f"PDFs/{site}", abnormal=abnormal)

        if meType:
            meTypeAllMCruns = grouping(dfToGroup=siteDF, siteEndSimDF=siteEndSimDF, valueColName="emission", groupOptions=("METype", meType))
            meTypepdf = calcProbabilitiesAllMCs(meTypeAllMCruns.values())
            meTypepdf['equipCH4Emission_kg/h'] = meTypepdf['value']
            meTypepdf.drop(columns=['value', 'count'], inplace=True)
            dumpEmissions(meTypepdf, config, "equip_group_level", facID=f"PDFs/{site}_{meType}", abnormal=abnormal)
        else:
            for siMeType, meTyDF in siteDF.groupby('METype'):
                meTypeAllMCruns = grouping(dfToGroup=meTyDF, siteEndSimDF=siteEndSimDF, valueColName="emission")
                meTypepdf = calcProbabilitiesAllMCs(meTypeAllMCruns.values())
                meTypepdf['equipCH4Emission_kg/h'] = meTypepdf['value']
                meTypepdf.drop(columns=['value', 'count'], inplace=True)
                dumpEmissions(meTypepdf, config, "equip_group_level", facID=f"PDFs/{site}_PDF_for_all_{siMeType}", abnormal=abnormal)

        
        if unitID:
            unitAllMCruns = grouping(dfToGroup=siteDF, siteEndSimDF=siteEndSimDF, valueColName="emission", groupOptions=("unitID", unitID))
            unitPDF = calcProbabilitiesAllMCs(unitAllMCruns.values())
            unitPDF['unitCH4Emission_kg/h'] = unitPDF['value']
            unitPDF.drop(columns=['value', 'count'], inplace=True)
            dumpEmissions(unitPDF, config, "unit_level", facID=f"PDFs/{site}_{unitID}", abnormal=abnormal)
        else:
            for unitID, unitIDDF in siteDF.groupby('unitID'):
                unitAllMCruns = grouping(dfToGroup=unitIDDF, siteEndSimDF=siteEndSimDF, valueColName="emission")
                unitPDF = calcProbabilitiesAllMCs(unitAllMCruns.values())
                unitPDF['unitCH4Emission_kg/h'] = unitPDF['value']
                unitPDF.drop(columns=['value', 'count'], inplace=True)
                dumpEmissions(unitPDF, config, "unit_level", facID=f"PDFs/{site}_PDF_for_{unitID}", abnormal=abnormal)

def allModelReadableNamesDict():
    result_dict = {}
    folder_path = "./input/ModelFormulation"
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        if os.path.isfile(file_path) and file_name.endswith(".json"):
            with open(file_path, 'r') as file:
                data = json.load(file)

            # Extracting the "Value" for "Compressors" from "Model Parameters"
            compressor_value = None
            for param in data.get("Model Parameters", []):
                if param.get("Python Parameter") == "modelCategory" and param.get("Value"):
                    compressor_value = param["Value"]
                    break

            # Extracting "modelReadableName" and "modelEmissionCategory" values from "Emitters"
            emitters = [
                {"modelReadableName": emitter["Readable Name"], "modelEmissionCategory": emitter["Emission Category"]}
                for emitter in data.get("Emitters", [])
            ]

            if compressor_value:
                if compressor_value in result_dict:
                    result_dict[compressor_value].extend(emitters)
                else:
                    result_dict[compressor_value] = emitters

    # Remove duplicate values for each key
    for key in result_dict:
        unique_emitters = []
        seen_emitters = set()
        for emitter in result_dict[key]:
            emitter_tuple = (emitter["modelReadableName"], emitter["modelEmissionCategory"])
            if emitter_tuple not in seen_emitters:
                seen_emitters.add(emitter_tuple)
                unique_emitters.append(emitter)
        result_dict[key] = unique_emitters

    # Remove keys with empty lists
    result_dict = {key: value for key, value in result_dict.items() if value}

    return result_dict

def fillEmptyDataWithZero(full_df, eq_df):
    full_df = full_df[full_df['METype'].notnull() & (full_df['METype'] != "")]
    unit_info = {r['unitID']: {'METype': r['METype'], 'emitterID': r['emitterID']}
                 for _, r in full_df.iterrows()}
    model_dict = allModelReadableNamesDict()
    overall_species = list(eq_df['species'].unique())
    mcRuns, unitIDs = eq_df['mcRun'].unique(), set(unit_info.keys())
    missing = []

    for mc in mcRuns:
        for uid in unitIDs:
            METype, emitterID = unit_info[uid]['METype'], unit_info[uid]['emitterID']
            group = eq_df[(eq_df['mcRun'] == mc) & (eq_df['unitID'] == uid)]
            if METype not in model_dict:
                # Add missing species rows for units without a defined model dictionary.
                pres_species = set(group['species'].unique())
                for sp in set(overall_species) - pres_species:
                    missing.append({'mcRun': mc, 'unitID': uid, 'METype': METype, 'species': sp,
                                    'modelReadableName': None, 'modelEmissionCategory': None,
                                    'emitterID': emitterID, 'emissions_USTonsPerYear': 0})
            else:
                # For units with a model dictionary, for each species add missing model events.
                for sp in overall_species:
                    pres_models = set(group[group['species'] == sp]['modelReadableName'].dropna().unique())
                    for m in model_dict[METype]:
                        if m['modelReadableName'] not in pres_models:
                            missing.append({'mcRun': mc, 'unitID': uid, 'METype': METype, 'species': sp,
                                            'modelReadableName': m['modelReadableName'],
                                            'modelEmissionCategory': m['modelEmissionCategory'],
                                            'emitterID': emitterID, 'emissions_USTonsPerYear': 0})
    df_missing = pd.DataFrame(missing)
    df_complete = pd.concat([eq_df, df_missing], ignore_index=True)
    df_complete['emissions_USTonsPerYear'] = df_complete['emissions_USTonsPerYear'].fillna(0)
    return df_complete

def generatedCsvSummaries(config, df, fac, abnormal):
     # Get DFs for emissions for the summaries
    logging.info("Creating dataframes for Emission by Categories...")
    emissCatDF = processEmissionsCat(df)
    logging.info("Creating dataframes for Emission by Equipment...")
    emissEquipDF = processEquipEmissions(df)
    logging.info("Creating dataframes for Instantaneous Emissions by Equipment...")
    emissInstEquipDF = processInstantEquipEmissions(df)

    if config['fullSummaries']:
        # Get PDF at Site Level for CH4 Emissions
        generatePDFs(config=config, df=df, abnormal=abnormal)

        # Create a table showing the average emission rate and average duration of each emission type (modelReadableName)
        avgERandDur = create_summary_table(df, species="METHANE")
        avgERandDur = pd.concat([avgERandDur,create_summary_table(df,species="ETHANE")])

        # Get 5 number summary for emission categories (vented, fugitives, combusted, total)
        CategorySummaryDF = calcFiveNumberSummary(emissCatDF, species='METHANE', confidence_level=0.95)
        CategorySummaryDF = pd.concat([CategorySummaryDF, calcFiveNumberSummary(emissCatDF, species='ETHANE', confidence_level=0.95)])  # add ethane summary


        # Get 5 number instant emissions summary for emission categories (vented, fugitives, combusted, total)
        CategoryInstantSummaryDF = calcFiveNumberSummary(emissCatDF, species='METHANE', confidence_level=0.95, instantEmissions=True)
        CategoryInstantSummaryDF = pd.concat([CategoryInstantSummaryDF, calcFiveNumberSummary(emissCatDF, species='ETHANE', confidence_level=0.95, instantEmissions=True)])  # add ethane summary

        # Get detailed emissions
        detailed_emissionsDF = calc_detailed_emissions_summary(emissEquipDF, emissions_colmn="emissions_USTonsPerYear", converted_emission_colmn="emissions_MetricTonsPerYear")
        detailed_inst_emissionsDF = calc_detailed_emissions_summary(emissInstEquipDF, emissions_colmn="emissions_kgPerH")

        # Get emissions summary by METype
        equipEmissSummaryDF = calcEmissSummaryByMEType(emissEquipDF, species='METHANE', confidence_level=0.95)
        equipEmissSummaryDF = pd.concat([equipEmissSummaryDF, calcEmissSummaryByMEType(emissEquipDF, species='ETHANE', confidence_level=0.95)])  # add ethane summary

        # Get instant emissions summary by METype
        equipEmissInstantDF = calcEmissSummaryByMEType(emissEquipDF, species='METHANE', confidence_level=0.95, instantEmissions=True)
        equipEmissInstantDF = pd.concat([equipEmissInstantDF, calcEmissSummaryByMEType(emissEquipDF, species='ETHANE', confidence_level=0.95, instantEmissions=True)])  # add ethane summary

        # Dump summaries
        dumpEmissions(detailed_emissionsDF, config, "detailed_annualEmissions_summary", facID=f"AnnualEmissions/{fac}", abnormal=abnormal)
        dumpEmissions(CategorySummaryDF, config, "facility", facID=f"AnnualEmissions/{fac}", abnormal=abnormal)
        dumpEmissions(equipEmissSummaryDF , config, "equipment", facID=f"AnnualEmissions/{fac}", abnormal=abnormal)
        dumpEmissions(detailed_inst_emissionsDF, config, "detailed_instantEmissions_summary", facID=f"InstantaneousEmissions/{fac}", abnormal=abnormal)
        dumpEmissions(CategoryInstantSummaryDF, config, "inst_site", facID=f"InstantaneousEmissions/{fac}", abnormal=abnormal)
        dumpEmissions(equipEmissInstantDF, config, "instMETypes", facID=f"InstantaneousEmissions/{fac}", abnormal=abnormal)
        dumpEmissions(avgERandDur, config, "avgERandDur", facID=fac, abnormal=abnormal)

    if config['annualSummaries']:
        detailed_emissionsDF = calc_detailed_emissions_summary(emissEquipDF, emissions_colmn="emissions_USTonsPerYear", converted_emission_colmn="emissions_MetricTonsPerYear")
        # Get 5 number summary for emission categories (vented, fugitives, combusted, total)
        CategorySummaryDF = calcFiveNumberSummary(emissCatDF, species='METHANE', confidence_level=0.95)
        CategorySummaryDF = pd.concat([CategorySummaryDF, calcFiveNumberSummary(emissCatDF, species='ETHANE', confidence_level=0.95)])  # add ethane summary
          # Get emissions summary by METype
        equipEmissSummaryDF = calcEmissSummaryByMEType(emissEquipDF, species='METHANE', confidence_level=0.95)
        equipEmissSummaryDF = pd.concat([equipEmissSummaryDF, calcEmissSummaryByMEType(emissEquipDF, species='ETHANE', confidence_level=0.95)])  # add ethane summary

        # Dump summaries
        dumpEmissions(detailed_emissionsDF, config, "detailed_annualEmissions_summary", facID=f"AnnualEmissions/{fac}", abnormal=abnormal)
        dumpEmissions(CategorySummaryDF, config, "facility", facID=f"AnnualEmissions/{fac}", abnormal=abnormal)
        dumpEmissions(equipEmissSummaryDF , config, "equipment", facID=f"AnnualEmissions/{fac}", abnormal=abnormal)

    if config['instantaneousSummaries']:
        # Get detailed emissions
        detailed_inst_emissionsDF = calc_detailed_emissions_summary(emissInstEquipDF, emissions_colmn="emissions_kgPerH")
        # Get 5 number instant emissions summary for emission categories (vented, fugitives, combusted, total)
        CategoryInstantSummaryDF = calcFiveNumberSummary(emissCatDF, species='METHANE', confidence_level=0.95, instantEmissions=True)
        CategoryInstantSummaryDF = pd.concat([CategoryInstantSummaryDF, calcFiveNumberSummary(emissCatDF, species='ETHANE', confidence_level=0.95, instantEmissions=True)])  # add ethane summary
         # Get instant emissions summary by METype
        equipEmissInstantDF = calcEmissSummaryByMEType(emissEquipDF, species='METHANE', confidence_level=0.95, instantEmissions=True)
        equipEmissInstantDF = pd.concat([equipEmissInstantDF, calcEmissSummaryByMEType(emissEquipDF, species='ETHANE', confidence_level=0.95, instantEmissions=True)])  # add ethane summary

        dumpEmissions(detailed_inst_emissionsDF, config, "detailed_instantEmissions_summary", facID=f"InstantaneousEmissions/{fac}", abnormal=abnormal)
        dumpEmissions(CategoryInstantSummaryDF, config, "inst_site", facID=f"InstantaneousEmissions/{fac}", abnormal=abnormal)
        dumpEmissions(equipEmissInstantDF, config, "instMETypes", facID=f"InstantaneousEmissions/{fac}", abnormal=abnormal)
    
    if config['pdfSummaries']:
        # Get PDF at Site Level for CH4 Emissions
        generatePDFs(config=config, df=df, abnormal=abnormal)

    if config['avgDurSummaries']:
        avgERandDur = create_summary_table(df, species="METHANE")
        avgERandDur = pd.concat([avgERandDur,create_summary_table(df,species="ETHANE")])
        dumpEmissions(avgERandDur, config, "avgERandDur", facID=fac, abnormal=abnormal)

    return None
   
def filterAbnormalEmissions(df):
    valid_emitter_ids = df[df['modelEmissionCategory'] != 'FUGITIVE']['emitterID']
    df = df[df['emitterID'].isin(valid_emitter_ids)]
    return df

def postProcessParquetResults(config, df, fac):
    simDuration = config['simDurationDays']
    df['emissions_USTonsPerYear'] = (df['emission'] * df['duration'] * u.KG_TO_SHORT_TONS) * u.DAYS_PER_YEAR / simDuration

    avg_duration = df[df['command'] == 'EMISSION']['duration'].mean()
    total_events = len(df[df['command'] == 'EMISSION'])
    total_years = simDuration / u.DAYS_PER_YEAR  # Assuming u.DAYS_PER_YEAR is defined in Units module
    avg_frequency = total_events / total_years

    # Get DFs for emissions for the parquet files
    logging.info("Creating Parquet Files for Emission by Categories for...")
    emissCatDFParq = processEmissionsCat(df)
    logging.info("Creating Parquet Files for Emission by Equipment...")
    emissEquipDFParq = processEquipEmissions(df)
    logging.info("Creating Parquet Files for Instantaneous Emissions by Equipment...")
    emissInstEquipDFParq = processInstantEquipEmissions(df)
    #Check for abnormal condition

    if not config['abnormal']:
        generatedCsvSummaries(config, df, fac, abnormal="ON")
        dfAbnormalOFF = filterAbnormalEmissions(df)
        generatedCsvSummaries(config, dfAbnormalOFF, fac, abnormal="OFF")
    elif config['abnormal'].upper() == "OFF":
        dfAbnormalOFF = filterAbnormalEmissions(df)
        generatedCsvSummaries(config, dfAbnormalOFF, fac, abnormal="OFF")
    elif config['abnormal'].upper() == "ON":
        generatedCsvSummaries(config, df, fac, abnormal="ON")

    else:
        raise(ValueError("abnormal value should be on or off"))

   
    toBaseParquet(config, emissCatDFParq, 'siteEmissionsbyCat', partition_cols=['facilityID'])
    toBaseParquet(config, emissEquipDFParq, 'siteEmissionsByEquip', partition_cols=['facilityID'])
    toBaseParquet(config, emissInstEquipDFParq, 'siteInstantEmissionsByEquip', partition_cols=['facilityID'])

    avg_data = pd.DataFrame({
        'facilityID': df['facilityID'].unique(),
        'average_duration_days': avg_duration,
        'average_annual_frequency': avg_frequency
    })
    toBaseParquet(config, avg_data, 'averageEmissionMetrics', partition_cols=['facilityID'])

    return None  # to aggregate stats across sites


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
            postProcessParquetResults(config, df, fac)

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