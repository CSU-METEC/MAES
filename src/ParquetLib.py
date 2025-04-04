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
    df = df.groupby(['site', 'facilityID', 'mcRun', 'METype', 'unitID', 'modelReadableName', 'modelEmissionCategory',
                     'species', 'emitterID'])['emissions_USTonsPerYear'].sum().reset_index()
    return df



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

    return df_filtered["emissions_kgPerH"].mean(), df_filtered["duration_s"].mean()


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
    df['emissions_kgPerH'] = df['emission'] * SECONDSINHOUR
    df = df[['site', 'facilityID', 'mcRun', 'METype', 'unitID', 'modelReadableName', 'modelEmissionCategory',
             'timestamp', 'duration', 'species', 'emission', 'emissions_kgPerH']]
    newColumnNames = {'emission': 'emissions_kgPerS', 'duration': 'duration_s', 'timestamp': 'timestamp_s'}
    df.rename(columns=newColumnNames, inplace=True)
    df = df[df['emissions_kgPerS'] > 0]
    return df


def calc_instemiss_by_modelReadableName(df):
    df_grouped = df.groupby(["METype", "unitID", "modelReadableName", "species"], as_index=False)[
        "emissions_kgPerH"].mean()
    df_grouped.rename(columns={"emissions_kgPerH": "mean_emissions"}, inplace=True)


    # Compute the 95% confidence interval for each group (unitID, modelReadableName, species)
    ci = 95
    alpha = 100 - ci
    ci_lower = df.groupby(["unitID", "modelReadableName", "species"])["emissions_kgPerH"].apply(
        lambda x: np.percentile(x, alpha / 2))
    ci_upper = df.groupby(["unitID", "modelReadableName", "species"])["emissions_kgPerH"].apply(
        lambda x: np.percentile(x, 100 - alpha / 2))

    # Merge CI back into grouped df
    df_grouped = df_grouped.merge(ci_lower.rename(f"{ci}%_ci_lower"), on=["unitID", "modelReadableName", "species"],
                                  how="left")
    df_grouped = df_grouped.merge(ci_upper.rename(f"{ci}%_ci_upper"), on=["unitID", "modelReadableName", "species"],
                                  how="left")
    df_grouped["Unit"] = "kg/hour"
    df_grouped = df_grouped.sort_values(
        by=["species", "METype", "unitID"],
        ascending=[False, True, True]
    ).reset_index(drop=True)

    return df_grouped



def calc_detailed_emissions_summary(emissionsDf, emissions_colmn, species, inst_emissions = False):
    if inst_emissions:
        emissionsDf[emissions_colmn] = emissionsDf[emissions_colmn] * US_TO_PER_HOUR_TO_KG_PER_HOUR
        mt = "kg/hour"
    else:
        emissionsDf[emissions_colmn] = emissionsDf[emissions_colmn] / US_TO_PER_METRIC_TON
        mt = "mt/year"

    ci = float(95)
    mean_header= "mean_emissions"
    ci_lower_header = f"{int(ci)}%_ci_lower"
    ci_upper_header = f"{int(ci)}%_ci_upper"
    alpha = 100 - ci
    emissionsDf = emissionsDf[emissionsDf['species'] == species]

    mcNameDf = emissionsDf.groupby(["mcRun","METype", "unitID", "modelReadableName"], as_index=False)[emissions_colmn].sum()
    mdNameDf = mcNameDf.groupby(["METype","unitID", "modelReadableName"], as_index=False)[emissions_colmn].mean()

    mdNameDf.rename(columns={emissions_colmn: mean_header}, inplace=True)

    ci_lower = mcNameDf.groupby(["METype", "unitID", "modelReadableName"])[emissions_colmn].apply(lambda x: np.percentile(x, alpha / 2))
    ci_upper = mcNameDf.groupby(["METype", "unitID", "modelReadableName"])[emissions_colmn].apply(lambda x: np.percentile(x, (100 - alpha / 2)))
    mdNameDf = mdNameDf.merge(ci_lower.rename(ci_lower_header), on=["METype", "unitID", "modelReadableName"], how="left")
    mdNameDf = mdNameDf.merge(ci_upper.rename(ci_upper_header), on=["METype", "unitID", "modelReadableName"], how="left")

    unitIDDF = mdNameDf.groupby(["METype", "unitID"], as_index=False)[[mean_header,ci_lower_header,ci_upper_header]].sum()
    unitIDDF["modelReadableName"] = "summed_modelReadableName"

    meTypeDf = unitIDDF.groupby(["METype","modelReadableName"], as_index=False)[[mean_header,ci_lower_header,ci_upper_header]].sum()
    meTypeDf["unitID"] = "summed_unitID"

    final_df = pd.concat([mdNameDf,unitIDDF,meTypeDf], ignore_index=True)
    
    total = mdNameDf.sum(numeric_only=True, axis=0)
    total["METype"] = "summed_METype"
    total["unitID"] = "summed_unitID"
    total["modelReadableName"] = "summed_modelReadableName"

    final_df = pd.concat([final_df, total.to_frame().T], ignore_index=True)
    final_df["species"] = species
    final_df["Unit"] = mt
    final_df = final_df.drop(final_df[(final_df[ci_lower_header] ==0) & (final_df[ci_upper_header] ==0) & (final_df[mean_header] ==0)].index)
    return final_df.sort_values(["METype"])

 
def calcFiveNumberSummary(emissCatDF, species, confidence_level=95, instantEmissions=False):
    if instantEmissions:
        emissionsColumn = "emissions_kgPerH"
        emissCatDF[emissionsColumn] = emissCatDF['emissions_USTonsPerYear'] * US_TO_PER_HOUR_TO_KG_PER_HOUR
        mt = "kg/hour"
    else:
        emissionsColumn = "emissions_MetricTonsPerYear"
        emissCatDF[emissionsColumn] = emissCatDF['emissions_USTonsPerYear'] / US_TO_PER_METRIC_TON # convert from US tons to metric tons
        mt = "mt/year"

    alpha = 100 - float(confidence_level)

    ci_lower_col = f"{confidence_level}%_ci_lower"
    ci_upper_col = f"{confidence_level}%_ci_upper"

    emissCatDF = emissCatDF[emissCatDF.species == species]
    mdCat = emissCatDF.groupby(["modelEmissionCategory"], as_index=False)[emissionsColumn].mean()

    min = emissCatDF.groupby(["modelEmissionCategory"])[emissionsColumn].min()
    max = emissCatDF.groupby(["modelEmissionCategory"])[emissionsColumn].max()

    lower = emissCatDF.groupby(["modelEmissionCategory"])[emissionsColumn].apply(lambda x: np.percentile(x, 25))
    upper = emissCatDF.groupby(["modelEmissionCategory"])[emissionsColumn].apply(lambda x: np.percentile(x, 75))

    ci_lower = emissCatDF.groupby(["modelEmissionCategory"])[emissionsColumn].apply(lambda x: np.percentile(x, alpha / 2))
    ci_upper = emissCatDF.groupby(["modelEmissionCategory"])[emissionsColumn].apply(lambda x: np.percentile(x, (100 - alpha / 2)))

    mdCat = mdCat.merge(min.rename("Min"), on=["modelEmissionCategory"], how="left")
    mdCat = mdCat.merge(max.rename("Max"), on=["modelEmissionCategory"], how="left")

    mdCat = mdCat.merge(lower.rename("Lower"), on=["modelEmissionCategory"], how="left")
    mdCat = mdCat.merge(upper.rename("Upper"), on=["modelEmissionCategory"], how="left")

    mdCat = mdCat.merge(ci_lower.rename(ci_lower_col), on=["modelEmissionCategory"], how="left")
    mdCat = mdCat.merge(ci_upper.rename(ci_upper_col), on=["modelEmissionCategory"], how="left")

    mdCat.rename(columns={emissionsColumn:'mean_emissions'}, inplace=True)
    mdCat["species"] = species
    mdCat["Unit"] = mt

    return mdCat

def calcEmissSummaryByMEType(emissEquipDF, species, confidence_level=95, instantEmissions = False):
    if instantEmissions:
        emissionsColumn = "emissions_kgPerH"
        mt = "kg/hour"
        emissEquipDF[emissionsColumn] = emissEquipDF['emissions_USTonsPerYear'] * US_TO_PER_HOUR_TO_KG_PER_HOUR
    else:
        emissionsColumn = "emissions_MetricTonsPerYear"
        emissEquipDF[emissionsColumn] = emissEquipDF['emissions_USTonsPerYear'] / US_TO_PER_METRIC_TON
        mt = "mt/year"

    emissEquipDF = emissEquipDF[emissEquipDF["species"] == species]
    alpha = 100 - float(confidence_level)

    mcEq = emissEquipDF.groupby(["mcRun", "METype"], as_index=False)[emissionsColumn].sum()

    medf = mcEq.groupby("METype", as_index=False)[emissionsColumn].mean()

    min = mcEq.groupby("METype")[emissionsColumn].min()
    max = mcEq.groupby("METype")[emissionsColumn].max()

    lower = mcEq.groupby("METype")[emissionsColumn].apply(lambda x : np.percentile(x, 25))
    upper = mcEq.groupby("METype")[emissionsColumn].apply(lambda x : np.percentile(x, 75))

    ci_lower = mcEq.groupby("METype")[emissionsColumn].apply(lambda x : np.percentile(x, alpha / 2))
    ci_upper = mcEq.groupby("METype")[emissionsColumn].apply(lambda x : np.percentile(x, (100 - alpha / 2)))

    medf = medf.merge(min.rename("Min"), on=["METype"], how="left")
    medf = medf.merge(max.rename("Max"), on=["METype"], how="left")
    medf = medf.merge(lower.rename("Lower"), on=["METype"], how="left")
    medf = medf.merge(upper.rename("Upper"), on=["METype"], how="left")
    medf = medf.merge(ci_lower.rename(f"{confidence_level}%_ci_lower"), on=["METype"], how="left")
    medf = medf.merge(ci_upper.rename(f"{confidence_level}%_ci_upper"), on=["METype"], how="left")

    total = medf.sum(numeric_only=True, axis=0)
    total["METype"] = "summed_METype"
    total = pd.concat([medf, total.to_frame().T], ignore_index=True)
    total.rename(columns={emissionsColumn:'mean_emissions'}, inplace=True)

    total["species"] = species
    total["Unit"] = mt

    return total


def dumpEmissions(summaryDF, config, summaryType, facID=None, abnormal=None):
    abnormal = abnormal.lower()

    match summaryType:
        case "facility":
            extension = f"annualEmissions_by_site_abnormal_{abnormal}"

        case "equipment":
            extension = f"annualEmissions_by_METype_abnormal_{abnormal}"

        case "unit_level":
            extension = f"_abnormal_{abnormal}"

        case "equip_group_level":
            extension = f"_abnormal_{abnormal}"

        case "pdf_site_aggregate":
            extension = f"PDF_for_site_abnormal_{abnormal}"

        case "detailed_annualEmissions_summary":
            extension = f"annualEmissions_by_modelReadableName_abnormal_{abnormal}"

        case "detailed_instantEmissions_summary":
            extension = f"instantEmissions_by_modelReadableName_abnormal_{abnormal}"

        case "avgERandDur":
            extension = f"avg_ER_and_duration_by_modelReadableName_abnormal_{abnormal}"

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
    if input_df.empty:
        logger.warning(f"Where {group_options[0]} = {group_options[1]} and the selected abnormal emissions options do not match input data")
        pass
    for _, subset_df in input_df.groupby(grouping_cols):
        timeseries_set.append(TimeseriesClass(subset_df, valueColName=value_column))
    return timeseries_set

def grouping(dfToGroup, siteEndSimDF, valueColName, groupOptions=None):
    AllMcRuns = {}
    for mcRun, mcRunDF in dfToGroup.groupby('mcRun'):
        EndSimDF = siteEndSimDF[siteEndSimDF['mcRun'] == mcRun]
        simDuration = EndSimDF.loc[EndSimDF['command'] == 'SIM-STOP', 'timestamp'].values[0]
        totalTimeseriesSet = ts.TimeseriesSet(aggrSet(input_df=mcRunDF.sort_values(by=['nextTS'], ascending=[True]), value_column=valueColName, group_options=groupOptions))

        if valueColName == "emission":
            tdf = totalTimeseriesSet.sum(filterZeros=False)
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
    combined_ts = ts.TimeseriesRLE(combined_ts_df.sort_values(by=['nextTS'], ascending=[True]), filterZeros=False)
    pdf = combined_ts.toPDF()
    return pdf.data

def generatePDFs(config, df, abnormal, fac):
    df = df[df['modelReadableName'] != 'Blowdown Event']    # exclude maintenance emissions
    facilityDF = df[df['species'] == 'METHANE']
    meType = config['METype']
    unitID = config['unitID']
    for site, Sdf in facilityDF.groupby('site'):
        siteDF = Sdf[Sdf['site'] == site]
        siteEndSimDF = readParquetSummary(config, site=site)
        allMCruns = grouping(dfToGroup=siteDF, siteEndSimDF=siteEndSimDF, valueColName="emission")
        pdf = calcProbabilitiesAllMCs(allMCruns.values())
        pdf['CH4_EmissionRate_kg/h'] = pdf['value']
        pdf.drop(columns=['value', 'count'], inplace=True)

        dumpEmissions(pdf, config, "pdf_site_aggregate", facID=f"PDFs/site={fac}/", abnormal=abnormal)

        if meType:
            meTypeAllMCruns = grouping(dfToGroup=siteDF, siteEndSimDF=siteEndSimDF, valueColName="emission", groupOptions=("METype", meType))
            meTypepdf = calcProbabilitiesAllMCs(meTypeAllMCruns.values())
            meTypepdf['CH4_EmissionRate_kg/h'] = meTypepdf['value']
            meTypepdf.drop(columns=['value', 'count'], inplace=True)
            dumpEmissions(meTypepdf, config, "equip_group_level", facID=f"PDFs/site={fac}/{meType}", abnormal=abnormal)
        else:
            for siMeType, meTyDF in siteDF.groupby('METype'):
                meTypeAllMCruns = grouping(dfToGroup=meTyDF, siteEndSimDF=siteEndSimDF, valueColName="emission")
                meTypepdf = calcProbabilitiesAllMCs(meTypeAllMCruns.values())
                meTypepdf['CH4_EmissionRate_kg/h'] = meTypepdf['value']
                meTypepdf.drop(columns=['value', 'count'], inplace=True)
                dumpEmissions(meTypepdf, config, "equip_group_level", facID=f"PDFs/site={fac}/PDF_for_all_{siMeType}", abnormal=abnormal)

        
        if unitID:
            unitAllMCruns = grouping(dfToGroup=siteDF, siteEndSimDF=siteEndSimDF, valueColName="emission", groupOptions=("unitID", unitID))
            unitPDF = calcProbabilitiesAllMCs(unitAllMCruns.values())
            unitPDF['CH4_EmissionRate_kg/h'] = unitPDF['value']
            unitPDF.drop(columns=['value', 'count'], inplace=True)
            dumpEmissions(unitPDF, config, "unit_level", facID=f"PDFs/site={fac}/{unitID}", abnormal=abnormal)
        else:
            for unitID, unitIDDF in siteDF.groupby('unitID'):
                unitAllMCruns = grouping(dfToGroup=unitIDDF, siteEndSimDF=siteEndSimDF, valueColName="emission")
                unitPDF = calcProbabilitiesAllMCs(unitAllMCruns.values())
                unitPDF['CH4_EmissionRate_kg/h'] = unitPDF['value']
                unitPDF.drop(columns=['value', 'count'], inplace=True)
                dumpEmissions(unitPDF, config, "unit_level", facID=f"PDFs/site={fac}/PDF_for_{unitID}", abnormal=abnormal)

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

def fillEmptyDataWithZero(df,emissionCol):
    me_df = df[df['METype'].notnull() & (df['METype'] != "")]
    unit_info = {r['unitID']: {'METype': r['METype'], 'emitterID': r['emitterID']}
                 for _, r in me_df.iterrows()}
    model_dict = allModelReadableNamesDict()
    overall_species = list(df['species'].unique())
    mcRuns, unitIDs = df['mcRun'].unique(), set(unit_info.keys())
    missing = []

    for mc in mcRuns:
        for uid in unitIDs:
            METype, emitterID = unit_info[uid]['METype'], unit_info[uid]['emitterID']
            group = df[(df['mcRun'] == mc) & (df['unitID'] == uid)]
            if METype not in model_dict:
                # Add missing species rows for units without a defined model dictionary.
                pres_species = set(group['species'].unique())
                for sp in set(overall_species) - pres_species:
                    missing.append({'mcRun': mc, 'unitID': uid, 'METype': METype, 'species': sp,
                                    'modelReadableName': None, 'modelEmissionCategory': None,
                                    'emitterID': emitterID, emissionCol: 0})
            else:
                # For units with a model dictionary, for each species add missing model events.
                for sp in overall_species:
                    pres_models = set(group[group['species'] == sp]['modelReadableName'].dropna().unique())
                    for m in model_dict[METype]:
                        if m['modelReadableName'] not in pres_models:
                            missing.append({'mcRun': mc, 'unitID': uid, 'METype': METype, 'species': sp,
                                            'modelReadableName': m['modelReadableName'],
                                            'modelEmissionCategory': m['modelEmissionCategory'],
                                            'emitterID': emitterID, emissionCol: 0})
    df_missing = pd.DataFrame(missing)
    df_complete = pd.concat([df, df_missing], ignore_index=True)
    df_complete[emissionCol] = df_complete[emissionCol].fillna(0)
    return df_complete

def generatedCsvSummaries(config, df, fac, abnormal):
    fac = str(fac).capitalize()
     # Get DFs for emissions for the summaries
    zerosDF = fillEmptyDataWithZero(df.copy(), emissionCol="emissions_USTonsPerYear")
    # logging.info("Creating dataframes for Emission by Categories...")
    emissCatDF = processEmissionsCat(zerosDF.copy())
    # logging.info("Creating dataframes for Emission by Equipment...")
    # emissEquipDF = processEquipEmissions(zerosDF)
    # logging.info("Creating dataframes for Instantaneous Emissions by Equipment...")
    emissInstEquipDF = processInstantEquipEmissions(df)

    if config['fullSummaries']:
        # Get PDF at Site Level for CH4 Emissions
        generatePDFs(config=config, df=zerosDF.copy(), abnormal=abnormal, fac=fac)

        # Create a table showing the average emission rate and average duration of each emission type (modelReadableName)
        avgERandDur = create_summary_table(emissInstEquipDF.copy(), species="METHANE")
        avgERandDur = pd.concat([avgERandDur, create_summary_table(emissInstEquipDF.copy(), species="ETHANE")])

        # Get 5 number summary for emission categories (vented, fugitives, combusted, total)
        CategorySummaryDF = calcFiveNumberSummary(emissCatDF.copy(), species='METHANE', confidence_level=95)
        CategorySummaryDF = pd.concat([CategorySummaryDF, calcFiveNumberSummary(emissCatDF.copy(), species='ETHANE', confidence_level=95)])  # add ethane summary

        # Get detailed emissions
        detailed_emissionsDF = calc_detailed_emissions_summary(zerosDF.copy(), emissions_colmn="emissions_USTonsPerYear", species="METHANE")
        detailed_emissionsDF = pd.concat([detailed_emissionsDF, calc_detailed_emissions_summary(zerosDF.copy(), emissions_colmn="emissions_USTonsPerYear", species="ETHANE")])

        # Get emissions summary by METype
        equipEmissSummaryDF = calcEmissSummaryByMEType(zerosDF.copy(), species='METHANE', confidence_level=95)
        equipEmissSummaryDF = pd.concat([equipEmissSummaryDF, calcEmissSummaryByMEType(zerosDF.copy(), species='ETHANE', confidence_level=95)])  # add ethane summary

        # Get instantaneous emissions summary by modelReadableName
        instEmissByModelReadName = calc_instemiss_by_modelReadableName(emissInstEquipDF.copy())


        # Dump summaries
        dumpEmissions(detailed_emissionsDF, config, "detailed_annualEmissions_summary", facID=f"AnnualEmissions/site={fac}/", abnormal=abnormal)
        dumpEmissions(CategorySummaryDF, config, "facility", facID=f"AnnualEmissions/site={fac}/", abnormal=abnormal)
        dumpEmissions(equipEmissSummaryDF, config, "equipment", facID=f"AnnualEmissions/site={fac}/", abnormal=abnormal)
        dumpEmissions(instEmissByModelReadName, config, "detailed_instantEmissions_summary",
                      facID=f"InstantaneousEmissions/site={fac}/", abnormal=abnormal)
        dumpEmissions(avgERandDur, config, "avgERandDur", facID=f"AvgEmissionRatesAndDurations/site={fac}/", abnormal=abnormal)

    if config['annualSummaries']:
        detailed_emissionsDF = calc_detailed_emissions_summary(zerosDF.copy(), emissions_colmn="emissions_USTonsPerYear", species="METHANE")
        detailed_emissionsDF = pd.concat([detailed_emissionsDF, calc_detailed_emissions_summary(zerosDF.copy(), emissions_colmn="emissions_USTonsPerYear", species="ETHANE")])
                  # Get 5 number summary for emission categories (vented, fugitives, combusted, total)
        CategorySummaryDF = calcFiveNumberSummary(emissCatDF.copy(), species='METHANE', confidence_level=95)
        CategorySummaryDF = pd.concat([CategorySummaryDF, calcFiveNumberSummary(emissCatDF.copy(), species='ETHANE', confidence_level=95)])  # add ethane summary
          # Get emissions summary by METype
        equipEmissSummaryDF = calcEmissSummaryByMEType(zerosDF.copy(), species='METHANE', confidence_level=95)
        equipEmissSummaryDF = pd.concat([equipEmissSummaryDF, calcEmissSummaryByMEType(zerosDF.copy(), species='ETHANE', confidence_level=95)])  # add ethane summary

        # Dump summaries
        dumpEmissions(detailed_emissionsDF, config, "detailed_annualEmissions_summary", facID=f"AnnualEmissions/site={fac}/", abnormal=abnormal)
        dumpEmissions(CategorySummaryDF, config, "facility", facID=f"AnnualEmissions/site={fac}/", abnormal=abnormal)
        dumpEmissions(equipEmissSummaryDF, config, "equipment", facID=f"AnnualEmissions/site={fac}/", abnormal=abnormal)

    if config['instantaneousSummaries']:
        # Get instantaneous emissions summary by modelReadableName
        instEmissByModelReadName = calc_instemiss_by_modelReadableName(emissInstEquipDF.copy())
        dumpEmissions(instEmissByModelReadName, config, "detailed_instantEmissions_summary", facID=f"InstantaneousEmissions/site={fac}/", abnormal=abnormal)
    
    if config['pdfSummaries']:
        # Get PDF at Site Level for CH4 Emissions
        generatePDFs(config=config, df=zerosDF.copy(), abnormal=abnormal, fac=fac)

    if config['avgDurSummaries']:
        avgERandDur = create_summary_table(emissInstEquipDF.copy(), species="METHANE")
        avgERandDur = pd.concat([avgERandDur,create_summary_table(emissInstEquipDF.copy(),species="ETHANE")])
        dumpEmissions(avgERandDur, config, "avgERandDur", facID=f"AvgEmissionRatesAndDurations/site={fac}/", abnormal=abnormal)

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
        if dfAbnormalOFF.empty:
            logger.info("No non fugitive emissions where found")
        else:    
            generatedCsvSummaries(config, dfAbnormalOFF, fac, abnormal="OFF")
       
    elif config['abnormal'].upper() == "OFF":
        dfAbnormalOFF = filterAbnormalEmissions(df)
        if dfAbnormalOFF.empty:
            logger.info("No non fugitive emissions where found")
        else:    
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