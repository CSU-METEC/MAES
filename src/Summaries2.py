import pandas as pd
import AppUtils as au
import os
import glob
import json
import logging
import numpy as np
import Timeseries as ts
import ParquetLib as pl
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.patches import Patch
from matplotlib.ticker import FuncFormatter
from scipy.stats import norm
from Timer import Timer
import Units as u
from pathlib import Path

logger = logging.getLogger(__name__)


US_TO_PER_METRIC_TON = 1.10231
US_TO_PER_HOUR_TO_KG_PER_HOUR = 0.1035
KG_PER_HOUR_TO_MT_PER_HOUR = .001

SPECIES = ['METHANE','ETHANE']

KG_PER_YEAR_UNITS_NAME = 'kg/year'
KG_PER_HOUR_UNITS_NAME = 'kg/hour'
US_TONS_PER_YEAR_UNITS_NAME = 'US tons/year'
METRIC_TONS_PER_YEAR_UNITS_NAME = 'mt/year'

def summarizeByModelEmissionCategory(emissDF, speciesList=['METHANE', 'ETHANE'], confidenceLevel=95, unitsToColumns=None):
    ret = genCIStats2(emissDF,
                      speciesList,
                      confidenceLevel=confidenceLevel,
                      groupColumns=['site', 'modelEmissionCategory', 'mcRun'],
                      CICategory='modelEmissionCategory',
                      columnConversions=unitsToColumns)
    return ret

def summarizeByMEType(emissDF, speciesList=['METHANE', 'ETHANE'], confidenceLevel=95, unitsToColumns=None):
    ret = genCIStats2(emissDF,
                      speciesList,
                      confidenceLevel=confidenceLevel,
                      groupColumns=['site', 'METype', 'mcRun'],
                      CICategory='METype',
                      columnConversions=unitsToColumns)
    return ret

def summarizeByModelReadableName(emissDF, speciesList=['METHANE', 'ETHANE'], confidenceLevel=95, unitsToColumns=None):
    ret = genCIStats2(emissDF,
                      speciesList,
                      confidenceLevel=confidenceLevel,groupColumns=['site', "modelReadableName", 'mcRun'],
                      CICategory='modelReadableName',
                      columnConversions=unitsToColumns)
    return ret

def summarizeByUnitID(emissDF, speciesList=['METHANE', 'ETHANE'], confidenceLevel=95,
                                 unitsToColumns=None):
    ret = genCIStats2(emissDF,
                      speciesList,
                      confidenceLevel=confidenceLevel, groupColumns=['site', 'unitID', 'mcRun'],
                      CICategory='unitID',
                      columnConversions=unitsToColumns)
    return ret

def summarizeEmissionRates(emissDF, speciesList=['METHANE', 'ETHANE'], confidenceLevel=95, unitsToColumns=None):
    additionalConversions = [
        # {'colName': 'emissions_kgPerYear', 'units': KG_PER_YEAR_UNITS_NAME,          'conversion': _convertKGPerYear2KGPerYear},
        {'colName': 'emissions_kgPerYear', 'units': US_TONS_PER_YEAR_UNITS_NAME, 'conversion': _convertKGPerYear2USTonsPerYear},
        {'colName': 'emissions_kgPerYear', 'units': METRIC_TONS_PER_YEAR_UNITS_NAME, 'conversion': _convertKGPerYear2MetricTonsPerYear},
        {'colName': 'emissions_kgPerYear', 'units': KG_PER_HOUR_UNITS_NAME, 'conversion': _convertKGPerYear2KGPerHour},

    ]
    erRet = genCIStats2(emissDF,
                        speciesList,
                        confidenceLevel=confidenceLevel,
                        groupColumns=['site', 'METype', "unitID", 'modelReadableName'],
                        CICategory='emissionRateSummary',
                        columnConversions=additionalConversions)
    edRet = genCIStats2(emissDF,
                        ['METHANE'],
                        confidenceLevel=confidenceLevel,
                        groupColumns=['site', 'METype', "unitID", 'modelReadableName'],
                        CICategory='emissionDurationSummary',
                        columnConversions=[],
                        aggColumn='duration',
                        aggUnits='s')

    ret = pd.concat([erRet, edRet])
    return ret

def genCIStats2Forward(inDF, speciesList=['METHANE', 'ETHANE'], confidenceLevel=95, groupColumns=[], CICategory='', columnConversions=[]):
    AGG_COLUMNS = {
        'total': ('emissions_kgPerYear', 'sum'),
        'count': ('emissions_kgPerYear', 'count'),
        'mean': ('emissions_kgPerYear', 'mean'),
        'min': ('emissions_kgPerYear', 'min'),
        'max': ('emissions_kgPerYear', 'max'),
        'lower': ('emissions_kgPerYear', lambda x: np.percentile(x, 25)),
        'upper': ('emissions_kgPerYear', lambda x: np.percentile(x, 75)),
        'lowerCI': ('emissions_kgPerYear', lambda x: np.percentile(x, alpha / 2)),
        'upperCI': ('emissions_kgPerYear', lambda x: np.percentile(x, (100 - alpha / 2))),
    }
    # logger.info(f"in genCIStats2, {CICategory}")
    alpha = 100 - float(confidenceLevel)
    lowerCIColName = f"{confidenceLevel}%_ci_lower"
    upperCIColName = f"{confidenceLevel}%_ci_upper"

    cumResults = []
    for singleSpecies in speciesList:
        emissCatDF = inDF[inDF['species'] == singleSpecies]
        cumCols = []
        for singleCol in groupColumns:
            singleConversion = None
            cumCols.append(singleCol)

            with Timer("        groupby", loglevel=logging.DEBUG) as t1:
                thisResult = (
                    emissCatDF.groupby(cumCols, as_index=False)
                    .agg(**AGG_COLUMNS)
                    .assign(species=singleSpecies,
                            units='kg/year',
                            emissionList=None
                            )
                )
                cumResults.append(thisResult)
                t1.setCount(len(thisResult))

                with Timer("        convert", loglevel=logging.DEBUG) as t2:
                    for singleConversion in columnConversions:
                        # calculate the converted values
                        newResult = singleConversion['conversion'](thisResult[AGG_COLUMNS.keys()])
                        # pull in values from the aggregation that:
                        #  a. don't want to be converted (such as count)
                        #  b. need to be updated based on the conversion (units)
                        #  c. are not included in aggregation (species, emissionList)
                        newResult = newResult.assign(count=thisResult['count'],
                                                     units=singleConversion['units'],
                                                     species=singleSpecies,
                                                     emissionList=None
                                                     )
                        # retain the original values
                        assignDict = {}
                        for singleOrigCol in cumCols:
                            assignDict[singleOrigCol] = thisResult[singleOrigCol]
                        newResult = newResult.assign(**assignDict)
                        cumResults.append(newResult)
                    t2.setCount(len(columnConversions))

    mdCat = pd.concat(cumResults)
    nanUnits = mdCat['units'].isna().any()
    if nanUnits:
        logger.info(f"{CICategory=}, {singleSpecies=}, {len(thisResult)=}, {cumCols=}, {singleConversion['units']=}, {emitterIDLen=}")

    mdCat['CICategory'] = CICategory
    colsInOrder = ['CICategory', 'species', *groupColumns, 'units', *AGG_COLUMNS.keys(), 'emissionList']
    # logger.info(colsInOrder)
    mdCat = mdCat[colsInOrder]
    mdCat = mdCat.rename(columns={'lowerCI': lowerCIColName, 'upperCI': upperCIColName})

    filterMask = ((mdCat["mean"] ==0 ) & (mdCat["max"] == 0))
    ret = mdCat[~filterMask]
    return ret

def _convertResultsList(convertFn, resList):
    convMap = map(lambda x: convertFn(x), resList)
    filterMap = filter(lambda x: not np.isnan(x), convMap)
    ret = list(filterMap)
    return ret

def genCIStats2(inDF,
                speciesList=['METHANE', 'ETHANE'],
                confidenceLevel=95,
                groupColumns=[],
                CICategory='',
                columnConversions=[],
                aggColumn='emissions_kgPerYear',
                aggUnits='kg/year'):
    # todo: break this into two functions -- one to calculate the basic stats, & one to calculate the conversions
    AGG_COLUMNS = {
        'total': ('total', 'sum'),
        'count': ('total', 'count'),
        'mean': ('total', 'mean'),
        'min': ('total', 'min'),
        'max': ('total', 'max'),
        'lower': ('total', lambda x: np.percentile(x, 25)),
        'upper': ('total', lambda x: np.percentile(x, 75)),
        'lowerCI': ('total', lambda x: np.percentile(x, alpha / 2)),
        'upperCI': ('total', lambda x: np.percentile(x, (100 - alpha / 2)))
    }
    AGG_COLUMNS_WITH_READINGS = {**AGG_COLUMNS, 'readings': ('total', list)}
    # todo: where does an emitterID of 0.0 come from???
    zeroEmitterIDMask = (inDF['emitterID'] == 0.0)
    inDF = inDF[~zeroEmitterIDMask]
    # logger.info(f"in genCIStats2, {CICategory}")
    alpha = 100 - float(confidenceLevel)
    lowerCIColName = f"{confidenceLevel}%_ci_lower"
    upperCIColName = f"{confidenceLevel}%_ci_upper"

    cumResults = []
    for singleSpecies in speciesList:
        emissCatDF = inDF[inDF['species'] == singleSpecies]
        emissCatDF = emissCatDF.assign(total=emissCatDF[aggColumn])
        firstResult = (
                    emissCatDF.groupby(groupColumns, as_index=False, dropna=False)
                    .agg(**AGG_COLUMNS_WITH_READINGS)
                    .assign(species=singleSpecies,
                            units=aggUnits,
                            emissionList=None
                            )
                )

        iterList = groupColumns
        thisResult = pd.DataFrame()
        while iterList:
            # logging.info(f"{len(iterList)=}, {len(thisResult)=}")
            singleConversion = None

            with Timer("        groupby", loglevel=logging.DEBUG) as t1:
                if thisResult.empty:
                    thisResult = firstResult
                else:
                    thisResult = (
                        thisResult.groupby(iterList, as_index=False)
                        .agg(**AGG_COLUMNS_WITH_READINGS)
                        .assign(species=singleSpecies,
                                units=aggUnits,
                                emissionList=None
                                )
                    )
                cumResults.append(thisResult)
                t1.setCount(len(thisResult))

                with Timer("        convert", loglevel=logging.DEBUG) as t2:
                    for singleConversion in columnConversions:
                        # calculate the converted values
                        newResult = singleConversion['conversion'](thisResult[AGG_COLUMNS.keys()])
                        convReadings = thisResult['readings'].apply(lambda x: _convertResultsList(singleConversion['conversion'], x))
                        # pull in values from the aggregation that:
                        #  a. don't want to be converted (such as count)
                        #  b. need to be updated based on the conversion (units)
                        #  c. are not included in aggregation (species, emissionList)
                        newResult = newResult.assign(count=thisResult['count'],
                                                     units=singleConversion['units'],
                                                     species=singleSpecies,
                                                     emissionList=None,
                                                     readings=convReadings
                                                     )
                        # retain the original values
                        assignDict = {}
                        for singleOrigCol in iterList:
                            assignDict[singleOrigCol] = thisResult[singleOrigCol]
                        newResult = newResult.assign(**assignDict)
                        cumResults.append(newResult)
                    t2.setCount(len(columnConversions))
            iterList = iterList[:-1]

    mdCat = pd.concat(cumResults)
    nanUnits = mdCat['units'].isna().any()
    if nanUnits:
        logger.info(f"{CICategory=}, {singleSpecies=}, {len(thisResult)=}, {cumCols=}, {singleConversion['units']=}, {emitterIDLen=}")

    mdCat['CICategory'] = CICategory
    colsInOrder = ['CICategory', 'species', *groupColumns, 'units', *AGG_COLUMNS_WITH_READINGS.keys(), 'emissionList']
    # logger.info(colsInOrder)
    mdCat = mdCat[colsInOrder]
    mdCat = mdCat.rename(columns={'lowerCI': lowerCIColName, 'upperCI': upperCIColName})

    ret = mdCat
    # filterMask = ((mdCat["mean"] ==0 ) & (mdCat["max"] == 0))
    # ret = mdCat[~filterMask]
    return ret

def aggrSet(input_df, value_column, group_options=None):
    """Aggregates a DataFrame by specified options, creating Timeseries objects."""
    timeseries_set = []
    if group_options:
        input_df = input_df[input_df[group_options[0]] == group_options[1]]
    grouping_cols = ['facilityID', 'METype'] if value_column == "state" else ['facilityID', 'unitID', 'emitterID']
    TimeseriesClass = ts.TimeseriesCategorical if value_column == "state" else ts.TimeseriesRLE
    if input_df.empty:
        logger.info(f"Where {group_options[0]} = {group_options[1]}, no timeseries data were found")
        pass
    subsetLengths = []
    for subsetKey, subsetDF in input_df.groupby(grouping_cols):
        logger.debug(f"        {subsetKey=}, {len(subsetDF)=}")
        ts1 = TimeseriesClass(subsetDF, valueColName=value_column)
        timeseries_set.append(ts1)
        subsetLengths.append(len(subsetDF))
    subsetCount = len(subsetLengths)
    subsetSum = sum(subsetLengths)
    subsetMean = subsetSum / subsetCount
    logger.debug(f"      {subsetSum=}, {subsetCount=}, {subsetMean=}")
    return timeseries_set

def readParquetFiles(config, site, abnormal, mergeGC, additionalEventFilters):
    siteEVDF = pl.readParquetEvents(config, site=site, mergeGC=mergeGC, species="METHANE", additionalEventFilters=additionalEventFilters)
    siteEVDF = siteEVDF[siteEVDF["nextTS"] - siteEVDF["timestamp"] == siteEVDF["duration"]]
    siteEVDF = siteEVDF[siteEVDF['duration'] >= 0]
    siteEndSimDF = pl.readParquetSummary(config, site=site)

    if abnormal == "OFF":
        valid_emitter_ids = siteEVDF[siteEVDF['modelEmissionCategory'] != 'FUGITIVE']['emitterID']
        siteEVDF = siteEVDF[siteEVDF['emitterID'].isin(valid_emitter_ids)]

    return siteEVDF, siteEndSimDF

def grouping(dfToGroup, siteEndSimDF, valueColName, groupOptions=None):
    # todo: should this return a TSSet?
    AllMcRuns = {}
    with Timer("grouping", loglevel=logging.DEBUG) as t000:
        for mcRun, mcRunDF in dfToGroup.groupby('mcRun'):
            with Timer(f"  mcRun: {mcRun}", loglevel=logging.INFO) as t00:
                EndSimDF = siteEndSimDF[siteEndSimDF['mcRun'] == mcRun]
                simDuration = EndSimDF.loc[EndSimDF['command'] == 'SIM-STOP', 'timestamp'].values[0]
                with Timer("    aggr", loglevel=logging.INFO) as t0:
                    aggregatedSet = aggrSet(input_df=mcRunDF.sort_values(by=['nextTS'], ascending=True), value_column=valueColName, group_options=groupOptions)
                    t0.setCount(len(aggregatedSet))
                totalTimeseriesSet = ts.TimeseriesSet(aggregatedSet)

                if valueColName == "emission":
                    with Timer("    sum", loglevel=logging.DEBUG) as t1:
                        # tdf = totalTimeseriesSet.sumNew()
                        tdf = totalTimeseriesSet.sumNew()
                        t1.setCount(len(aggregatedSet))
                    tdf.df = tdf.df[tdf.df['nextTS'] <= simDuration]
                    tdf.df.loc[:, 'tsValue'] = tdf.df['tsValue'] * u.SECONDS_PER_HOUR
                    AllMcRuns[mcRun] = tdf
                else:
                    for tscat in totalTimeseriesSet.tsSetList:
                        tscat.df = tscat.df[tscat.df["nextTS"] <= simDuration]

                    AllMcRuns[mcRun] = totalTimeseriesSet.tsSetList

    return AllMcRuns

def _stripEmitterID(eIDCol):
    allFields = eIDCol.split('_')
    if len(allFields) == 1:
        return eIDCol
    ret = '_'.join(allFields[0:-1])
    return ret

def testFlatten(df):
    # colsForEmpty = ["modelReadableName", "unitID", "facilityID",
    #                  "site", "species", "METype", "modelEmissionCategory",
    #                 'mcRun', 'emitterID']
    # for keys, grp in df.groupby(colsForEmpty):
    #     pass
    colsForPivot = ['mcRun', 'emitterID']
    expectedLen = 1
    for singleCol in colsForPivot:
        expectedLen *= len(df[singleCol].unique())
    pt1 = df.pivot_table(index=colsForPivot, values='emissions_kgPerYear', aggfunc='sum').reset_index()
    pt2 = df.pivot_table(index=colsForPivot, values='emissions_kgPerYear', aggfunc='sum', dropna=False).reset_index()
    ptMask = pt2['emissions_kgPerYear'].isna()

    newCol = df['emitterID'].apply(_stripEmitterID)
    df2 = df.assign(emitterID=newCol)
    expectedLen2 = 1
    for singleCol in colsForPivot:
        expectedLen2 *= len(df2[singleCol].unique())
    pt3 = df2.pivot_table(index=colsForPivot, values='emissions_kgPerYear', aggfunc='sum').reset_index()
    pt4 = df2.pivot_table(index=colsForPivot, values='emissions_kgPerYear', aggfunc='sum', dropna=False).reset_index()
    pt4Mask = pt4['emissions_kgPerYear'].isna()

    return df

def fillEmptyDataWithZero(df, emissionCol):
    # testFlatten(df)
    colsForEmpty = ["modelReadableName", "unitID", "facilityID",
                     "site", "species", "METype", "modelEmissionCategory"]
    colsForMerge = colsForEmpty.copy()
    colsForMerge.append("mcRun")
    emitterColsToFill = ['emission', 'emissions_USTonsPerYear', 'emissions_kgPerH']
    emitterColsToFill = [c for c in emitterColsToFill if c in df.columns]

    pairs = df[colsForEmpty].drop_duplicates()
    runs = df['mcRun'].unique()
    full_grid = pairs.merge(pd.DataFrame({'mcRun': runs}), how="cross")
    df_full = full_grid.merge(df, on=colsForMerge, how="left")
    df_full[emitterColsToFill] = df_full[emitterColsToFill].fillna(0)
    return df_full

def _createAnnualSummaries(zerosDF, additionalConversions):
    with Timer("      summarizeByModelReadableName", loglevel=logging.DEBUG) as t0:
        unitEmissionsDF = summarizeByModelReadableName(zerosDF, confidenceLevel=95, unitsToColumns=additionalConversions)
        t0.setCount(len(unitEmissionsDF))

    with Timer("      summarizeByModelEmissionCategory", loglevel=logging.DEBUG) as t1:
        siteSummaryDF = summarizeByModelEmissionCategory(zerosDF, confidenceLevel=95, unitsToColumns=additionalConversions)
        t1.setCount(len(siteSummaryDF))

    with Timer("      summarizeByMEType", loglevel=logging.DEBUG) as t2:
        equipEmissSummaryDF = summarizeByMEType(zerosDF, confidenceLevel=95, unitsToColumns=additionalConversions)
        t2.setCount(len(equipEmissSummaryDF))

    with Timer("      summarizeUnitID", loglevel=logging.DEBUG) as t2:
        unitIDSummaryDF = summarizeByUnitID(zerosDF, confidenceLevel=95, unitsToColumns=additionalConversions)
        t2.setCount(len(unitIDSummaryDF))

    with Timer("      summarize Virtual Pneumatics", loglevel=logging.DEBUG) as t3:
        virtualPneumaticDF = createVirtualPneumaticMEType(df=zerosDF)
        pneumaticSummaryDF = summarizeByMEType(virtualPneumaticDF, confidenceLevel=95, unitsToColumns=additionalConversions)
        pneumaticSummaryDF = pneumaticSummaryDF.assign(CICategory='pneumatic')

    with Timer("      summarize emissionRates and durations", loglevel=logging.DEBUG) as t3:
        erDurationSummaryDF = summarizeEmissionRates(zerosDF, confidenceLevel=95, unitsToColumns=additionalConversions)
        erDurationSummaryDF = erDurationSummaryDF.assign(CICategory='erDurationSummary')

    annualSummaryDF = pd.concat([
        siteSummaryDF,
        equipEmissSummaryDF,
        unitIDSummaryDF,
        pneumaticSummaryDF,
        unitEmissionsDF,
        erDurationSummaryDF
    ])
    return annualSummaryDF

def generateSiteSummaries(df, additionalConversion):
    # zerosDF = df
    # todo: fix fillEmptyDataWithZero
    # zerosDF = fillEmptyDataWithZero(df, emissionCol='emissions_USTonsPerYear')
    with Timer("    fillEmptyDataWithZero", loglevel=logging.DEBUG) as t0:
        zerosDF = fillEmptyDataWithZero(df, emissionCol='emissions_kgPerYear')
        t0.setCount(len(zerosDF))
    
    # emissCatDF = pl.processEmissionsCat(zerosDF)
    # emissInstEquipDF = pl.processInstantEquipEmissions(df)

    with Timer("    _createAnnualSummaries", loglevel=logging.DEBUG) as t1:
        annualSummaryDF = _createAnnualSummaries(zerosDF, additionalConversion)
        t1.setCount(len(annualSummaryDF))

    return annualSummaryDF

# New summaries

def filterAbnormalEmissions(df):
    # todo: this comparison is very suss, because there are emitterIDs that are common across some MECs,
    # in particular there are MALFUNCTIONING emitterIDs shared across COMBUSTION & FUGITIVE for Buckland_Station, et. al.
    valid_emitter_ids = df[df['modelEmissionCategory'] != 'FUGITIVE']['emitterID']
    df = df[df['emitterID'].isin(valid_emitter_ids)]
    return df

def filterAbnormalEmissions2(df):
    fugitiveMask = df['modelEmissionCategory'] == 'FUGITIVE'
    df = df[~fugitiveMask]
    return df

def _convertKGPerYear2USTonsPerYear(x):
    return x * u.KG_TO_SHORT_TONS

def _convertKGPerYear2MetricTonsPerYear(x):
    return x * KG_PER_HOUR_TO_MT_PER_HOUR

def _convertKGPerYear2KGPerHour(x):
    return x / u.HOURS_PER_YEAR

def createVirtualPneumaticMEType(df):
    pneumaticMask = df['modelReadableName'].str.contains('Pneumatic')
    pneumaticDF = df[pneumaticMask]
    nonPneumaticDF = df[~pneumaticMask]
    pneumaticDF['METype'] = 'Pneumatics'
    combined_df = pd.concat([pneumaticDF, nonPneumaticDF])
    return combined_df

def processVirtualPneumatics(df):
    pneumaticDF = createVirtualPneumaticMEType(df)
    pneumaticEmissionDF = processEmissionsCat(pneumaticDF)
    return pneumaticEmissionDF

def _calcNewSummariesAndVariations(inDF, additionalConversions):
    annualSummaryAbnormalDF = generateSiteSummaries(inDF, additionalConversions)
    annualSummaryAbnormalDF = annualSummaryAbnormalDF.assign(abnormal="ON")
    normalEmissTypeDFPQ = filterAbnormalEmissions(inDF)
    annualSummaryNormalDF = generateSiteSummaries(normalEmissTypeDFPQ, additionalConversions)
    annualSummaryNormalDF = annualSummaryNormalDF.assign(abnormal="OFF")
    annualSummaryDF = pd.concat([
        annualSummaryAbnormalDF,
        annualSummaryNormalDF
    ]).reset_index(drop=True)
    return annualSummaryDF

def _fillEmissionsWithEmitters(inDF):
    # We need to ensure that every unitID has a value for all the emitterIDs for that unitID across *all*
    # mcRuns.  If there are missing emitterIDs for a unitID, fill the emission & emission_kgPerYear values with NaN.
    EMITTER_COL_LIST = ['site', 'unitID', 'species', 'modelEmissionCategory', 'modelReadableName']
    unitDFList = []
    for unitID, unitGrp in inDF.groupby('unitID'):
        for singleEmitter, emitterGrp in unitGrp.groupby('emitterID'):
            if len(emitterGrp) != len(emitterGrp.duplicated(subset=EMITTER_COL_LIST, keep=False)):
                logger.info(f"{unitID=}, {singleEmitter=}, {len(emitterGrp)=}, {len(emitterGrp.duplicated(subset=EMITTER_COL_LIST, keep=False))=}")
            pass

    retDF = inDF
    return retDF

def calculateAndWriteAnnualEmissionSummaries(inDF, site, additionalConversions, config):
    simDurationDays = config['simDurationDays']
    meanDuration = inDF[inDF['command'] == 'EMISSION']['duration'].mean()
    totalEvents = len(inDF[inDF['command'] == 'EMISSION'])
    totalYears = simDurationDays / u.DAYS_PER_YEAR  # Assuming u.DAYS_PER_YEAR is defined in Units module
    meanFrequency = totalEvents / totalYears

    with Timer("  legacy stats", loglevel=logging.DEBUG) as t00:
        # legacyStatsDF = _calcLegacyStats(inDF)

        avgDataDF = pd.DataFrame({
            'site': site,
            'facilityID': inDF['facilityID'].unique(),
            'meanDurationDays': meanDuration,
            'averageAnnualFrequency': meanFrequency,
            'totalEvents': totalEvents,
            'totalYears': totalYears,
            'CICategory': 'siteAverage'
        })

        # completeLegacyDF = pd.concat([legacyStatsDF, avgDataDF])
        # completeLegacyDF = completeLegacyDF.rename(columns={'emissions_USTonsPerYear': 'total'})
        # completeLegacyDF = completeLegacyDF.assign(units=US_TONS_PER_YEAR_UNITS_NAME)
        completeLegacyDF = avgDataDF

        t00.setCount(len(completeLegacyDF))

    # with Timer("fill emitters") as t00:
    #     logging.info(f"filling emitters, {len(inDF)=}")
    #     filledInDF = _fillEmissionsWithEmitters(inDF)
    #     t00.setCount(len(filledInDF))

    with Timer("  new summaries", loglevel=logging.DEBUG) as t01:
        annualSummaryDF = _calcNewSummariesAndVariations(inDF, additionalConversions)
        annualSummaryDF = annualSummaryDF.assign(site=site)
        t01.setCount(len(annualSummaryDF))

    with Timer("  write parquet", loglevel=logging.DEBUG) as t02:
        outDF = pd.concat([annualSummaryDF, completeLegacyDF])
        # todo: change the destinationDS to something more reasonable
        pl.toBaseParquet(config, outDF, 'newSummary', partition_cols=['site'])
        t02.setCount(len(outDF))

    if site == 'Buckland_Station':
        i = 10

        a1DF = filterAbnormalEmissions(inDF)
        a2DF = filterAbnormalEmissions2(inDF)

        pass

    pass

def calculateAndwriteInstEmissionSummaries(inDF, site, conversions, config):
    COLUMNS_TO_SAVE = ['site', 'mcRun', 'species', 'METype', 'unitID', 'modelReadableName', 'modelEmissionCategory',
                       'emitterID', 'operator', 'psno',
                       'timestamp', 'duration', 'emission', ]
    COLUMNS_TO_RENAME = {'timestamp': 'timestamp_s', 'duration': 'duration_s'}

    retDF = inDF[COLUMNS_TO_SAVE].rename(columns=COLUMNS_TO_RENAME)

    for singleConversion in conversions:
        newCol = singleConversion['conversion'](retDF[singleConversion['colName']])
        assignDict = {singleConversion['newColName']: newCol}
        retDF = retDF.assign(**assignDict)

    retDF = retDF.sort_values(['site', 'mcRun', 'timestamp_s'])

    # todo: change the destinationDS to something more reasonable
    pl.toBaseParquet(config, retDF, 'newInstEmissions', partition_cols=['site'])

def _convertkgPerS2kgPerS(x):
    ret = x
    return ret

def _convertkgPerS2kgPerH(x):
    ret = x * u.SECONDS_PER_HOUR
    return ret

def summarizeSingleSite(config, inDF, site):
    with Timer("postprocess") as t0:
        simDuration = config['simDurationDays']
        inDF = inDF.assign(emissions_kgPerYear=(inDF['emission'] * inDF['duration']) / simDuration * u.DAYS_PER_YEAR)
        additionalConversions = [
            # {'colName': 'emissions_kgPerYear', 'units': KG_PER_YEAR_UNITS_NAME,          'conversion': _convertKGPerYear2KGPerYear},
            {'colName': 'emissions_kgPerYear', 'units': US_TONS_PER_YEAR_UNITS_NAME,     'conversion': _convertKGPerYear2USTonsPerYear},
            {'colName': 'emissions_kgPerYear', 'units': METRIC_TONS_PER_YEAR_UNITS_NAME, 'conversion': _convertKGPerYear2MetricTonsPerYear},
        ]

        calculateAndWriteAnnualEmissionSummaries(inDF, site, additionalConversions, config)

        instEmissionConversions = [
            {'colName': 'emission', 'newColName': 'emissions_kgPerS', 'conversion': _convertkgPerS2kgPerS},
            {'colName': 'emission', 'newColName': 'emissions_kgPerH', 'conversion': _convertkgPerS2kgPerH},
        ]
        calculateAndwriteInstEmissionSummaries(inDF, site, instEmissionConversions, config)

    pass

def summarize(config):
    # this method assumes that the summarization is at a single site level, not the entire simulation.  This should
    # be implemented in generateSite Names in SiteMain2.
    logger.info(f"{config['siteName']=}")
    with Timer("Read events") as t0:
        logging.info("Read Parquet Files")
        eventDF2 = pl.readParquetEvents(config,
                                        site=config['siteName'],
                                        mergeGC=True,
                                        species=['METHANE', 'ETHANE'],
                                        additionalEventFilters=[('command', '=', 'EMISSION')])
        if eventDF2 is None:
            return
        t0.setCount(len(eventDF2))

    with Timer("Process events") as t2:
        for fac, df in eventDF2.groupby('facilityID'):
            site = df['site'].unique()[0]
            summarizeSingleSite(config, df, site)

def _filterAndPivot(inDF, CICategory, pivotField=None):
    AGG_FIELDS = {
        'total': ('total', 'sum'),
        'count': ('total', 'count'),
        'mean':  ('total', 'mean'),
        'min':  ('total', 'min'),
        'max':  ('total', 'max'),
        'lower':  ('total', lambda x: np.percentile(x, 25)),
        'upper':  ('total', lambda x: np.percentile(x, 75)),
        'lowerCI':  ('total', lambda x: np.percentile(x, alpha / 2)),
        'upperCI':  ('total', lambda x: np.percentile(x, (100 - alpha / 2))),
        'readings':  ('total', list)
    }

    confidenceLevel = 95
    alpha = 100 - float(confidenceLevel)

    if pivotField is None:
        pivotField = CICategory

    filteredDF = inDF[inDF['CICategory'] == CICategory]

    with Timer(CICategory) as t0:
        if pivotField != 'simulation':
            summaryDF = (
                filteredDF.groupby(['species', pivotField, 'units', 'abnormal'])
                .agg(**AGG_FIELDS)
                .reset_index()
            )
        else:
            summaryDF = (
                filteredDF.groupby(['species', 'units', 'abnormal'])
                .agg(**AGG_FIELDS)
                .reset_index()
            )
        summaryDF = summaryDF.assign(CICategory=CICategory)
        t0.setCount(len(summaryDF))
    return summaryDF

def summarizeSimulation(config):
    # this method depends on site-level simulations (aka 'summarize' function) being performed prior to this call.
    logger.info(f"{config['parquetNewSummary']=}")
    with Timer("Read summaries") as t0:
        logging.info("Read summary parquet files")
        fullSummaryDF = pd.read_parquet(config['parquetNewSummary'])
        t0.setCount(len(fullSummaryDF))

    mecSimSummaryDF = _filterAndPivot(fullSummaryDF, 'modelEmissionCategory')
    readableNameSummaryDF = _filterAndPivot(fullSummaryDF, 'modelReadableName')
    unitIDSummaryDF = _filterAndPivot(fullSummaryDF, 'unitID')
    METypeSummaryDF = _filterAndPivot(fullSummaryDF, 'METype')
    pneumaticsDF = _filterAndPivot(fullSummaryDF, 'pneumatic', pivotField='METype')
    siteSummaryDF = _filterAndPivot(fullSummaryDF, 'modelEmissionCategory', pivotField='simulation')
    siteSummaryDF = siteSummaryDF.assign(CICategory='simulation')

    fullSimSummaryDF = pd.concat([
        mecSimSummaryDF,
        readableNameSummaryDF,
        unitIDSummaryDF,
        METypeSummaryDF,
        pneumaticsDF,
        siteSummaryDF
    ])

    simSummaryRoot = Path(config['parquetNewSimSummary'])
    simSummaryDS = f"newSimSummary"
    pl.toBaseParquet(config, fullSimSummaryDF, simSummaryDS, partition_cols={})

    pass



