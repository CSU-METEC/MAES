import pandas as pd
import AppUtils as au
import os
import glob
import json
import logging
import numpy as np
import Timeseries as ts
import ParquetLib as pl
from scipy.stats import norm

from ParquetLib import SUMMARY_DS
from Timer import Timer
import Units as u
from pathlib import Path

logger = logging.getLogger(__name__)


US_TO_PER_METRIC_TON = 1.10231
US_TO_PER_HOUR_TO_KG_PER_HOUR = 0.1035
KG_PER_HOUR_TO_MT_PER_HOUR = .001
KG_PER_YEAR_TO_KG_PER_HOUR = 1 / u.HOURS_PER_YEAR

SPECIES = ['METHANE','ETHANE']

KG_PER_YEAR_UNITS_NAME = 'kg/year'
KG_PER_HOUR_UNITS_NAME = 'kg/hour'
US_TONS_PER_YEAR_UNITS_NAME = 'US tons/year'
METRIC_TONS_PER_YEAR_UNITS_NAME = 'mt/year'

SUMMARY_KEY_COLS = ['site', 'species', 'operator', 'psno']

def _convertkgPerS2kgPerH(x):
    return x * u.SECONDS_PER_HOUR

def _convertKGPerYear2USTonsPerYear(x):
    return x * u.KG_TO_SHORT_TONS

def _convertKGPerYear2MetricTonsPerYear(x):
    return x * KG_PER_HOUR_TO_MT_PER_HOUR

def _convertKGPerYear2KGPerHour(x):
    return x * KG_PER_YEAR_TO_KG_PER_HOUR

def _createEmissionDF(inDF):
    COLS_TO_KEEP = {'mcRun': 'mcRun',
                    'site': 'site',
                    'species': 'species',
                    'operator': 'operator',
                    'psno': 'psno',
                    'emitterID': 'emitterID',
                    'timestamp': 'timestamp_s',
                    'duration': 'duration_s',
                    'emission_kgPerS': 'emission_kgPerS',
                    'totalEmission_kg': 'totalEmission_kg',
                    'METype': 'METype',
                    'unitID': 'unitID',
                    'modelReadableName': 'modelReadableName',
                    'modelEmissionCategory': 'modelEmissionCategory'}
    emissionDF = inDF.assign(
        emission_kgPerS=inDF['emission'],
        # replace NaN operator & psno values with empty string -- otherwise groupby doesn't work
        operator=inDF['operator'].fillna(''),
        psno=inDF['psno'].fillna('')
    )

    emissionDF = emissionDF.assign(
        totalEmission_kg=emissionDF['emission_kgPerS']*emissionDF['duration'],
    )

    emissionDF = emissionDF.rename(columns=COLS_TO_KEEP)

    retDF = emissionDF[COLS_TO_KEEP.values()]

    return retDF

_DATASET_PARAMS = {
    'InstEmissions': {'configKey': 'parquetNewInstEmissions', 'partition_cols': ['site']},
    'SiteSummary':   {'configKey': 'parquetNewSummary',       'partition_cols': ['site']},
    'EventSummary':  {'configKey': 'parquetNewEventSummary',  'partition_cols': ['site']},
    'SimSummary':    {'configKey': 'parquetNewSimSummary',    'partition_cols': []},
}

def _saveSummaryDS(config, df, dataset):
    params = _DATASET_PARAMS[dataset]
    pl.toBaseParquetFullConfig(config, df, params['configKey'], partition_cols=params['partition_cols'], basename=dataset)

def _doAgg(df, groupbyCols, aggFieldList, varCol):
    summaryDFByMCRun = (
        df.groupby(groupbyCols, as_index=False)
        .agg(**aggFieldList)
        .assign(CICategory=varCol,
                units=KG_PER_YEAR_UNITS_NAME)
    )
    return summaryDFByMCRun

def _doAggHierarchy(df, aggColumnList, mcIterations, varCol, detailGroupbyCols, rollupCols):
    resultDFList = []
    currentGroupbyCols = list(detailGroupbyCols)

    # Level 0: per-MC-run (internal only — not added to resultDFList)
    mcRunDF = _doAgg(df, [*currentGroupbyCols, 'mcRun'], aggColumnList, varCol)

    # Level 1: cross-MC with mean correction
    crossMcDF = _doAgg(mcRunDF.assign(emissions_kgPerYear=mcRunDF['total']),
                       currentGroupbyCols, aggColumnList, varCol)
    crossMcDF = crossMcDF.assign(rawCount=crossMcDF['count'],
                                 rawMean=crossMcDF['mean'],
                                 mean=crossMcDF['total'] / mcIterations,
                                 count=mcIterations)
    resultDFList.append(crossMcDF)

    # Rollup levels: drop one column at a time, re-aggregate from previous level
    for col in rollupCols:
        currentGroupbyCols = [c for c in currentGroupbyCols if c != col]
        prevDF = resultDFList[-1]
        rolledDF = _doAgg(prevDF.assign(emissions_kgPerYear=prevDF['total']),
                          currentGroupbyCols, aggColumnList, varCol)
        resultDFList.append(rolledDF)

    return pd.concat(resultDFList)

def calculateAnnualSummaries(instEmissionDF, simDurationDays, aggColumnList, mcIterations):
    instEmissionDF = instEmissionDF.assign(emissions_kgPerYear=(instEmissionDF['totalEmission_kg']) / simDurationDays * u.DAYS_PER_YEAR)
    # first aggregation -- there may be multiple emissions per emitterID (e.g. leaks from the same emitter multiple times per sim)
    #   aggregate by emitterID to eliminate these
    aggregatedEmissionsByEmitterID = (
        instEmissionDF.groupby(['site', 'mcRun', 'species', 'emitterID', 'operator', 'psno', 'METype', 'unitID', 'modelReadableName', 'modelEmissionCategory'],
                               as_index=False)
        .agg(emissions_kgPerYear=('emissions_kgPerYear', 'sum'),
             count=('emissions_kgPerYear', 'count'))
    )

    resultDFList = []
    for varCol in ['METype', 'unitID', 'modelEmissionCategory']:
        resultDFList.append(
            _doAggHierarchy(aggregatedEmissionsByEmitterID, aggColumnList, mcIterations,
                            varCol=varCol,
                            detailGroupbyCols=[*SUMMARY_KEY_COLS, varCol],
                            rollupCols=[varCol])
        )
    combinedDF = aggregatedEmissionsByEmitterID.assign(modelEmissionCategory='COMBINED')
    resultDFList.append(
        _doAggHierarchy(combinedDF, aggColumnList, mcIterations,
                        varCol='modelEmissionCategory',
                        detailGroupbyCols=[*SUMMARY_KEY_COLS, 'modelEmissionCategory'],
                        rollupCols=[])
    )
    resultDFList.append(
        _doAggHierarchy(aggregatedEmissionsByEmitterID, aggColumnList, mcIterations,
                        varCol='modelReadableName',
                        detailGroupbyCols=[*SUMMARY_KEY_COLS, 'modelReadableName', 'unitID', 'METype'],
                        rollupCols=['modelReadableName', 'unitID', 'METype'])
    )
    return pd.concat(resultDFList)

def _removeZeroEmissionEvents(instEmissionDF):
    return instEmissionDF[instEmissionDF['emission_kgPerS'] > 0]


def calculateEmissionSummary(instEmissionDF, mcIterations):
    instEmissionDF = _removeZeroEmissionEvents(instEmissionDF)
    ci = 95
    alpha = 100 - ci
    df = instEmissionDF.assign(emission_kgPerH=instEmissionDF['emission_kgPerS'] * u.SECONDS_PER_HOUR)
    groupCols = [*SUMMARY_KEY_COLS, 'METype', 'unitID', 'modelReadableName']
    resDF = (
        df.groupby(groupCols, as_index=False)
        .agg(
            total=('emission_kgPerH', 'sum'),
            count=('emission_kgPerH', 'count'),
            mean=('emission_kgPerH', 'mean'),
            min=('emission_kgPerH', 'min'),
            max=('emission_kgPerH', 'max'),
            lowerQuintile=('emission_kgPerH', lambda x: np.percentile(x, 25)),
            upperQuintile=('emission_kgPerH', lambda x: np.percentile(x, 75)),
            lowerCI=('emission_kgPerH', lambda x: np.percentile(x, alpha / 2)),
            upperCI=('emission_kgPerH', lambda x: np.percentile(x, 100 - alpha / 2)),
            readings=('emission_kgPerH', list)
        )
        .assign(
            CICategory='instantEmissionsByModelReadableName',
            units=KG_PER_HOUR_UNITS_NAME,
            mcRun=float(mcIterations),
            rawCount=lambda x: x['count'],
            rawMean=lambda x: x['mean']
        )
    )
    return resDF

def _convertResultsList(convertFn, resList):
    convMap = map(lambda x: convertFn(x), resList)
    filterMap = filter(lambda x: not np.isnan(x), convMap)
    ret = list(filterMap)
    return ret

def applyConversions(summaryDF, additionalConversions, aggColumnDict):
    resultList = [summaryDF]
    for singleConversion in additionalConversions:
        # calculate the converted values
        tmpSummaryDF = summaryDF.assign(readings=0.0)
        newResult = singleConversion['conversion'](tmpSummaryDF[aggColumnDict.keys()])
        convReadings = summaryDF['readings'].apply(lambda x: _convertResultsList(singleConversion['conversion'], x))
        # pull in values from the aggregation that:
        #  a. don't want to be converted (such as count)
        #  b. need to be updated based on the conversion (units)
        #  c. are not included in aggregation (species, emissionList)
        newResult = newResult.assign(count=summaryDF['count'],
                                     units=singleConversion['units'],
                                     readings=convReadings
                                     )
        assignDict = {'count': summaryDF['count'],
                      'units': singleConversion['units'],
                      'readings': convReadings}
        for singleCol in aggColumnDict.keys():
            assignDict[singleCol] = assignDict.get(singleCol, newResult[singleCol])
        retResult = summaryDF.assign(**assignDict)
        resultList.append(retResult)

    retDF = pd.concat(resultList)
    return retDF

def calculateEventSummary(instEmissionDF, simDurationDays, mcIterations, varCol='eventSummary'):
    instEmissionDF = _removeZeroEmissionEvents(instEmissionDF)
    AGG_COLS = {
        'eventCount': ('emission_kgPerS', 'count'),
        'totalEmission_kg': ('totalEmission_kg', 'sum'),
        'totalEventDuration_s': ('duration_s', 'sum'),
        'meanEventDuration_s': ('duration_s', 'mean'),
        'simpleMean': ('emission_kgPerS', 'mean'),
        # 'emissionEvents': ('emission_kgPerS', list),
        'durationEvents': ('duration_s', list),
        'totalEmissionEvents': ('totalEmission_kg', list),

    }
    
    groupbyCols = [*SUMMARY_KEY_COLS, 'unitID', 'modelReadableName']
    # mcGroupbyCols = [*groupbyCols, 'mcRun']
    # mcEventSummary = (
    #     instEmissionDF
    #     .groupby(mcGroupbyCols, as_index=False)
    #     .agg(**AGG_COLS)
    #     .assign(CICategory=varCol,
    #             mcRuns=1,
    #             emissionRateUnits='kg/s'
    #             )
    #     )
    # mcEventSummary = mcEventSummary.assign(eventsPerMCRun=mcEventSummary['eventCount'] / mcEventSummary['mcRuns'],
    #                                        nonZeroEventsPerMCRun=mcEventSummary['nonZeroEventCount'] / mcEventSummary['mcRuns'],
    #                                        meanEmissionRate=mcEventSummary['totalEmission_kg'] / mcEventSummary['totalEventDuration_s'],
    #                                        zeroEmissionEvents=(mcEventSummary['emissionEvents']==0).sum()
    # )
    eventSummary = (
        instEmissionDF
        .groupby(groupbyCols, as_index=False)
        .agg(**AGG_COLS)
        .assign(CICategory=varCol,
                mcRuns=mcIterations,
                emissionRateUnits='kg/s')
        )
    eventSummary = eventSummary.assign(eventsPerMCRun=eventSummary['eventCount'] / eventSummary['mcRuns'],
                                       meanEmissionRate=eventSummary['totalEmission_kg'] / eventSummary['totalEventDuration_s'])
    eventSummary_kgPerh = eventSummary.assign(meanEmissionRate=eventSummary['meanEmissionRate'] * u.SECONDS_PER_HOUR, 
                                              simpleMean=eventSummary['simpleMean'] * u.SECONDS_PER_HOUR,
                                              emissionRateUnits='kg/h',
                                              )
    siteSummary = (
        instEmissionDF
        .groupby(SUMMARY_KEY_COLS, as_index=False)
        .agg(**AGG_COLS)
        .assign(CICategory=varCol,
                mcRuns=mcIterations,
                emissionRateUnits='kg/s')
    )
    siteSummary = siteSummary.assign(eventsPerMCRun=siteSummary['eventCount'] / siteSummary['mcRuns'],
                                     meanEmissionRate=siteSummary['totalEmission_kg'] / siteSummary['totalEventDuration_s'])
    siteSummary_kgPerh = siteSummary.assign(meanEmissionRate=siteSummary['meanEmissionRate'] * u.SECONDS_PER_HOUR, simpleMean=siteSummary['simpleMean'] * u.SECONDS_PER_HOUR, emissionRateUnits='kg/h')

    retDF = pd.concat([eventSummary, eventSummary_kgPerh, siteSummary, siteSummary_kgPerh])
    return retDF



def calculateC2C1Ratios(summaryDF, confidenceLevel):
    alpha = 100 - float(confidenceLevel)
    STAT_COLS = {'total', 'count', 'mean', 'min', 'max', 'lowerQuintile', 'upperQuintile',
                 'lowerCI', 'upperCI', 'readings', 'rawCount', 'rawMean', 'units', 'species'}

    kgDF = summaryDF[summaryDF['units'] == KG_PER_YEAR_UNITS_NAME]
    methaneDF = kgDF[kgDF['species'] == 'METHANE']
    ethaneDF = kgDF[kgDF['species'] == 'ETHANE']

    join_cols = [c for c in methaneDF.columns if c not in STAT_COLS]

    NULL = '__NULL__'
    obj_join_cols = [c for c in join_cols if methaneDF[c].dtype == object]
    methaneDF = methaneDF.assign(**{c: methaneDF[c].fillna(NULL) for c in obj_join_cols})
    ethaneDF = ethaneDF.assign(**{c: ethaneDF[c].fillna(NULL) for c in obj_join_cols})

    merged = methaneDF.merge(ethaneDF, on=join_cols, suffixes=('_ch4', '_c2h6'))
    if merged.empty:
        return pd.DataFrame()

    ratioReadings = pd.Series(
        [[e / m if m != 0 else np.nan for m, e in zip(ch4, c2h6)]
         for ch4, c2h6 in zip(merged['readings_ch4'], merged['readings_c2h6'])],
        index=merged.index
    )

    ratioDF = merged[join_cols].assign(
        species='C2/C1',
        units='unitless',
        readings=ratioReadings,
        total=merged['total_c2h6'] / merged['total_ch4'],
        count=merged['count_ch4'],
        mean=ratioReadings.apply(np.nanmean),
        min=ratioReadings.apply(np.nanmin),
        max=ratioReadings.apply(np.nanmax),
        lowerQuintile=ratioReadings.apply(lambda x: np.nanpercentile(x, 25)),
        upperQuintile=ratioReadings.apply(lambda x: np.nanpercentile(x, 75)),
        lowerCI=ratioReadings.apply(lambda x: np.nanpercentile(x, alpha / 2)),
        upperCI=ratioReadings.apply(lambda x: np.nanpercentile(x, 100 - alpha / 2)),
        rawCount=merged['rawCount_ch4'],
        rawMean=merged['rawMean_c2h6'] / merged['rawMean_ch4'],
    )

    for c in obj_join_cols:
        ratioDF[c] = ratioDF[c].replace(NULL, np.nan)

    return ratioDF

def summarizeSingleSite(config, instEmissionDF):
    CONFIDENCE_LEVEL = 95
    AGG_FIELDS = {
        'total': ('emissions_kgPerYear', 'sum'),
        'count': ('emissions_kgPerYear', 'count'),
        'mean': ('emissions_kgPerYear', 'mean'),
        'min': ('emissions_kgPerYear', 'min'),
        'max': ('emissions_kgPerYear', 'max'),
        'lowerQuintile': ('emissions_kgPerYear', lambda x: np.percentile(x, 25)),
        'upperQuintile': ('emissions_kgPerYear', lambda x: np.percentile(x, 75)),
        'lowerCI': ('emissions_kgPerYear', lambda x: np.percentile(x, alpha / 2)),
        'upperCI': ('emissions_kgPerYear', lambda x: np.percentile(x, (100 - alpha / 2))),
        'readings': ('emissions_kgPerYear', list)
    }
    alpha = 100 - float(CONFIDENCE_LEVEL)

    mcIterations = config['monteCarloIterations']
    with Timer("summarize") as t0:
        simDurationDays = config['simDurationDays']
        instEmissionDF = _createEmissionDF(instEmissionDF)
        _saveSummaryDS(config, instEmissionDF, 'InstEmissions')
        instEmissionNoFugitiveDF = instEmissionDF[instEmissionDF['modelEmissionCategory'] != 'FUGITIVE']

        additionalConversions = [
            # {'colName': 'emissions_kgPerYear', 'units': KG_PER_YEAR_UNITS_NAME,          'conversion': _convertKGPerYear2KGPerYear},
            {'colName': 'emissions_kgPerYear', 'units': US_TONS_PER_YEAR_UNITS_NAME,     'conversion': _convertKGPerYear2USTonsPerYear},
            {'colName': 'emissions_kgPerYear', 'units': METRIC_TONS_PER_YEAR_UNITS_NAME, 'conversion': _convertKGPerYear2MetricTonsPerYear},
        ]

        with Timer("calculate annual summaries") as t0:
            summaryEmissionFugitiveDF = calculateAnnualSummaries(instEmissionDF, simDurationDays, AGG_FIELDS, mcIterations)
            summaryEmissionFugitiveDF = summaryEmissionFugitiveDF.assign(includeFugitive=True)
            summaryEmissionNoFugitiveDF = calculateAnnualSummaries(instEmissionNoFugitiveDF, simDurationDays, AGG_FIELDS, mcIterations)
            summaryEmissionNoFugitiveDF = summaryEmissionNoFugitiveDF.assign(includeFugitive=False)
            t0.setCount(len(summaryEmissionFugitiveDF) + len(summaryEmissionNoFugitiveDF))

        logging.info("Before apply additional conversions")

        with Timer("apply additional conversions") as t1:
            fullSummaryEmissionFugitiveDF = applyConversions(summaryEmissionFugitiveDF, additionalConversions, AGG_FIELDS)
            fullSummaryEmissionNoFugitiveDF = applyConversions(summaryEmissionNoFugitiveDF, additionalConversions, AGG_FIELDS)
            t1.setCount(len(fullSummaryEmissionFugitiveDF) + len(fullSummaryEmissionNoFugitiveDF))

        logging.info("Before special summaries")
        
        with Timer("special summaries") as t2:
            emissionSummaryFugitiveDF = calculateEmissionSummary(instEmissionDF, mcIterations)
            emissionSummaryFugitiveDF = emissionSummaryFugitiveDF.assign(includeFugitive=True)
            emissionSummaryNoFugitiveDF = calculateEmissionSummary(instEmissionNoFugitiveDF, mcIterations)
            emissionSummaryNoFugitiveDF = emissionSummaryNoFugitiveDF.assign(includeFugitive=False)

            logging.info("  special summaries done")

            fullSummaryEmissionDF = pd.concat([
                fullSummaryEmissionFugitiveDF,
                fullSummaryEmissionNoFugitiveDF,
                emissionSummaryFugitiveDF,
                emissionSummaryNoFugitiveDF
                ])
    

            fullSummaryEmissionDF = fullSummaryEmissionDF.assign(confidenceLevel=CONFIDENCE_LEVEL)

            c2c1DF = calculateC2C1Ratios(fullSummaryEmissionDF, CONFIDENCE_LEVEL)
            if not c2c1DF.empty:
                fullSummaryEmissionDF = pd.concat([fullSummaryEmissionDF, c2c1DF])

            fullSummaryEmissionDF = fullSummaryEmissionDF.assign(simDurationDays=simDurationDays)
            t2.setCount(len(fullSummaryEmissionDF))

        _saveSummaryDS(config, fullSummaryEmissionDF, 'SiteSummary')

        with Timer("event summaries") as t3:
            eventSummaryFugitiveDF = calculateEventSummary(instEmissionDF, simDurationDays, mcIterations, 'eventSummary')
            eventSummaryFugitiveDF = eventSummaryFugitiveDF.assign(includeFugitive=True)
            eventSummaryNoFugitiveDF = calculateEventSummary(instEmissionNoFugitiveDF, simDurationDays, mcIterations, 'eventSummary')
            eventSummaryNoFugitiveDF = eventSummaryNoFugitiveDF.assign(includeFugitive=False)

            fullEventSummaryDF = pd.concat([eventSummaryFugitiveDF, eventSummaryNoFugitiveDF])
            fullEventSummaryDF = fullEventSummaryDF.assign(simDurationDays=simDurationDays)
            t3.setCount(len(fullEventSummaryDF))

        _saveSummaryDS(config, fullEventSummaryDF, 'EventSummary')

    pass

def summarize(config):
    logger.info(f"Summarizing site {config['siteName']}")
    with Timer("Read events") as t0:
        logger.info("Read Parquet Files")
        eventDF = pl.readParquetEvents(config,
                                        site=config['siteName'],
                                        mergeGC=True,
                                        species=SPECIES,
                                        additionalEventFilters=[('command', '=', 'EMISSION')])
        if eventDF is None:
            return
        t0.setCount(len(eventDF))

    with Timer("Process events") as t2:
        summarizeSingleSite(config, eventDF)

def _filterAndPivot(inDF, CICategory, mcIterations, pivotField=None):
    confidenceLevel = 95
    alpha = 100 - float(confidenceLevel)

    AGG_FIELDS = {
        'total': ('mean', 'sum'),
        'min':   ('mean', 'min'),
        'max':   ('mean', 'max'),
        'lower':  ('mean', lambda x: np.percentile(x, 25)),
        'upper':  ('mean', lambda x: np.percentile(x, 75)),
        'lowerCI':  ('mean', lambda x: np.percentile(x, alpha / 2)),
        'upperCI':  ('mean', lambda x: np.percentile(x, (100 - alpha / 2))),
        'readings':  ('mean', list)
    }

    if pivotField is None:
        pivotField = CICategory

    filteredDF = inDF[inDF['CICategory'] == CICategory]
    groupCols = ['species', 'units', 'includeFugitive'] if pivotField == 'simulation' else ['species', pivotField, 'units', 'includeFugitive']

    with Timer(CICategory) as t0:
        summaryDF = (
            filteredDF.groupby(groupCols)
            .agg(**AGG_FIELDS)
            .reset_index()
        )
        # total = Σ site_mean = simulation_total / mcIterations (mathematically equivalent to old mean_emissions)
        summaryDF = summaryDF.assign(
            count=mcIterations,
            mean=summaryDF['total'],
            CICategory=CICategory
        )
        t0.setCount(len(summaryDF))
    return summaryDF

def _computeSimC2C1(inDF, CICategory, mcIterations, pivotField=None):
    if pivotField is None:
        pivotField = CICategory

    kgDF = inDF[(inDF['units'] == KG_PER_YEAR_UNITS_NAME) & (inDF['CICategory'] == CICategory)]

    groupCols = ['includeFugitive'] if pivotField == 'simulation' else [pivotField, 'includeFugitive']

    methaneDF = (kgDF[kgDF['species'] == 'METHANE']
                 .groupby(groupCols)['mean'].sum()
                 .reset_index()
                 .rename(columns={'mean': 'total_ch4'}))
    ethaneDF = (kgDF[kgDF['species'] == 'ETHANE']
                .groupby(groupCols)['mean'].sum()
                .reset_index()
                .rename(columns={'mean': 'total_c2h6'}))

    merged = methaneDF.merge(ethaneDF, on=groupCols)
    if merged.empty:
        return pd.DataFrame()

    ratio = merged['total_c2h6'] / merged['total_ch4']
    n = len(merged)
    retDF = merged[groupCols].assign(
        species='C2/C1',
        units='unitless',
        total=ratio,
        mean=ratio,
        count=mcIterations,
        min=[np.nan] * n,
        max=[np.nan] * n,
        lower=[np.nan] * n,
        upper=[np.nan] * n,
        lowerCI=[np.nan] * n,
        upperCI=[np.nan] * n,
        readings=[[] for _ in range(n)],
        CICategory=CICategory,
    )
    return retDF


def summarizeSimulation(config):
    # this method depends on site-level simulations (aka 'summarize' function) being performed prior to this call.
    logger.info(f"{config['parquetNewSummary']=}")
    with Timer("Read summaries") as t0:
        logging.info("Read summary parquet files")
        fullSummaryDF = pd.read_parquet(config['parquetNewSummary'])
        t0.setCount(len(fullSummaryDF))

    mcIterations = config['monteCarloIterations']
    # Exclude per-site C2/C1 ratios before aggregating; recompute from aggregated METHANE/ETHANE totals below.
    nonRatioDF = fullSummaryDF[fullSummaryDF['species'] != 'C2/C1']

    mecSimSummaryDF = _filterAndPivot(nonRatioDF, 'modelEmissionCategory', mcIterations)
    readableNameSummaryDF = _filterAndPivot(nonRatioDF, 'modelReadableName', mcIterations)
    unitIDSummaryDF = _filterAndPivot(nonRatioDF, 'unitID', mcIterations)
    METypeSummaryDF = _filterAndPivot(nonRatioDF, 'METype', mcIterations)
    pneumaticsDF = _filterAndPivot(nonRatioDF, 'pneumatic', mcIterations, pivotField='METype')
    siteSummaryDF = _filterAndPivot(nonRatioDF, 'modelEmissionCategory', mcIterations, pivotField='simulation')
    siteSummaryDF = siteSummaryDF.assign(CICategory='simulation')

    c2c1Parts = list(filter(lambda df: not df.empty, [
        _computeSimC2C1(nonRatioDF, 'modelEmissionCategory', mcIterations),
        _computeSimC2C1(nonRatioDF, 'modelReadableName', mcIterations),
        _computeSimC2C1(nonRatioDF, 'unitID', mcIterations),
        _computeSimC2C1(nonRatioDF, 'METype', mcIterations),
        _computeSimC2C1(nonRatioDF, 'pneumatic', mcIterations, pivotField='METype'),
    ]))

    fullSimSummaryDF = pd.concat([
        mecSimSummaryDF,
        readableNameSummaryDF,
        unitIDSummaryDF,
        METypeSummaryDF,
        pneumaticsDF,
        siteSummaryDF,
        *c2c1Parts
    ])

    fullSimSummaryDF = fullSimSummaryDF.assign(simDurationDays=config['simDurationDays'])
    _saveSummaryDS(config, fullSimSummaryDF, 'SimSummary')

    pass
