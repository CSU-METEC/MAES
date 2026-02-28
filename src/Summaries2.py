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

def calculateEmissionSummary(instEmissionDF, simDurationDays, aggColumnList, mcIterations):
    instEmissionDF = instEmissionDF.assign(emissions_kgPerYear=(instEmissionDF['totalEmission_kg']) / simDurationDays * u.DAYS_PER_YEAR)
    # first aggregation -- there may be multiple emissions per emitterID (e.g. leaks from the same emitter multiple times per sim)
    #   aggregate by emitterID to eliminate these
    aggregatedEmissionsByEmitterID = (
        instEmissionDF.groupby(['site', 'mcRun', 'species', 'emitterID', 'operator', 'psno', 'METype', 'unitID', 'modelReadableName'],
                               as_index=False)
        .agg(emissions_kgPerYear=('emissions_kgPerYear', 'sum'),
             count=('emissions_kgPerYear', 'count'))
    )

    resDF = _doAggHierarchy(aggregatedEmissionsByEmitterID, aggColumnList, mcIterations,
                            varCol='instantEmissionsByModelReadableName',
                            detailGroupbyCols=[*SUMMARY_KEY_COLS, 'METype', 'unitID', 'modelReadableName'],
                            rollupCols=['modelReadableName', 'unitID', 'METype'])
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
    AGG_COLS = {
        'eventCount': ('emission_kgPerS', 'count'),
        'totalEmission_kg': ('totalEmission_kg', 'sum'),
        'totalEventDuration_s': ('duration_s', 'sum'),
        'meanEventDuration_s': ('duration_s', 'mean'),
        # 'emissionEvents': ('emission_kgPerS', list),
        'durationEvents': ('duration_s', list),
        'totalEmissionEvents': ('totalEmission_kg', list)

    }
    groupbyCols = [*SUMMARY_KEY_COLS, 'unitID', 'modelReadableName']
    mcGroupbyCols = [*groupbyCols, 'mcRun']
    mcEventSummary = (
        instEmissionDF
        .groupby(mcGroupbyCols, as_index=False)
        .agg(**AGG_COLS)
        .assign(CICategory=varCol,
                mcRuns=1,
                emissionRateUnits='kg/s'
                )
        )
    mcEventSummary = mcEventSummary.assign(eventsPerMCRun=mcEventSummary['eventCount'] / mcEventSummary['mcRuns'],
                                           meanEmissionRate=mcEventSummary['totalEmission_kg'] / mcEventSummary['totalEventDuration_s'])
    mcEventSummary_kgPerh = mcEventSummary.assign(meanEmissionRate=mcEventSummary['meanEmissionRate'] * u.SECONDS_PER_HOUR, emissionRateUnits='kg/h')
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
    eventSummary_kgPerh = eventSummary.assign(meanEmissionRate=eventSummary['meanEmissionRate'] * u.SECONDS_PER_HOUR, emissionRateUnits='kg/h')
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
    siteSummary_kgPerh = siteSummary.assign(meanEmissionRate=siteSummary['meanEmissionRate'] * u.SECONDS_PER_HOUR, emissionRateUnits='kg/h')

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

        with Timer("apply additional conversions") as t1:
            fullSummaryEmissionFugitiveDF = applyConversions(summaryEmissionFugitiveDF, additionalConversions, AGG_FIELDS)
            fullSummaryEmissionNoFugitiveDF = applyConversions(summaryEmissionNoFugitiveDF, additionalConversions, AGG_FIELDS)
            t1.setCount(len(fullSummaryEmissionFugitiveDF) + len(fullSummaryEmissionNoFugitiveDF))
        
        specialConversions = [
            {'colName': 'emissions_kgPerYear', 'units': KG_PER_HOUR_UNITS_NAME, 'conversion': _convertKGPerYear2KGPerHour},
        ]
            
        with Timer("special summaries") as t2:
            emissionSummaryFugitiveDF = calculateEmissionSummary(instEmissionDF, simDurationDays, AGG_FIELDS, mcIterations)
            emissionSummaryFugitiveDF = emissionSummaryFugitiveDF.assign(includeFugitive=True)
            emissionSummaryNoFugitiveDF = calculateEmissionSummary(instEmissionNoFugitiveDF, simDurationDays, AGG_FIELDS, mcIterations)
            emissionSummaryNoFugitiveDF = emissionSummaryNoFugitiveDF.assign(includeFugitive=False)
            fullEmissionSummaryFugitiveDF = applyConversions(emissionSummaryFugitiveDF, specialConversions, AGG_FIELDS)
            fullEmissionSummaryNoFugitiveDF = applyConversions(emissionSummaryNoFugitiveDF, specialConversions, AGG_FIELDS)

            fullSummaryEmissionDF = pd.concat([
                fullSummaryEmissionFugitiveDF, 
                fullSummaryEmissionNoFugitiveDF,
                fullEmissionSummaryFugitiveDF,
                fullEmissionSummaryNoFugitiveDF
                ])
    

            fullSummaryEmissionDF = fullSummaryEmissionDF.assign(confidenceLevel=CONFIDENCE_LEVEL)

            c2c1DF = calculateC2C1Ratios(fullSummaryEmissionDF, CONFIDENCE_LEVEL)
            if not c2c1DF.empty:
                fullSummaryEmissionDF = pd.concat([fullSummaryEmissionDF, c2c1DF])

            t2.setCount(len(fullSummaryEmissionDF))

        _saveSummaryDS(config, fullSummaryEmissionDF, 'SiteSummary')

        with Timer("event summaries") as t3:
            eventSummaryFugitiveDF = calculateEventSummary(instEmissionDF, simDurationDays, mcIterations, 'eventSummary')
            eventSummaryFugitiveDF = eventSummaryFugitiveDF.assign(includeFugitive=True)
            eventSummaryNoFugitiveDF = calculateEventSummary(instEmissionNoFugitiveDF, simDurationDays, mcIterations, 'eventSummary')
            eventSummaryNoFugitiveDF = eventSummaryNoFugitiveDF.assign(includeFugitive=False)

            fullEventSummaryDF = pd.concat([eventSummaryFugitiveDF, eventSummaryNoFugitiveDF])
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

def _filterAndPivot(inDF, CICategory, pivotField=None):
    AGG_FIELDS = {
        'total': ('total', 'sum'),
        'count': ('total', 'count'),
        'mean':  ('total', 'mean'),        'min':  ('total', 'min'),
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
                filteredDF.groupby(['species', pivotField, 'units', 'includeFugitive'])
                .agg(**AGG_FIELDS)
                .reset_index()
            )
        else:
            summaryDF = (
                filteredDF.groupby(['species', 'units', 'includeFugitive'])
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

    _saveSummaryDS(config, fullSimSummaryDF, 'SimSummary')

    pass
