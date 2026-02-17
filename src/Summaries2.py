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

from ParquetLib import SUMMARY_DS
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

SUMMARY_KEY_COLS = ['site', 'species', 'operator', 'psno']

def _convertkgPerS2kgPerH(x):
    return x * u.SECONDS_PER_HOUR

def _convertKGPerYear2USTonsPerYear(x):
    return x * u.KG_TO_SHORT_TONS

def _convertKGPerYear2MetricTonsPerYear(x):
    return x * KG_PER_HOUR_TO_MT_PER_HOUR

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

def _saveInstEmissionDF(config, instEmissionDF):
    pl.toBaseParquet(config, instEmissionDF, 'newInstEmissions', partition_cols=['site'])

def _saveSummaryEmissionDF(config, summaryEmissionDF):
    pl.toBaseParquet(config, summaryEmissionDF, 'newSummary', partition_cols=['site'])

def _doAgg(df, groupbyCols, aggFieldList, varCol):
    summaryDFByMCRun = (
        df.groupby(groupbyCols, as_index=False)
        .agg(**aggFieldList)
        .assign(CICategory=varCol,
                units='kg/year')
    )
    return summaryDFByMCRun

def _doSingleLevelSummary(aggregatedEmissionsByEmitterID, aggColumnList, mcIterations):
    resultDFList = []
    for varCol in ['METype', 'unitID', 'modelEmissionCategory']:
        groupbyCols = [*SUMMARY_KEY_COLS, varCol]
        mcRunSummaryDF = _doAgg(aggregatedEmissionsByEmitterID, [*groupbyCols, 'mcRun'], aggColumnList, varCol)
        resultDFList.append(mcRunSummaryDF)
        mcRunSummaryAggDF = mcRunSummaryDF.assign(emissions_kgPerYear=mcRunSummaryDF['total'])
        runSummaryDF = _doAgg(mcRunSummaryAggDF, groupbyCols, aggColumnList, varCol)
        # Patch the runSummaryDF to calculate the adjusted mean, using the full MCIteration count
        runSummaryDF = runSummaryDF.assign(rawCount=runSummaryDF['count'],
                                           rawMean=runSummaryDF['mean'],
                                           mean=runSummaryDF['total'] / mcIterations,
                                           count=mcIterations
                                           )
        resultDFList.append(runSummaryDF)
        siteSummaryAggDF = runSummaryDF.assign(emissions_kgPerYear=runSummaryDF['total'])
        siteSummaryDF = _doAgg(siteSummaryAggDF, SUMMARY_KEY_COLS, aggColumnList, varCol)
        resultDFList.append(siteSummaryDF)
    retDF = pd.concat(resultDFList)
    return retDF

def _doMultiLevelSummary(aggregatedEmissionsByEmitterID, aggColumnList, mcIterations):
    resultDFList = []

    modelReadleNameSummaryCols = ['modelReadableName', 'unitID', 'METype']
    mrGroupbyCols = [*SUMMARY_KEY_COLS, *modelReadleNameSummaryCols]
    mrRunSummaryDF = _doAgg(aggregatedEmissionsByEmitterID, [*mrGroupbyCols, 'mcRun'], aggColumnList, 'modelReadableName')
    resultDFList.append(mrRunSummaryDF)
    theseSummaryCols = mrGroupbyCols
    for singleSummaryLevel in modelReadleNameSummaryCols:
        mrRunSummaryDF = mrRunSummaryDF.assign(emissions_kgPerYear=mrRunSummaryDF['total'])
        mrRunSummaryDF = _doAgg(mrRunSummaryDF, theseSummaryCols, aggColumnList, 'modelReadableName')
        if singleSummaryLevel == modelReadleNameSummaryCols[0]:
            mrRunSummaryDF = mrRunSummaryDF.assign(rawCount=mrRunSummaryDF['count'],
                                                   rawMean=mrRunSummaryDF['mean'],
                                                   mean=mrRunSummaryDF['total'] / mcIterations,
                                                   count=mcIterations
                                                   )
        resultDFList.append(mrRunSummaryDF)
        theseSummaryCols = list(filter(lambda x: x != singleSummaryLevel, theseSummaryCols))
    retDF = pd.concat(resultDFList)
    return retDF

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

    singleLevelSummaryDF = _doSingleLevelSummary(aggregatedEmissionsByEmitterID, aggColumnList, mcIterations)
    multiLevelSummaryDF = _doMultiLevelSummary(aggregatedEmissionsByEmitterID, aggColumnList, mcIterations)

    resDF = pd.concat([singleLevelSummaryDF, multiLevelSummaryDF])
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
        _saveInstEmissionDF(config, instEmissionDF)
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

        fullSummaryEmissionDF = pd.concat([fullSummaryEmissionFugitiveDF, fullSummaryEmissionNoFugitiveDF])

        fullSummaryEmissionDF = fullSummaryEmissionDF.assign(confidenceLevel=CONFIDENCE_LEVEL)
        _saveSummaryEmissionDF(config, fullSummaryEmissionDF)

    pass

def summarize(config):
    logger.info(f"Summarizing site {config['siteName']}")
    with Timer("Read events") as t0:
        logger.info("Read Parquet Files")
        eventDF = pl.readParquetEvents(config,
                                        site=config['siteName'],
                                        mergeGC=True,
                                        species=['METHANE', 'ETHANE'],
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

    simSummaryRoot = Path(config['parquetNewSimSummary'])
    simSummaryDS = f"newSimSummary"
    pl.toBaseParquet(config, fullSimSummaryDF, simSummaryDS, partition_cols={})

    pass
