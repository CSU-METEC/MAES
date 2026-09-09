import datetime
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
import pyarrow as _pa
import pyarrow.dataset as _ds

from ParquetLib import SUMMARY_DS
from Timer import Timer
import Units as u
from pathlib import Path

logger = logging.getLogger(__name__)


def _read_parquet_site(path, site_name):
    partitioning = _ds.partitioning(
        _pa.schema([('site', _pa.string())]),
        flavor='hive',
    )
    dataset = _ds.dataset(str(path), format='parquet', partitioning=partitioning)
    table = dataset.to_table(filter=_ds.field('site') == str(site_name))
    return table.to_pandas()


def _read_parquet_site_mcrun(path, site_name, mcRun):
    """Read one (site, mcRun)'s rows. InstEmissions is partitioned by site only, with
    mcRun a native int64 data column, so this pushes both predicates into the read
    itself instead of reading the whole site and filtering mcRun in pandas after --
    each concurrent per-mcRun caller only ever materializes its own slice."""
    if not os.path.exists(str(path)):
        return pd.DataFrame()
    partitioning = _ds.partitioning(
        _pa.schema([('site', _pa.string())]),
        flavor='hive',
    )
    dataset = _ds.dataset(str(path), format='parquet', partitioning=partitioning)
    filt = (_ds.field('site') == str(site_name)) & (_ds.field('mcRun') == int(mcRun))
    return dataset.to_table(filter=filt).to_pandas()


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
CACHE_IDENTITY_COLS = [*SUMMARY_KEY_COLS, 'METype', 'unitID', 'modelReadableName', 'modelEmissionCategory']

def _convertkgPerS2kgPerH(x):
    return x * u.SECONDS_PER_HOUR

def _convertKGPerYear2USTonsPerYear(x):
    return x * u.KG_TO_SHORT_TONS

def _convertKGPerYear2MetricTonsPerYear(x):
    return x * KG_PER_HOUR_TO_MT_PER_HOUR

def _convertKGPerYear2KGPerHour(x):
    return x * KG_PER_YEAR_TO_KG_PER_HOUR

def _createEmissionDF(inDF, simDurationSecs):
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

    # Issue #87: clip events that overrun the simulation window so every downstream
    # consumer integrates emissions only over [0, simDurationSecs]. The engine logs the
    # full sampled duration of whatever state is in progress when simpy stops at
    # simDurationSecs, so `timestamp + duration` can exceed the window. Left unclipped,
    # the overrun adds spurious emission credit past the window and biases all
    # rate-integrated quantities (annual summaries, PDFs) upward for long / fat-tailed
    # event classes. This is the single point where the emission DataFrame is built and
    # saved as InstEmissions -- the dataset the PDF cascade reads back -- so clipping
    # here keeps the rate summaries, the event summaries, and the PDFs mutually
    # consistent over [0, simDurationSecs]. The raw events parquet retains the full
    # sampled durations. The rate (emission_kgPerS) is unchanged; only duration_s and
    # totalEmission_kg shrink, for the at-most-one in-progress event per emitter state
    # machine whose end exceeds the window. `.clip(lower=0)` guards the degenerate case
    # of an event timestamped at or after the window end (which the engine should not
    # produce) from yielding a negative duration.
    clippedDuration = np.minimum(
        inDF['duration'],
        (simDurationSecs - inDF['timestamp']).clip(lower=0)
    )
    emissionDF = emissionDF.assign(
        duration=clippedDuration,
        totalEmission_kg=emissionDF['emission_kgPerS'] * clippedDuration,
    )

    emissionDF = emissionDF.rename(columns=COLS_TO_KEEP)

    retDF = emissionDF[COLS_TO_KEEP.values()]

    return retDF

DATASET_PARAMS = {
    'InstEmissions': {'configKey': 'parquetNewInstEmissions', 'partition_cols': ['site']},
    'SiteSummary':   {'configKey': 'parquetNewSummary',       'partition_cols': ['site']},
    'EventSummary':  {'configKey': 'parquetNewEventSummary',  'partition_cols': ['site']},
    'SimSummary':    {'configKey': 'parquetNewSimSummary',    'partition_cols': []},
    'PDF':           {'configKey': 'parquetNewPDF',           'partition_cols': ['site']},
    'PDFCache':      {'configKey': 'parquetNewPDFCache',      'partition_cols': ['site']},
    'SimPDF':        {'configKey': 'parquetNewSimPDF',        'partition_cols': []},
}

def _saveSummaryDS(config, df, dataset, basename=None, existingDataBehavior=None):
    params = DATASET_PARAMS[dataset]
    kwargs = {}
    if existingDataBehavior is not None:
        kwargs['existing_data_behavior'] = existingDataBehavior
    pl.toBaseParquetFullConfig(config, df, params['configKey'], partition_cols=params['partition_cols'],
                                basename=basename or dataset, **kwargs)

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

def aggregateEmittersByRun(instEmissionDF, simDurationDays):
    """Level-0 reduction for calculateAnnualSummaries, split out so it can run on a single
    mcRun's slice instead of the full multi-mcRun frame. Mathematically identical either way:
    'mcRun' is already one of the groupby keys here, so partitioning the input by mcRun first
    and reducing each partition separately produces the same rows as reducing the whole frame
    at once -- this is just associativity of groupby-sum/count, not an approximation."""
    instEmissionDF = instEmissionDF.assign(emissions_kgPerYear=(instEmissionDF['totalEmission_kg']) / simDurationDays * u.DAYS_PER_YEAR)
    # first aggregation -- there may be multiple emissions per emitterID (e.g. leaks from the same emitter multiple times per sim)
    #   aggregate by emitterID to eliminate these
    return (
        instEmissionDF.groupby(['site', 'mcRun', 'species', 'emitterID', 'operator', 'psno', 'METype', 'unitID', 'modelReadableName', 'modelEmissionCategory'],
                               as_index=False)
        .agg(emissions_kgPerYear=('emissions_kgPerYear', 'sum'),
             count=('emissions_kgPerYear', 'count'))
    )

def calculateAnnualSummariesFromAggregated(aggregatedEmissionsByEmitterID, aggColumnList, mcIterations):
    """calculateAnnualSummaries's cross-mcRun logic, taking an already-Level-0-reduced frame
    (whether built in one pass by aggregateEmittersByRun or concatenated from several
    per-mcRun calls to it). Unchanged from the prior single-pass code."""
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

def calculateAnnualSummaries(instEmissionDF, simDurationDays, aggColumnList, mcIterations):
    """Unchanged entry point/behavior -- now a thin wrapper over the two pieces above, kept
    for any caller that still wants to pass the full multi-mcRun frame in one call."""
    aggregatedEmissionsByEmitterID = aggregateEmittersByRun(instEmissionDF, simDurationDays)
    return calculateAnnualSummariesFromAggregated(aggregatedEmissionsByEmitterID, aggColumnList, mcIterations)

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
            lowerQuartile=('emission_kgPerH', lambda x: np.percentile(x, 25)),
            upperQuartile=('emission_kgPerH', lambda x: np.percentile(x, 75)),
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

def aggregateEmissionSummaryByRun(instEmissionDF):
    """Per-mcRun piece of calculateEmissionSummary's 'instantEmissionsByModelReadableName'
    stat. Unlike aggregateEmittersByRun, calculateEmissionSummary normally pools readings
    across ALL mcRuns at once (no 'mcRun' groupby key) -- this variant adds 'mcRun'
    temporarily so it can run on one mcRun's own slice. mergeEmissionSummaryByRun below
    concatenates the per-mcRun readings LISTS (not DataFrames) and recomputes percentiles
    fresh over the pooled list, which is exact (percentiles don't care about insertion
    order)."""
    instEmissionDF = _removeZeroEmissionEvents(instEmissionDF)
    df = instEmissionDF.assign(emission_kgPerH=instEmissionDF['emission_kgPerS'] * u.SECONDS_PER_HOUR)
    groupCols = [*SUMMARY_KEY_COLS, 'mcRun', 'METype', 'unitID', 'modelReadableName']
    return (
        df.groupby(groupCols, as_index=False)
        .agg(total=('emission_kgPerH', 'sum'),
             count=('emission_kgPerH', 'count'),
             min=('emission_kgPerH', 'min'),
             max=('emission_kgPerH', 'max'),
             readings=('emission_kgPerH', list))
    )

def mergeEmissionSummaryByRun(perMcRunPieces, mcIterations):
    """Combine aggregateEmissionSummaryByRun's per-mcRun pieces into the same shape
    calculateEmissionSummary returns. sum/min/max/readings-concat across the small
    per-mcRun accumulator rows are all exact -- no raw event data is re-touched here."""
    ci = 95
    alpha = 100 - ci
    groupCols = [*SUMMARY_KEY_COLS, 'METype', 'unitID', 'modelReadableName']
    emptyCols = groupCols + ['total', 'count', 'min', 'max', 'readings']
    nonEmptyPieces = [p for p in perMcRunPieces if not p.empty]
    if not nonEmptyPieces:
        merged = pd.DataFrame(columns=emptyCols)
    else:
        combined = pd.concat(nonEmptyPieces, ignore_index=True)
        merged = combined.groupby(groupCols, as_index=False).agg(
            total=('total', 'sum'),
            count=('count', 'sum'),
            min=('min', 'min'),
            max=('max', 'max'),
            readings=('readings', lambda lists: [v for lst in lists for v in lst]),
        )
    merged = merged.assign(mean=merged['total'] / merged['count'])
    merged = merged.assign(
        lowerQuartile=merged['readings'].apply(lambda x: np.percentile(x, 25)),
        upperQuartile=merged['readings'].apply(lambda x: np.percentile(x, 75)),
        lowerCI=merged['readings'].apply(lambda x: np.percentile(x, alpha / 2)),
        upperCI=merged['readings'].apply(lambda x: np.percentile(x, 100 - alpha / 2)),
        CICategory='instantEmissionsByModelReadableName',
        units=KG_PER_HOUR_UNITS_NAME,
        mcRun=float(mcIterations),
    )
    merged = merged.assign(rawCount=merged['count'], rawMean=merged['mean'])
    return merged

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

def aggregateEventSummaryByRun(instEmissionDF):
    """Per-mcRun piece of calculateEventSummary. Every field calculateEventSummary computes
    is a sum, count, or list-collection -- there is no percentile step here at all -- so this
    is exactly decomposable per mcRun with zero approximation. The one subtlety: 'simpleMean'
    and 'meanEventDuration_s' must be recombined from sums at merge time
    (mergeEventSummaryByRun), not averaged across mcRuns, since per-mcRun event counts are
    uneven (mean-of-means != true mean when group sizes differ) -- this accumulator carries
    the extra sum ('totalEmissionRate_kgPerS') needed to do that correctly."""
    instEmissionDF = _removeZeroEmissionEvents(instEmissionDF)
    AGG_COLS = {
        'eventCount': ('emission_kgPerS', 'count'),
        'totalEmission_kg': ('totalEmission_kg', 'sum'),
        'totalEventDuration_s': ('duration_s', 'sum'),
        'totalEmissionRate_kgPerS': ('emission_kgPerS', 'sum'),
        'durationEvents': ('duration_s', list),
        'totalEmissionEvents': ('totalEmission_kg', list),
    }
    eventGroupCols = [*SUMMARY_KEY_COLS, 'mcRun', 'unitID', 'modelReadableName']
    siteGroupCols = [*SUMMARY_KEY_COLS, 'mcRun']
    eventPiece = instEmissionDF.groupby(eventGroupCols, as_index=False).agg(**AGG_COLS)
    sitePiece = instEmissionDF.groupby(siteGroupCols, as_index=False).agg(**AGG_COLS)
    return eventPiece, sitePiece

def _mergeEventSummaryPieces(pieces, groupCols, varCol, mcIterations):
    emptyCols = groupCols + ['eventCount', 'totalEmission_kg', 'totalEventDuration_s',
                              'totalEmissionRate_kgPerS', 'durationEvents', 'totalEmissionEvents']
    nonEmptyPieces = [p for p in pieces if not p.empty]
    if not nonEmptyPieces:
        merged = pd.DataFrame(columns=emptyCols)
    else:
        combined = pd.concat(nonEmptyPieces, ignore_index=True)
        merged = combined.groupby(groupCols, as_index=False).agg(
            eventCount=('eventCount', 'sum'),
            totalEmission_kg=('totalEmission_kg', 'sum'),
            totalEventDuration_s=('totalEventDuration_s', 'sum'),
            totalEmissionRate_kgPerS=('totalEmissionRate_kgPerS', 'sum'),
            durationEvents=('durationEvents', lambda lists: [v for lst in lists for v in lst]),
            totalEmissionEvents=('totalEmissionEvents', lambda lists: [v for lst in lists for v in lst]),
        )
    merged = merged.assign(
        meanEventDuration_s=merged['totalEventDuration_s'] / merged['eventCount'],
        simpleMean=merged['totalEmissionRate_kgPerS'] / merged['eventCount'],
        CICategory=varCol,
        mcRuns=mcIterations,
        emissionRateUnits='kg/s',
    )
    return merged.drop(columns=['totalEmissionRate_kgPerS'])

def mergeEventSummaryByRun(eventPieces, sitePieces, mcIterations, varCol='eventSummary'):
    """Combine aggregateEventSummaryByRun's per-mcRun pieces into calculateEventSummary's
    exact output shape (4 concatenated blocks: event/site level x kg/s and kg/h units)."""
    eventGroupCols = [*SUMMARY_KEY_COLS, 'unitID', 'modelReadableName']
    siteGroupCols = list(SUMMARY_KEY_COLS)

    eventSummary = _mergeEventSummaryPieces(eventPieces, eventGroupCols, varCol, mcIterations)
    eventSummary = eventSummary.assign(
        eventsPerMCRun=eventSummary['eventCount'] / eventSummary['mcRuns'],
        meanEmissionRate=eventSummary['totalEmission_kg'] / eventSummary['totalEventDuration_s'])
    eventSummary_kgPerh = eventSummary.assign(
        meanEmissionRate=eventSummary['meanEmissionRate'] * u.SECONDS_PER_HOUR,
        simpleMean=eventSummary['simpleMean'] * u.SECONDS_PER_HOUR,
        emissionRateUnits='kg/h')

    siteSummary = _mergeEventSummaryPieces(sitePieces, siteGroupCols, varCol, mcIterations)
    siteSummary = siteSummary.assign(
        eventsPerMCRun=siteSummary['eventCount'] / siteSummary['mcRuns'],
        meanEmissionRate=siteSummary['totalEmission_kg'] / siteSummary['totalEventDuration_s'])
    siteSummary_kgPerh = siteSummary.assign(
        meanEmissionRate=siteSummary['meanEmissionRate'] * u.SECONDS_PER_HOUR,
        simpleMean=siteSummary['simpleMean'] * u.SECONDS_PER_HOUR,
        emissionRateUnits='kg/h')

    return pd.concat([eventSummary, eventSummary_kgPerh, siteSummary, siteSummary_kgPerh])

def calculateC2C1Ratios(summaryDF, confidenceLevel):
    alpha = 100 - float(confidenceLevel)
    STAT_COLS = {'total', 'count', 'mean', 'min', 'max', 'lowerQuartile', 'upperQuartile',
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
        lowerQuartile=ratioReadings.apply(lambda x: np.nanpercentile(x, 25)),
        upperQuartile=ratioReadings.apply(lambda x: np.nanpercentile(x, 75)),
        lowerCI=ratioReadings.apply(lambda x: np.nanpercentile(x, alpha / 2)),
        upperCI=ratioReadings.apply(lambda x: np.nanpercentile(x, 100 - alpha / 2)),
        rawCount=merged['rawCount_ch4'],
        rawMean=merged['rawMean_c2h6'] / merged['rawMean_ch4'],
    )

    for c in obj_join_cols:
        ratioDF[c] = ratioDF[c].replace(NULL, np.nan)

    return ratioDF

PDF_GROUPINGS = [
    ('siteTotals',        SUMMARY_KEY_COLS),
    ('METype',            [*SUMMARY_KEY_COLS, 'METype']),
    ('unitID',            [*SUMMARY_KEY_COLS, 'unitID']),
    ('modelReadableName', [*SUMMARY_KEY_COLS, 'METype', 'unitID', 'modelReadableName']),
]

# Maps per-site PDF CICategory → sim-level CICategory and group columns (no 'site').
# createSimPDF reads from the PDF dataset and computes the mixture distribution:
# p_sim(rate) = (1/N) * sum_i p_i(rate), where N = number of (site, operator, psno) components.
# See issue #30 for discussion of convolution as an alternative for 1000s-of-sites scale.
SIM_PDF_LEVEL_MAP = [
    ('siteTotals',        'simulation',        ['species']),
    ('METype',            'METype',            ['species', 'METype']),
    ('unitID',            'unitID',            ['species', 'unitID']),
    ('modelReadableName', 'modelReadableName', ['species', 'METype', 'unitID', 'modelReadableName']),
]

def _cacheGroupToTimeseriesRLE(groupDF):
    return ts.TimeseriesRLE.fromCollections(
        groupDF['startTime_s'].values,
        groupDF['endTime_s'].values,
        groupDF['emission_kgPerH'].values,
        startTimeColName='timestamp',
        endTimeColName='nextTS',
        valueColName='valueCollection'
    )

def _roundForPDF(values, decimals=6):
    """Round emission rates to 6 decimal places before PDF/CDF construction.

    Background
    ----------
    MAES computes per-emitter emission rates as emission_kgPerS * SECONDS_PER_HOUR.
    When multiple emitter timeseries are combined via TimeseriesSet.sum(), the sweep-line
    algorithm performs floating-point arithmetic that can introduce rounding noise at the
    ULP (Unit in the Last Place) level — typically ~1e-16 for rates in the 0.1–10 kg/h
    range.  This means what is physically one emission rate (e.g. a compressor operating
    at a fixed 0.105718 kg/h) may appear as 2–3 distinct float64 values differing only
    in the 15th–16th decimal place after summation across MC runs.

    Effect on PDF construction
    --------------------------
    TimeseriesPDF.fromDataFrame groups intervals by exact float64 value before summing
    durations.  Without rounding, ULP-variant values are treated as distinct bins in the
    PDF, producing multiple near-identical steps in the CDF.  When compared against legacy
    PDFs (which were written to CSV at 6 decimal places), the x-axis shift between old
    (6 dp) and new (16 dp) values causes np.interp to linearly interpolate across what
    should be a single step, inflating the KS statistic by up to ~0.47.

    Why 6 decimal places
    --------------------
    Six decimal places (resolution 1e-6 kg/h) matches the precision of the legacy CSV
    output from Summaries.py, which rounded emission rates when writing PDF_for_* files.
    This ensures old and new CDFs share the same x-axis binning at the comparison
    resolution.  Differences smaller than 1e-6 kg/h are physically meaningless for
    emissions reporting purposes.

    Rounding must be applied AFTER TimeseriesSet.sum() and before storing values to the
    cache DataFrames.  It cannot be applied only at input because sum() may reintroduce
    ULP noise when combining COMBUSTION and FUGITIVE timeseries intervals.

    Parameters
    ----------
    values : array-like
        Emission rate values in kg/h (numpy array or pandas Series).
    decimals : int
        Number of decimal places to round to.  Default 6.

    Returns
    -------
    numpy ndarray
        Values rounded to the specified number of decimal places.
    """
    return np.round(values, decimals)


def _buildCoarseCacheLevel(fineDF, groupCols, levelName):
    # Accumulate raw numpy arrays per group and build exactly one DataFrame at the end
    # (np.concatenate), instead of building a full pandas DataFrame per group and
    # pd.concat-ing potentially thousands of them -- avoids repeated small-object
    # construction inside a Python group loop. sort=False on both groupbys skips an
    # unnecessary sort pass; nothing downstream depends on group order here.
    aggGroupCols = [*groupCols, 'modelEmissionCategory', 'mcRun']
    coarseCols = {col: [] for col in aggGroupCols}
    coarseStart, coarseEnd, coarseRate, coarseLevel = [], [], [], []
    for groupKey, groupDF in fineDF.groupby(aggGroupCols, sort=False):
        catTSList = []
        for _, subDF in groupDF.groupby(CACHE_IDENTITY_COLS, sort=False):
            catTSList.append(_cacheGroupToTimeseriesRLE(subDF))
        summedTS = ts.TimeseriesSet(catTSList).sum()
        if summedTS.isempty():
            continue
        tsDF = summedTS.df
        n = len(tsDF)
        for col, val in zip(aggGroupCols, groupKey):
            coarseCols[col].append(np.full(n, val))
        coarseStart.append(tsDF[summedTS.startTimeColName].values)
        coarseEnd.append(tsDF[summedTS.endTimeColName].values)
        coarseRate.append(_roundForPDF(tsDF[summedTS.valueColName].values))
        coarseLevel.append(np.full(n, levelName))
    if not coarseStart:
        return pd.DataFrame()
    return pd.DataFrame({
        **{col: np.concatenate(arrs) for col, arrs in coarseCols.items()},
        'startTime_s': np.concatenate(coarseStart),
        'endTime_s': np.concatenate(coarseEnd),
        'emission_kgPerH': np.concatenate(coarseRate),
        'cacheLevel': np.concatenate(coarseLevel),
    })

def createPDFCache(config):
    logger.info(f"Creating PDF cache for site {config['siteName']}")
    with Timer("Read InstEmissions") as t0:
        instEmissionDF = _read_parquet_site(config['parquetNewInstEmissions'], config['siteName'])
        if instEmissionDF.empty:
            logger.info(f"No InstEmissions data for site {config['siteName']}, skipping PDF cache")
            return pd.DataFrame()
        t0.setCount(len(instEmissionDF))

    instEmissionDF = _removeZeroEmissionEvents(instEmissionDF)
    groupCols = [*CACHE_IDENTITY_COLS, 'mcRun']
    cacheRowsList = []
    with Timer("Build PDF cache") as t1:
        for groupKey, groupDF in instEmissionDF.groupby(groupCols):
            summedTS = _buildMCRunTimeseries(groupDF)
            if summedTS.isempty():
                continue
            n = len(summedTS.df)
            identityDict = dict(zip(groupCols, groupKey))
            cacheRowsList.append(pd.DataFrame({
                **{col: [val] * n for col, val in identityDict.items()},
                'startTime_s': summedTS.df[summedTS.startTimeColName].values,
                'endTime_s': summedTS.df[summedTS.endTimeColName].values,
                'emission_kgPerH': _roundForPDF(summedTS.df[summedTS.valueColName].values),
            }))
        t1.setCount(len(cacheRowsList))

    if not cacheRowsList:
        logger.info(f"No cache rows for site {config['siteName']}, skipping PDF cache")
        return pd.DataFrame()

    fineCacheDF = pd.concat(cacheRowsList, ignore_index=True)
    fineCacheDF = fineCacheDF.assign(cacheLevel='modelReadableName')
    allLevelDFs = [fineCacheDF]
    statsRows = [{'cacheLevel': 'modelReadableName', 'groupCount': len(cacheRowsList),
                  'intervalRows': len(fineCacheDF), 'buildSeconds': t1.deltat.total_seconds()}]

    for levelName, levelGroupCols in PDF_GROUPINGS[:-1]:
        with Timer(f"Build coarse cache {levelName}", loglevel=logging.DEBUG) as t2:
            coarseDF = _buildCoarseCacheLevel(fineCacheDF, levelGroupCols, levelName)
            t2.setCount(len(coarseDF))
        if not coarseDF.empty:
            for col in [*CACHE_IDENTITY_COLS, 'mcRun']:
                if col not in coarseDF.columns:
                    coarseDF = coarseDF.assign(**{col: ''})
            allLevelDFs.append(coarseDF)
            groupCount = coarseDF.groupby([*levelGroupCols, 'modelEmissionCategory', 'mcRun']).ngroups
            statsRows.append({'cacheLevel': levelName, 'groupCount': groupCount,
                              'intervalRows': len(coarseDF), 'buildSeconds': t2.deltat.total_seconds()})

    cacheDF = pd.concat(allLevelDFs, ignore_index=True)
    _saveSummaryDS(config, cacheDF, 'PDFCache')
    logger.info(f"PDF cache: {len(cacheDF)} rows for site {config['siteName']}")

    totalSimSecs = config['simDurationDays'] * 86400.0 * config['monteCarloIterations']
    with Timer("Build PDFs") as tPDF:
        fullPDFDF, noFugPDFDF, pdfStatsDF = calculatePDFSummaryFromCache(cacheDF, totalSimSecs)
        fullPDFDF = fullPDFDF.assign(includeFugitive=True)
        noFugPDFDF = noFugPDFDF.assign(includeFugitive=False)
        pdfDF = pd.concat([fullPDFDF, noFugPDFDF])
        tPDF.setCount(len(pdfDF))
    _saveSummaryDS(config, pdfDF, 'PDF')
    logger.info(f"PDF: {len(pdfDF)} rows for site {config['siteName']}")

    cacheStatsDF = pd.DataFrame(statsRows).assign(siteName=config['siteName'])
    pdfStatsDF = pdfStatsDF.assign(siteName=config['siteName'], buildSeconds=tPDF.deltat.total_seconds())
    return pd.concat([cacheStatsDF, pdfStatsDF], ignore_index=True)


def _buildCacheSliceForMCRun(config, mcRun):
    """Build the fine + all coarse PDF cache levels for a single MC run, reading only that
    run's own InstEmissions slice. Splitting the site-wide cache build (createPDFCache,
    above) into one call per MC run lets the outer per-work-type dispatch parallelize this
    across real worker processes instead of one process looping over every MC run
    sequentially -- the dominant cost of a large run's PDF cache phase.

    Returns (cacheDF, groupCount); an empty site/MC-run returns (empty DataFrame, 0).
    """
    instEmissionDF = _read_parquet_site_mcrun(config['parquetNewInstEmissions'], config['siteName'], mcRun)
    if instEmissionDF.empty:
        return pd.DataFrame(), 0
    instEmissionDF = _removeZeroEmissionEvents(instEmissionDF)
    if instEmissionDF.empty:
        return pd.DataFrame(), 0

    # Accumulate raw numpy arrays per group and build exactly one DataFrame at the end,
    # instead of a full pandas DataFrame per group + pd.concat of potentially thousands of
    # them -- see _buildCoarseCacheLevel's comment for the full rationale. sort=False skips
    # an unnecessary sort pass; nothing downstream depends on group order here.
    cacheCols = {col: [] for col in CACHE_IDENTITY_COLS}
    cacheStart, cacheEnd, cacheRate = [], [], []
    groupCount = 0
    for groupKey, groupDF in instEmissionDF.groupby(CACHE_IDENTITY_COLS, sort=False):
        summedTS = _buildMCRunTimeseries(groupDF)
        if summedTS.isempty():
            continue
        tsDF = summedTS.df
        n = len(tsDF)
        for col, val in zip(CACHE_IDENTITY_COLS, groupKey):
            cacheCols[col].append(np.full(n, val))
        cacheStart.append(tsDF[summedTS.startTimeColName].values)
        cacheEnd.append(tsDF[summedTS.endTimeColName].values)
        cacheRate.append(_roundForPDF(tsDF[summedTS.valueColName].values))
        groupCount += 1

    if groupCount == 0:
        return pd.DataFrame(), 0

    fineCacheDF = pd.DataFrame({
        **{col: np.concatenate(arrs) for col, arrs in cacheCols.items()},
        'mcRun': mcRun,
        'startTime_s': np.concatenate(cacheStart),
        'endTime_s': np.concatenate(cacheEnd),
        'emission_kgPerH': np.concatenate(cacheRate),
    })
    fineCacheDF = fineCacheDF.assign(cacheLevel='modelReadableName')
    allLevelDFs = [fineCacheDF]

    for levelName, levelGroupCols in PDF_GROUPINGS[:-1]:
        coarseDF = _buildCoarseCacheLevel(fineCacheDF, levelGroupCols, levelName)
        if not coarseDF.empty:
            for col in [*CACHE_IDENTITY_COLS, 'mcRun']:
                if col not in coarseDF.columns:
                    coarseDF = coarseDF.assign(**{col: ''})
            allLevelDFs.append(coarseDF)
            groupCount += coarseDF.groupby([*levelGroupCols, 'modelEmissionCategory', 'mcRun']).ngroups

    return pd.concat(allLevelDFs, ignore_index=True), groupCount


def finalizePDFCache(config, sliceResults):
    """Concatenate every MC run's cache slice for one site (each built by
    _buildCacheSliceForMCRun, above), write PDFCache, and build the site's PDF dataset --
    the same two outputs createPDFCache produces, just fed by parallel per-MC-run compute
    instead of one big serial groupby over the whole site's InstEmissions."""
    cacheDFs = [cacheDF for cacheDF, _ in sliceResults if not cacheDF.empty]
    if not cacheDFs:
        logger.info(f"No cache rows for site {config['siteName']}, skipping PDF cache")
        return pd.DataFrame()

    cacheDF = pd.concat(cacheDFs, ignore_index=True)
    _saveSummaryDS(config, cacheDF, 'PDFCache')
    logger.info(f"PDF cache: {len(cacheDF)} rows for site {config['siteName']}")

    # createPDFCache's own stats export breaks group counts down per cache level; that
    # breakdown isn't reconstructable here since _buildCacheSliceForMCRun returns one
    # combined count spanning every level for its own MC run (summing per-MC-run counts
    # is exact for a combined total -- mcRun partitions groups into non-overlapping
    # subsets -- but not separable back into per-level totals after the fact). Diagnostic
    # export only (not science/data output): one summary row (fine-level interval count,
    # combined group count) instead of one row per level.
    fineCacheDF = cacheDF[cacheDF['cacheLevel'] == 'modelReadableName']
    statsRows = [{
        'cacheLevel': 'modelReadableName',
        'groupCount': sum(groupCount for _, groupCount in sliceResults),
        'intervalRows': len(fineCacheDF),
    }]

    totalSimSecs = config['simDurationDays'] * 86400.0 * config['monteCarloIterations']
    with Timer("Build PDFs") as tPDF:
        fullPDFDF, noFugPDFDF, pdfStatsDF = calculatePDFSummaryFromCache(cacheDF, totalSimSecs, workers=config.get('workers') or 1)
        fullPDFDF = fullPDFDF.assign(includeFugitive=True)
        noFugPDFDF = noFugPDFDF.assign(includeFugitive=False)
        pdfDF = pd.concat([fullPDFDF, noFugPDFDF])
        tPDF.setCount(len(pdfDF))
    _saveSummaryDS(config, pdfDF, 'PDF')
    logger.info(f"PDF: {len(pdfDF)} rows for site {config['siteName']}")

    cacheStatsDF = pd.DataFrame(statsRows).assign(siteName=config['siteName'])
    pdfStatsDF = pdfStatsDF.assign(siteName=config['siteName'], buildSeconds=tPDF.deltat.total_seconds())
    return pd.concat([cacheStatsDF, pdfStatsDF], ignore_index=True)

def _writeCacheSlice(config, cacheDF, sliceIndex):
    """Write one MC run's PDF cache slice directly to its own small parquet file instead of
    holding it in memory for a later combined write (finalizePDFCache, above) -- frees each
    slice as soon as it's on disk, so a site's full MC set is never held in memory at once."""
    _saveSummaryDS(config, cacheDF, 'PDFCache', basename=f"PDFCache-{sliceIndex}",
                   existingDataBehavior='overwrite_or_ignore')

def finalizePDFCacheFromDisk(config, groupCount):
    """Finalize a site's PDF cache after all its per-MC-run slices have already been streamed
    to disk via _writeCacheSlice. Re-reads the full combined cache once via a single efficient
    pyarrow multi-file dataset scan instead of pandas-concatenating already-materialized
    per-MC-run DataFrames. Otherwise identical to finalizePDFCache's own Build-PDFs tail."""
    cacheDF = _read_parquet_site(config['parquetNewPDFCache'], config['siteName'])
    if cacheDF.empty:
        logger.info(f"No cache rows for site {config['siteName']}, skipping PDF cache")
        return pd.DataFrame()

    fineCacheDF = cacheDF[cacheDF['cacheLevel'] == 'modelReadableName']
    logger.info(f"PDF cache: {len(cacheDF)} rows for site {config['siteName']}")
    statsRows = [{'cacheLevel': 'modelReadableName', 'groupCount': groupCount,
                  'intervalRows': len(fineCacheDF)}]

    totalSimSecs = config['simDurationDays'] * 86400.0 * config['monteCarloIterations']
    with Timer("Build PDFs") as tPDF:
        fullPDFDF, noFugPDFDF, pdfStatsDF = calculatePDFSummaryFromCache(cacheDF, totalSimSecs, workers=config.get('workers') or 1)
        fullPDFDF = fullPDFDF.assign(includeFugitive=True)
        noFugPDFDF = noFugPDFDF.assign(includeFugitive=False)
        pdfDF = pd.concat([fullPDFDF, noFugPDFDF])
        tPDF.setCount(len(pdfDF))
    _saveSummaryDS(config, pdfDF, 'PDF')
    logger.info(f"PDF: {len(pdfDF)} rows for site {config['siteName']}")

    cacheStatsDF = pd.DataFrame(statsRows).assign(siteName=config['siteName'])
    pdfStatsDF = pdfStatsDF.assign(siteName=config['siteName'], buildSeconds=tPDF.deltat.total_seconds())
    return pd.concat([cacheStatsDF, pdfStatsDF], ignore_index=True)

def _buildMCRunTimeseries(mcRunDF):
    zeroDurationDF = mcRunDF[mcRunDF['duration_s'] <= 0]
    if not zeroDurationDF.empty:
        site = mcRunDF['site'].iloc[0]
        mcRun = mcRunDF['mcRun'].iloc[0]
        logger.warning(f"_buildMCRunTimeseries: {len(zeroDurationDF)} zero-duration events filtered out for site {site}, mcRun {mcRun}")
        mcRunDF = mcRunDF[mcRunDF['duration_s'] > 0]
    emitterTSList = []
    for _, emitterDF in mcRunDF.groupby('emitterID'):
        starts = emitterDF['timestamp_s'].values
        ends = starts + emitterDF['duration_s'].values
        values = emitterDF['emission_kgPerS'].values * u.SECONDS_PER_HOUR
        emitterTSList.append(ts.TimeseriesRLE.fromCollections(starts, ends, values))
    with Timer("emitter sum", loglevel=logging.DEBUG) as t:
        result = ts.TimeseriesSet(emitterTSList).sum()
        t.setCount(len(emitterTSList))
    return result

def _buildPDFForGroup(groupDF, identityCols, CICategory, totalSimSecs):
    with Timer("build MC run timeseries") as t:
        mcRunTSList = []
        for _, mcRunDF in groupDF.groupby('mcRun'):
            mcTS = _buildMCRunTimeseries(mcRunDF)
            if not mcTS.isempty():
                mcRunTSList.append(mcTS)
        t.setCount(len(mcRunTSList))
    stats = {
        'CICategory': CICategory,
        **identityCols,
        'mcRunCount': t.counter,
        'buildSeconds': t.deltat.total_seconds(),
    }
    if not mcRunTSList:
        return None, stats
    with Timer("mcRun toPDF", loglevel=logging.DEBUG) as t2:
        pdf = ts.TimeseriesSet(mcRunTSList).toPDF()
        t2.setCount(len(mcRunTSList))
    cdf = pdf.toCDF()
    if cdf.isempty():
        return None, stats
    n = len(cdf.data)
    pdfRows = pd.DataFrame({
        **{col: [val] * n for col, val in identityCols.items()},
        'CICategory': [CICategory] * n,
        'emissionRate_kgPerH': cdf.data['value'].values,
        'probability': (pdf.data['count'] / totalSimSecs).values,
        'cumulativeProbability': cdf.data['cumulative_probability'].values,
    })
    return pdfRows, stats

def calculatePDFSummary(instEmissionDF, totalSimSecs):
    instEmissionDF = _removeZeroEmissionEvents(instEmissionDF)
    resultDFList = []
    statsList = []
    for CICategory, groupCols in PDF_GROUPINGS:
        for _, groupDF in instEmissionDF.groupby(groupCols):
            identityCols = {col: groupDF[col].iloc[0] for col in groupCols}
            pdfRows, stats = _buildPDFForGroup(groupDF, identityCols, CICategory, totalSimSecs)
            statsList.append(stats)
            if pdfRows is not None:
                resultDFList.append(pdfRows)
    pdfDF = pd.concat(resultDFList) if resultDFList else pd.DataFrame()
    statsDF = pd.DataFrame(statsList) if statsList else pd.DataFrame()
    return pdfDF, statsDF

VALIDATE_QUANTILES = [0.1, 0.25, 0.5, 0.75, 0.9, 0.95]

def _makePDFRows(mcRunTSList, identityCols, CICategory, totalSimSecs):
    if not mcRunTSList:
        return None
    pdf = ts.TimeseriesSet(mcRunTSList).toPDF()
    cdf = pdf.toCDF()
    if cdf.isempty():
        return None
    n = len(cdf.data)
    return pd.DataFrame({
        **{col: [val] * n for col, val in identityCols.items()},
        'CICategory': [CICategory] * n,
        'emissionRate_kgPerH': cdf.data['value'].values,
        'probability': (pdf.data['count'] / totalSimSecs).values,
        'cumulativeProbability': cdf.data['cumulative_probability'].values,
    })

def _buildPDFForGroupFromCache(groupDF, identityCols, CICategory, totalSimSecs):
    # When this group has no FUGITIVE intervals at all, noFug == full by construction --
    # reuse the already-summed fullTS instead of recomputing an identical
    # TimeseriesSet(...).sum() call. Safe to share the same object reference across both
    # lists: downstream (_makePDFRows/TimeseriesSet.toPDF()) only ever reads .df, never
    # mutates it.
    hasFugitive = 'FUGITIVE' in groupDF['modelEmissionCategory'].values
    fullMCRunTSList = []
    noFugMCRunTSList = []
    with Timer("build MC run timeseries from coarse cache", loglevel=logging.DEBUG) as t:
        for _, mcRunDF in groupDF.groupby('mcRun'):
            catTSDict = {}
            for emCat, catDF in mcRunDF.groupby('modelEmissionCategory'):
                catTSDict[emCat] = _cacheGroupToTimeseriesRLE(catDF)
            fullTS = ts.TimeseriesSet(list(catTSDict.values())).sum()
            if not fullTS.isempty():
                fullTS.df = fullTS.df.assign(**{fullTS.valueColName: _roundForPDF(fullTS.df[fullTS.valueColName].values)})
                fullMCRunTSList.append(fullTS)
            if not hasFugitive:
                if not fullTS.isempty():
                    noFugMCRunTSList.append(fullTS)
            else:
                noFugItems = filter(lambda kv: kv[0] != 'FUGITIVE', catTSDict.items())
                noFugTS = ts.TimeseriesSet(list(map(lambda kv: kv[1], noFugItems))).sum()
                if not noFugTS.isempty():
                    noFugTS.df = noFugTS.df.assign(**{noFugTS.valueColName: _roundForPDF(noFugTS.df[noFugTS.valueColName].values)})
                    noFugMCRunTSList.append(noFugTS)
        t.setCount(len(fullMCRunTSList))
    stats = {
        'CICategory': CICategory,
        **identityCols,
        'mcRunCount': len(fullMCRunTSList),
        'buildSeconds': t.deltat.total_seconds(),
    }
    return _makePDFRows(fullMCRunTSList, identityCols, CICategory, totalSimSecs), _makePDFRows(noFugMCRunTSList, identityCols, CICategory, totalSimSecs), stats

def _buildPDFGroupTask(args):
    """Picklable task wrapper for calculatePDFSummaryFromCache's Pool (top-level, not a
    closure, so multiprocessing.Pool can pickle it)."""
    groupDF, identityCols, CICategory, totalSimSecs = args
    return _buildPDFForGroupFromCache(groupDF, identityCols, CICategory, totalSimSecs)

# Cache levels whose per-outer-group DataFrames are too large to send through multiprocessing
# IPC efficiently -- these are the coarse aggregate levels where all emitters of a type or the
# entire site are summed into a handful of groups, each holding a large slice of the cache.
_SEQUENTIAL_PDF_LEVELS = {'siteTotals', 'METype'}

def calculatePDFSummaryFromCache(cacheDF, totalSimSecs, groupings=None, workers=1):
    """Parallelizes the finer-grained PDF_GROUPINGS levels (unitID, modelReadableName --
    many small groups) via a Pool spawned here; the coarse levels (_SEQUENTIAL_PDF_LEVELS)
    stay sequential since each holds a large slice of the cache, too expensive to pickle
    through multiprocessing IPC. Safe to spawn a Pool here because this function is only
    ever called from finalizePDFCache/finalizePDFCacheFromDisk, which by construction always
    run in the main coordinating process (the loop consuming runCreatePDFCacheIncremental's
    imap_unordered results, or finalizeAllPDFCaches called after runLocal) -- never inside a
    worker. The in_daemon guard is kept anyway as cheap defensive parity for any future
    caller that might run inside one.
    """
    if groupings is None:
        groupings = PDF_GROUPINGS
    fullResultDFList = []
    noFugResultDFList = []
    statsList = []

    def _collect(result):
        fullPDFRows, noFugPDFRows, stats = result
        statsList.append(stats)
        if fullPDFRows is not None:
            fullResultDFList.append(fullPDFRows)
        if noFugPDFRows is not None:
            noFugResultDFList.append(noFugPDFRows)

    seqTasks = []
    parTasks = []
    for CICategory, groupCols in groupings:
        levelDF = cacheDF[cacheDF['cacheLevel'] == CICategory]
        for _, groupDF in levelDF.groupby(groupCols, sort=False):
            identityCols = {col: groupDF[col].iloc[0] for col in groupCols}
            task = (groupDF, identityCols, CICategory, totalSimSecs)
            if CICategory in _SEQUENTIAL_PDF_LEVELS:
                seqTasks.append(task)
            else:
                parTasks.append(task)

    for task in seqTasks:
        _collect(_buildPDFGroupTask(task))

    import multiprocessing as mp
    inDaemon = mp.current_process().daemon
    if workers and workers > 1 and parTasks and not inDaemon:
        with mp.Pool(min(workers, len(parTasks))) as pool:
            for result in pool.imap_unordered(_buildPDFGroupTask, parTasks):
                _collect(result)
    else:
        for task in parTasks:
            _collect(_buildPDFGroupTask(task))

    fullPDFDF = pd.concat(fullResultDFList) if fullResultDFList else pd.DataFrame()
    noFugPDFDF = pd.concat(noFugResultDFList) if noFugResultDFList else pd.DataFrame()
    statsDF = pd.DataFrame(statsList) if statsList else pd.DataFrame()
    return fullPDFDF, noFugPDFDF, statsDF

def validatePDFCache(config):
    logger.info(f"Validating PDF cache for site {config['siteName']}")
    instEmissionDF = _read_parquet_site(config['parquetNewInstEmissions'], config['siteName'])
    instEmissionDF = _removeZeroEmissionEvents(instEmissionDF)
    cacheDF = _read_parquet_site(config['parquetNewPDFCache'], config['siteName'])
    fineCacheDF = cacheDF[cacheDF['cacheLevel'] == 'modelReadableName']

    # Intermediate check: compare cached RLE intervals vs freshly built for a random sample of groups
    groupCols = [*CACHE_IDENTITY_COLS, 'mcRun']
    allGroups = list(instEmissionDF.groupby(groupCols))
    rng = np.random.default_rng(42)
    sampleIdx = rng.choice(len(allGroups), size=min(10, len(allGroups)), replace=False)
    mismatchCount = 0
    for idx in sampleIdx:
        groupKey, rawGroupDF = allGroups[idx]
        rawTS = _buildMCRunTimeseries(rawGroupDF)
        filterMask = pd.Series([True] * len(fineCacheDF), index=fineCacheDF.index)
        for col, val in zip(groupCols, groupKey):
            filterMask = filterMask & (fineCacheDF[col] == val)
        cacheGroupDF = fineCacheDF[filterMask]
        if cacheGroupDF.empty:
            logger.error(f"Intermediate check: group {groupKey} missing from cache")
            mismatchCount += 1
            continue
        cachedTS = _cacheGroupToTimeseriesRLE(cacheGroupDF)
        startMatch = np.array_equal(rawTS.df[rawTS.startTimeColName].values,
                                    cachedTS.df[cachedTS.startTimeColName].values)
        valueMatch = np.allclose(rawTS.df[rawTS.valueColName].values,
                                 cachedTS.df[cachedTS.valueColName].values)
        if not startMatch or not valueMatch:
            logger.error(f"Intermediate check: RLE mismatch for group {dict(zip(groupCols, groupKey))}")
            mismatchCount += 1
    logger.info(f"Intermediate check: {len(sampleIdx)} groups sampled, {mismatchCount} mismatches")

    # End-to-end check: compare CDFs from raw-instEmissions path vs cache path at fixed quantile points
    totalSimSecs = config['simDurationDays'] * 86400.0 * config['monteCarloIterations']
    pdfFromRaw, _ = calculatePDFSummary(instEmissionDF, totalSimSecs)
    pdfFromCache, _, _ = calculatePDFSummaryFromCache(cacheDF, totalSimSecs)

    joinCols = [c for c in pdfFromRaw.columns if c not in ('emissionRate_kgPerH', 'probability', 'cumulativeProbability')]
    rawSampled = _sampleCDFAtQuantiles(pdfFromRaw, joinCols)
    cacheSampled = _sampleCDFAtQuantiles(pdfFromCache, joinCols)

    merged = rawSampled.merge(cacheSampled, on=[*joinCols, 'quantile'], suffixes=('_raw', '_cache'))
    merged = merged.assign(
        relDelta=((merged['emissionRate_kgPerH_cache'] - merged['emissionRate_kgPerH_raw']).abs()
                  / merged['emissionRate_kgPerH_raw'].replace(0, np.nan))
    )
    failures = merged[merged['relDelta'] > 1e-6]
    if failures.empty:
        logger.info(f"End-to-end check: all {len(merged)} quantile samples match")
    else:
        logger.error(f"End-to-end check: {len(failures)} quantile samples differ:\n{failures.to_string()}")

def _sampleCDFAtQuantiles(cdfDF, joinCols):
    rows = []
    for _, groupDF in cdfDF.groupby(joinCols):
        sortedDF = groupDF.sort_values('cumulativeProbability')
        identityDict = {col: groupDF[col].iloc[0] for col in joinCols}
        sampled = np.interp(VALIDATE_QUANTILES,
                            sortedDF['cumulativeProbability'].values,
                            sortedDF['emissionRate_kgPerH'].values)
        for q, v in zip(VALIDATE_QUANTILES, sampled):
            rows.append({**identityDict, 'quantile': q, 'emissionRate_kgPerH': v})
    return pd.DataFrame(rows)

def _annualSummaryAggFields(alpha):
    """Extracted from summarizeSingleSite so finalizeSummariesForSite can build the identical
    AGG_FIELDS dict without duplicating the literal -- both
    calculateAnnualSummariesFromAggregated (needs the 'readings' key check) and
    applyConversions (needs .keys()) consume this same shape."""
    return {
        'total': ('emissions_kgPerYear', 'sum'),
        'count': ('emissions_kgPerYear', 'count'),
        'mean': ('emissions_kgPerYear', 'mean'),
        'min': ('emissions_kgPerYear', 'min'),
        'max': ('emissions_kgPerYear', 'max'),
        'lowerQuartile': ('emissions_kgPerYear', lambda x: np.percentile(x, 25)),
        'upperQuartile': ('emissions_kgPerYear', lambda x: np.percentile(x, 75)),
        'lowerCI': ('emissions_kgPerYear', lambda x: np.percentile(x, alpha / 2)),
        'upperCI': ('emissions_kgPerYear', lambda x: np.percentile(x, (100 - alpha / 2))),
        'readings': ('emissions_kgPerYear', list)
    }

def summarizeSingleSite(config, instEmissionDF):
    CONFIDENCE_LEVEL = 95
    alpha = 100 - float(CONFIDENCE_LEVEL)
    AGG_FIELDS = _annualSummaryAggFields(alpha)

    mcIterations = config['monteCarloIterations']
    with Timer("summarize") as t0:
        simDurationDays = config['simDurationDays']
        instEmissionDF = _createEmissionDF(instEmissionDF, simDurationDays * u.SECONDS_PER_DAY)
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

def summarizeMCRunPieces(config, mcRun):
    """Per-mcRun 'Option A' accumulator step. Reads exactly this mcRun's raw events (mcRun
    filter pushed into the pyarrow read, same technique as _buildCacheSliceForMCRun) and
    derives InstEmissions for just this slice. Returns the small per-mcRun accumulator pieces
    finalizeSummariesForSite needs to reconstruct the exact SiteSummary/EventSummary/
    InstEmissions output summarizeSingleSite produces monolithically -- or None if this mcRun
    has no qualifying events at all (mirrors summarize()'s own "eventDF is None: return"
    early-out)."""
    eventDF = pl.readParquetEvents(config, site=config['siteName'], mcRun=mcRun, mergeGC=True,
                                    species=SPECIES, additionalEventFilters=[('command', '=', 'EMISSION')])
    if eventDF is None or eventDF.empty:
        return None

    simDurationDays = config['simDurationDays']
    instEmissionDF = _createEmissionDF(eventDF, simDurationDays * u.SECONDS_PER_DAY)
    instEmissionNoFugitiveDF = instEmissionDF[instEmissionDF['modelEmissionCategory'] != 'FUGITIVE']

    return {
        'instEmissionDF': instEmissionDF,
        'annualFugitive': aggregateEmittersByRun(instEmissionDF, simDurationDays),
        'annualNoFugitive': aggregateEmittersByRun(instEmissionNoFugitiveDF, simDurationDays),
        'emissionFugitive': aggregateEmissionSummaryByRun(instEmissionDF),
        'emissionNoFugitive': aggregateEmissionSummaryByRun(instEmissionNoFugitiveDF),
        'eventFugitive': aggregateEventSummaryByRun(instEmissionDF),
        'eventNoFugitive': aggregateEventSummaryByRun(instEmissionNoFugitiveDF),
    }

def finalizeSummariesForSite(config, piecesList, confidenceLevel=95):
    """Combine summarizeMCRunPieces' per-mcRun accumulator pieces (collected across every
    mcRun for one site) into the exact SiteSummary/EventSummary/InstEmissions output
    summarizeSingleSite produces monolithically, and write them. Every downstream step here
    (calculateAnnualSummariesFromAggregated's _doAggHierarchy, applyConversions,
    calculateC2C1Ratios) is completely unchanged from the prior monolithic code -- only the
    inputs it's fed (the concatenated per-mcRun accumulators, instead of a fresh reduction
    over the full multi-mcRun instEmissionDF) are new.

    InstEmissions is written here once per site, combining every mcRun's piece into a single
    dataset (matching summarizeSingleSite's own single write and the shared single-file-per-
    site output convention), rather than as a separate file per mcRun."""
    validPieces = [p for p in piecesList if p is not None]
    if not validPieces:
        # Matches summarize()'s own "eventDF is None: return" -- a site with zero
        # qualifying events across every mcRun gets no SiteSummary/EventSummary at all.
        return

    mcIterations = config['monteCarloIterations']
    simDurationDays = config['simDurationDays']
    alpha = 100 - float(confidenceLevel)
    AGG_FIELDS = _annualSummaryAggFields(alpha)

    combinedInstEmissionDF = pd.concat([p['instEmissionDF'] for p in validPieces], ignore_index=True)
    _saveSummaryDS(config, combinedInstEmissionDF, 'InstEmissions')
    del combinedInstEmissionDF

    additionalConversions = [
        {'colName': 'emissions_kgPerYear', 'units': US_TONS_PER_YEAR_UNITS_NAME,     'conversion': _convertKGPerYear2USTonsPerYear},
        {'colName': 'emissions_kgPerYear', 'units': METRIC_TONS_PER_YEAR_UNITS_NAME, 'conversion': _convertKGPerYear2MetricTonsPerYear},
    ]

    annualFugAgg = pd.concat([p['annualFugitive'] for p in validPieces], ignore_index=True)
    annualNoFugAgg = pd.concat([p['annualNoFugitive'] for p in validPieces], ignore_index=True)

    summaryEmissionFugitiveDF = calculateAnnualSummariesFromAggregated(annualFugAgg, AGG_FIELDS, mcIterations).assign(includeFugitive=True)
    summaryEmissionNoFugitiveDF = calculateAnnualSummariesFromAggregated(annualNoFugAgg, AGG_FIELDS, mcIterations).assign(includeFugitive=False)

    fullSummaryEmissionFugitiveDF = applyConversions(summaryEmissionFugitiveDF, additionalConversions, AGG_FIELDS)
    fullSummaryEmissionNoFugitiveDF = applyConversions(summaryEmissionNoFugitiveDF, additionalConversions, AGG_FIELDS)

    emissionSummaryFugitiveDF = mergeEmissionSummaryByRun([p['emissionFugitive'] for p in validPieces], mcIterations).assign(includeFugitive=True)
    emissionSummaryNoFugitiveDF = mergeEmissionSummaryByRun([p['emissionNoFugitive'] for p in validPieces], mcIterations).assign(includeFugitive=False)

    fullSummaryEmissionDF = pd.concat([
        fullSummaryEmissionFugitiveDF,
        fullSummaryEmissionNoFugitiveDF,
        emissionSummaryFugitiveDF,
        emissionSummaryNoFugitiveDF
    ])
    fullSummaryEmissionDF = fullSummaryEmissionDF.assign(confidenceLevel=confidenceLevel)

    c2c1DF = calculateC2C1Ratios(fullSummaryEmissionDF, confidenceLevel)
    if not c2c1DF.empty:
        fullSummaryEmissionDF = pd.concat([fullSummaryEmissionDF, c2c1DF])
    fullSummaryEmissionDF = fullSummaryEmissionDF.assign(simDurationDays=simDurationDays)

    _saveSummaryDS(config, fullSummaryEmissionDF, 'SiteSummary')

    eventFugEventPieces = [p['eventFugitive'][0] for p in validPieces]
    eventFugSitePieces = [p['eventFugitive'][1] for p in validPieces]
    eventNoFugEventPieces = [p['eventNoFugitive'][0] for p in validPieces]
    eventNoFugSitePieces = [p['eventNoFugitive'][1] for p in validPieces]

    eventSummaryFugitiveDF = mergeEventSummaryByRun(eventFugEventPieces, eventFugSitePieces, mcIterations, 'eventSummary').assign(includeFugitive=True)
    eventSummaryNoFugitiveDF = mergeEventSummaryByRun(eventNoFugEventPieces, eventNoFugSitePieces, mcIterations, 'eventSummary').assign(includeFugitive=False)

    fullEventSummaryDF = pd.concat([eventSummaryFugitiveDF, eventSummaryNoFugitiveDF])
    fullEventSummaryDF = fullEventSummaryDF.assign(simDurationDays=simDurationDays)

    _saveSummaryDS(config, fullEventSummaryDF, 'EventSummary')

def _filterAndPivot(inDF, CICategory, mcIterations, pivotField=None):
    # Implements issue #27: for each MC run, sum values across all sites to produce
    # a distribution of cross-site run totals, then compute all statistics from that
    # distribution. This ensures mean <= max and CI bounds are meaningful.
    #
    # Note: SiteSummary `readings` lists are not zero-filled (see SummarySchema.md
    # "CI bounds and readings are not zero-filled"). When a site has zero emissions
    # for a given group in some MC runs, its readings list is shorter than
    # mcIterations. The mcIdx assigned here is a positional index within each list,
    # not the actual mcRun number, so the cross-site sums are approximate when any
    # site has absent MC-run entries. In practice this affects only low-prevalence
    # groups; the improvement over the previous implementation (which computed stats
    # across per-site means rather than per-run totals) is large.
    confidenceLevel = 95
    alpha = 100 - float(confidenceLevel)

    if pivotField is None:
        pivotField = CICategory

    filteredDF = inDF[inDF['CICategory'] == CICategory]
    groupCols = ['species', 'units', 'includeFugitive'] if pivotField == 'simulation' else ['species', pivotField, 'units', 'includeFugitive']

    with Timer(CICategory) as t0:
        # Explode per-site readings to one row per (original_row, MC-run-index).
        # explode() preserves the original DataFrame index for all elements of each
        # list, so groupby(level=0).cumcount() gives the position within each row
        # (0 = first MC run, 1 = second, ...) without needing an explicit mcRun col.
        explodedDF = filteredDF[groupCols + ['readings']].explode('readings')
        explodedDF = explodedDF.assign(
            readings=explodedDF['readings'].astype(float),
            mcIdx=explodedDF.groupby(level=0).cumcount()
        )

        # Sum across sites for each (group, mcIdx) → distribution of cross-site run totals.
        runTotalsDF = (
            explodedDF
            .groupby(groupCols + ['mcIdx'], as_index=False)['readings']
            .sum()
        )

        # Compute statistics from the distribution of cross-site run totals. Quantiles are
        # computed via one vectorized groupby().quantile() call covering every group at once,
        # instead of 4 separate per-group Python-UDF np.percentile() calls -- same interpolation
        # method (linear, both functions' default), so results are numerically identical.
        aggDF = (
            runTotalsDF
            .groupby(groupCols)
            .agg(
                total=('readings', 'sum'),
                mean=('readings', lambda x: x.sum() / mcIterations),
                min=('readings', 'min'),
                max=('readings', 'max'),
                readings=('readings', list)
            )
        )
        qLowerCI, qUpperCI = alpha / 200, 1 - alpha / 200
        if runTotalsDF.empty:
            # groupby(...).quantile() on zero rows produces no columns at all to unstack
            # (unlike .agg(), which keeps its named columns regardless of row count) --
            # build the same empty-but-correctly-shaped frame directly instead.
            quantilesDF = pd.DataFrame(
                columns=['lowerQuartile', 'upperQuartile', 'lowerCI', 'upperCI'],
                index=aggDF.index,
            )
        else:
            quantilesDF = (
                runTotalsDF
                .groupby(groupCols)['readings']
                .quantile([0.25, 0.75, qLowerCI, qUpperCI])
                .unstack()
                .rename(columns={0.25: 'lowerQuartile', 0.75: 'upperQuartile',
                                  qLowerCI: 'lowerCI', qUpperCI: 'upperCI'})
            )
        summaryDF = aggDF.join(quantilesDF).reset_index()[
            groupCols + ['total', 'mean', 'min', 'max',
                          'lowerQuartile', 'upperQuartile', 'lowerCI', 'upperCI', 'readings']
        ]
        summaryDF = summaryDF.assign(
            count=mcIterations,
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
        lowerQuartile=[np.nan] * n,
        upperQuartile=[np.nan] * n,
        lowerCI=[np.nan] * n,
        upperCI=[np.nan] * n,
        readings=[[] for _ in range(n)],
        CICategory=CICategory,
    )
    return retDF


def _readSummaryAcrossSites(config, dirsKey, fallbackKey, **readKwargs):
    """Read and concatenate a per-site Summary dataset (e.g. SiteSummary, PDF)
    across every site in the simulation.

    ``dirsKey`` holds the list of per-site dataset directories threaded onto the
    simSummary workitem by ``generateWorkitems``. When absent (single-study runs,
    or direct callers that did not populate it) this falls back to the single
    configured ``fallbackKey`` path. Each per-site directory is hive-partitioned
    by ``site``, so the column is reconstructed on read and preserved by concat.
    """
    dirs = config.get(dirsKey) or [config[fallbackKey]]
    frames = [pd.read_parquet(d, **readKwargs) for d in dirs]
    return pd.concat(frames, ignore_index=True)

def createSimPDF(config):
    logger.info("Creating simulation-level PDF (mixture approach)")

    siteList = _readSummaryAcrossSites(config, 'allSitePDFDirs', 'parquetNewPDF',
                                       columns=['site'])['site'].unique().tolist()
    if not siteList:
        logger.info("No PDF data, skipping SimPDF")
        return
    logger.info(f"SimPDF mixture: {len(siteList)} sites")

    allPDFRowsList = []
    for siteCacheLevel, simCacheLevel, simGroupCols in SIM_PDF_LEVEL_MAP:
        logger.info(f"SimPDF mixture: {siteCacheLevel} -> {simCacheLevel}")
        sitePDFDF = _readSummaryAcrossSites(config, 'allSitePDFDirs', 'parquetNewPDF',
                                            filters=[('CICategory', '=', siteCacheLevel)])
        if sitePDFDF.empty:
            continue

        identityGroupCols = [*simGroupCols, 'includeFugitive']
        for groupKey, groupDF in sitePDFDF.groupby(identityGroupCols):
            identityCols = dict(zip(identityGroupCols, groupKey))
            nComponents = groupDF.groupby(['site', 'operator', 'psno']).ngroups
            scaledDF = groupDF.assign(probability=groupDF['probability'] / nComponents)
            mixtureDF = (scaledDF
                         .groupby('emissionRate_kgPerH', as_index=False)['probability']
                         .sum()
                         .sort_values('emissionRate_kgPerH'))
            mixtureDF = mixtureDF.assign(cumulativeProbability=mixtureDF['probability'].cumsum())
            n = len(mixtureDF)
            allPDFRowsList.append(pd.DataFrame({
                **{col: [val] * n for col, val in identityCols.items()},
                'CICategory': [simCacheLevel] * n,
                'emissionRate_kgPerH': mixtureDF['emissionRate_kgPerH'].values,
                'probability': mixtureDF['probability'].values,
                'cumulativeProbability': mixtureDF['cumulativeProbability'].values,
            }))

    if not allPDFRowsList:
        logger.info("No SimPDF rows, skipping")
        return

    pdfDF = pd.concat(allPDFRowsList, ignore_index=True)
    _saveSummaryDS(config, pdfDF, 'SimPDF')
    logger.info(f"SimPDF: {len(pdfDF)} rows")

def summarizeSimulation(config):
    # this method depends on site-level simulations (aka 'summarize' function) being performed prior to this call.
    summaryDirs = config.get('allSiteSummaryDirs') or [config['parquetNewSummary']]
    logger.info(f"summarizeSimulation: aggregating {len(summaryDirs)} site summary dir(s)")
    with Timer("Read summaries") as t0:
        logging.info("Read summary parquet files")
        fullSummaryDF = _readSummaryAcrossSites(config, 'allSiteSummaryDirs', 'parquetNewSummary')
        t0.setCount(len(fullSummaryDF))

    mcIterations = config['monteCarloIterations']
    # Exclude per-site C2/C1 ratios before aggregating; recompute from aggregated METHANE/ETHANE totals below.
    nonRatioDF = fullSummaryDF[fullSummaryDF['species'] != 'C2/C1']

    mecSimSummaryDF = _filterAndPivot(nonRatioDF, 'modelEmissionCategory', mcIterations)
    readableNameSummaryDF = _filterAndPivot(nonRatioDF, 'modelReadableName', mcIterations)
    unitIDSummaryDF = _filterAndPivot(nonRatioDF, 'unitID', mcIterations)
    METypeSummaryDF = _filterAndPivot(nonRatioDF, 'METype', mcIterations)
    pneumaticsDF = _filterAndPivot(nonRatioDF, 'pneumatic', mcIterations, pivotField='METype')
    # Issue #77: the 'modelEmissionCategory' CICategory tag is shared by three
    # full-total aggregation levels emitted by calculateAnnualSummaries — the
    # per-category detail, the category-dropped rollup (NaN category), and the
    # COMBINED total row. With pivotField='simulation' the group key omits the
    # category column, so none of them are filtered out and all three are summed,
    # triple-counting emissions. The COMBINED rows alone are the per-site totals;
    # restrict to them before rolling up across sites.
    combinedSiteTotalsDF = nonRatioDF[nonRatioDF['modelEmissionCategory'] == 'COMBINED']
    siteSummaryDF = _filterAndPivot(combinedSiteTotalsDF, 'modelEmissionCategory', mcIterations, pivotField='simulation')
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

    if config.get('noPDF'):
        logger.info("noPDF set: skipping SimPDF generation")
    else:
        createSimPDF(config)

    pass
