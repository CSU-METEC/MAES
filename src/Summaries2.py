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
from collections import defaultdict

from ParquetLib import SUMMARY_DS
from Timer import Timer
import Units as u
from pathlib import Path

logger = logging.getLogger(__name__)


def _read_parquet_site(path, site_name):
    """Read all rows for a given site from a hive-partitioned parquet dataset.

    Uses auto-detected hive partitioning so the reader handles any partition depth
    (e.g. site=X/ or site=X/mcRun=Y/) without a hardcoded schema.
    """
    dataset = _ds.dataset(str(path), format='parquet', partitioning='hive')
    table = dataset.to_table(filter=_ds.field('site') == str(site_name))
    return table.to_pandas()


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
EMISSION_SUMMARY_GROUP_COLS = [*SUMMARY_KEY_COLS, 'METype', 'unitID', 'modelReadableName']
EVENT_EMITTER_GROUP_COLS = [*SUMMARY_KEY_COLS, 'unitID', 'modelReadableName']

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
                    'facilityID': 'facilityID',
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

    zeroCount = (retDF['emission_kgPerS'] == 0).sum()
    if zeroCount > 0:
        logger.info(f"_createEmissionDF: {zeroCount}/{len(retDF)} events have zero emission_kgPerS ({100*zeroCount/len(retDF):.1f}%)")

    return retDF

DATASET_PARAMS = {
    'InstEmissions': {'configKey': 'parquetNewInstEmissions', 'partition_cols': ['site', 'mcRun']},
    'SiteSummary':   {'configKey': 'parquetNewSummary',       'partition_cols': ['site']},
    'EventSummary':  {'configKey': 'parquetNewEventSummary',  'partition_cols': ['site']},
    'SimSummary':    {'configKey': 'parquetNewSimSummary',    'partition_cols': []},
    'PDF':           {'configKey': 'parquetNewPDF',           'partition_cols': ['site']},
    'PDFCache':      {'configKey': 'parquetNewPDFCache',      'partition_cols': ['site']},
    'SimPDF':        {'configKey': 'parquetNewSimPDF',        'partition_cols': []},
}

def _saveSummaryDS(config, df, dataset):
    params = DATASET_PARAMS[dataset]
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

def _aggregateEmittersByRun(instEmissionDF: pd.DataFrame, simDurationDays: float) -> pd.DataFrame:
    """Aggregate emission events to per-emitter-per-mc-run totals in kg/year.

    Returns a small DataFrame (one row per emitter group per mc run) suitable for
    incremental accumulation across mc runs before cross-mc statistics are computed.
    """
    df = instEmissionDF.assign(
        emissions_kgPerYear=instEmissionDF['totalEmission_kg'] / simDurationDays * u.DAYS_PER_YEAR
    )
    ret = (
        df.groupby(
            ['site', 'mcRun', 'species', 'emitterID', 'operator', 'psno',
             'METype', 'unitID', 'modelReadableName', 'modelEmissionCategory'],
            as_index=False,
        )
        .agg(emissions_kgPerYear=('emissions_kgPerYear', 'sum'),
             count=('emissions_kgPerYear', 'count'))
    )
    return ret


def calculateAnnualSummaries(aggregatedEmissionsByEmitterID: pd.DataFrame, aggColumnList: dict, mcIterations: int) -> pd.DataFrame:
    """Compute cross-MC annual emission statistics from per-emitter-per-run totals.

    Expects aggregatedEmissionsByEmitterID from _aggregateEmittersByRun (one row per
    emitter group per mc run), accumulated across all mc runs before this call.
    """
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


def _convertEmissionAccToNumpy(emissionAcc: dict) -> None:
    """Convert all Python list values in emissionAcc to numpy arrays in-place.

    Pops each list before creating the numpy array so peak memory is bounded to
    (remaining lists) + (one new array), not 2× total.
    """
    for key in list(emissionAcc.keys()):
        emissionAcc[key] = np.array(emissionAcc.pop(key))


def _convertEventAccToNumpy(eventAcc: dict) -> None:
    """Convert all Python list fields in each event accumulator entry to numpy arrays in-place.

    Same pop-before-convert strategy as _convertEmissionAccToNumpy.
    """
    for key in list(eventAcc.keys()):
        entry = eventAcc.pop(key)
        eventAcc[key] = {
            'duration_s':        np.array(entry['duration_s']),
            'totalEmission_kg':  np.array(entry['totalEmission_kg']),
            'emission_kgPerS':   np.array(entry['emission_kgPerS']),
        }


def _accumulateEmissionData(instEmissionDF: pd.DataFrame, emissionAcc: dict) -> None:
    """Extend per-group emission_kgPerH lists in emissionAcc from one mc run's events.

    instEmissionDF must already have zero-emission rows removed. emissionAcc is a
    defaultdict(list) keyed by EMISSION_SUMMARY_GROUP_COLS tuples.
    """
    df = instEmissionDF.assign(emission_kgPerH=instEmissionDF['emission_kgPerS'] * u.SECONDS_PER_HOUR)
    for key, grp in df.groupby(EMISSION_SUMMARY_GROUP_COLS):
        emissionAcc[key].extend(grp['emission_kgPerH'].tolist())


def _finalizeEmissionSummary(emissionAcc: dict, mcIterations: int) -> pd.DataFrame:
    """Produce the same DataFrame as calculateEmissionSummary from accumulated per-group lists.

    emissionAcc must be fully populated (all mc runs accumulated) before calling.
    Returns an empty DataFrame if emissionAcc is empty.
    """
    if not emissionAcc:
        ret = pd.DataFrame()
        return ret
    CI = 95
    alpha = 100 - float(CI)
    rows = []
    for key, values in emissionAcc.items():
        vals = np.asarray(values)
        row = dict(zip(EMISSION_SUMMARY_GROUP_COLS, key))
        row.update({
            'total':          float(vals.sum()),
            'count':          len(vals),
            'mean':           float(vals.mean()),
            'min':            float(vals.min()),
            'max':            float(vals.max()),
            'lowerQuartile':  float(np.percentile(vals, 25)),
            'upperQuartile':  float(np.percentile(vals, 75)),
            'lowerCI':        float(np.percentile(vals, alpha / 2)),
            'upperCI':        float(np.percentile(vals, 100 - alpha / 2)),
            'readings':       values,
            'CICategory':     'instantEmissionsByModelReadableName',
            'units':          KG_PER_HOUR_UNITS_NAME,
            'mcRun':          float(mcIterations),
            'rawCount':       len(vals),
            'rawMean':        float(vals.mean()),
        })
        rows.append(row)
    ret = pd.DataFrame(rows)
    return ret


def _accumulateEventData(instEmissionDF: pd.DataFrame, emitterAcc: dict, siteAcc: dict) -> None:
    """Extend per-group event field lists in emitterAcc and siteAcc from one mc run's events.

    instEmissionDF must already have zero-emission rows removed. emitterAcc is keyed by
    EVENT_EMITTER_GROUP_COLS tuples; siteAcc by SUMMARY_KEY_COLS tuples. Both are
    defaultdict(lambda: {'duration_s': [], 'totalEmission_kg': [], 'emission_kgPerS': []}).
    """
    for key, grp in instEmissionDF.groupby(EVENT_EMITTER_GROUP_COLS):
        entry = emitterAcc[key]
        entry['duration_s'].extend(grp['duration_s'].tolist())
        entry['totalEmission_kg'].extend(grp['totalEmission_kg'].tolist())
        entry['emission_kgPerS'].extend(grp['emission_kgPerS'].tolist())
    for key, grp in instEmissionDF.groupby(SUMMARY_KEY_COLS):
        entry = siteAcc[key]
        entry['duration_s'].extend(grp['duration_s'].tolist())
        entry['totalEmission_kg'].extend(grp['totalEmission_kg'].tolist())
        entry['emission_kgPerS'].extend(grp['emission_kgPerS'].tolist())


def _buildEventSummaryLevel(acc: dict, groupCols: list, mcIterations: int) -> pd.DataFrame:
    """Build eventSummary + eventSummary_kgPerh rows from one level of accumulated event data.

    Produces the same two-unit-variant output that calculateEventSummary generates for
    a single groupby level. Returns an empty DataFrame if acc is empty.
    """
    if not acc:
        ret = pd.DataFrame()
        return ret
    rows = []
    for key, fields in acc.items():
        emCount = len(fields['emission_kgPerS'])
        totalEmission = float(sum(fields['totalEmission_kg']))
        totalDuration = float(sum(fields['duration_s']))
        row = dict(zip(groupCols, key))
        row.update({
            'eventCount':            emCount,
            'totalEmission_kg':      totalEmission,
            'totalEventDuration_s':  totalDuration,
            'meanEventDuration_s':   float(np.mean(fields['duration_s'])),
            'simpleMean':            float(np.mean(fields['emission_kgPerS'])),
            'durationEvents':        fields['duration_s'],
            'totalEmissionEvents':   fields['totalEmission_kg'],
            'CICategory':            'eventSummary',
            'mcRuns':                mcIterations,
            'emissionRateUnits':     'kg/s',
            'eventsPerMCRun':        emCount / mcIterations,
            'meanEmissionRate':      totalEmission / totalDuration,
        })
        rows.append(row)
    kgPerS = pd.DataFrame(rows)
    kgPerH = kgPerS.assign(
        meanEmissionRate=kgPerS['meanEmissionRate'] * u.SECONDS_PER_HOUR,
        simpleMean=kgPerS['simpleMean'] * u.SECONDS_PER_HOUR,
        emissionRateUnits='kg/h',
    )
    ret = pd.concat([kgPerS, kgPerH], ignore_index=True)
    return ret


def _finalizeEventSummary(emitterAcc: dict, siteAcc: dict, mcIterations: int) -> pd.DataFrame:
    """Produce the same DataFrame as calculateEventSummary from accumulated per-group lists.

    Combines emitter-level (EVENT_EMITTER_GROUP_COLS) and site-level (SUMMARY_KEY_COLS)
    rows in both kg/s and kg/h variants, matching the output of calculateEventSummary.
    """
    emitterDF = _buildEventSummaryLevel(emitterAcc, EVENT_EMITTER_GROUP_COLS, mcIterations)
    siteDF = _buildEventSummaryLevel(siteAcc, SUMMARY_KEY_COLS, mcIterations)
    ret = pd.concat([emitterDF, siteDF], ignore_index=True)
    return ret


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
    aggGroupCols = [*groupCols, 'modelEmissionCategory', 'mcRun']
    rowsList = []
    for groupKey, groupDF in fineDF.groupby(aggGroupCols):
        catTSList = []
        for _, subDF in groupDF.groupby(['facilityID', *CACHE_IDENTITY_COLS]):
            catTSList.append(_cacheGroupToTimeseriesRLE(subDF))
        summedTS = ts.TimeseriesSet(catTSList).sum()
        if summedTS.isempty():
            continue
        identityDict = dict(zip(aggGroupCols, groupKey))
        n = len(summedTS.df)
        rowsList.append(pd.DataFrame({
            **{col: [val] * n for col, val in identityDict.items()},
            'startTime_s': summedTS.df[summedTS.startTimeColName].values,
            'endTime_s': summedTS.df[summedTS.endTimeColName].values,
            'emission_kgPerH': _roundForPDF(summedTS.df[summedTS.valueColName].values),
            'cacheLevel': [levelName] * n,
        }))
    return pd.concat(rowsList, ignore_index=True) if rowsList else pd.DataFrame()

def createPDFCache(config):
    logger.info(f"Creating PDF cache for site {config['siteName']}")
    with Timer("Read InstEmissions") as t0:
        instEmissionDF = _read_parquet_site(config['parquetNewInstEmissions'], config['siteName'])
        if instEmissionDF.empty:
            logger.info(f"No InstEmissions data for site {config['siteName']}, skipping PDF cache")
            return pd.DataFrame()
        t0.setCount(len(instEmissionDF))

    instEmissionDF = _removeZeroEmissionEvents(instEmissionDF)
    groupCols = ['facilityID', *CACHE_IDENTITY_COLS, 'mcRun']
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

    with Timer("Build PDFs") as tPDF:
        fullPDFDF, noFugPDFDF, pdfStatsDF = calculatePDFSummaryFromCache(cacheDF)
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

def _buildPDFForGroup(groupDF, identityCols, CICategory):
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
    totalCount = pdf.data['count'].sum()
    pdfRows = pd.DataFrame({
        **{col: [val] * n for col, val in identityCols.items()},
        'CICategory': [CICategory] * n,
        'emissionRate_kgPerH': cdf.data['value'].values,
        'probability': (pdf.data['count'] / totalCount).values,
        'cumulativeProbability': cdf.data['cumulative_probability'].values,
    })
    return pdfRows, stats

def calculatePDFSummary(instEmissionDF):
    instEmissionDF = _removeZeroEmissionEvents(instEmissionDF)
    resultDFList = []
    statsList = []
    for CICategory, groupCols in PDF_GROUPINGS:
        for _, groupDF in instEmissionDF.groupby(groupCols):
            identityCols = {col: groupDF[col].iloc[0] for col in groupCols}
            pdfRows, stats = _buildPDFForGroup(groupDF, identityCols, CICategory)
            statsList.append(stats)
            if pdfRows is not None:
                resultDFList.append(pdfRows)
    pdfDF = pd.concat(resultDFList) if resultDFList else pd.DataFrame()
    statsDF = pd.DataFrame(statsList) if statsList else pd.DataFrame()
    return pdfDF, statsDF

VALIDATE_QUANTILES = [0.1, 0.25, 0.5, 0.75, 0.9, 0.95]

def _makePDFRows(mcRunTSList, identityCols, CICategory):
    if not mcRunTSList:
        return None
    pdf = ts.TimeseriesSet(mcRunTSList).toPDF()
    cdf = pdf.toCDF()
    if cdf.isempty():
        return None
    n = len(cdf.data)
    totalCount = pdf.data['count'].sum()
    return pd.DataFrame({
        **{col: [val] * n for col, val in identityCols.items()},
        'CICategory': [CICategory] * n,
        'emissionRate_kgPerH': cdf.data['value'].values,
        'probability': (pdf.data['count'] / totalCount).values,
        'cumulativeProbability': cdf.data['cumulative_probability'].values,
    })

def _buildPDFForGroupFromCache(groupDF, identityCols, CICategory):
    fullMCRunTSList = []
    noFugMCRunTSList = []
    with Timer("build MC run timeseries from coarse cache", loglevel=logging.DEBUG) as t:
        for _, mcRunDF in groupDF.groupby('mcRun'):
            catTSDict = {}
            for emCat, catDF in mcRunDF.groupby('modelEmissionCategory'):
                if 'facilityID' in catDF.columns and catDF['facilityID'].notna().any():
                    facilityRLEs = [_cacheGroupToTimeseriesRLE(fdf) for _, fdf in catDF.groupby('facilityID')]
                    catTSDict[emCat] = ts.TimeseriesSet(facilityRLEs).sum()
                else:
                    catTSDict[emCat] = _cacheGroupToTimeseriesRLE(catDF)
            fullTS = ts.TimeseriesSet(list(catTSDict.values())).sum()
            noFugItems = filter(lambda kv: kv[0] != 'FUGITIVE', catTSDict.items())
            noFugTS = ts.TimeseriesSet(list(map(lambda kv: kv[1], noFugItems))).sum()
            if not fullTS.isempty():
                fullTS.df = fullTS.df.assign(**{fullTS.valueColName: _roundForPDF(fullTS.df[fullTS.valueColName].values)})
                fullMCRunTSList.append(fullTS)
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
    return _makePDFRows(fullMCRunTSList, identityCols, CICategory), _makePDFRows(noFugMCRunTSList, identityCols, CICategory), stats

def calculatePDFSummaryFromCache(cacheDF, groupings=None):
    if groupings is None:
        groupings = PDF_GROUPINGS
    fullResultDFList = []
    noFugResultDFList = []
    statsList = []
    for CICategory, groupCols in groupings:
        levelDF = cacheDF[cacheDF['cacheLevel'] == CICategory]
        for _, groupDF in levelDF.groupby(groupCols):
            identityCols = {col: groupDF[col].iloc[0] for col in groupCols}
            fullPDFRows, noFugPDFRows, stats = _buildPDFForGroupFromCache(groupDF, identityCols, CICategory)
            statsList.append(stats)
            if fullPDFRows is not None:
                fullResultDFList.append(fullPDFRows)
            if noFugPDFRows is not None:
                noFugResultDFList.append(noFugPDFRows)
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
    groupCols = ['facilityID', *CACHE_IDENTITY_COLS, 'mcRun']
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
    pdfFromRaw, _ = calculatePDFSummary(instEmissionDF)
    pdfFromCache, _, _ = calculatePDFSummaryFromCache(cacheDF)

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

def summarizeSingleSite(config, instEmissionDF=None, emitterTotals=None,
                         prebuiltEmissionSummaryAll=None, prebuiltEmissionSummaryNoFugitive=None,
                         prebuiltEventSummaryAll=None, prebuiltEventSummaryNoFugitive=None):
    """Compute and write all site-level summary datasets.

    instEmissionDF may be raw event data (when emitterTotals is None) or already
    column-reduced (when emitterTotals is provided and the pre-built summary params are
    None). When all four pre-built params are provided, instEmissionDF is unused —
    calculateEmissionSummary and calculateEventSummary are skipped entirely, eliminating
    the need to hold the full cross-run event table in memory.
    """
    CONFIDENCE_LEVEL = 95
    AGG_FIELDS = {
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
    alpha = 100 - float(CONFIDENCE_LEVEL)

    mcIterations = config['monteCarloIterations']
    with Timer("summarize") as t0:
        simDurationDays = config['simDurationDays']
        if emitterTotals is None:
            instEmissionDF = _createEmissionDF(instEmissionDF)
            _saveSummaryDS(config, instEmissionDF, 'InstEmissions')
            emitterTotals = _aggregateEmittersByRun(instEmissionDF, simDurationDays)
        instEmissionNoFugitiveDF = None
        if instEmissionDF is not None:
            instEmissionNoFugitiveDF = instEmissionDF[instEmissionDF['modelEmissionCategory'] != 'FUGITIVE']
        emitterTotalsNoFugitive = emitterTotals[emitterTotals['modelEmissionCategory'] != 'FUGITIVE']

        additionalConversions = [
            # {'colName': 'emissions_kgPerYear', 'units': KG_PER_YEAR_UNITS_NAME,          'conversion': _convertKGPerYear2KGPerYear},
            {'colName': 'emissions_kgPerYear', 'units': US_TONS_PER_YEAR_UNITS_NAME,     'conversion': _convertKGPerYear2USTonsPerYear},
            {'colName': 'emissions_kgPerYear', 'units': METRIC_TONS_PER_YEAR_UNITS_NAME, 'conversion': _convertKGPerYear2MetricTonsPerYear},
        ]

        with Timer("calculate annual summaries") as t0:
            summaryEmissionFugitiveDF = calculateAnnualSummaries(emitterTotals, AGG_FIELDS, mcIterations)
            summaryEmissionFugitiveDF = summaryEmissionFugitiveDF.assign(includeFugitive=True)
            summaryEmissionNoFugitiveDF = calculateAnnualSummaries(emitterTotalsNoFugitive, AGG_FIELDS, mcIterations)
            summaryEmissionNoFugitiveDF = summaryEmissionNoFugitiveDF.assign(includeFugitive=False)
            t0.setCount(len(summaryEmissionFugitiveDF) + len(summaryEmissionNoFugitiveDF))

        logging.info("Before apply additional conversions")

        with Timer("apply additional conversions") as t1:
            fullSummaryEmissionFugitiveDF = applyConversions(summaryEmissionFugitiveDF, additionalConversions, AGG_FIELDS)
            fullSummaryEmissionNoFugitiveDF = applyConversions(summaryEmissionNoFugitiveDF, additionalConversions, AGG_FIELDS)
            t1.setCount(len(fullSummaryEmissionFugitiveDF) + len(fullSummaryEmissionNoFugitiveDF))

        logging.info("Before special summaries")

        with Timer("special summaries") as t2:
            if prebuiltEmissionSummaryAll is not None:
                emissionSummaryFugitiveDF = prebuiltEmissionSummaryAll.assign(includeFugitive=True)
                emissionSummaryNoFugitiveDF = prebuiltEmissionSummaryNoFugitive.assign(includeFugitive=False)
            else:
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
            if prebuiltEventSummaryAll is not None:
                eventSummaryFugitiveDF = prebuiltEventSummaryAll.assign(includeFugitive=True)
                eventSummaryNoFugitiveDF = prebuiltEventSummaryNoFugitive.assign(includeFugitive=False)
            else:
                eventSummaryFugitiveDF = calculateEventSummary(instEmissionDF, simDurationDays, mcIterations, 'eventSummary')
                eventSummaryFugitiveDF = eventSummaryFugitiveDF.assign(includeFugitive=True)
                eventSummaryNoFugitiveDF = calculateEventSummary(instEmissionNoFugitiveDF, simDurationDays, mcIterations, 'eventSummary')
                eventSummaryNoFugitiveDF = eventSummaryNoFugitiveDF.assign(includeFugitive=False)

            fullEventSummaryDF = pd.concat([eventSummaryFugitiveDF, eventSummaryNoFugitiveDF])
            fullEventSummaryDF = fullEventSummaryDF.assign(simDurationDays=simDurationDays)
            t3.setCount(len(fullEventSummaryDF))

        _saveSummaryDS(config, fullEventSummaryDF, 'EventSummary')

    pass

def _newEventAccDict():
    return {'duration_s': [], 'totalEmission_kg': [], 'emission_kgPerS': []}


def computePartialAccumulator(config, mergedEmissionDF):
    """Compute a per-mc-run partial accumulator from an in-memory merged emission event DF.

    mergedEmissionDF must be the output of ParquetLib.buildMergedEmissionDF — EMISSION events
    already merged with GC, timeseries, and metadata. Writes the InstEmissions parquet partition
    for this mc run and returns a partial accumulator dict suitable for merging across all mc
    runs via finalizeAccumulators. Returns None when mergedEmissionDF is None or empty.
    """
    if mergedEmissionDF is None or mergedEmissionDF.empty:
        return None

    simDurationDays = config['simDurationDays']
    instEmissionDF = _createEmissionDF(mergedEmissionDF)
    _saveSummaryDS(config, instEmissionDF, 'InstEmissions')
    emitterTotalsDF = _aggregateEmittersByRun(instEmissionDF, simDurationDays)

    nonZeroDF = _removeZeroEmissionEvents(instEmissionDF)
    noFugitiveDF = nonZeroDF[nonZeroDF['modelEmissionCategory'] != 'FUGITIVE']

    emissionAccAll        = defaultdict(list)
    emissionAccNoFugitive = defaultdict(list)
    eventEmitterAccAll        = defaultdict(_newEventAccDict)
    eventEmitterAccNoFugitive = defaultdict(_newEventAccDict)
    eventSiteAccAll           = defaultdict(_newEventAccDict)
    eventSiteAccNoFugitive    = defaultdict(_newEventAccDict)

    _accumulateEmissionData(nonZeroDF,    emissionAccAll)
    _accumulateEmissionData(noFugitiveDF, emissionAccNoFugitive)
    _accumulateEventData(nonZeroDF,    eventEmitterAccAll,        eventSiteAccAll)
    _accumulateEventData(noFugitiveDF, eventEmitterAccNoFugitive, eventSiteAccNoFugitive)

    return {
        'site':                     config['siteName'],
        'emitterTotalsDF':          emitterTotalsDF,
        'emissionAccAll':           emissionAccAll,
        'emissionAccNoFugitive':    emissionAccNoFugitive,
        'eventEmitterAccAll':       eventEmitterAccAll,
        'eventEmitterAccNoFugitive': eventEmitterAccNoFugitive,
        'eventSiteAccAll':          eventSiteAccAll,
        'eventSiteAccNoFugitive':   eventSiteAccNoFugitive,
    }


def finalizeAccumulators(config, partials):
    """Merge partial accumulators from all mc runs for one site and write summary datasets.

    partials is a list of dicts returned by computePartialAccumulator, one per mc run.
    Merges the per-run accumulators, converts to numpy, finalizes summary DataFrames, and
    calls summarizeSingleSite to write SiteSummary and EventSummary parquet.
    """
    nonNullPartials = list(filter(lambda p: p is not None, partials))
    if not nonNullPartials:
        return

    numMCRuns = int(config['monteCarloIterations'])

    allEmitterTotals = pd.concat(
        list(map(lambda p: p['emitterTotalsDF'], nonNullPartials)),
        ignore_index=True,
    )

    emissionAccAll        = defaultdict(list)
    emissionAccNoFugitive = defaultdict(list)
    eventEmitterAccAll        = defaultdict(_newEventAccDict)
    eventEmitterAccNoFugitive = defaultdict(_newEventAccDict)
    eventSiteAccAll           = defaultdict(_newEventAccDict)
    eventSiteAccNoFugitive    = defaultdict(_newEventAccDict)

    for p in nonNullPartials:
        for key, vals in p['emissionAccAll'].items():
            emissionAccAll[key].extend(vals)
        for key, vals in p['emissionAccNoFugitive'].items():
            emissionAccNoFugitive[key].extend(vals)
        for merged, partial in [
            (eventEmitterAccAll,        p['eventEmitterAccAll']),
            (eventEmitterAccNoFugitive, p['eventEmitterAccNoFugitive']),
            (eventSiteAccAll,           p['eventSiteAccAll']),
            (eventSiteAccNoFugitive,    p['eventSiteAccNoFugitive']),
        ]:
            for key, fields in partial.items():
                entry = merged[key]
                entry['duration_s'].extend(fields['duration_s'])
                entry['totalEmission_kg'].extend(fields['totalEmission_kg'])
                entry['emission_kgPerS'].extend(fields['emission_kgPerS'])

    _convertEmissionAccToNumpy(emissionAccAll)
    _convertEmissionAccToNumpy(emissionAccNoFugitive)
    _convertEventAccToNumpy(eventEmitterAccAll)
    _convertEventAccToNumpy(eventEmitterAccNoFugitive)
    _convertEventAccToNumpy(eventSiteAccAll)
    _convertEventAccToNumpy(eventSiteAccNoFugitive)

    emissionSummaryAll        = _finalizeEmissionSummary(emissionAccAll,        numMCRuns)
    emissionSummaryNoFugitive = _finalizeEmissionSummary(emissionAccNoFugitive, numMCRuns)
    eventSummaryAll        = _finalizeEventSummary(eventEmitterAccAll,        eventSiteAccAll,        numMCRuns)
    eventSummaryNoFugitive = _finalizeEventSummary(eventEmitterAccNoFugitive, eventSiteAccNoFugitive, numMCRuns)

    summarizeSingleSite(
        config,
        emitterTotals=allEmitterTotals,
        prebuiltEmissionSummaryAll=emissionSummaryAll,
        prebuiltEmissionSummaryNoFugitive=emissionSummaryNoFugitive,
        prebuiltEventSummaryAll=eventSummaryAll,
        prebuiltEventSummaryNoFugitive=eventSummaryNoFugitive,
    )


def summarize(config):
    """Load parquet events one mc run at a time and compute site summary statistics.

    Iterates over mc runs individually to bound peak memory. Each run's events are
    loaded, column-reduced, written to InstEmissions parquet, and then reduced to
    accumulated per-group lists — the raw event DataFrame is freed after each run.
    No full cross-run concat is ever built; the accumulated lists are finalized into
    summary DataFrames after the loop and passed directly to summarizeSingleSite.
    """
    logger.info(f"Summarizing site {config['siteName']}")
    numMCRuns = int(config['monteCarloIterations'])
    simDurationDays = config['simDurationDays']
    site = config['siteName']

    perRunEmitterRows = []
    emissionAccAll        = defaultdict(list)
    emissionAccNoFugitive = defaultdict(list)
    eventEmitterAccAll        = defaultdict(lambda: {'duration_s': [], 'totalEmission_kg': [], 'emission_kgPerS': []})
    eventEmitterAccNoFugitive = defaultdict(lambda: {'duration_s': [], 'totalEmission_kg': [], 'emission_kgPerS': []})
    eventSiteAccAll        = defaultdict(lambda: {'duration_s': [], 'totalEmission_kg': [], 'emission_kgPerS': []})
    eventSiteAccNoFugitive = defaultdict(lambda: {'duration_s': [], 'totalEmission_kg': [], 'emission_kgPerS': []})

    for mcRun in range(numMCRuns):
        logger.info(f"Read Parquet Files: mcRun {mcRun + 1}/{numMCRuns}")
        eventDF = pl.readParquetEvents(
            config,
            site=site,
            mcRun=mcRun,
            mergeGC=True,
            species=SPECIES,
            additionalEventFilters=[('command', '=', 'EMISSION')],
        )
        if eventDF is None or eventDF.empty:
            continue
        instEmissionDF = _createEmissionDF(eventDF)
        _saveSummaryDS(config, instEmissionDF, 'InstEmissions')
        perRunEmitterRows.append(_aggregateEmittersByRun(instEmissionDF, simDurationDays))

        nonZeroDF    = _removeZeroEmissionEvents(instEmissionDF)
        noFugitiveDF = nonZeroDF[nonZeroDF['modelEmissionCategory'] != 'FUGITIVE']

        _accumulateEmissionData(nonZeroDF,    emissionAccAll)
        _accumulateEmissionData(noFugitiveDF, emissionAccNoFugitive)
        _accumulateEventData(nonZeroDF,    eventEmitterAccAll,        eventSiteAccAll)
        _accumulateEventData(noFugitiveDF, eventEmitterAccNoFugitive, eventSiteAccNoFugitive)

    if not perRunEmitterRows:
        return

    _convertEmissionAccToNumpy(emissionAccAll)
    _convertEmissionAccToNumpy(emissionAccNoFugitive)
    _convertEventAccToNumpy(eventEmitterAccAll)
    _convertEventAccToNumpy(eventEmitterAccNoFugitive)
    _convertEventAccToNumpy(eventSiteAccAll)
    _convertEventAccToNumpy(eventSiteAccNoFugitive)

    allEmitterTotals = pd.concat(perRunEmitterRows, ignore_index=True)

    emissionSummaryAll        = _finalizeEmissionSummary(emissionAccAll,        numMCRuns)
    emissionSummaryNoFugitive = _finalizeEmissionSummary(emissionAccNoFugitive, numMCRuns)
    eventSummaryAll        = _finalizeEventSummary(eventEmitterAccAll,        eventSiteAccAll,        numMCRuns)
    eventSummaryNoFugitive = _finalizeEventSummary(eventEmitterAccNoFugitive, eventSiteAccNoFugitive, numMCRuns)

    summarizeSingleSite(
        config,
        emitterTotals=allEmitterTotals,
        prebuiltEmissionSummaryAll=emissionSummaryAll,
        prebuiltEmissionSummaryNoFugitive=emissionSummaryNoFugitive,
        prebuiltEventSummaryAll=eventSummaryAll,
        prebuiltEventSummaryNoFugitive=eventSummaryNoFugitive,
    )

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

        # Compute statistics from the distribution of cross-site run totals.
        summaryDF = (
            runTotalsDF
            .groupby(groupCols)
            .agg(
                total=('readings', 'sum'),
                mean=('readings', lambda x: x.sum() / mcIterations),
                min=('readings', 'min'),
                max=('readings', 'max'),
                lowerQuartile=('readings', lambda x: np.percentile(x, 25)),
                upperQuartile=('readings', lambda x: np.percentile(x, 75)),
                lowerCI=('readings', lambda x: np.percentile(x, alpha / 2)),
                upperCI=('readings', lambda x: np.percentile(x, 100 - alpha / 2)),
                readings=('readings', list)
            )
            .reset_index()
        )
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


def createSimPDF(config):
    logger.info("Creating simulation-level PDF (mixture approach)")

    siteList = pd.read_parquet(config['parquetNewPDF'], columns=['site'])['site'].unique().tolist()
    if not siteList:
        logger.info("No PDF data, skipping SimPDF")
        return
    logger.info(f"SimPDF mixture: {len(siteList)} sites")

    allPDFRowsList = []
    for siteCacheLevel, simCacheLevel, simGroupCols in SIM_PDF_LEVEL_MAP:
        logger.info(f"SimPDF mixture: {siteCacheLevel} -> {simCacheLevel}")
        sitePDFDF = pd.read_parquet(config['parquetNewPDF'],
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

def computeSimSummary(config):
    """Compute cross-site simulation summary statistics and write SimSummary parquet.

    Reads SiteSummary parquet, aggregates per-MC-run cross-site totals for each emission
    grouping (modelEmissionCategory, modelReadableName, unitID, METype, pneumatics,
    simulation), computes C2/C1 ratios, and writes SimSummary. No PDF/CDF dependency.
    Depends on site-level summarize having been run first.
    """
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


def summarizeSimulation(config):
    """Run computeSimSummary then createSimPDF.

    Preserved for backward compatibility with external callers. New code should call
    computeSimSummary and createSimPDF directly as separate phases.
    """
    computeSimSummary(config)
    createSimPDF(config)
