import pandas as pd
import logging
from Timer import Timer
import SiteMain2 as sm
import AppUtils as au
import ParquetLib as pl
import json
from pathlib import Path
import datetime as dt
import numpy as np


ABS_EPSILON = 0.01   # absolute tolerance (mt/year or kg/h depending on context)
REL_EPSILON = 0.01   # 1% relative tolerance — both must be exceeded for a failure
KS_EPSILON = 0.05    # max CDF deviation (KS statistic) tolerated for PDF comparisons
REGENERATE_SUMMARIES = False

SUMMARY_LAYOUTS = {
    'AnnualEmissions': {
        'typeList': ['METype', 'modelReadableName', 'site'],
        'fileFormat': 'annualEmissions_by_{type}_abnormal_{abnormal}'
    },
    'InstantaneousEmissions': {
        'typeList': ['modelReadableName'],
        'fileFormat': 'instantEmissions_by_{type}_abnormal_{abnormal}'
    },
    'AvgEmissionRatesAndDurations': {
        'typeList': ['modelReadableName'],
        'fileFormat': 'avg_ER_and_duration_by_{type}_abnormal_{abnormal}'
    },
    'AggregatedSimulationEmissions': {
        'typeList': ['category', 'METype', 'modelReadableName', 'unitID'],
        'fileFormat': 'aggregated_sim_emissions_by_{type}_abnormal_{abnormal}',
        'simulationWide': True
    }
}
SUMMARY_FILE_TEMPLATE = "{simulationRoot}/summaries/{summaryType}/{siteDir}/{fname}.csv"

_IDENTITY_COLS = ['species', 'METype', 'unitID', 'modelReadableName', 'modelEmissionCategory']


def _readOldSummaries(config):
    simulationRoot = config['simulationRoot']

    ret = {}
    for singleSummary, summaryData in SUMMARY_LAYOUTS.items():
        for singleType in summaryData['typeList']:
            for abnormal in ['on', 'off']:
                if summaryData.get('simulationWide', False):
                    siteDir = ''
                else:
                    siteDir = f"site={config['siteName']}"
                thisFname = summaryData['fileFormat'].format(type=singleType, abnormal=abnormal)
                thisSummary = {
                    'simulationRoot': simulationRoot,
                    'summaryType': singleSummary,
                    'siteDir': siteDir,
                    'fname': thisFname
                }
                logging.debug(thisSummary)
                summaryFilename = SUMMARY_FILE_TEMPLATE.format(**thisSummary)
                logging.debug(f"  {summaryFilename}")
                summaryPath = Path(summaryFilename)
                if summaryPath.exists():
                    summaryDF = pd.read_csv(summaryFilename)
                    if singleType == 'site':
                        keyType = 'modelEmissionCategory'
                    else:
                        keyType = singleType
                    summaryKey = (singleSummary, keyType, abnormal)
                    ret[summaryKey] = summaryDF

    return ret


def _readNewSummaries(config):
    return pd.read_parquet(config['parquetNewSummary'], filters=[('site', '=', config['siteName'])])

def _readNewEventSummaries(config):
    return pd.read_parquet(config['parquetNewEventSummary'], filters=[('site', '=', config['siteName'])])

def _readNewSimulationSummary(config):
    return pd.read_parquet(config['parquetNewSimSummary'])


def filterFlaredGasMalfunction(oldSummaryDF, oldSummaryAbnormal, oldSummaryKey):
    if oldSummaryAbnormal != 'off':
        return oldSummaryDF, 0
    flaredGasMalfunctionMask = oldSummaryDF['modelReadableName'] == 'Flared Gas Malfunction'
    filteredDF = oldSummaryDF[~flaredGasMalfunctionMask]
    count = 0
    if flaredGasMalfunctionMask.any():
        count = int(flaredGasMalfunctionMask.sum())
        logging.warning(f"  flared gas malfunctions detected, {oldSummaryKey=}, out of {len(filteredDF)=} {count=}")
    return filteredDF, count


def _addDeltas(comparisonDF, oldMeanCol, newMeanCol):
    absoluteDelta = (comparisonDF[oldMeanCol] - comparisonDF[newMeanCol]).abs()
    relativeDelta = absoluteDelta / comparisonDF[oldMeanCol].abs().clip(lower=1e-9)
    return comparisonDF.assign(absoluteDelta=absoluteDelta, relativeDelta=relativeDelta)


def _buildDetailDF(comparisonDF, siteName, oldSummaryKey, oldMeanCol, newMeanCol):
    summaryType, by, abnormal = oldSummaryKey
    presentIdentityCols = [c for c in _IDENTITY_COLS if c in comparisonDF.columns]
    keepCols = [c for c in presentIdentityCols + ['absoluteDelta', 'relativeDelta', '_merge'] if c in comparisonDF.columns]

    detailDF = comparisonDF[keepCols].copy()
    detailDF = detailDF.assign(
        old_mean=comparisonDF[oldMeanCol] if oldMeanCol in comparisonDF.columns else np.nan,
        new_mean=comparisonDF[newMeanCol] if newMeanCol in comparisonDF.columns else np.nan,
        summaryType=summaryType,
        by=by,
        abnormal=abnormal,
        siteName=siteName
    )
    detailDF = detailDF.rename(columns={'_merge': 'mergeStatus'})

    if 'compCount' in comparisonDF.columns:
        detailDF = detailDF.assign(old_readingsLength=comparisonDF['compCount'])
    if 'count' in comparisonDF.columns:
        detailDF = detailDF.assign(new_count=comparisonDF['count'])

    return detailDF


def _doComparisons(comparisonDF, siteName, oldSummaryKey):
    summaryType, by, abnormal = oldSummaryKey
    tag = f"[{summaryType}/{by}/{abnormal}/{siteName}]"

    roMissing = (comparisonDF['_merge'] == 'right_only') & (comparisonDF['mean'] != 0.0)
    loMissing = (comparisonDF['_merge'] == 'left_only')
    missingItems = roMissing | loMissing
    missingItemCount = 0
    if missingItems.any():
        missingItemCount = int(missingItems.sum())
        logging.warning(f"  {tag} missing={missingItemCount} of {len(comparisonDF)}")

    roSpurious = (comparisonDF['_merge'] == 'right_only') & (comparisonDF['mean'] == 0.0)
    comparisonDF = comparisonDF[~roSpurious]

    comparisonDF = _addDeltas(comparisonDF, 'mean_emissions', 'mean')

    maxAbsoluteDelta = float(comparisonDF['absoluteDelta'].max(skipna=True)) if len(comparisonDF) > 0 else 0.0
    maxRelativeDelta = float(comparisonDF['relativeDelta'].max(skipna=True)) if len(comparisonDF) > 0 else 0.0

    outOfRange = (comparisonDF['absoluteDelta'] > ABS_EPSILON) & (comparisonDF['relativeDelta'] > REL_EPSILON)
    outOfRangeCount = int(outOfRange.sum())
    outOfRangeRelativeCount = int((comparisonDF['relativeDelta'] > REL_EPSILON).sum())
    if outOfRangeCount > 0:
        logging.warning(f"  {tag} out_of_range={outOfRangeCount} of {len(comparisonDF)} (maxAbs={maxAbsoluteDelta:.4f}, maxRel={maxRelativeDelta:.2%})")

    countsDifferCount = 0
    readingsLengthMismatchCount = 0
    if 'compCount' in comparisonDF.columns and 'count' in comparisonDF.columns:
        countsDiffer = comparisonDF['compCount'] != comparisonDF['count']
        countsDifferCount = int(countsDiffer.sum())
        readingsLengthMismatchCount = countsDifferCount
        if countsDifferCount > 0:
            logging.warning(f"  {tag} counts_differ={countsDifferCount} of {len(comparisonDF)}, readings_length_mismatch={readingsLengthMismatchCount}")

    detailDF = _buildDetailDF(comparisonDF, siteName, oldSummaryKey, 'mean_emissions', 'mean')

    thisRet = {
        'siteName': siteName,
        'oldSummaryKey': oldSummaryKey,
        'comparedItems': len(comparisonDF),
        'missingItemCount': missingItemCount,
        'outOfRangeCount': outOfRangeCount,
        'outOfRangeRelativeCount': outOfRangeRelativeCount,
        'readingsLengthMismatchCount': readingsLengthMismatchCount,
        'countsDifferCount': countsDifferCount,
        'maxAbsoluteDelta': maxAbsoluteDelta,
        'maxRelativeDelta': maxRelativeDelta,
    }

    return thisRet, detailDF


def doAnnualEmissionComparison(siteName, oldSummaryDict, newSummaryDF):
    OLD_SUMMARY_TO_NEW_SUMMARY_MAP = {
        ('AnnualEmissions', 'METype', 'off'):
            {'CICategory': 'METype', 'summaryColumn': 'METype', 'includeFugitive': False,
             'sumRows': ['summed_METype']},
        ('AnnualEmissions', 'METype', 'on'):
            {'CICategory': 'METype', 'summaryColumn': 'METype', 'includeFugitive': True,
             'sumRows': ['summed_METype']},

        # These have a different structure, so they are handled differently
        #
        # ('AnnualEmissions', 'modelReadableName', 'on'):
        #     {'CICategory': 'modelReadableName', 'summaryColumn': 'modelReadableName', 'includeFugitive': True},
        # ('AnnualEmissions', 'modelReadableName', 'off'):
        #     {'CICategory': 'modelReadableName', 'summaryColumn': 'modelReadableName', 'includeFugitive': False},

        ('AnnualEmissions', 'modelEmissionCategory', 'on'):
            {'CICategory': 'modelEmissionCategory', 'summaryColumn': 'modelEmissionCategory', 'includeFugitive': True,
             'sumRows': ['TOTAL'], 'excludeNewRows': ['COMBINED']},
        ('AnnualEmissions', 'modelEmissionCategory', 'off'):
            {'CICategory': 'modelEmissionCategory', 'summaryColumn': 'modelEmissionCategory', 'includeFugitive': False,
             'sumRows': ['TOTAL'], 'excludeNewRows': ['COMBINED']},
    }
    retList = []
    detailDFList = []
    for oldSummaryKey, newSummaryKey in OLD_SUMMARY_TO_NEW_SUMMARY_MAP.items():
        oldSummaryDF = oldSummaryDict.get(oldSummaryKey, None)
        if oldSummaryDF is None:
            continue

        _, oldSummaryType, _ = oldSummaryKey
        oldSummaryDF = oldSummaryDF.assign(compCount=oldSummaryDF['MCRuns_emission_list'].apply(lambda x: len(json.loads(x))))
        oldSummaryDF = oldSummaryDF[~oldSummaryDF[oldSummaryType].isin(newSummaryKey['sumRows'])]

        newSummaryColumn = newSummaryKey['summaryColumn']
        newSummaryMask = (
                (newSummaryDF['CICategory'] == newSummaryKey['CICategory'])
                & (~newSummaryDF[newSummaryColumn].isna())
                & (newSummaryDF['includeFugitive'] == newSummaryKey['includeFugitive'])
                & (newSummaryDF['units'] == 'mt/year')
                )
        newSummarySubsetDF = newSummaryDF[newSummaryMask]
        if 'excludeNewRows' in newSummaryKey:
            newSummarySubsetDF = newSummarySubsetDF[~newSummarySubsetDF[newSummaryColumn].isin(newSummaryKey['excludeNewRows'])]

        comparisonDF = oldSummaryDF.merge(newSummarySubsetDF,
                                          left_on=['species', oldSummaryType],
                                          right_on=['species', newSummaryColumn],
                                          how='outer',
                                          indicator=True
                                          )
        thisRet, detailDF = _doComparisons(comparisonDF, siteName, oldSummaryKey)
        retList.append(thisRet)
        detailDFList.append(detailDF)

    return retList, detailDFList


def compareReadableNameSummaries(siteName, oldSummaryDict, newSummaryDF, summaryMap, unitsFilter, addCompCount):
    retList = []
    detailDFList = []
    for oldSummaryKey, newSummaryKey in summaryMap.items():
        _, _, oldSummaryAbnormal = oldSummaryKey

        oldSummaryDF = oldSummaryDict.get(oldSummaryKey, None)
        if oldSummaryDF is None:
            continue

        oldSummaryDF, flaredGasMalfunctionCount = filterFlaredGasMalfunction(oldSummaryDF, oldSummaryAbnormal, oldSummaryKey)

        newSummaryMask = (
                (newSummaryDF['CICategory'] == newSummaryKey['CICategory'])
                & (newSummaryDF['includeFugitive'] == newSummaryKey['includeFugitive'])
                & (newSummaryDF['units'] == unitsFilter)
        )
        newSummarySubsetDF = newSummaryDF[newSummaryMask]

        compNewDF = newSummarySubsetDF[~newSummarySubsetDF['modelReadableName'].isna()]
        compOldDF = oldSummaryDF[oldSummaryDF['modelReadableName'] != 'summed_modelReadableName']
        if addCompCount:
            compOldDF = compOldDF.assign(compCount=compOldDF['MCRuns_emission_list'].apply(lambda x: len(json.loads(x))))

        comparisonDF = compOldDF.merge(compNewDF,
                                       on=['METype', 'unitID', 'modelReadableName', 'species'],
                                       how='outer',
                                       indicator=True
                                       )

        thisRet, detailDF = _doComparisons(comparisonDF, siteName, oldSummaryKey)
        thisRet = {**thisRet, 'flaredGasMalfunctionCount': flaredGasMalfunctionCount}
        retList.append(thisRet)
        detailDFList.append(detailDF)

    return retList, detailDFList


def doAggregatedEmissionComparison(siteName, oldSummaryDict, newSummaryDF):
    summaryMap = {
        ('AnnualEmissions', 'modelReadableName', 'on'):
            {'CICategory': 'modelReadableName', 'includeFugitive': True,
             'comparisonHierarchy': ['METype', 'unitID', 'modelReadableName']},
        ('AnnualEmissions', 'modelReadableName', 'off'):
            {'CICategory': 'modelReadableName', 'includeFugitive': False,
             'comparisonHierarchy': ['METype', 'unitID', 'modelReadableName']},
    }
    return compareReadableNameSummaries(siteName, oldSummaryDict, newSummaryDF, summaryMap, 'mt/year', addCompCount=True)


def doInstantaneousEmissionComparison(siteName, oldSummaryDict, newSummaryDF):
    summaryMap = {
        ('InstantaneousEmissions', 'modelReadableName', 'on'):
            {'CICategory': 'instantEmissionsByModelReadableName', 'includeFugitive': True,
             'comparisonHierarchy': ['METype', 'unitID', 'modelReadableName']},
        ('InstantaneousEmissions', 'modelReadableName', 'off'):
            {'CICategory': 'instantEmissionsByModelReadableName', 'includeFugitive': False,
             'comparisonHierarchy': ['METype', 'unitID', 'modelReadableName']},
    }
    return compareReadableNameSummaries(siteName, oldSummaryDict, newSummaryDF, summaryMap, 'kg/hour', addCompCount=False)


def doEventComparison(siteName, oldSummaryDict, newSummaryDF):
    OLD_SUMMARY_TO_NEW_SUMMARY_MAP = {
        ('AvgEmissionRatesAndDurations', 'modelReadableName', 'on'):
            {'CICategory': 'eventSummary', 'includeFugitive': True},
        ('AvgEmissionRatesAndDurations', 'modelReadableName', 'off'):
            {'CICategory': 'eventSummary', 'includeFugitive': False},
    }

    if siteName == 'Bluestone_Gas_Processing_Plant':
        i = 10

    retList = []
    detailDFList = []
    for oldSummaryKey, newSummaryKey in OLD_SUMMARY_TO_NEW_SUMMARY_MAP.items():
        summaryType, by, abnormal = oldSummaryKey
        tag = f"[{summaryType}/{by}/{abnormal}/{siteName}]"

        oldSummaryDF = oldSummaryDict.get(oldSummaryKey, None)
        if oldSummaryDF is None:
            continue

        oldSummaryDF, flaredGasMalfunctionCount = filterFlaredGasMalfunction(oldSummaryDF, abnormal, oldSummaryKey)

        newSummaryMask = (
                (newSummaryDF['CICategory'] == newSummaryKey['CICategory'])
                & (newSummaryDF['includeFugitive'] == newSummaryKey['includeFugitive'])
                & (newSummaryDF['emissionRateUnits'] == 'kg/h')
        )
        newSummarySubsetDF = newSummaryDF[newSummaryMask]

        compNewDF = newSummarySubsetDF[~newSummarySubsetDF['modelReadableName'].isna()]
        compOldDF = oldSummaryDF[oldSummaryDF['modelReadableName'] != 'summed_modelReadableName']

        comparisonDF = compOldDF.merge(compNewDF,
                                       on=['unitID', 'modelReadableName', 'species'],
                                       how='outer',
                                       indicator=True
                                       )

        roMask = comparisonDF['_merge'] == 'right_only'
        roNonZeroMask = roMask & (comparisonDF['simpleMean'] != 0.0)
        roNonZeroCount = 0
        if roNonZeroMask.any():
            roNonZeroCount = int(roNonZeroMask.sum())
            logging.warning(f"  {tag} right_only_non_zero={roNonZeroCount} of {len(comparisonDF)}")

        loMask = comparisonDF['_merge'] == 'left_only'
        loCount = 0
        if loMask.any():
            loCount = int(loMask.sum())
            logging.warning(f"  {tag} left_only={loCount} of {len(comparisonDF)}")

        comparisonDF = comparisonDF[~(roMask | loMask)]

        rateAbsDelta = (comparisonDF['avg_emission_rate (kg/h)'] - comparisonDF['simpleMean']).abs()
        rateRelDelta = rateAbsDelta / comparisonDF['avg_emission_rate (kg/h)'].abs().clip(lower=1e-9)
        eventAbsDelta = (comparisonDF['avg_event_count'] - comparisonDF['eventsPerMCRun']).abs()
        eventRelDelta = eventAbsDelta / comparisonDF['avg_event_count'].abs().clip(lower=1e-9)

        comparisonDF = comparisonDF.assign(
            rateAbsDelta=rateAbsDelta,
            rateRelDelta=rateRelDelta,
            eventAbsDelta=eventAbsDelta,
            eventRelDelta=eventRelDelta
        )

        maxRateAbsDelta = float(rateAbsDelta.max(skipna=True)) if len(comparisonDF) > 0 else 0.0
        maxRateRelDelta = float(rateRelDelta.max(skipna=True)) if len(comparisonDF) > 0 else 0.0

        emissionRateOutOfRange = (rateAbsDelta > ABS_EPSILON) & (rateRelDelta > REL_EPSILON)
        emissionRateOutOfRangeCount = int(emissionRateOutOfRange.sum())
        emissionRateOutOfRangeRelativeCount = int((rateRelDelta > REL_EPSILON).sum())
        if emissionRateOutOfRangeCount > 0:
            logging.warning(f"  {tag} emission_rate_out_of_range={emissionRateOutOfRangeCount} of {len(comparisonDF)} (maxAbs={maxRateAbsDelta:.4f}, maxRel={maxRateRelDelta:.2%})")

        eventOutOfRange = (eventAbsDelta > ABS_EPSILON) & (eventRelDelta > REL_EPSILON)
        eventOutOfRangeCount = int(eventOutOfRange.sum())
        if eventOutOfRangeCount > 0:
            logging.warning(f"  {tag} event_count_out_of_range={eventOutOfRangeCount} of {len(comparisonDF)}")

        presentIdentityCols = [c for c in _IDENTITY_COLS if c in comparisonDF.columns]
        detailDF = comparisonDF[presentIdentityCols].copy()
        detailDF = detailDF.assign(
            old_mean=comparisonDF['avg_emission_rate (kg/h)'],
            new_mean=comparisonDF['simpleMean'],
            absoluteDelta=comparisonDF['rateAbsDelta'],
            relativeDelta=comparisonDF['rateRelDelta'],
            old_eventCount=comparisonDF['avg_event_count'],
            new_eventCount=comparisonDF['eventsPerMCRun'],
            eventAbsoluteDelta=comparisonDF['eventAbsDelta'],
            eventRelativeDelta=comparisonDF['eventRelDelta'],
            mergeStatus='both',
            summaryType=summaryType,
            by=by,
            abnormal=abnormal,
            siteName=siteName
        )
        detailDFList.append(detailDF)

        thisRet = {
            'siteName': siteName,
            'oldSummaryKey': oldSummaryKey,
            'comparedItems': len(comparisonDF),
            'roNonZeroCount': roNonZeroCount,
            'loCount': loCount,
            'eventOutOfRangeCount': eventOutOfRangeCount,
            'emissionRateOutOfRangeCount': emissionRateOutOfRangeCount,
            'emissionRateOutOfRangeRelativeCount': emissionRateOutOfRangeRelativeCount,
            'maxAbsoluteDelta': maxRateAbsDelta,
            'maxRelativeDelta': maxRateRelDelta,
        }
        retList.append(thisRet)

    return retList, detailDFList


def doSimSummaryComparison(siteName, oldSummaryDict, newSummaryDF):
    OLD_SUMMARY_TO_NEW_SUMMARY_MAP = {
        ('AggregatedSimulationEmissions', 'modelReadableName', 'on'):
            {'CICategory': 'modelReadableName', 'includeFugitive': True,
             'comparisonHierarchy': ['species', 'modelReadableName']},
        ('AggregatedSimulationEmissions', 'modelReadableName', 'off'):
            {'CICategory': 'modelReadableName', 'includeFugitive': False,
             'comparisonHierarchy': ['species', 'modelReadableName']},

        ('AggregatedSimulationEmissions', 'category', 'on'):
            {'CICategory': 'modelEmissionCategory', 'includeFugitive': True,
             'comparisonHierarchy': ['species', 'modelEmissionCategory']},
        ('AggregatedSimulationEmissions', 'category', 'off'):
            {'CICategory': 'modelEmissionCategory', 'includeFugitive': False,
             'comparisonHierarchy': ['species', 'modelEmissionCategory']},

        ('AggregatedSimulationEmissions', 'METype', 'on'):
            {'CICategory': 'METype', 'includeFugitive': True,
             'comparisonHierarchy': ['species', 'METype']},
        ('AggregatedSimulationEmissions', 'METype', 'off'):
            {'CICategory': 'METype', 'includeFugitive': False,
             'comparisonHierarchy': ['species', 'METype']},

        ('AggregatedSimulationEmissions', 'unitID', 'on'):
            {'CICategory': 'unitID', 'includeFugitive': True,
             'comparisonHierarchy': ['species', 'unitID']},
        ('AggregatedSimulationEmissions', 'unitID', 'off'):
            {'CICategory': 'unitID', 'includeFugitive': False,
             'comparisonHierarchy': ['species', 'unitID']},
    }

    retList = []
    detailDFList = []
    for oldSummaryKey, newSummaryKey in OLD_SUMMARY_TO_NEW_SUMMARY_MAP.items():
        summaryType, by, abnormal = oldSummaryKey
        tag = f"[{summaryType}/{by}/{abnormal}/{siteName}]"

        oldSummaryDF = oldSummaryDict.get(oldSummaryKey, None)
        if oldSummaryDF is None:
            continue

        if 'modelEmissionCategory' in oldSummaryDF.columns:
            oldSummaryDF = oldSummaryDF.assign(
                modelEmissionCategory=oldSummaryDF['modelEmissionCategory'].replace('TOTAL', 'COMBINED')
            )

        newSummaryMask = (
                (newSummaryDF['CICategory'] == newSummaryKey['CICategory'])
                & (newSummaryDF['includeFugitive'] == newSummaryKey['includeFugitive'])
                & ((newSummaryDF['units'] == 'mt/year') | (newSummaryDF['units'] == 'unitless'))
        )
        newSummarySubsetDF = newSummaryDF[newSummaryMask]

        comparisonDF = oldSummaryDF.merge(newSummarySubsetDF,
                                          on=newSummaryKey['comparisonHierarchy'],
                                          how='outer',
                                          indicator=True
                                          )

        roMask = comparisonDF['_merge'] == 'right_only'
        comparisonDF = comparisonDF.assign(mean=comparisonDF['mean'].fillna(0.0))
        roNonZeroMask = roMask & (comparisonDF['mean'] != 0.0)
        roNonZeroCount = 0
        if roNonZeroMask.any():
            roNonZeroCount = int(roNonZeroMask.sum())
            logging.warning(f"  {tag} right_only_non_zero={roNonZeroCount} of {len(comparisonDF)}")

        loMask = comparisonDF['_merge'] == 'left_only'
        loCount = 0
        if loMask.any():
            loCount = int(loMask.sum())
            logging.warning(f"  {tag} left_only={loCount} of {len(comparisonDF)}")

        comparisonDF = comparisonDF[~(roMask | loMask)]

        comparisonDF = _addDeltas(comparisonDF, 'mean_emissions', 'mean')

        maxAbsoluteDelta = float(comparisonDF['absoluteDelta'].max(skipna=True)) if len(comparisonDF) > 0 else 0.0
        maxRelativeDelta = float(comparisonDF['relativeDelta'].max(skipna=True)) if len(comparisonDF) > 0 else 0.0

        outOfRange = (comparisonDF['absoluteDelta'] > ABS_EPSILON) & (comparisonDF['relativeDelta'] > REL_EPSILON)
        emissionRateOutOfRangeCount = int(outOfRange.sum())
        emissionRateOutOfRangeRelativeCount = int((comparisonDF['relativeDelta'] > REL_EPSILON).sum())
        if emissionRateOutOfRangeCount > 0:
            logging.warning(f"  {tag} out_of_range={emissionRateOutOfRangeCount} of {len(comparisonDF)} (maxAbs={maxAbsoluteDelta:.4f}, maxRel={maxRelativeDelta:.2%})")

        detailDF = _buildDetailDF(comparisonDF, siteName, oldSummaryKey, 'mean_emissions', 'mean')
        detailDFList.append(detailDF)

        thisRet = {
            'siteName': siteName,
            'oldSummaryKey': oldSummaryKey,
            'comparedItems': len(comparisonDF),
            'roNonZeroCount': roNonZeroCount,
            'loCount': loCount,
            'emissionRateOutOfRangeCount': emissionRateOutOfRangeCount,
            'emissionRateOutOfRangeRelativeCount': emissionRateOutOfRangeRelativeCount,
            'maxAbsoluteDelta': maxAbsoluteDelta,
            'maxRelativeDelta': maxRelativeDelta,
        }
        retList.append(thisRet)

    return retList, detailDFList


_ABNORMAL_TO_INCLUDE_FUGITIVE = {'on': True, 'off': False}


def _readOldPDFSummaries(config):
    simulationRoot = config['simulationRoot']
    siteName = config['siteName']
    pdfDir = Path(simulationRoot) / 'summaries' / 'PDFs' / f'site={siteName}'
    results = []
    for abnormal in ['on', 'off']:
        siteFile = pdfDir / f'PDF_for_site_abnormal_{abnormal}.csv'
        if siteFile.exists():
            results.append({'CICategory': 'site', 'categoryValue': None, 'abnormal': abnormal, 'df': pd.read_csv(siteFile)})

        for f in pdfDir.glob(f'PDF_for_all_*_abnormal_{abnormal}.csv'):
            meType = f.stem.replace('PDF_for_all_', '').replace(f'_abnormal_{abnormal}', '')
            results.append({'CICategory': 'METype', 'categoryValue': meType, 'abnormal': abnormal, 'df': pd.read_csv(f)})

        for f in pdfDir.glob(f'PDF_for_*_abnormal_{abnormal}.csv'):
            if f.stem.startswith('PDF_for_all_') or f.stem.startswith('PDF_for_site_'):
                continue
            unitID = f.stem.replace('PDF_for_', '').replace(f'_abnormal_{abnormal}', '')
            results.append({'CICategory': 'unitID', 'categoryValue': unitID, 'abnormal': abnormal, 'df': pd.read_csv(f)})
    return results


def _readNewPDFSummaries(config):
    if 'parquetNewPDF' not in config:
        return pd.DataFrame()
    pdfPath = Path(config['parquetNewPDF'])
    if not pdfPath.exists():
        return pd.DataFrame()
    return pd.read_parquet(config['parquetNewPDF'], filters=[('site', '=', config['siteName'])])


def doPDFComparison(siteName, oldPDFList, newPDFDF):
    if newPDFDF.empty:
        return [], []
    retList = []
    detailDFList = []

    for entry in oldPDFList:
        CICategory = entry['CICategory']
        categoryValue = entry['categoryValue']
        abnormal = entry['abnormal']
        oldDF = entry['df']
        includeFugitive = _ABNORMAL_TO_INCLUDE_FUGITIVE[abnormal]
        oldSummaryKey = ('CDF', f"{CICategory}/{categoryValue}", abnormal)
        tag = f"[CDF/{CICategory}/{categoryValue}/{abnormal}/{siteName}]"

        newMask = (
            (newPDFDF['CICategory'] == CICategory) &
            (newPDFDF['species'] == 'METHANE') &
            (newPDFDF['includeFugitive'] == includeFugitive)
        )
        if categoryValue is not None:
            newMask = newMask & (newPDFDF[CICategory] == categoryValue)
        newSubset = newPDFDF[newMask]

        if newSubset.empty:
            logging.warning(f"  {tag} no matching new CDF entry")
            retList.append({
                'siteName': siteName, 'oldSummaryKey': oldSummaryKey,
                'comparedItems': 0, 'missingItemCount': 1,
                'outOfRangeCount': 0, 'maxAbsoluteDelta': np.nan, 'maxRelativeDelta': np.nan,
            })
            continue

        # Legacy toPDF() could produce near-duplicate x values (sub-ULP float noise)
        # representing a point mass. Keep the max (post-jump) probability per bin
        # before interpolating to avoid a spurious KS distance at the jump point.
        oldDedup = (oldDF.assign(_key=oldDF['CH4_EmissionRate_kg/h'].round(10))
                        .groupby('_key', as_index=False)
                        .agg({'probability': 'max', 'CH4_EmissionRate_kg/h': 'first'})
                        .sort_values('CH4_EmissionRate_kg/h'))
        oldX = oldDedup['CH4_EmissionRate_kg/h'].values.astype(float)
        oldP = oldDedup['probability'].values.astype(float)

        newX = newSubset['emissionRate_kgPerH'].values.astype(float)
        newP = newSubset['cumulativeProbability'].values.astype(float)
        newIdx = np.argsort(newX)
        newX, newP = newX[newIdx], newP[newIdx]

        allX = np.union1d(oldX, newX)
        oldInterp = np.interp(allX, oldX, oldP)
        newInterp = np.interp(allX, newX, newP)
        diff = np.abs(oldInterp - newInterp)
        ksD = float(diff.max())
        maxDiffAt = float(allX[diff.argmax()])

        outOfRangeCount = 1 if ksD > KS_EPSILON else 0
        if outOfRangeCount:
            logging.warning(f"  {tag} ksD={ksD:.4f} maxDiffAt={maxDiffAt:.4f} kg/h")

        detailDF = pd.DataFrame([{
            'CICategory': CICategory,
            'categoryValue': str(categoryValue),
            'summaryType': 'CDF',
            'by': CICategory,
            'abnormal': abnormal,
            'siteName': siteName,
            'mergeStatus': 'both',
            'ksStatistic': ksD,
            'ksMaxAt_kgPerH': maxDiffAt,
            'oldRows': len(oldDF),
            'newRows': len(newSubset),
        }])
        detailDFList.append(detailDF)

        retList.append({
            'siteName': siteName, 'oldSummaryKey': oldSummaryKey,
            'comparedItems': len(allX), 'missingItemCount': 0,
            'outOfRangeCount': outOfRangeCount,
            'maxAbsoluteDelta': ksD, 'maxRelativeDelta': np.nan,
        })

    return retList, detailDFList


def compareSummaries(job):
    siteName = job['siteName']
    logging.info(f"Comparing {siteName=}")
    oldSummaryDict = _readOldSummaries(job)
    newSummaryDF = _readNewSummaries(job)
    newEventSummaryDF = _readNewEventSummaries(job)
    newPDFDF = _readNewPDFSummaries(job)

    annualRet, annualDetail = doAnnualEmissionComparison(siteName, oldSummaryDict, newSummaryDF)
    aggregatedRet, aggregatedDetail = doAggregatedEmissionComparison(siteName, oldSummaryDict, newSummaryDF)
    instRet, instDetail = doInstantaneousEmissionComparison(siteName, oldSummaryDict, newSummaryDF)
    eventRet, eventDetail = doEventComparison(siteName, oldSummaryDict, newEventSummaryDF)
    pdfRet, pdfDetail = doPDFComparison(siteName, _readOldPDFSummaries(job), newPDFDF)

    summaryList = [*annualRet, *aggregatedRet, *instRet, *eventRet, *pdfRet]
    detailDFList = [*annualDetail, *aggregatedDetail, *instDetail, *eventDetail, *pdfDetail]
    return summaryList, detailDFList


def checkSimSummaryConsistency(siteName, newSummaryDF):
    # Self-consistency check on the new SimSummary — validates statistical invariants
    # that hold regardless of the legacy reference values. This catches bugs that both
    # old and new implementations share (e.g. mean > max), which old-vs-new comparison
    # cannot detect. See SummarySchema.md "SummaryTest blind spot" for details.
    #
    # mean > max is always a structural bug (counted as a violation).
    # mean > upperCI or lowerCI > mean is logged as a warning only — it is
    # statistically valid for heavily right-skewed distributions with extreme outlier
    # MC runs, but warrants investigation. See SummarySchema.md "Extreme outlier MC runs".
    #
    # C2/C1 rows are excluded — their min/max/CI columns are NaN by design.
    checkDF = newSummaryDF[newSummaryDF['species'] != 'C2/C1']
    total = len(checkDF)

    meanGtMax = checkDF[checkDF['mean'] > checkDF['max']]
    lowerCIGtMean = checkDF[checkDF['lowerCI'] > checkDF['mean']]
    meanGtUpperCI = checkDF[checkDF['mean'] > checkDF['upperCI']]
    ciWarningCount = len(lowerCIGtMean) + len(meanGtUpperCI)

    tag = f"[SimSummaryConsistency/self/check/{siteName}]"
    if len(meanGtMax) > 0:
        logging.warning(f"  {tag} mean > max: {len(meanGtMax)} of {total} rows (violation)")
    if len(lowerCIGtMean) > 0:
        logging.warning(f"  {tag} lowerCI > mean: {len(lowerCIGtMean)} of {total} rows (warning — possible extreme outlier MC runs)")
    if len(meanGtUpperCI) > 0:
        logging.warning(f"  {tag} mean > upperCI: {len(meanGtUpperCI)} of {total} rows (warning — possible extreme outlier MC runs)")

    thisRet = {
        'siteName': siteName,
        'oldSummaryKey': ('SimSummaryConsistency', 'self', 'check'),
        'comparedItems': total,
        'emissionRateOutOfRangeCount': len(meanGtMax),
        'emissionRateOutOfRangeRelativeCount': len(meanGtMax),
        'ciWarningCount': ciWarningCount,
        'roNonZeroCount': 0,
        'loCount': 0,
        'maxAbsoluteDelta': 0.0,
        'maxRelativeDelta': 0.0,
    }
    return [thisRet], [pd.DataFrame()]


def compareSimSummaries(job):
    logging.info(f"Comparing simulation summaries")
    oldSummaryDict = _readOldSummaries(job)
    newSimulationSummaryDF = _readNewSimulationSummary(job)
    compResults, detailList = doSimSummaryComparison('simulation', oldSummaryDict, newSimulationSummaryDF)
    consistencyResults, consistencyDetails = checkSimSummaryConsistency('simulation', newSimulationSummaryDF)
    return compResults + consistencyResults, detailList + consistencyDetails


def _transformResult(inDict):
    outKey = inDict['oldSummaryKey']
    outDict = dict(filter(lambda x: x[0] != 'oldSummaryKey', inDict.items()))
    expKey = {'summaryType': outKey[0], 'by': outKey[1], 'abnormal': outKey[2]}
    return {**expKey, **outDict}


def regenerateOldSummaries(summaryJobs):
    oldSummaryArgs = {
        'annualSummaries': True,
        'instantaneousSummaries': True,
        'pdfSummaries': True,
        'avgDurSummaries': True,
        'statesAndTsPloting': False,
        'simulationEmissions': True,
        'plot': False,
        'fullSummaries': False,
        'siteEmiss': True,
        'METype': True,
        'unitID': True,
        'Pneumatics': True,
    }
    for singleJob in summaryJobs:
        pl.postprocess({**singleJob, **oldSummaryArgs})


def main(cMgr):
    logging.basicConfig(level=logging.INFO, format=au.LOG_FORMAT)
    workitemQueues = sm.generateWorkitems(cMgr, phasesToInclude=['summarize', 'simSummary'])
    summaryJobs = workitemQueues[0]
    simSummaryJobs = workitemQueues[1]

    if REGENERATE_SUMMARIES:
        with Timer("old summaries") as t0:
            regenerateOldSummaries(summaryJobs)
            t0.setCount(len(summaryJobs))

    compResults = []
    allDetailDFs = []
    with Timer("compare summaries") as t1:
        for singleJob in summaryJobs:
            res, detailList = compareSummaries(singleJob)
            compResults.extend(res)
            allDetailDFs.extend(detailList)
        t1.setCount(len(summaryJobs))

    with Timer("compare simulation wide summaries") as t2:
        for singleJob in simSummaryJobs:
            res, detailList = compareSimSummaries(singleJob)
            compResults.extend(res)
            allDetailDFs.extend(detailList)
        t2.setCount(len(simSummaryJobs))

    timestamp = dt.datetime.now()
    scenarioTimestamp = cMgr.getConfigVar('scenarioTimestamp')

    outResList = map(_transformResult, compResults)
    resDF = pd.DataFrame(outResList).fillna(0.0)
    resDF = resDF.assign(scenarioTimestamp=scenarioTimestamp)
    resFileFormat = f"SummaryTest_results_{cMgr.getConfigVar('scenarioTimestampFormat')}.csv"
    resFilename = timestamp.strftime(resFileFormat)
    resDF.to_csv(resFilename, index=False)
    logging.info(f"Wrote {resFilename}")

    nonEmptyDetails = [df for df in allDetailDFs if not df.empty]
    if nonEmptyDetails:
        detailDF = pd.concat(nonEmptyDetails, ignore_index=True)
        detailDF = detailDF.assign(scenarioTimestamp=scenarioTimestamp)
        detailFileFormat = f"SummaryTest_detail_{cMgr.getConfigVar('scenarioTimestampFormat')}.parquet"
        detailFilename = timestamp.strftime(detailFileFormat)
        detailDF.to_parquet(detailFilename, index=False)
        logging.info(f"Wrote {detailFilename}")


if __name__ == "__main__":
    cMgr, args = au.getConfig()
    if not args.scenarioTimestamp:
        au.findMostRecentScenario(cMgr)
    main(cMgr)
