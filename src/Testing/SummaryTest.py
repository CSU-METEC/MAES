import pandas as pd
import logging
from Timer import Timer
import SiteMain2 as sm
import AppUtils as au
import json
from pathlib import Path
import datetime as dt


VALUE_EPSILON = 0.01

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


def _readOldSummaries(config):
    simulationRoot = config['simulationRoot']

    ret = {}
    for singleSummary, summaryData in SUMMARY_LAYOUTS.items():
        for singleType in summaryData['typeList']:
            for abnormal in ['on', 'off']:
                if summaryData.get('simulationWide', False):
                    siteDir = ''                           # This is a simulation wide summary -- no site parameter
                else:
                    siteDir = f"site={config['siteName']}" # This is a site specific summary
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
    summaryDF = pd.read_parquet(config['parquetNewSummary'], filters=[
        ('site', '=', config['siteName'])
    ])
    return summaryDF

def _readNewEventSummaries(config):
    summaryDF = pd.read_parquet(config['parquetNewEventSummary'], filters=[
        ('site', '=', config['siteName'])
    ])
    return summaryDF

def _readNewSimulationSummary(config):
    summaryDF = pd.read_parquet(config['parquetNewSimSummary'])  # No filters because this is the simulation wide summary
    return summaryDF

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


def _doComparisons(comparisonDF, siteName, oldSummaryKey):
    roMissing = (comparisonDF['_merge'] == 'right_only') & (comparisonDF['mean'] != 0.0)
    loMissing = (comparisonDF['_merge'] == 'left_only')
    missingItems = roMissing | loMissing
    missingItemCount = 0
    if missingItems.any():
        missingItemCount = int(missingItems.sum())
        logging.warning(f"  missing items {oldSummaryKey=},  out of {len(comparisonDF)=} {missingItemCount=}")
    # exclude any 'roMissing' values from further consideration
    roSpurious = (comparisonDF['_merge'] == 'right_only') & (comparisonDF['mean'] == 0.0)
    comparisonDF = comparisonDF[~roSpurious]

    outOfRange = (comparisonDF['delta'] > VALUE_EPSILON)
    outOfRangeCount = 0
    if outOfRange.any():
        outOfRangeCount = int(outOfRange.sum())
        logging.warning(f"  out of range {oldSummaryKey=},  out of {len(comparisonDF)=} {outOfRangeCount=}")

    if 'compCount' in comparisonDF:
        countsDiffer = (comparisonDF['compCount'] != comparisonDF['count'])
        countsDifferCount = 0
        if countsDiffer.any():
            countsDifferCount = int(countsDiffer.sum())
            logging.warning(f"  counts differ {oldSummaryKey=},  out of {len(comparisonDF)=} {countsDifferCount=}")
    else:
        countsDifferCount = 0

    thisRet = {
        'siteName': siteName,
        'oldSummaryKey': oldSummaryKey,
        'comparedItems': len(comparisonDF),
        'missingItemCount': missingItemCount,
        'outOfRangeCount': outOfRangeCount,
        'countsDifferCount': countsDifferCount
    }

    return thisRet

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
             'sumRows': ['TOTAL']},
        ('AnnualEmissions', 'modelEmissionCategory', 'off'):
            {'CICategory': 'modelEmissionCategory', 'summaryColumn': 'modelEmissionCategory', 'includeFugitive': False,
             'sumRows': ['TOTAL']},
    }
    retList = []
    for oldSummaryKey, newSummaryKey in OLD_SUMMARY_TO_NEW_SUMMARY_MAP.items():
        oldSummaryDF = oldSummaryDict.get(oldSummaryKey, None)
        if oldSummaryDF is None:
            continue

        _, oldSummaryType, _ = oldSummaryKey
        oldSummaryDF = oldSummaryDF.assign(compCount=oldSummaryDF['MCRuns_emission_list'].apply(lambda x: len(json.loads(x))))
        # Filter out summary rows
        oldSummaryDF = oldSummaryDF[~oldSummaryDF[oldSummaryType].isin(newSummaryKey['sumRows'])]

        newSummaryColumn = newSummaryKey['summaryColumn']
        newSummaryMask = (
                (newSummaryDF['CICategory'] == newSummaryKey['CICategory'])
                & (~newSummaryDF[newSummaryColumn].isna())
                & (newSummaryDF['includeFugitive'] == newSummaryKey['includeFugitive'])
                & (newSummaryDF['units'] == 'mt/year')
                )
        newSummarySubsetDF = newSummaryDF[newSummaryMask]

        comparisonDF = oldSummaryDF.merge(newSummarySubsetDF,
                                          left_on=['species', oldSummaryType],
                                          right_on=['species', newSummaryColumn],
                                          how='outer',
                                          indicator=True
                                          )
        comparisonDF = comparisonDF.assign(delta=(comparisonDF['mean_emissions'] - comparisonDF['mean']).abs())
        thisRet = _doComparisons(comparisonDF, siteName, oldSummaryKey)
        retList.append(thisRet)

    return retList

def compareReadableNameSummaries(siteName, oldSummaryDict, newSummaryDF, summaryMap, unitsFilter, addCompCount):
    retList = []
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

        comparisonDF = comparisonDF.assign(delta=(comparisonDF['mean_emissions'] - comparisonDF['mean']).abs())

        thisRet = _doComparisons(comparisonDF, siteName, oldSummaryKey)
        thisRet = {**thisRet, 'flaredGasMalfunctionCount': flaredGasMalfunctionCount}

        retList.append(thisRet)
    return retList

def doAggregatedEmissionComparison(siteName, oldSummaryDict, newSummaryDF):
    summaryMap = {
        ('AnnualEmissions', 'modelReadableName', 'on'):
            {'CICategory': 'modelReadableName', 'includeFugitive': True,
             'comparisonHierarchy': ['METype', 'unitID', 'modelReadableName']},
        ('AnnualEmissions', 'modelReadableName', 'off'):
            {'CICategory': 'modelReadableName', 'includeFugitive': False,
             'comparisonHierarchy': ['METype', 'unitID', 'modelReadableName']},
    }
    ret = compareReadableNameSummaries(siteName, oldSummaryDict, newSummaryDF, summaryMap, 'mt/year', addCompCount=True)
    return ret

def doInstantaneousEmissionComparison(siteName, oldSummaryDict, newSummaryDF):
    summaryMap = {
        ('InstantaneousEmissions', 'modelReadableName', 'on'):
            {'CICategory': 'instantEmissionsByModelReadableName', 'includeFugitive': True,
             'comparisonHierarchy': ['METype', 'unitID', 'modelReadableName']},
        ('InstantaneousEmissions', 'modelReadableName', 'off'):
            {'CICategory': 'instantEmissionsByModelReadableName', 'includeFugitive': False,
             'comparisonHierarchy': ['METype', 'unitID', 'modelReadableName']},
    }
    ret = compareReadableNameSummaries(siteName, oldSummaryDict, newSummaryDF, summaryMap, 'kg/hour', addCompCount=False)
    return ret

def doEventComparison(siteName, oldSummaryDict, newSummaryDF):
    OLD_SUMMARY_TO_NEW_SUMMARY_MAP = {
        ('AvgEmissionRatesAndDurations', 'modelReadableName', 'on'):
            {'CICategory': 'eventSummary', 'includeFugitive': True,
             'comparisonHierarchy': ['METype', 'unitID', 'modelReadableName']},
        ('AvgEmissionRatesAndDurations', 'modelReadableName', 'off'):
            {'CICategory': 'eventSummary', 'includeFugitive': False,
             'comparisonHierarchy': ['METype', 'unitID', 'modelReadableName']},
    }

    retList = []
    for oldSummaryKey, newSummaryKey in OLD_SUMMARY_TO_NEW_SUMMARY_MAP.items():
        _, _, oldSummaryAbnormal = oldSummaryKey

        oldSummaryDF = oldSummaryDict.get(oldSummaryKey, None)
        if oldSummaryDF is None:
            continue

        oldSummaryDF, flaredGasMalfunctionCount = filterFlaredGasMalfunction(oldSummaryDF, oldSummaryAbnormal, oldSummaryKey)

        newSummaryMask = (
                (newSummaryDF['CICategory'] == newSummaryKey['CICategory'])
                & (newSummaryDF['includeFugitive'] == newSummaryKey['includeFugitive'])
                & (newSummaryDF['emissionRateUnits'] == 'kg/h')  # Units are different than other summaries
        )
        newSummarySubsetDF = newSummaryDF[newSummaryMask]

        compNewDF = newSummarySubsetDF[~newSummarySubsetDF['modelReadableName'].isna()]
        compOldDF = oldSummaryDF[oldSummaryDF['modelReadableName'] != 'summed_modelReadableName']

        comparisonDF = compOldDF.merge(compNewDF,
                                       on=['unitID', 'modelReadableName', 'species'],
                                       how='outer',
                                       indicator=True
                                       )
        
        # first, filter out entries in the new summary that are not in original
        roMask = comparisonDF['_merge'] == 'right_only'
        ## right_only values that are zero are OK
        roNonZeroMask = roMask & (comparisonDF['meanEmissionRate'] != 0.0)
        roNonZeroCount = 0
        if roNonZeroMask.any():
            roNonZeroCount = int(roNonZeroMask.sum())
            logging.warning(f"  right-only rows with non-zero emissionRate values {oldSummaryKey=},  out of {len(comparisonDF)=} {roNonZeroMask=}")
        # See if we are missing any new summary vaues
        loMask = comparisonDF['_merge'] == 'left_only'
        loCount = 0
        if loMask.any():
            loCount = int(loMask.sum())
            logging.warning(f"  left-only rows {oldSummaryKey=},  out of {len(comparisonDF)=} {loCount=}")

        # filter out lo & ro values
        comparisonDF = comparisonDF[~(roMask | loMask)]

        comparisonDF = comparisonDF.assign(eventCountDelta=(comparisonDF['avg_event_count'] - comparisonDF['eventsPerMCRun']).abs(),
                                           emissionRateDelta=(comparisonDF['avg_emission_rate (kg/h)'] - comparisonDF['meanEmissionRate']).abs()
                                           )
        

        eventOutOfRange = (comparisonDF['eventCountDelta'] > VALUE_EPSILON)
        eventOutOfRangeCount = 0
        if eventOutOfRange.any():
            eventOutOfRangeCount = int(eventOutOfRange.sum())
            logging.warning(f"  events per MC Run out of range {oldSummaryKey=},  out of {len(comparisonDF)=} {eventOutOfRangeCount=}")

        emissionRateOutOfRange = (comparisonDF['emissionRateDelta'] > VALUE_EPSILON)
        emissionRateOutOfRangeCount = 0
        if emissionRateOutOfRange.any():
            emissionRateOutOfRangeCount = int(emissionRateOutOfRange.sum())
            logging.warning(f"  emission rates out of range {oldSummaryKey=},  out of {len(comparisonDF)=} {emissionRateOutOfRangeCount=}")

        thisRet = {
            'siteName': siteName,
            'oldSummaryKey': oldSummaryKey,
            'comparedItems': len(comparisonDF),
            'roNonZeroCount': roNonZeroCount,
            'loCount': loCount,
            'eventOutOfRangeCount': eventOutOfRangeCount,
            'emissionRateOutOfRangeCount': emissionRateOutOfRangeCount
        }

        retList.append(thisRet)

    return retList

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
    for oldSummaryKey, newSummaryKey in OLD_SUMMARY_TO_NEW_SUMMARY_MAP.items():
        oldSummaryDF = oldSummaryDict.get(oldSummaryKey, None)
        if oldSummaryDF is None:
            continue

        newSummaryMask = (
                (newSummaryDF['CICategory'] == newSummaryKey['CICategory'])
                & (newSummaryDF['includeFugitive'] == newSummaryKey['includeFugitive'])
                & ((newSummaryDF['units'] == 'mt/year') | (newSummaryDF['units'] == 'unitless')) # sim-wide summaries include unitless C2/C1 ratios
        )
        newSummarySubsetDF = newSummaryDF[newSummaryMask]

        compNewDF = newSummarySubsetDF
        compOldDF = oldSummaryDF

        comparisonDF = compOldDF.merge(compNewDF,
                                       on=newSummaryKey['comparisonHierarchy'],
                                       how='outer',
                                       indicator=True
                                       )
        
        # first, filter out entries in the new summary that are not in original
        roMask = comparisonDF['_merge'] == 'right_only'
        ## right_only values that are zero or NaN are ok.
        ## convert any mean values in comparisonDF to 0.0
        comparisonDF = comparisonDF.assign(mean=comparisonDF['mean'].fillna(0.0))
        roNonZeroMask = roMask & (comparisonDF['mean'] != 0.0)
        roNonZeroCount = 0
        if roNonZeroMask.any():
            roNonZeroCount = int(roNonZeroMask.sum())
            logging.warning(f"  right-only rows with non-zero emissionRate values {oldSummaryKey=},  out of {len(comparisonDF)=}, {roNonZeroCount=}")
        # See if we are missing any new summary vaues
        loMask = comparisonDF['_merge'] == 'left_only'
        loCount = 0
        if loMask.any():
            loCount = int(loMask.sum())
            logging.warning(f"  left-only rows {oldSummaryKey=},  out of {len(comparisonDF)=} {loCount=}")

        # filter out lo & ro values
        comparisonDF = comparisonDF[~(roMask | loMask)]

        comparisonDF = comparisonDF.assign(emissionRateDelta=(comparisonDF['mean_emissions'] - comparisonDF['mean']).abs())

        emissionRateOutOfRange = (comparisonDF['emissionRateDelta'] > VALUE_EPSILON)
        emissionRateOutOfRangeCount = 0
        if emissionRateOutOfRange.any():
            emissionRateOutOfRangeCount = int(emissionRateOutOfRange.sum())
            logging.warning(f"  emission rates out of range {oldSummaryKey=},  out of {len(comparisonDF)=} {emissionRateOutOfRangeCount=}")

        thisRet = {
            'siteName': siteName,
            'oldSummaryKey': oldSummaryKey,
            'comparedItems': len(comparisonDF),
            'roNonZeroCount': roNonZeroCount,
            'loCount': loCount,
            'emissionRateOutOfRangeCount': emissionRateOutOfRangeCount
        }

        retList.append(thisRet)

    return retList

def compareSummaries(job):
    siteName = job['siteName']
    logging.info(f"Comparing {siteName=}")
    # read old summary files
    oldSummaryDict = _readOldSummaries(job)
    # read new summary files
    newSummaryDF = _readNewSummaries(job)
    newEventSummaryDF = _readNewEventSummaries(job)
    newSimulationSummaryDF = _readNewSimulationSummary(job)

    annualRet = doAnnualEmissionComparison(siteName, oldSummaryDict, newSummaryDF)
    aggregatedRet = doAggregatedEmissionComparison(siteName, oldSummaryDict, newSummaryDF)
    instRet = doInstantaneousEmissionComparison(siteName, oldSummaryDict, newSummaryDF)
    eventRet = doEventComparison(siteName, oldSummaryDict, newEventSummaryDF)

    ret = [*annualRet, *aggregatedRet, *instRet, *eventRet]
    return ret

def compareSimSummaries(job):
    logging.info(f"Comparing simulation summaries")
    # read old summary files
    oldSummaryDict = _readOldSummaries(job)
    # read new summary files
    newSimulationSummaryDF = _readNewSimulationSummary(job)

    ret = doSimSummaryComparison('simulation', oldSummaryDict, newSimulationSummaryDF)
    return ret

def _transformResult(inDict):
    outKey = inDict['oldSummaryKey']
    outDict = dict(filter(lambda x: x[0] != 'oldSummaryKey', inDict.items()))
    expKey = {'summaryType': outKey[0], 'by': outKey[1], 'abnormal': outKey[2]}
    ret = {**expKey, **outDict}
    return ret

def main(cMgr):
    logging.basicConfig(level=logging.INFO, format=au.LOG_FORMAT)
    workitemQueues = sm.generateWorkitems(cMgr, phasesToInclude=['summarize', 'simSummary'])
    summaryJobs = workitemQueues[0]
    simSummaryJobs = workitemQueues[1]
    oldSummaryArgs = {
        'annualSummaries': True,
        'instantaneousSummaries': True,
        'pdfSummaries': False,
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
    # with Timer("old summaries") as t0:
    #     for singleJob in summaryJobs:
    #         jobWithSummaryArgs = {**singleJob, **oldSummaryArgs}
    #         pl.postprocess(jobWithSummaryArgs)
    #     t0.setCount(len(summaryJobs))

    compResults = []
    with Timer("compare summaries") as t1:
        for singleJob in summaryJobs:
            res = compareSummaries(singleJob)
            compResults.extend(res)

    with Timer("compare simulation wide summaries") as t2:
        for singleJob in simSummaryJobs:
            res = compareSimSummaries(singleJob)
            compResults.extend(res)

    outResList = map(_transformResult, compResults)
    resDF = pd.DataFrame(outResList).fillna(0.0)

    resFileFormat = f"SummaryTest_results_{cMgr.getConfigVar('scenarioTimestampFormat')}.csv"
    resFilename = dt.datetime.now().strftime(resFileFormat)
    resDF = resDF.assign(scenarioTimestamp=cMgr.getConfigVar('scenarioTimestamp'))
    resDF.to_csv(resFilename, index=False)
    logging.info(f"Wrote {resFilename}")


if __name__ == "__main__":
    cMgr, args = au.getConfig()
    if not args.scenarioTimestamp:
        au.findMostRecentScenario(cMgr)
    main(cMgr)