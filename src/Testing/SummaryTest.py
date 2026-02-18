import pandas as pd
import logging
from Timer import Timer
import SiteMain2 as sm
import AppUtils as au
import json


VALUE_EPSILON = 0.01

def _readOldSummaries(config):
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
        # 'AggregatedSimulationEmissions': {
        #     'typeList': ['category', 'METype', 'modelReadableName', 'unitID'],
        #     'fileFormat': 'aggregated_sim_emissions_by_{type}_abnormal_{abnormal}'
        # }
    }
    SUMMARY_FILE_TEMPLATE = "{simulationRoot}/summaries/{summaryType}/{siteDir}/{fname}.csv"

    simulationRoot = config['simulationRoot']
    siteDir = f"site={config['siteName']}"

    ret = {}
    for singleSummary, summaryData in SUMMARY_LAYOUTS.items():
        for singleType in summaryData['typeList']:
            for abnormal in ['on', 'off']:
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

def _doComparisons(comparisonDF, siteName, oldSummaryKey):
    roMissing = (comparisonDF['_merge'] == 'right_only') & (comparisonDF['mean'] != 0.0)
    loMissing = (comparisonDF['_merge'] == 'left_only')
    missingItems = roMissing | loMissing
    missingItemCount = 0
    if missingItems.any():
        missingItemCount = missingItems.sum()
        logging.warning(f"  missing items {oldSummaryKey=},  out of {len(comparisonDF)=} {missingItemCount=}")
    # exclude any 'roMissing' values from further consideration
    roSpurious = (comparisonDF['_merge'] == 'right_only') & (comparisonDF['mean'] == 0.0)
    comparisonDF = comparisonDF[~roSpurious]

    outOfRange = (comparisonDF['delta'] > VALUE_EPSILON)
    outOfRangeCount = 0
    if outOfRange.any():
        outOfRangeCount = outOfRange.sum()
        logging.warning(f"  out of range {oldSummaryKey=},  out of {len(comparisonDF)=} {outOfRangeCount=}")

    countsDiffer = (comparisonDF['compCount'] != comparisonDF['count'])
    countsDifferCount = 0
    if countsDiffer.any():
        countsDifferCount = countsDiffer.sum()
        logging.warning(f"  counts differ {oldSummaryKey=},  out of {len(comparisonDF)=} {countsDifferCount=}")

    thisRet = {
        'siteName': siteName,
        'oldSummaryKey': oldSummaryKey,
        'missingItemCount': int(missingItemCount),
        'outOfRangeCount': int(outOfRangeCount),
        'countsDifferCount': int(countsDifferCount)
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
        newSummaryKey = OLD_SUMMARY_TO_NEW_SUMMARY_MAP.get(oldSummaryKey, None)

        oldSummaryDF = oldSummaryDict[oldSummaryKey]

        oldSummaryCategory, oldSummaryType, oldSummaryAbnormal = oldSummaryKey
        oldSummaryDF = oldSummaryDF.assign(compCount=oldSummaryDF['MCRuns_emission_list'].apply(lambda x: len(json.loads(x))))
        # Filter out summary rows
        oldSummaryDF = oldSummaryDF[~oldSummaryDF[oldSummaryType].isin(newSummaryKey['sumRows'])]

        newSummaryColumn = newSummaryKey['summaryColumn']
        newSummaryMask = (
                (newSummaryDF['CICategory'] == newSummaryKey['CICategory'])
                & (~newSummaryDF[newSummaryColumn].isna())
                & (newSummaryDF['includeFugitive'] == newSummaryKey['includeFugitive'])
                & (newSummaryDF['units'] == 'mt/year')
                & (newSummaryDF['mcRun'].isna())
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

def doAggregatedEmissionComparison(siteName, oldSummaryDict, newSummaryDF):
    OLD_SUMMARY_TO_NEW_SUMMARY_MAP = {
        ('AnnualEmissions', 'modelReadableName', 'on'):
            {'CICategory': 'modelReadableName', 'includeFugitive': True,
             'comparisonHierarchy': ['METype', 'unitID', 'modelReadableName']},
        ('AnnualEmissions', 'modelReadableName', 'off'):
            {'CICategory': 'modelReadableName', 'includeFugitive': False,
             'comparisonHierarchy': ['METype', 'unitID', 'modelReadableName']},
    }

    retList = []
    for oldSummaryKey, newSummaryKey in OLD_SUMMARY_TO_NEW_SUMMARY_MAP.items():
        oldSummaryType, oldSummaryCategory, oldSummaryAbnormal = oldSummaryKey
        newSummaryKey = OLD_SUMMARY_TO_NEW_SUMMARY_MAP.get(oldSummaryKey, None)

        oldSummaryDF = oldSummaryDict[oldSummaryKey]

        flaredGasMalfunctionCount = 0
        if oldSummaryAbnormal == 'off':
            flaredGasMalfunctionMask = oldSummaryDF['modelReadableName'] == 'Flared Gas Malfunction'
            oldSummaryDF = oldSummaryDF[~flaredGasMalfunctionMask]
            if flaredGasMalfunctionMask.any():
                flaredGasMalfunctionCount = flaredGasMalfunctionMask.sum()
                logging.warning(f"  flared gas malfunctions detected, {oldSummaryKey=}, out of {len(oldSummaryDF)=} {flaredGasMalfunctionCount=}")
        else:
            flaredGasMalfunctionCount = 0


        newSummaryMask = (
                (newSummaryDF['CICategory'] == newSummaryKey['CICategory'])
                & (newSummaryDF['includeFugitive'] == newSummaryKey['includeFugitive'])
                & (newSummaryDF['units'] == 'mt/year')
                & (newSummaryDF['mcRun'].isna())
        )
        newSummarySubsetDF = newSummaryDF[newSummaryMask]

        compNewDF = newSummarySubsetDF[~newSummarySubsetDF['modelReadableName'].isna()]
        compOldDF = oldSummaryDF[oldSummaryDF['modelReadableName'] != 'summed_modelReadableName']
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

def compareSummaries(job):
    siteName = job['siteName']
    logging.info(f"Comparing {siteName=}")
    # read old summary files
    oldSummaryDict = _readOldSummaries(job)
    # read new summary files
    newSummaryDF = _readNewSummaries(job)

    annualRet = doAnnualEmissionComparison(siteName, oldSummaryDict, newSummaryDF)
    aggregatedRet = doAggregatedEmissionComparison(siteName, oldSummaryDict, newSummaryDF)

    ret = [*annualRet, *aggregatedRet]
    return ret

def main(cMgr):
    logging.basicConfig(level=logging.INFO, format=au.LOG_FORMAT)
    workitemQueues = sm.generateWorkitems(cMgr, phasesToInclude=['summarize'])
    summaryJobs = workitemQueues[0]
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

    resDF = pd.DataFrame(compResults).fillna(0.0)

    pass


if __name__ == "__main__":
    cMgr, args = au.getConfig()
    if not args.scenarioTimestamp:
        au.findMostRecentScenario(cMgr)
    main(cMgr)