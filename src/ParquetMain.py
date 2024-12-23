import GraphUtils as gu
import AppUtils as au
import logging
from Timer import Timer
import ParquetLib as pl
import SiteMain2 as sm


logger = logging.getLogger(__name__)

def main(cMgr):
    logging.basicConfig(level=logging.INFO)
    workItems = sm.generateSummaryWorkitems(cMgr)
    summaryWorkitems = workItems[0]
    # for singleSummaryWorkitem in summaryWorkItems:
    #     logging.info(f"Processing study {singleSummaryWorkitem['studyName']}, site {singleSummaryWorkitem['siteName']}, mcIter: {singleSummaryWorkitem['MCIteration']}")
    #     with Timer(f"To parquet") as t0:
    #         pl.toParquet(singleSummaryWorkitem)

    # summaryWorkitems = workItems[3]
    for singleSumWorkitem in summaryWorkitems:
        pl.postprocess(singleSumWorkitem)


if __name__ == "__main__":
    cMgr, args = au.getConfig()
    if not args.scenarioTimestamp:
        au.findMostRecentScenario(cMgr)
    main(cMgr)