import GraphUtils as gu
import AppUtils as au
import logging
from Timer import Timer
import ParquetLib as pl
import SiteMain2 as sm


logger = logging.getLogger(__name__)

def main(cMgr):
    logging.basicConfig(level=logging.INFO, format=au.LOG_FORMAT)
    # workitemQueues = sm.generateWorkitems(cMgr, phasesToInclude=['parquet', 'summarize'])
    # workitemQueues = sm.generateWorkitems(cMgr, phasesToInclude=['simSummary'])
    workitemQueues = sm.generateWorkitems(cMgr, phasesToInclude=['parquet', 'summarize', 'simSummary'])
    sm.main(cMgr, workitemQueues=workitemQueues)


if __name__ == "__main__":
    cMgr, args = au.getConfig()
    if not args.scenarioTimestamp:
        au.findMostRecentScenario(cMgr)
    main(cMgr)