import GraphUtils as gu
import AppUtils as au
import logging
from Timer import Timer
import ParquetLib as pl
import SiteMain2 as sm


logger = logging.getLogger(__name__)

def main(cMgr):
    logging.basicConfig(level=logging.INFO)
    # workitemQueues = sm.generateWorkitems(cMgr, phasesToInclude=['parquet', 'summarize'])
    workitemQueues = sm.generateWorkitems(cMgr, phasesToInclude=['summarize'])
    sm.main(cMgr, workitemQueues=workitemQueues)


if __name__ == "__main__":
    cMgr, args = au.getConfig()
    if not args.scenarioTimestamp:
        au.findMostRecentScenario(cMgr)
    main(cMgr)