import os
# Cap BLAS / OpenMP thread counts to 1 BEFORE pandas/numpy/pyarrow load (GraphUtils
# below imports pandas). Forked workers inherit these caps instead of each spawning
# N threads and oversubscribing the host. Must stay above every other import here.
for envVar in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
               "BLIS_NUM_THREADS", "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ.setdefault(envVar, "1")

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
    # workitemQueues = sm.generateWorkitems(cMgr, phasesToInclude=['summarize', 'simSummary'])
    workitemQueues = sm.generateWorkitems(cMgr, phasesToInclude=['simSummary'])
    # workitemQueues = sm.generateWorkitems(cMgr, phasesToInclude=['parquet', 'summarize', 'simSummary'])
    # workitemQueues = sm.generateWorkitems(cMgr, phasesToInclude=['createPDFCache'])
    sm.main(cMgr, workitemQueues=workitemQueues)


if __name__ == "__main__":
    cMgr, args = au.getConfig()
    if not args.scenarioTimestamp:
        au.findMostRecentScenario(cMgr)
    main(cMgr)