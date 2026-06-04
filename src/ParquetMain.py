import os
# Cap BLAS / OpenMP threads BEFORE pandas/numpy/pyarrow are imported (see
# SiteMain2.py for explanation). GraphUtils below imports pandas, so this must
# stay above every other import in this entry-point file.
for _var in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
             "BLIS_NUM_THREADS", "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ.setdefault(_var, "1")

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