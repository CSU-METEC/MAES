import AppUtils as au
import ParquetLib as pl
import SiteMain2 as sm
import logging

logger = logging.getLogger(__name__)

def main(cMgr):
    logging.basicConfig(level=logging.INFO)
    config = sm.configFromConfigMgr(cMgr)
    eventDF = pl.readParquetEvents(config)
    #
    # eventDF has the events for the simulation.  Include your processing below
    #
    pass
    #Calculating Pivot Table
    eventPT = eventDF[eventDF['command'] == 'STATE_TRANSITION'].pivot_table(index=['unitID','state'], values='duration', aggfunc = {'count','min','max','mean'})
    eventPT.to_csv('intermittentEmitter.csv')




if __name__ == "__main__":
    cMgr, args = au.getConfig()
    if not args.scenarioTimestamp:
        au.findMostRecentScenario(cMgr)
    main(cMgr)
