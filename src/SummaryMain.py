import GraphUtils as gu
import AppUtils as au
import logging

logger = logging.getLogger(__name__)

def dumpSummary(df, config, tagName):
    outFile = au.expandFilename(config[tagName], config)
    df.to_csv(outFile, index=False)
    logger.info(f"Wrote {outFile}")

def main(config):
    logging.basicConfig(level=logging.INFO)

    coalesceEventTSDF, newConfig = gu.readCompleteEvents(config)

    stateTiming = gu.calculateStateTiming(coalesceEventTSDF)
    dumpSummary(stateTiming, newConfig, 'stateSummaryTemplate')

    ffDF, ffRollupDF = gu.calculateFluidFlows(coalesceEventTSDF)
    dumpSummary(ffDF, newConfig, 'ffSummaryTemplate')
    dumpSummary(ffRollupDF, newConfig, 'ffRollupTemplate')

    emitterPT, emissionRollupPT = gu.calculateEmissions(coalesceEventTSDF)
    dumpSummary(emitterPT, newConfig, 'emitterSummaryTemplate')
    dumpSummary(emissionRollupPT, newConfig, 'emissionSummaryTemplate')


if __name__ == "__main__":
    config, args = au.getConfig()
    if not args.scenarioTimestamp:
        config = gu.findMostRecentScenario(config, args)
    main(config)