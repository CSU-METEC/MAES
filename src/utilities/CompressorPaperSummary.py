import AppUtils as au
import pandas as pd

DEFAULT_CONFIG = "config/MEET2Config.json"

SIM_TIMESTAMP = "20201215_105529"

def main(config):
    fullConfig = {**config, 'scenarioTimestamp': SIM_TIMESTAMP}
    summaryFilename = au.expandFilename(config['Summary'], fullConfig, readonly=True)
    summaryDF = pd.read_excel(summaryFilename, sheet_name="state_summary")
    mdFilename = au.expandFilename2(config['MetadataFile'], fullConfig, readonly=True)
    mdDF = pd.read_csv(mdFilename)
    pass


if __name__ == "__main__":
    config, _ = au.getConfig(DEFAULT_CONFIG)
    main(config)