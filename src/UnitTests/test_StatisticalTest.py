import AppUtils as au
import argparse
import json
from datetime import datetime
import EventLogger as el
import EquipmentTable as eq
import os
from EquipmentTable import EquipmentTableEntry
import MEETClasses as mc

SCENARIO_TS = '20200827_190042'
MC_DIR = 0
DEFAULT_CONFIG = "config/SimpleGEFTSCompressorWithStateBasedEmittersActivityProfile.json"

def getArgs(defaultConfig):

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-c", "--config", default=defaultConfig, help="configuration file")
    args = parser.parse_args()
    return args

def getConfig(defaultConfig=DEFAULT_CONFIG):
    args = getArgs(defaultConfig)
    configFile = args.config
    with open(configFile, "r") as cf:
        config = json.load(cf)
    if 'scenarioTimestampFormat' in config:
        scenarioTimestamp = datetime.now().strftime(config['scenarioTimestampFormat'])
        config = {**config, 'scenarioTimestamp': scenarioTimestamp}
    return config, args

def main(config):
    eventLogFilename = au.expandFilename(config['DESEventLog'], {**config, 'scenarioTimestamp': SCENARIO_TS, 'MCScenario': 0})
    scenarioDir = au.expandFilename(config['MCScenarioDir'], {**config, 'scenarioTimestamp': SCENARIO_TS, 'MCScenario': 0})
    timingTable = {}
    with el.EventLogger(eventLogFilename, "r") as eLogger, \
         eq.EquipmentTableObsolete.restore(scenarioDir) as eqTable:
        for singleEvent in eLogger.streamEvents():
            if singleEvent['command'] != 'STATE_TRANSITION' or singleEvent['event'] != 'START':
                continue
            devName = singleEvent['name']
            equipmentRecord = eqTable.getEquipmentByName(devName)
            if not isinstance(equipmentRecord, mc.GEFTSCompressor):
                continue
            if devName not in timingTable:
                timingTable[devName] = [dict()]
            state = singleEvent['state']
            duration = singleEvent['nextTS'] - singleEvent['timestamp']
            if state == 'OPERATING':
                thisEntry = {'OPERATING': duration}
                timingTable[devName].append(thisEntry)
            else:
                thisEntry = timingTable[devName][-1]
                thisEntry[state] = duration
            i = 1

    pass

if __name__ == "__main__":
    os.chdir("..")
    config, _ = getConfig()
    main(config)
