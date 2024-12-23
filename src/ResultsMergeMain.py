import logging
import SimDataManager as sdm
import AppUtils as au

def main(config):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s  %(message)s")
    with sdm.SimDataManager(config) as simdm:
        simdm.restore(0)
        eventLog = simdm.rawEventLog
        gasComp = simdm.rawGasCompositionTable

        emissionEvents = eventLog[eventLog['command'] == 'EMISSION']
        emissionKeys = set(emissionEvents.gcFingerprint)
        gcKeys = set(gasComp.gcKey)

        pass

def preMain():
    config, _ = au.getConfig()
    main(config)

if __name__ == "__main__":
    preMain()