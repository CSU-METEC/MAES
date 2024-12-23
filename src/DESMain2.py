import logging
import simpy
from MEETClasses import DESEnabled
from Timer import Timer

def applyToDESEnabled(eqList, fn):
    for singleEQ in eqList:
        if isinstance(singleEQ, DESEnabled):
            fn(singleEQ)

class Tally():
    def __init__(self):
        self.total = 0

    def tally(self, val):
        self.total += val

def main(simdm, mcRunNum=''):

    # initialize simulation environment & create emitter objects

    env = simpy.Environment()
    eqList = simdm.getEquipmentTable().getEquipment(mcRunNum=mcRunNum)
    with simdm.eventLogWriteCache() as eventHandler:
        eventHandler.logRawEvent(0, (None, None, None, mcRunNum), "SIM-START")
        applyToDESEnabled(eqList, lambda x: x.initializeDES(simdm, env, eventHandler))

        with Timer("DES events") as t1:
            env.run(until=simdm.config['simDurationSeconds'])
            eventHandler.logRawEvent(int(env.now), (None, None, None, mcRunNum), "SIM-STOP")
            eventTotal = Tally()
            applyToDESEnabled(eqList, lambda x: eventTotal.tally(x.numEvents))
            t1.setCount(eventTotal.total)

    pass

def preMain():
    logging.basicConfig(level=logging.DEBUG)
    pass

if __name__ == "__main__":
    preMain()