import unittest
from EventLogger import EventLogger
from StateManager import StateManager, PostFactoStateManager

class MemoryEventLogger(EventLogger):
    def __init__(self):
        super().__init__('', '')
        self.eventList = []

    def logRawEvent(self, timestamp, name, command, **kwargs):
        event = {'eventID': self.eventSerialNumber, 'timestamp': timestamp, 'name': name, 'command': command, **kwargs}
        self.eventSerialNumber += 1
        self.eventList.append(event)
        return event['eventID']

class DummyEventLogger(EventLogger):
    def __init__(self):
        super().__init__('', '')

    def logRawEvent(self, timestamp, name, command, **kwargs):
        eventID = self.eventSerialNumber
        self.eventSerialNumber += 1
        return eventID


def checkEvents(ePair):
    expected = ePair[0]
    actual = ePair[1]
    ret = (expected['timestamp'] == actual['timestamp']
            and expected['state'] == actual['state']
            and expected['event'] == actual['event']
            )
    return ret

class Trinary():
    def __init__(self, modVal=3):
        self.counter = 0
        self.modVal = modVal

    def incr(self):
        self.counter += 1
        return self.counter % self.modVal


class StateManagerTest(unittest.TestCase):

    def test_SimpleStateTable(self):
        stateTable = {
            'STATE1': {'stateDuration': 10, 'nextState': 'STATE2'},
            'STATE2': {'stateDuration': 20, 'nextState': 'STATE3'},
            'STATE3': {'stateDuration': 30, 'nextState': 'DONE'},
            'DONE': {'stateDuration': 0, 'nextState': 'DONE'}
        }

        expectedEvents = [
            {'timestamp': 0, 'state': 'STATE1', 'event': 'START'},
            {'timestamp': 10, 'state': 'STATE1', 'event': 'STOP'},
            {'timestamp': 10, 'state': 'STATE2', 'event': 'START'},
            {'timestamp': 30, 'state': 'STATE2', 'event': 'STOP'},
            {'timestamp': 30, 'state': 'STATE3', 'event': 'START'},
            {'timestamp': 60, 'state': 'STATE3', 'event': 'STOP'},
            {'timestamp': 60, 'state': 'DONE', 'event': 'START'}
        ]

        with MemoryEventLogger() as eh:
            sMgr = StateManager("dev1", stateTable, eh)
            curTime = 0
            while sMgr.currentState != 'DONE':
                delay = sMgr.calcNextState(curTime)
                curTime += delay

        map(checkEvents, zip(expectedEvents, eh.eventList))

        assert (all(map(checkEvents, zip(expectedEvents, eh.eventList))))

    def test_forceNextState(self):
        stateTable = {
            'STATE1': {'stateDuration': 10, 'nextState': 'STATE2'},
            'STATE2': {'stateDuration': 20, 'nextState': 'STATE3'},
            'STATE3': {'stateDuration': 30, 'nextState': 'DONE'},
            'DONE':   {'stateDuration': 0, 'nextState': 'DONE'}
        }

        expectedEvents = [
            {'timestamp':  0, 'state': 'STATE1',  'event': 'START'},
            {'timestamp': 10, 'state': 'STATE1',  'event': 'STOP'},
            {'timestamp': 10, 'state': 'STATE2',  'event': 'START'},
            {'timestamp': 20, 'state': 'STATE2',  'event': 'STOP'},
            {'timestamp': 20, 'state': 'DONE',    'event': 'START'}
        ]

        with MemoryEventLogger() as eh:
            sMgr = StateManager("dev1", stateTable, eh)
            curTime = 0
            while sMgr.currentState != 'DONE':
                delay = sMgr.calcNextState(curTime)
                if sMgr.currentState == 'STATE2':
                    curTime += 10
                    delay = sMgr.calcNextState(curTime, 'DONE')
                curTime += delay

        map(checkEvents, zip(expectedEvents, eh.eventList))

        assert(all(map(checkEvents, zip(expectedEvents, eh.eventList))))

    def test_stochasticNextState(self):
        stateTable = {
            'STATE1': {'stateDuration': 10, 'nextState': {'STATE1': 0.50, 'STATE2': 0.50}},
            'STATE2': {'stateDuration': 20, 'nextState': {'STATE1': 0.50, 'STATE2': 0.50}},
        }

        stateCounter = {'STATE1': 0, 'STATE2': 0}
        numTransitions = 100000

        with DummyEventLogger() as eh:
            sMgr = StateManager("dev1", stateTable, eh)
            for i in range(numTransitions):
                sMgr.calcNextState(1)
                stateCounter[sMgr.currentState] += 1

        self.assertAlmostEqual(stateCounter['STATE1'] / numTransitions, 0.5, delta=0.01)
        self.assertAlmostEqual(stateCounter['STATE2'] / numTransitions, 0.5, delta=0.01)


    def test_stochasticNextStateUnequalProb(self):
        stateProbs = {'STATE1': 0.10, 'STATE2': 0.60, 'STATE3': 0.30}
        stateTable = {
            'STATE1': {'stateDuration': 1, 'nextState': stateProbs},
            'STATE2': {'stateDuration': 1, 'nextState': stateProbs},
            'STATE3': {'stateDuration': 1, 'nextState': stateProbs},
        }

        stateCounter = dict.fromkeys(stateProbs.keys(), 0)
        numTransitions = 100000

        with DummyEventLogger() as eh:
            sMgr = StateManager("dev1", stateTable, eh)
            for i in range(numTransitions):
                sMgr.calcNextState(1)
                stateCounter[sMgr.currentState] += 1

        for stateName, stateProb in stateProbs.items():
            actualStateVal = stateCounter[stateName] / numTransitions
            self.assertAlmostEqual(actualStateVal, stateProb, delta=0.01)

    def test_functionalArg(self):
        tVal = Trinary(4)

        stateTable = {
            0: {'stateDuration': 1, 'nextState': lambda: tVal.incr()},
            1: {'stateDuration': 1, 'nextState': lambda: tVal.incr()},
            2: {'stateDuration': 1, 'nextState': lambda: tVal.incr()},
            3: {'stateDuration': 1, 'nextState': lambda: tVal.incr()},
        }

        stateCounter = {0: 0, 1: 0, 2: 0, 3: 0}
        numTransitions = 100000

        with DummyEventLogger() as eh:
            sMgr = StateManager("dev1", stateTable, eh)
            for i in range(numTransitions):
                sMgr.calcNextState(1)
                stateCounter[sMgr.currentState] += 1

        self.assertAlmostEqual(stateCounter[0] / numTransitions, 0.25, delta=0.01)
        self.assertAlmostEqual(stateCounter[1] / numTransitions, 0.25, delta=0.01)
        self.assertAlmostEqual(stateCounter[2] / numTransitions, 0.25, delta=0.01)
        self.assertAlmostEqual(stateCounter[3] / numTransitions, 0.25, delta=0.01)

def synthesizeStateEvents(transitionList, initialState):
    simTime = 0
    currentState = initialState
    synthEventList = [{'timestamp': simTime, 'state': currentState, 'event': 'START'}]
    for singleTransition in transitionList:
        simTime += singleTransition['stateDuration']
        synthStopEvent = {'timestamp': simTime, 'state': currentState, 'event': 'STOP'}
        synthEventList.append(synthStopEvent)
        currentState = singleTransition['nextState']
        synthStopEvent = {'timestamp': simTime, 'state': currentState, 'event': 'START'}
        synthEventList.append(synthStopEvent)
    return synthEventList[:-2]

class StateCounter():
    def __init__(self, stateName):
        self.stateName = stateName
        self.entries = 0
        self.exits = 0

    def entryHook(self, currentTime, currentState, stateData, **kwargs):
        self.entries += 1

    def exitHook(self, currentTime, currentState, stateData, **kwargs):
        self.exits += 1

def checkCounts(singleStateData):
    ret = True
    counter = singleStateData['counter']
    if singleStateData['expectedEntries'] != counter.entries:
        ret = False
    if singleStateData['expectedExits'] != counter.exits:
        ret = False
    return ret


class PostFactoStateManagerTest(unittest.TestCase):

    def test_basicPFStateManager(self):
        stateTable = {
            'STATE1': {},
            'STATE2': {},
            'STATE3': {},
            'DONE':   {}
        }

        stateTransitions = [
            {'stateDuration': 10, 'nextState': 'STATE2'},
            {'stateDuration': 20, 'nextState': 'STATE1'},
            {'stateDuration': 30, 'nextState': 'STATE3'},
            {'stateDuration': 40, 'nextState': 'STATE2'},
            {'stateDuration': 50, 'nextState': 'DONE'},
        ]

        startState = 'STATE1'

        currentTime = 0
        with MemoryEventLogger() as eh:
            sMgr = PostFactoStateManager("dev1", stateTable, eh, startState)
            for singleTransition in stateTransitions:
                currentTime += singleTransition['stateDuration']
                sMgr.closePrevState(currentTime, singleTransition['nextState'])

        expectedEvents = synthesizeStateEvents(stateTransitions, startState)
        pairedEvents = list(zip(expectedEvents, eh.eventList))
        assert (all(map(checkEvents, pairedEvents)))

    def test_basicPFStateManagerWithHooks(self):
        state1Counter = StateCounter('STATE1')
        state2Counter = StateCounter('STATE2')
        state3Counter = StateCounter('STATE3')
        doneCounter = StateCounter('DONE')

        stateTable = {
            'STATE1': {'entryHook': state1Counter.entryHook, 'exitHook': state1Counter.exitHook, 'counter': state1Counter, 'expectedEntries': 2, 'expectedExits': 2},
            'STATE2': {'entryHook': state2Counter.entryHook, 'exitHook': state2Counter.exitHook, 'counter': state2Counter, 'expectedEntries': 3, 'expectedExits': 3},
            'STATE3': {'entryHook': state3Counter.entryHook, 'exitHook': state3Counter.exitHook, 'counter': state3Counter, 'expectedEntries': 1, 'expectedExits': 1},
            'DONE':   {'entryHook': doneCounter.entryHook,   'exitHook': doneCounter.exitHook,   'counter': doneCounter,   'expectedEntries': 0, 'expectedExits': 0}
        }

        stateTransitions = [
            {'stateDuration': 10, 'nextState': 'STATE2'},
            {'stateDuration': 20, 'nextState': 'STATE1'},
            {'stateDuration': 30, 'nextState': 'STATE3'},
            {'stateDuration': 40, 'nextState': 'STATE2'},
            {'stateDuration': 50, 'nextState': 'STATE2'},
            {'stateDuration': 60, 'nextState': 'DONE'},
        ]

        startState = 'STATE1'

        currentTime = 0
        with MemoryEventLogger() as eh:
            sMgr = PostFactoStateManager("dev1", stateTable, eh, startState)
            for singleTransition in stateTransitions:
                currentTime += singleTransition['stateDuration']
                sMgr.closePrevState(currentTime, singleTransition['nextState'])

        expectedEvents = synthesizeStateEvents(stateTransitions, startState)
        pairedEvents = list(zip(expectedEvents, eh.eventList))
        assert(all(map(checkEvents, pairedEvents)))
        assert(all(map(checkCounts, stateTable.values())))
