import unittest
from MEETClasses import DESEnabled, StateBasedAccumulator

class MockDESEnabled(DESEnabled):
    def __init__(self, initialState='INITIAL', **kwargs):
        super().__init__(**kwargs)
        self.state = initialState

    def getCurrentState(self):
        return self.state

    def setState(self, state):
        self.state = state

    def getStateMachine(self):
        pass

class StateBasedAccumulatorTest(unittest.TestCase):

    def test_SingleEmission(self):
        emitter = MockDESEnabled()
        accumulator = StateBasedAccumulator(emitter=emitter)
        emissionList = [{'timestamp': 100, 'name': 'emitter1', 'emissionCategory': 'CAT_1', 'emissionRateValuePerSecond': 1, 'emissionRateUnits': 'scf'}]
        accumulator.updateEmissions(emissionList)
        accumEmissions = accumulator.getEmissions()
        self.assertEqual(1, len(accumEmissions))
        singleAccum = accumEmissions[0]
        self.assertEqual(singleAccum['timestamp'], 100, 'timestamp')
        self.assertEqual(singleAccum['accumulatedEmissionValue'], 0, 'emission')

    def test_MultiEmission(self):
        emitter = MockDESEnabled()
        accumulator = StateBasedAccumulator(emitter=emitter)
        emissionList1 = [{'timestamp': 100, 'name': 'emitter1', 'emissionCategory': 'CAT_1', 'emissionRateValuePerSecond': 1, 'emissionRateUnits': 'scf'}]
        accumulator.updateEmissions(emissionList1)
        emissionList2 = [{'timestamp': 200, 'name': 'emitter1', 'emissionCategory': 'CAT_1', 'emissionRateValuePerSecond': 0, 'emissionRateUnits': 'scf'}]
        accumulator.updateEmissions(emissionList2)
        accumEmissions = accumulator.getEmissions()
        self.assertEqual(1, len(accumEmissions))
        singleAccum = accumEmissions[0]
        self.assertEqual(singleAccum['timestamp'], 200)
        self.assertEqual(singleAccum['accumulatedEmissionValue'], 100)

    def test_SingleEmissionTimestamp(self):
        emitter = MockDESEnabled()
        accumulator = StateBasedAccumulator(emitter=emitter)
        emissionList = [{'timestamp': 100, 'name': 'emitter1', 'emissionCategory': 'CAT_1', 'emissionRateValuePerSecond': 1, 'emissionRateUnits': 'scf'}]
        accumulator.updateEmissions(emissionList)
        accumEmissions = accumulator.handleTimestep(200)
        accumulator.reset()
        singleAccum = accumEmissions[0]
        self.assertEqual(singleAccum['timestamp'], 200, 'timestamp')
        self.assertEqual(singleAccum['accumulatedEmissionValue'], 100, 'emission')

        accumEmissions = accumulator.handleTimestep(300)
        accumulator.reset()
        singleAccum = accumEmissions[0]
        self.assertEqual(singleAccum['timestamp'], 300, 'timestamp')
        self.assertEqual(singleAccum['accumulatedEmissionValue'], 100, 'emission')
