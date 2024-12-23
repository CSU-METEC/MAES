import GasComposition2 as gc
import StoredProfile as sp
import unittest

TIMESERIES_TABLE = {}
GASCOMP_TABLE = None

def evalEmissionEvent(eEvent):
    t0 = eEvent['timestamp']
    t1 = t0 + eEvent['duration']
    ts = TIMESERIES_TABLE[eEvent['timeseries']]

    emission = ts.integratedEmission(t0, t1)
    speciesEmissions = GASCOMP_TABLE.convert(*emission)

    pass

class BasicEETest(unittest.TestCase):

    def test_BasicDriver4ECEvaluation(self):
        d4EC = sp.Driver4ECProfile.readProfile("testInput/testBasicDist4EC.csv")
        d4ECInstance = d4EC.pick()
        val, units = d4ECInstance.integratedEmission(0, 10)
        self.assertEqual(101.0, val)
        self.assertEqual('scf_wholegas', units)

    def test_basicGCProfile(self):
        gComp = gc.GasComposition({'scf_wholegas': {'METHANE': 1.0, 'ETHANE': 2.0}})
        species = gComp.convert(1, 'scf_wholegas')
        self.assertEqual(1.0, species['METHANE'])
        self.assertEqual(2.0, species['ETHANE'])
        species = gComp.convert(2, 'scf_wholegas')
        self.assertEqual(2.0, species['METHANE'])
        self.assertEqual(4.0, species['ETHANE'])

    def test_emissionEventEvaluation(self):
        global TIMESERIES_TABLE
        global GASCOMP_TABLE

        d4EC = sp.Driver4ECProfile.readProfile("testInput/testBasicDist4EC.csv")
        d4ECTimeseries = d4EC.pick()
        TIMESERIES_TABLE['ts1'] = d4ECTimeseries
        gComp = gc.GasComposition({'scf_wholegas': {'METHANE': 1.0, 'ETHANE': 2.0}})
        GASCOMP_TABLE = gComp

        emissionEvent = {'emitter': 'emitter1', 'timestamp': 0, 'duration': 50, 'timeseries': 'ts1', 'fingerprint': 'default'}

        emissionForEmitter = evalEmissionEvent(emissionEvent)
        pass
