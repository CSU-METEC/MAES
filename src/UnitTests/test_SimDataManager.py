import EmissionDriver as ed
import SimDataManager as sdm
import unittest
import pandas as pd
from datetime import datetime

FAKE_CONFIG = {
    'scenarioTimestampFormat': "%Y%m%d_%H%M%S",
    "inputDir":  			   "../MEET2Input",
    "outputDir":               "testOutput",
    "scenarioTimestampFormat": "%Y%m%d_%H%M%S",
    "emitterProfileDir":       "{inputDir}/DefaultData",
    "MCTemplateDir":           "{outputDir}/MC_{scenarioTimestamp}/template/",
}

def getConfig():
    retConfig = {**FAKE_CONFIG, 'scenarioTimestamp': datetime.now().strftime(FAKE_CONFIG['scenarioTimestampFormat'])}
    return retConfig

class BasicSDMTest(unittest.TestCase):

    def simpleEDTest(self, d1ts):
        self.assertEqual(1.5, d1ts.instantaneousEmission(0))
        self.assertEqual(1.5, d1ts.instantaneousEmission(100))
        self.assertEqual(15, d1ts.integratedEmission(0, 10))
        self.assertEqual(15, d1ts.integratedEmission(1, 11))
        self.assertEqual(15, d1ts.integratedEmission(1005, 1015))

    def tsEDTest(self, d2ts):
        self.assertEqual(0, d2ts.instantaneousEmission(0))
        self.assertEqual(10, d2ts.instantaneousEmission(10))
        self.assertEqual(20, d2ts.instantaneousEmission(20))
        self.assertEqual(10, d2ts.instantaneousEmission(30))
        self.assertEqual(0, d2ts.instantaneousEmission(40))

        self.assertEqual(400, d2ts.integratedEmission(0, 40))
        self.assertEqual(300, d2ts.integratedEmission(10, 30))
        self.assertEqual(375, d2ts.integratedEmission(5, 35))

    def test_basicEmissionDriver(self):
        d1 = ed.EmissionDriver('ed1', '../MEET2Input/DefaultData/Sample/EmissionDrivers/TestDriver1.csv')
        d1ts = d1.pick()
        self.simpleEDTest(d1ts)

        d2 = ed.EmissionDriver('ed2', '../MEET2Input/DefaultData/Sample/EmissionDrivers/TestTimeseriesDriver.csv')
        d2ts = d2.pick()
        self.tsEDTest(d2ts)

    def driverTestTemplate(self, name, compFn):
        sdm1 = sdm.JsonSimDataManager(getConfig())
        driver1Key = sdm1.getDriver4EC(name)
        key1, ts1 = sdm1.getTimeseries(driver1Key)
        compFn(ts1)
        ts2 = sdm1.getTimeseriesByKey(key1)
        compFn(ts2)

        key3, ts3 = sdm1.getTimeseries(driver1Key)
        self.assertNotEqual(key1, key3)  # getTimeseries() does a pick, so keys must be different...
        compFn(ts3)                      # ... even though they return the same result


    def test_SDMEmissionDriver(self):
        self.driverTestTemplate('Sample/EmissionDrivers/TestDriver1.csv', self.simpleEDTest)
        self.driverTestTemplate('Sample/EmissionDrivers/TestTimeseriesDriver.csv', self.tsEDTest)

    def verifyGC(self, gcDist):
        species = gcDist.convert(1.0, "scf_wholegas")
        self.assertTrue(1.0, species.get('METHANE', None))
        self.assertTrue(2.0, species.get('ETHANE', None))
        self.assertTrue(3.0, species.get('FAKESPECIES', None))

        species = gcDist.convert(2.0, "scf_wholegas")
        self.assertTrue(2.0, species.get('METHANE', None))
        self.assertTrue(4.0, species.get('ETHANE', None))
        self.assertTrue(6.0, species.get('FAKESPECIES', None))

    def test_basicGC(self):
        sdm1 = sdm.JsonSimDataManager(getConfig())
        gcKey1, gcDist1 = sdm1.getGasComposition('Sample/GasCompositions/TestGasComposition.csv')
        self.verifyGC(gcDist1)
        gcDist2 = sdm1.getGasCompositionByKey(gcKey1)
        self.verifyGC(gcDist2)

    def test_basicEventList(self):
        sdm1 = sdm.JsonSimDataManager(getConfig())
        driver1Key = sdm1.getDriver4EC('Sample/EmissionDrivers/TestDriver1.csv')
        tsKey1, ts1 = sdm1.getTimeseries(driver1Key)
        gcKey1, gcDist1 = sdm1.getGasComposition('Sample/GasCompositions/TestGasComposition.csv')
        myInterval = (0, 100)
        with sdm1.eventLogWriteCache() as eh:
            eh.logEmission( 0, 25, 'dev1', tsKey1, gcKey1)
            eh.logEmission(25, 25, 'dev1', tsKey1, gcKey1)
            eh.logEmission(50, 25, 'dev1', tsKey1, gcKey1)
            eh.logEmission(75, 25, 'dev1', tsKey1, gcKey1)
        eh = sdm1.getEventLog()
        events = eh.queryIntervals([myInterval])
        speciesDF = pd.DataFrame(map(lambda x: sdm1.evalEmissionEvent(x, myInterval), events))
        speciesSums = speciesDF.apply(lambda x: x.sum())
        self.assertEqual(150, speciesSums['METHANE'])
        self.assertEqual(300, speciesSums['ETHANE'])
        self.assertEqual(450, speciesSums['FAKESPECIES'])
        pass




