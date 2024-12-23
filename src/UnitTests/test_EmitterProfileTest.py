import numpy as np
import unittest
import EmitterProfile as ep

NUMRUNS = 100000
# testInputFiles = "C:/GTI-CAMS/DES1/UnitTests/testInput/{FileName}"
testInputFiles = "testInput/{FileName}"

class EmitterProfileTest(unittest.TestCase):

    def test_constantDistribution(self):
        fName = testInputFiles.format(FileName="testConstantEP.csv")
        epInstance = ep.EmitterProfile.readEmitterFile(fName)
        self.assertTrue(epInstance)
        self.assertEqual(12.5, epInstance.distribution.pick())
        print(epInstance.md)
        print(epInstance.distribution)

    def test_normalDistribution(self):
        fName = testInputFiles.format(FileName="testNormalEP.csv")
        epInstance = ep.EmitterProfile.readEmitterFile(fName)
        self.assertTrue(epInstance)
        print(epInstance.distribution.pick())
        pass

    def test_logNormalDistribution(self):
        fName = testInputFiles.format(FileName="testLogNormalEP.csv")
        epInstance = ep.EmitterProfile.readEmitterFile(fName)
        self.assertTrue(epInstance)
        print(epInstance.distribution.pick())
        pass

    def test_TriangularDistribution(self):
        fName = testInputFiles.format(FileName="testTriangularEP.csv")
        epInstance = ep.EmitterProfile.readEmitterFile(fName)
        self.assertTrue(epInstance)
        print(epInstance.distribution.pick())
        pass

    def test_ScaledDistribution(self):
        fName = testInputFiles.format(FileName="testScaledEP.csv")
        epInstance = ep.EmitterProfile.readEmitterFile(fName)
        self.assertTrue(epInstance)
        print(epInstance.distribution.pick())
        pass

    def test_ExponentialDistribution(self):
        fName = testInputFiles.format(FileName="testExponentialEP.csv")
        epInstance = ep.EmitterProfile.readEmitterFile(fName)
        self.assertTrue(epInstance)
        print(epInstance.distribution.pick())
        pass

    def test_SampledDistribution(self):
        fName = testInputFiles.format(FileName="testSampledEP.csv")
        epInstance = ep.EmitterProfile.readEmitterFile(fName)
        self.assertTrue(epInstance)
        print(epInstance.distribution.pick())
        pass

    def test_HistogramDistribution(self):

        fName = testInputFiles.format(FileName="testHistogram5050EP.csv")
        epInstance = ep.EmitterProfile.readEmitterFile(fName)
        self.assertTrue(epInstance)
        print(epInstance.distribution.pick())
        tally = {0:0, 1:0}
        for i in range(0, NUMRUNS):
            p = epInstance.distribution.pick()
            tally[p] +=1
        tally[0] = tally[0] / NUMRUNS
        tally[1] = tally[1] / NUMRUNS
        np.testing.assert_almost_equal(tally[0], 0.5, decimal=2)
        np.testing.assert_almost_equal(tally[1], 0.5, decimal=2)
        print(tally)

        fName = testInputFiles.format(FileName="testHistogram_1vs10k_EP.csv")
        epInstance = ep.EmitterProfile.readEmitterFile(fName)
        self.assertTrue(epInstance)
        print(epInstance.distribution.pick())
        tally = {0:0, 1:0}
        for i in range(0,NUMRUNS):
            p = epInstance.distribution.pick()
            tally[p] +=1
        tally[0] = tally[0] / NUMRUNS
        tally[1] = tally[1] / NUMRUNS
        np.testing.assert_almost_equal(tally[0], 1/NUMRUNS, decimal=2)
        np.testing.assert_almost_equal(tally[1], 1, decimal=2)
        print(tally)

        fName = testInputFiles.format(FileName="testHistogram_10x10_EP.csv")
        epInstance = ep.EmitterProfile.readEmitterFile(fName)
        self.assertTrue(epInstance)
        print(epInstance.distribution.pick())
        tally = {0:0, 1:0, 2:0, 3:0, 4:0, 5:0, 6:0, 7:0, 8:0, 9:0}
        for i in range(0, NUMRUNS):
            p = epInstance.distribution.pick()
            tally[p] +=1
        for i in range(0, 10):
            tally[i] = tally[i] / NUMRUNS
            np.testing.assert_almost_equal(tally[i], 0.10, decimal=2)
        print(tally)

        pass
