import unittest
import DistributionProfile as dp

TEST_FLASH_FILENAME = "testInput/FlashCompositions_site5.csv"

class BasicFlashComp(unittest.TestCase):

    def test_ReadFlashComp(self):
        fComp = dp.FlashComposition.readFile(TEST_FLASH_FILENAME)
        self.assertIsNotNone(fComp)
        testGCName = 'Well-Condensate.Stage1-CondensateFlash'
        scf = fComp.convertUnits(testGCName, 1, 'bbl', 'scf')
        self.assertIsNotNone(scf)
        gcBbl = fComp.calculateGasComposition(testGCName, 1, 'bbl')
        gcSCF = fComp.calculateGasComposition(testGCName, scf, 'scf')
        for singleSpeciesBbl, singleValBbl in gcBbl.items():
            singleValSCF = gcSCF.get(singleSpeciesBbl, None)
            self.assertIsNotNone(singleValSCF)
            self.assertAlmostEqual(singleValBbl, singleValSCF)
