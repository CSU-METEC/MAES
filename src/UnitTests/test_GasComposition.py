import unittest
import GasComposition3 as gc3
import SimDataManager as sdm

TEST_FLASH_FILENAME = "UnitTests/testInput/testGasCompositions.csv"

sdm.SimDataManager.initStubSimDataManager()

class BasicFlashComp(unittest.TestCase):

    def test_basicGC(self):
        testGC = gc3.FluidFlowGC(fluidFlowGCFilename=TEST_FLASH_FILENAME,
                               flow='Condensate',
                               fluidFlowID='Well-Condensate',
                               gcUnits='bbl')
        self.assertIsNotNone(testGC.serialForm())

    def test_GCConversion(self):
        testGC = gc3.FluidFlowGC(fluidFlowGCFilename=TEST_FLASH_FILENAME,
                               flow='Condensate',
                               fluidFlowID='Well-Condensate',
                               gcUnits='bbl')
        newGC, conversion = testGC.convert('scf')
        self.assertIsNotNone(newGC)
        self.assertIsNotNone(conversion)

