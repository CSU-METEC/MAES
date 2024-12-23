import unittest
import ModelFormulation as mf
import logging

class BasicModelFormulationTest(unittest.TestCase):

    def test_basicFormulation(self):
        logging.basicConfig(level=logging.INFO)
        rawIntake = mf.parseIntakeSpreadsheet("../input/ModelFormulation/BasicModelFormulation.xlsx")
        self.assertTrue('masterEquipment' in rawIntake)
        self.assertTrue('modelFormulation' in rawIntake)
        self.assertTrue('simParameters' in rawIntake)

        intakeInstance = mf.instantiateIntake(rawIntake)

        pass





