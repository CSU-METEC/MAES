import unittest
from ModelFormulation import toParamKey



class ParamKeyTest(unittest.TestCase):

    def validateParamKey(self, pKey, key, units=None, val=None):
        self.assertEqual(pKey['valKey'], key)
        if units is not None:
            self.assertEqual(pKey['units'], units)
        if val is not None:
            self.assertEqual(pKey['val'], val)

    def test_SimpleParamKeys(self):
        self.validateParamKey(toParamKey("Facility ID"), "facility_id")
        self.validateParamKey(toParamKey("Facility  ID"), "facility_id")
        self.validateParamKey(toParamKey(" Facility  ID"), "facility_id")
        self.validateParamKey(toParamKey("Facility  ID "), "facility_id")
        self.validateParamKey(toParamKey(" Facility  ID "), "facility_id")

    def test_ParamKeyCapitalization(self):
        self.validateParamKey(toParamKey("Facility ID"), "facility_id")
        self.validateParamKey(toParamKey("FACILITY ID"), "facility_id")
        self.validateParamKey(toParamKey("FACILITY id"), "facility_id")
        self.validateParamKey(toParamKey("facility id"), "facility_id")

    def test_ParamKeyUnits(self):
        self.validateParamKey(toParamKey("Facility ID [units]"), "facility_id", units='units')
        self.validateParamKey(toParamKey("Facility ID[units]"), "facility_id", units='units')

    def test_ParamKeySyntaxErrors(self):
        self.assertRaises(Exception, toParamKey, "Facility ID [units")
        self.assertRaises(Exception, toParamKey, "Facility ID units]")
