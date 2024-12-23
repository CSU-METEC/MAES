import unittest
from StoredProfile import Driver4ECProfile, ActivityProfile

def isNumeric(val):
    return isinstance(val, float) or isinstance(val, int)

class GeneratedProfileTest(unittest.TestCase):

    def test_genAF(self):
        ap1 = ActivityProfile.readProfile("testInput/AFTest.csv")
        val1 = ap1.pick()
        self.assertTrue(True, isNumeric(val1))
        pass

    def test_genEF(self):
        ep1 = Driver4ECProfile.readProfile("testInput/EFTest.csv")
        val1 = ep1.pick()
        self.assertTrue(True, isNumeric(val1))
        pass

