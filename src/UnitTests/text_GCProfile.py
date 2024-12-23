import GasComposition2 as gc
import unittest

class BasicGCTest(unittest.TestCase):

    def test_basics(self):
        gComp = gc.GasComposition({'default': {'METHANE': 1.0, 'ETHANE': 2.0}})
        species = gComp.convert(1, 'default')
        pass