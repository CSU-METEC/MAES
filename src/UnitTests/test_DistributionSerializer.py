import unittest
import Distribution as d
import json

class DistributionReaderTest(unittest.TestCase):

    def test_NormalDistributionSerializer(self):
        d1 = d.distFactory({'distribution': 'normal', 'mu': 5, 'sigma': 2})
        serD1 = json.dumps(d1, cls=d.JSONEncode)
        d2 = json.loads(serD1, cls=d.JSONDecode)
        self.assertEqual(d1, d2)

    def test_lognormalDistributionSerializer(self):
            d1 = d.distFactory({'distribution': 'lognormal', 'mu': 5, 'sigma': 1})
            serD1 = json.dumps(d1, cls=d.JSONEncode)
            d2 = json.loads(serD1, cls=d.JSONDecode)
            self.assertEqual(d1, d2)


    def test_triangularDistributionSerializer(self):
        d1 = d.distFactory({'distribution': 'triangular', 'min': 1, 'mean': 4, 'max': 11})
        serD1 = json.dumps(d1, cls=d.JSONEncode)
        d2 = json.loads(serD1, cls=d.JSONDecode)
        self.assertEqual(d1, d2)

    def test_exponentialDistributionSerializer(self):
        d1 = d.distFactory({'distribution': 'exponential', 'scale': 3})
        serD1 = json.dumps(d1, cls=d.JSONEncode)
        d2 = json.loads(serD1, cls=d.JSONDecode)
        self.assertEqual(d1, d2)