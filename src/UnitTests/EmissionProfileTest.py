import unittest
from StoredProfile import Driver4ECProfile


class GCTests(unittest.TestCase):
    def test_BasicEmissionProfile(self):
        ep1 = Driver4ECProfile.readProfile("testInput/testConstantEmission.csv")
        self.assertEqual((0.01, 'scf_whole_gas'), ep1.instantaneousEmission(0))
        self.assertEqual((0.01, 'scf_whole_gas'), ep1.instantaneousEmission(0, emissionName='emissionRate1'))
        self.assertEqual((0.02, 'scf_whole_gas'), ep1.instantaneousEmission(0, emissionName='emissionRate2'))
        self.assertEqual((0.03, 'scf_whole_gas'), ep1.instantaneousEmission(0, emissionName='emissionRate3'))
        
        self.assertEqual(ep1.integratedEmission(0, 10), (0.1, 'scf_whole_gas'))
        self.assertEqual(ep1.integratedEmission(0, 10, emissionName='emissionRate1'), (0.1, 'scf_whole_gas'))
        self.assertEqual(ep1.integratedEmission(0, 10, emissionName='emissionRate2'), (0.2, 'scf_whole_gas'))
        self.assertEqual(ep1.integratedEmission(0, 10, emissionName='emissionRate3'), (0.3, 'scf_whole_gas'))

        pass