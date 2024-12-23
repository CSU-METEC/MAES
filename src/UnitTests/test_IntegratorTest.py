import unittest
from Integrator import ConstantIntegrator, EmpericalIntegrator
import EmissionAccumulator as eu
import itertools as it

class BasicIntegrator(unittest.TestCase):

    def test_basics(self):
        ci = ConstantIntegrator(2)
        print(ci.integrate(0,1))
        self.assertEqual(ci.integrate(0, 1), (2, 'scf_whole_gas', None, None))
        self.assertEqual(ci.integrate(0, 10), (20, 'scf_whole_gas', None, None))
        self.assertEqual(ci.integrate(10, 30), (40, 'scf_whole_gas', None, None))

    def test_emperical(self):
        ei = EmpericalIntegrator([1, 2, 3, 4, 5])
        self.assertEqual(ei.integrate(0, 5), (15, 'scf_whole_gas', None, None))
        self.assertEqual(ei.integrate(0, 1), (1, 'scf_whole_gas', None, None))
        self.assertEqual(ei.integrate(1, 5), (14, 'scf_whole_gas', None, None))

def grouper(iterable, n):
    args = [iter(iterable)] * n
    return it.zip_longest(fillvalue=0, *args)

class BasicEmissions(unittest.TestCase):
    def test_basicEmitter(self):
        ecToTest = 'COMBUSTED'
        eRate = 10
        eTime1 = 100
        eTime2 = 1000
        ci = ConstantIntegrator(eRate)
        em = eu.EmissionAccumulator()
        em.setIntegrator(ecToTest, ci)
        eMass1 = em.timestamp(eTime1)
        self.assertEqual(eMass1[ecToTest], eRate*eTime1)
        prevMass = eMass1[ecToTest]
        eMass2 = em.timestamp(eTime2)
        self.assertEqual(eMass2[ecToTest], (eRate*eTime2)-prevMass)

    def test_empiricalEmitter(self):
        ecToTest = 'FUGITIVE'
        ePattern = [1, 2, 3, 4, 5, 5, 4, 3, 2, 1]
        mPattern = sum(ePattern) / len(ePattern)
        eTime1 = 100
        ei = EmpericalIntegrator(ePattern)
        em = eu.EmissionAccumulator()
        em.setIntegrator(ecToTest, ei)
        eMass1 = em.timestamp(eTime1)
        self.assertEqual(eMass1[ecToTest], mPattern*eTime1)

    def test_empiricalEmitterStep(self):
        ecToTest='VENTED'
        ePattern = [1, 2, 3, 4, 5, 5, 4, 3, 2, 1]
        ei = EmpericalIntegrator(ePattern)
        em = eu.EmissionAccumulator()
        em.setIntegrator(ecToTest, ei)
        res = []
        for i in range(2, len(ePattern)+2, 2):
            emassI = em.timestamp(i)
            res.append(emassI[ecToTest])
        expected = list(map(lambda x: sum(x), grouper(ePattern, 2)))
        self.assertEqual(res, expected)

class EmissionEvents(unittest.TestCase):

    def test_intermittent1(self):
        ecToTest = 'MITIGATED'
        eRate = 10
        eTime = 100

        em = eu.EmissionAccumulator()

        leakingIntegrator = eu.EmissionAccumulator.newIntegratorTable()
        ciL = ConstantIntegrator(eRate)
        em.setIntegrator(ecToTest, ciL, leakingIntegrator)

        notLeakingIntegrator = eu.EmissionAccumulator.newIntegratorTable()
        ciNL = ConstantIntegrator(0.0)
        em.setIntegrator(ecToTest, ciNL, notLeakingIntegrator)

        em.handleEmission(ecToTest, 0, leakingIntegrator)  # START
        em.handleEmission(ecToTest, 50, leakingIntegrator)  # STOP
        em.handleEmission(ecToTest, 50, notLeakingIntegrator)  # START
        eMass1 = em.timestamp(eTime)
        self.assertEqual(eMass1[ecToTest], eRate * (eTime / 2))

    def test_intermittent2(self):
        ecToTest = 'MITIGATED'
        eRate = 10
        eTime = 100

        em = eu.EmissionAccumulator()

        leakingIntegrator = eu.EmissionAccumulator.newIntegratorTable()
        ciL = ConstantIntegrator(eRate)
        em.setIntegrator(ecToTest, ciL, leakingIntegrator)

        notLeakingIntegrator = eu.EmissionAccumulator.newIntegratorTable()
        ciNL = ConstantIntegrator(0.0)
        em.setIntegrator(ecToTest, ciNL, notLeakingIntegrator)

        em.handleEmission(ecToTest,  0, notLeakingIntegrator)    # START
        em.handleEmission(ecToTest, 50, notLeakingIntegrator)    # STOP
        em.handleEmission(ecToTest, 50, leakingIntegrator)       # START
        eMass1 = em.timestamp(eTime, leakingIntegrator)
        self.assertEqual(eMass1[ecToTest], eRate*(eTime/2))
