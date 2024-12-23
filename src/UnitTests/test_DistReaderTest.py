import unittest
import matplotlib.pyplot as plt
import Distribution as dist
import numpy as np

PLOT_TEMPLATE = "../output/{shortName}.png"
NUMRUNS = 100000


class DistributionReaderTest(unittest.TestCase):

    def _distTest(self, dist, runs, shortName):
        vals = []
        for _ in range(runs):
            vals.append(dist.pick())
        self.assertTrue(dist.test(vals))
        return vals

    def graphDist(self, vals, shortName):
        plt.hist(vals, bins=200)
        plt.show()
        plt.savefig(PLOT_TEMPLATE.format(shortName=shortName))
        plt.close()

    def _checkNormalDistribution(self, vals, mu, sigma):
        """ Checks for a normal distribution in values given mu and sigma """
        vals.sort()
        left, right = None, None
        for v in range(NUMRUNS):
            if left is None and vals[v] > mu-sigma:
                left = v
            if vals[v] > mu+sigma:
                right=v
                break
        self.assertAlmostEqual((right-left)/NUMRUNS, .68, delta=.02)


    def test_basic_distributions(self):
        constDist = dist.distFactory(20.0)
        self._distTest(constDist, NUMRUNS, 'constant')

        normalDist = dist.distFactory({'distribution': 'normal', 'mu': 5, 'sigma': 1})
        vals = self._distTest(normalDist, NUMRUNS, 'normal')
      #  self.graphDist(vals, 'normal')

        logNormalDist = dist.distFactory({'distribution': 'lognormal', 'mu': 5, 'sigma': 1})
        self._distTest(logNormalDist, NUMRUNS, 'lognormal')

        triDist = dist.distFactory({'distribution': 'triangular', 'min': 1, 'mean': 4, 'max': 11})
        self._distTest(triDist, NUMRUNS, 'triangular')

        normalDist = dist.distFactory({'distribution': 'normal', 'mu': 15000000, 'sigma': 2500000})
        self._distTest(normalDist, NUMRUNS, 'normal2')

        exponentialDist = dist.distFactory({'distribution': 'exponential', 'scale': 3})
        self._distTest(exponentialDist, NUMRUNS, 'exponential')

    def test_distribution_bounds(self):
        normalDist = dist.distFactory({'distribution': 'normal', 'mu': 5, 'sigma': 1})
        vals = self._distTest(normalDist, NUMRUNS, 'normal')

    def test_bad_distribution_values(self):
        """ These are bad distribution values. they should not create with the given values."""
        def bad_normalDist():
            normalDist = dist.distFactory({'distribution': 'normal', 'mu': 1, 'sigma': -1})
        def bad_lognormalDist():
            logNormalDist = dist.distFactory({'distribution': 'lognormal', 'mu': 5, 'sigma': -1})
        def bad_triDist():
            triDist = dist.distFactory({'distribution': 'triangular', 'min': 0, 'mean': 0, 'max': 0})
            triDist = dist.distFactory({'distribution': 'triangular', 'min': 0, 'mean': 0, 'max': 0})
            triDist = dist.distFactory({'distribution': 'triangular', 'min': 0, 'mean': 2, 'max': 1})
        def bad_exponentialDist():
            exponentialDist = dist.distFactory({'distribution': 'exponential', 'scale': -3})
        self.assertRaises(Exception, bad_normalDist())
        self.assertRaises(Exception, bad_lognormalDist())
        self.assertRaises(Exception, bad_triDist())
        self.assertRaises(Exception, bad_exponentialDist())

    def test_normalDistribution(self):
        """ Check if distribution is normal by ensuring sigma correctly represents standard deviation
            68% of numbers should fall between mean-sigma and mean+sigma
        """
        mu=1
        sigma=1
        normalDist = dist.distFactory({'distribution': 'normal', 'mu': mu, 'sigma': sigma})
        vals = []
        for _ in range(NUMRUNS):
            vals.append(normalDist.pick())
        self._checkNormalDistribution(vals, mu, sigma)

    def test_triangleDistribution(self):
        """ Checks if triangular distribution by checking if the slope of one side is linear"""
        leftx, modex, rightx = 0, 50, 100
        triDist = dist.distFactory({'distribution': 'triangular', 'min': leftx, 'mean': modex, 'max': rightx})
        numbins = 21
        middleBinIndex = int(numbins/2)
        bins = [0]*numbins
        for i in range(2*NUMRUNS):
            pick = triDist.pick()
            bins[int(pick/(rightx-leftx)*numbins)] += 1
        b = bins[0]
        m = (bins[middleBinIndex]-bins[0])/middleBinIndex
        for x in range(11):
            self.assertAlmostEqual(bins[x], int(m*x)+b, delta=bins[x]*.1)

    def test_lognormalDistribution(self):
        """ Tests if lognormal.
            The natural log of a lognormal distribution should be normally distributed
        """
        mu=0
        sigma=1
        lognormalDist = dist.distFactory({'distribution': 'lognormal', 'mu': mu, 'sigma': sigma})
        vals = []
        for _ in range(NUMRUNS):
            vals.append(np.log(lognormalDist.pick()))
        self._checkNormalDistribution(vals, mu, sigma)

    def test_exponentialDistribution(self):
        """ Tests exponential distribution
            ln(4/3), ln(2), and ln(4) represent the cut offs between the 4 quartiles.
        """
        exponential = dist.distFactory({'distribution': 'exponential', 'scale': 1})
        firstQ = 0
        secondQ = 0
        thirdQ = 0
        fourthQ = 0
        for _ in range(NUMRUNS):
            p = exponential.pick()
            if p<np.log(4/3):
                firstQ +=1
            elif p < np.log(2):
                secondQ += 1
            elif p < np.log(4):
                thirdQ += 1
            else:
                fourthQ += 1

        self.assertAlmostEqual(firstQ, NUMRUNS/4, delta=NUMRUNS*.05)
        self.assertAlmostEqual(secondQ, NUMRUNS/4, delta=NUMRUNS*.05)
        self.assertAlmostEqual(thirdQ, NUMRUNS/4, delta=NUMRUNS*.05)
        self.assertAlmostEqual(fourthQ, NUMRUNS/4, delta=NUMRUNS*.05)

class DistributionReaderTest2(unittest.TestCase):
    def test_multitest(self):
        self.assertTrue(True, "It worked!")


