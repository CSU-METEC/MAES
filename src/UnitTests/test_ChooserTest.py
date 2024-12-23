import unittest
import Chooser

class Trinary():
    def __init__(self, modVal=3):
        self.counter = 0
        self.modVal = modVal

    def incr(self):
        self.counter += 1
        return self.counter % self.modVal


class RandomDistChooser(unittest.TestCase):

    def basicTester(self, ch, dist):
        vals = dict.fromkeys(dist.keys(), 0)
        numIter = 1000000
        for i in range(numIter):
            randKey = ch.randomChoice()
            vals[randKey] += 1
        for singleKey in dist.keys():
            self.assertAlmostEqual(vals[singleKey] / numIter, dist[singleKey], delta=0.01)


    def test_Basics(self):
        dist = {'A': .1, 'B': .2, 'C': .2, 'D': .5}
        ch = Chooser.EmpiricalDistChooser(dist)
        self.basicTester(ch, dist)

    def test_Singleton(self):
        elt = 'A'
        dist = {elt: 1.00}
        ch = Chooser.SingletonChooser(elt)
        self.basicTester(ch, dist)

    def test_Funarg(self):
        tVal = Trinary(4)
        dist = {0: 0.25, 1: 0.25, 2: 0.25, 3: 0.25}
        ch = Chooser.FunctionalChooser(lambda: tVal.incr())
        self.basicTester(ch, dist)
    
    def test_FractionalDist(self):
        """ Tests empirical distribution with a distribution that contains fractions"""
        # print(1/3 + 1/3 + 1/9 + 2/9, '!=', 1/9 + 2/9 + 1/3 + 1/3) #These are not equal
        dist = {'A': 1/3, 'B': 1/3, 'C': 1/9, 'D': 2/9} #  this dist sums up to 0.9999999999999999 because they are floating point numbers
        ch = Chooser.EmpiricalDistChooser(dist)
        self.basicTester(ch, dist)
        dist = {'A': 2/9, 'B': 1/9, 'C': 1/3, 'D': 1/3}
        ch = Chooser.EmpiricalDistChooser(dist)
        self.basicTester(ch, dist)
        
    def test_NonNormalizedDistribution(self):
        """ Tests a distribution that doens't sum to 1.0 """
        dist = {'A': .5, 'B': .2}  # doesn't sum to 1.0
        self.assertRaises(ValueError, lambda: Chooser.EmpiricalDistChooser(dist)) #Empiral distribution will raise a ValueError due Sum not equaling to 1

    def test_SmallDecimal(self):
        """ Tests empirical distribution chooser with small decimal numbers"""
        dist = {'A': .000001, 'B': 0.999999}
        ch = Chooser.EmpiricalDistChooser(dist)
        self.basicTester(ch, dist)

    def test_SmallFraction(self):
        """ tests empirical     distribution chooser with small fractional numbers"""
        dist = {'A': 1/10000, 'B': 9999/10000}
        ch = Chooser.EmpiricalDistChooser(dist)
        self.basicTester(ch, dist)

    def test_LotsOfValues(self):
        """ tests empirical distribution chooser with a distribution with lots of keys"""
        numVals=1000
        dist = {k : 1/numVals for k in range(numVals)}
        ch = Chooser.EmpiricalDistChooser(dist)
        self.basicTester(ch, dist)

