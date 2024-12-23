import itertools
import random as r
from bisect import bisect_right
from abc import ABC, abstractmethod
from math import isclose
#
# Given a distribution, choose an element of that distribution at random
#

class Chooser(ABC):

    @abstractmethod
    def randomChoice(self):
        pass


#
# distributions are specified as a dictionary:
#  {<key1>: <prob1>, <key2>: prob2, ...}
#
# Probabilities should add up to 1
#

class EmpiricalDistChooser(Chooser):
    def __init__(self, dist):
        if not isclose(sum(dist.values()), 1, abs_tol=0.01):
            raise ValueError("Distribution must equal to 1 +/- 0.01")
        self.dist = dist.copy()
        self.cumulativeDist = list(itertools.accumulate(self.dist.values()))
        self.keys = list(self.dist.keys())

    def randomChoice(self, **kwargs):
        rand = r.random()
        i = bisect_right(self.cumulativeDist, rand)
        if i >= len(self.keys):
            return self.keys[0]
        return self.keys[i]

class UnscaledEmpiricalDistChooser(EmpiricalDistChooser):
    def __init__(self, unscaledDist):
        total = sum(map(lambda x: x[1], unscaledDist.items()))
        if total == 0:
            soloItem = list(unscaledDist.items())[0][0]
            newDist = {soloItem: 1.0}
        else:
            newDist = dict(map(lambda x: (x[0], x[1] / total), unscaledDist.items()))
        super().__init__(newDist)


class SingletonChooser(Chooser):
    def __init__(self, singleton):
        self.singleton = singleton

    def randomChoice(self, **kwargs):
        return self.singleton

class FunctionalChooser(Chooser):
    def __init__(self, funArg):
        self.funArg = funArg

    def randomChoice(self, **kwargs):
        try:
            return self.funArg(**kwargs)
        except Exception as e:
            raise e



