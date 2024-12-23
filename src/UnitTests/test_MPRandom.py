#
# Test for randomness in multiprocessing support
#

import unittest
import AppUtils as au
import random
import os
import numpy as np
import logging
import time

RANDOM_SEQ_LEN = 10
MC_ITERATIONS = 2

def mpInitForceNonrandom(fake):
    random.seed(1)
    np.random.seed(1)
    pass

def mpInit(fake):
    pass

def mpArgGen(mcRuns):
    for i in range(mcRuns):
        yield (i,)

def mpFun(mcRunNum):
    ret = []
    for i in range(RANDOM_SEQ_LEN):
        v = {'pid': os.getpid(), 'runNum': mcRunNum, 'i': i,
             'rand': random.random(), 'npRand': np.random.rand()}
        ret.append(v)
    time.sleep(2)  # need to sleep to ensure that the MP pool runs each job in a separate process
    return ret

class MPRandom(unittest.TestCase):

    def test_singleProcessRand(self):
        res = au.runJob(mpFun, mpArgGen(MC_ITERATIONS), init=mpInit, initArgs=(None,), numProcs=1)
        pass

    def test_multiProcessRand(self):
        logging.basicConfig(level=logging.INFO)
        random.seed()
        np.random.seed()
        res = au.runJob(mpFun, mpArgGen(MC_ITERATIONS), init=mpInit, initArgs=(None,), numProcs=MC_ITERATIONS)
        procSet = set()
        for singleRes in res:
            procSet.add(singleRes[0]['pid'])
        self.assertEqual(len(procSet), MC_ITERATIONS)  # ensure that results show that each sequence was generated in a separate process
        pass

    def test_multiProcessSameSeed(self):
        logging.basicConfig(level=logging.INFO)
        random.seed()
        np.random.seed()
        res = au.runJob(mpFun, mpArgGen(MC_ITERATIONS), init=mpInitForceNonrandom, initArgs=(None,), numProcs=MC_ITERATIONS)
        procSet = set()
        for singleRes in res:
            procSet.add(singleRes[0]['pid'])
        self.assertEqual(len(procSet), MC_ITERATIONS)  # ensure that results show that each sequence was generated in a separate process
        pass

if __name__ == '__main__':
    unittest.main()
