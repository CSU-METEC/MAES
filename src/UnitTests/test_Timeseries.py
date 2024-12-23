import unittest
import Timeseries as ts
import random
import pandas as pd
from Timer import Timer
import logging
import Units as u

logging.basicConfig(level=logging.INFO)

class BasicTSTest(unittest.TestCase):

    def genTS(self, tsName, tsUnits, dictList):
        ts1 = ts.TimeseriesRLE.fromDictList(dictList, name=tsName, units=tsUnits)
        return ts1

    def genTS1(self):
        ts1Name = "ts1"
        ts1Units = "kg/s"
        ts1 = self.genTS(ts1Name, ts1Units, [
            {'timestamp': 3,  'nextTS':  7, 'tsValue':     10},
            {'timestamp': 11, 'nextTS': 14, 'tsValue':      8},
            {'timestamp': 18, 'nextTS': 19, 'tsValue':      3},
            {'timestamp': 23, 'nextTS': 25, 'tsValue':      7},
            {'timestamp': 27, 'nextTS': 30, 'tsValue':      3},
            {'timestamp': 50, 'nextTS': 60, 'tsValue': -20.02}
            ])
        return ts1, ts1Name, ts1Units

    def genTS2(self):
        ts2Name = "ts2"
        ts2Units = "kg/s"
        ts2 = self.genTS(ts2Name, ts2Units, [
            {'timestamp':  2, 'nextTS':  6, 'tsValue':     0},
            {'timestamp':  6, 'nextTS':  8, 'tsValue':     4},
            {'timestamp':  8, 'nextTS':  9, 'tsValue':    20},
            {'timestamp':  9, 'nextTS': 12, 'tsValue':     4},
            {'timestamp': 14, 'nextTS': 17, 'tsValue': 20.01},
            {'timestamp': 17, 'nextTS': 25, 'tsValue':     7},
            {'timestamp': 32, 'nextTS': 36, 'tsValue':  0.01},
            {'timestamp': 38, 'nextTS': 53, 'tsValue': 20.02},
        ])
        return ts2, ts2Name, ts2Units

    def test_ts(self):
        ts1, ts1Name, ts1Units = self.genTS1()
        self.assertEqual(ts1Name, ts1._name)
        self.assertEqual(ts1Units, ts1._units)

        ts2, ts2Name, ts2Units = self.genTS2()
        self.assertEqual(ts2Name, ts2._name)
        self.assertEqual(ts2Units, ts2._units)

    def test_basicOps(self):
        ts1, _, _ = self.genTS1()
        ts2, _, _ = self.genTS2()

        self.assertNotEqual(ts1, ts2)

        tsSum = ts1.addSquare(ts2)

        tsExpected = self.genTS("expected", "kg/s", [
                         {'timestamp':  3, 'nextTS':  6, 'tsValue':     10},
                         {'timestamp':  6, 'nextTS':  7, 'tsValue':     14},
                         {'timestamp':  7, 'nextTS':  8, 'tsValue':      4},
                         {'timestamp':  8, 'nextTS':  9, 'tsValue':     20},
                         {'timestamp':  9, 'nextTS': 11, 'tsValue':      4},
                         {'timestamp': 11, 'nextTS': 12, 'tsValue':     12},
                         {'timestamp': 12, 'nextTS': 14, 'tsValue':      8},
                         {'timestamp': 14, 'nextTS': 17, 'tsValue':  20.01},
                         {'timestamp': 17, 'nextTS': 18, 'tsValue':      7},
                         {'timestamp': 18, 'nextTS': 19, 'tsValue':     10},
                         {'timestamp': 19, 'nextTS': 23, 'tsValue':      7},
                         {'timestamp': 23, 'nextTS': 25, 'tsValue':     14},
                         {'timestamp': 27, 'nextTS': 30, 'tsValue':      3},
                         {'timestamp': 32, 'nextTS': 36, 'tsValue':   0.01},
                         {'timestamp': 38, 'nextTS': 50, 'tsValue':  20.02},
                         {'timestamp': 53, 'nextTS': 60, 'tsValue': -20.02},
        ])

        self.assertEqual(tsSum, tsExpected)

    def genUnsortedTS1(self):
        ts1Name = "ts1"
        ts1Units = "kg/s"
        ts1 = self.genTS(ts1Name, ts1Units, [
            {'timestamp': 11, 'nextTS': 14, 'tsValue':      8},
            {'timestamp': 3,  'nextTS':  7, 'tsValue':     10},
            {'timestamp': 18, 'nextTS': 19, 'tsValue':      3},
            {'timestamp': 23, 'nextTS': 25, 'tsValue':      7},
            {'timestamp': 27, 'nextTS': 30, 'tsValue':      3},
            {'timestamp': 50, 'nextTS': 60, 'tsValue': -20.02}
            ])
        return ts1, ts1Name, ts1Units

    def test_unsortedException(self):
        self.assertRaises(ts.MalformedTimeseriesError, self.genUnsortedTS1)

    def test_resample(self):
        manTs1 = ts.TimeseriesRLE.fromCollections([0], [36000], [3600])
        manTS2 = manTs1.periodicAverage(range(0, 3600*11, 3600))

        ts1, _, _ = self.genTS1()
        tAvg = list(range(0, 36, 4))
        resTs1 = ts1.periodicAverage(tAvg)
        pass


class LongSeriesTest(unittest.TestCase):

    def genTS1(self):
        tsList = [
            {'timestamp':  3, 'nextTS': 10, 'tsValue': 1},
            {'timestamp': 25, 'nextTS': 40, 'tsValue': 1}
        ]
        ts1DF = pd.DataFrame.from_records(tsList)
        ts1TS = ts.TimeseriesRLE(ts1DF, name='TS1')
        return ts1TS

    def genTS2(self):
        tsList = [
            {'timestamp':  5, 'nextTS':  15, 'tsValue': 2},
            {'timestamp': 30, 'nextTS':  35, 'tsValue': 2},
            {'timestamp': 60, 'nextTS':  70, 'tsValue': 2},
            {'timestamp': 80, 'nextTS': 100, 'tsValue': 2}
        ]
        ts2DF = pd.DataFrame.from_records(tsList)
        ts2TS = ts.TimeseriesRLE(ts2DF, name='TS1')
        return ts2TS

    def genTS2Adjoining(self):
        tsList = [
            {'timestamp':  5, 'nextTS':  15, 'tsValue': 2},
            {'timestamp': 30, 'nextTS':  35, 'tsValue': 2},
            {'timestamp': 35, 'nextTS':  50, 'tsValue': 3},
            {'timestamp': 60, 'nextTS':  70, 'tsValue': 2},
            {'timestamp': 80, 'nextTS': 100, 'tsValue': 2}
        ]
        ts2DF = pd.DataFrame.from_records(tsList)
        ts2TS = ts.TimeseriesRLE(ts2DF, name='TS1')
        return ts2TS

    def genResTS(self):
        tsList = [
            {'timestamp':  3, 'nextTS':   5, 'tsValue': 1},
            {'timestamp':  5, 'nextTS':  10, 'tsValue': 3},
            {'timestamp': 10, 'nextTS':  15, 'tsValue': 2},
            {'timestamp': 25, 'nextTS':  30, 'tsValue': 1},
            {'timestamp': 30, 'nextTS':  35, 'tsValue': 3},
            {'timestamp': 35, 'nextTS':  40, 'tsValue': 1},
            {'timestamp': 60, 'nextTS':  70, 'tsValue': 2},
            {'timestamp': 80, 'nextTS': 100, 'tsValue': 2},

        ]
        resDF = pd.DataFrame.from_records(tsList)
        resTS = ts.TimeseriesRLE(resDF, name='TS1')
        return resTS

    def genResTSAdjoining(self):
        tsList = [
            {'timestamp':  3, 'nextTS':   5, 'tsValue': 1},
            {'timestamp':  5, 'nextTS':  10, 'tsValue': 3},
            {'timestamp': 10, 'nextTS':  15, 'tsValue': 2},
            {'timestamp': 25, 'nextTS':  30, 'tsValue': 1},
            {'timestamp': 30, 'nextTS':  35, 'tsValue': 3},
            {'timestamp': 35, 'nextTS':  40, 'tsValue': 4},
            {'timestamp': 40, 'nextTS':  50, 'tsValue': 3},
            {'timestamp': 60, 'nextTS':  70, 'tsValue': 2},
            {'timestamp': 80, 'nextTS': 100, 'tsValue': 2},

        ]
        resDF = pd.DataFrame.from_records(tsList)
        resTS = ts.TimeseriesRLE(resDF, name='TS1')
        return resTS


    def test_longseries(self):
        ts1 = self.genTS1()
        ts2 = self.genTS2()

        expTS = self.genResTS()

        resTS1 = ts1.addSquare(ts2)
        self.assertEqual(resTS1, expTS)
        resTS2 = ts2.addSquare(ts1)
        self.assertEqual(resTS2, expTS)

        expTSAdjoining = self.genResTSAdjoining()
        ts2Adjoining = self.genTS2Adjoining()
        resTS1Adjoining = ts1.addSquare(ts2Adjoining)
        self.assertEqual(resTS1Adjoining, expTSAdjoining)
        resTS2Adjoining = ts2Adjoining.addSquare(ts1)
        self.assertEqual(resTS2Adjoining, expTSAdjoining)

    def test_differentIndex(self):
        ts1 = self.genTS1()
        ts1.df = ts1.df.set_index(ts1.df.index * 2)
        ts2 = self.genTS2()
        ts2.df = ts2.df.set_index(ts2.df.index * 3 + 1)

        expTS = self.genResTS()

        resTS1 = ts1.addSquare(ts2)
        self.assertEqual(resTS1, expTS)
        resTS2 = ts2.addSquare(ts1)
        self.assertEqual(resTS2, expTS)

class ScaleTest(unittest.TestCase):

    MILLION = 1000000
    BILLION = 1000000000

    def genRandomTimeseries(self, maxInterval, numEntries, maxVal=MILLION):
        with Timer("Generate samples"):
            startTimes = random.sample(range(maxInterval), k=numEntries)
            vals = random.sample(range(maxVal), k=numEntries)
        tsDF = pd.DataFrame({'timestamp': startTimes, 'tsValue': vals})
        with Timer("Sort dataframe"):
            tsDF = tsDF.sort_values('timestamp')
        tsDF = tsDF.assign(delta=tsDF['timestamp'].shift(-1, fill_value=maxInterval * 2) - tsDF['timestamp'])
        tsDF = tsDF.assign(duration=tsDF['delta'].apply(random.randrange))
        tsDF = tsDF.assign(nextTS=tsDF['timestamp']+tsDF['duration'])

        retTS = ts.TimeseriesRLE(tsDF)
        return retTS

    def test_scale1(self):
        ts1 = self.genRandomTimeseries(self.BILLION, self.MILLION)
        ts2 = self.genRandomTimeseries(self.MILLION, self.MILLION)

        with Timer("_arithmeticPrep"):
            e1, e2, bpList = ts1._artithmeticPrep(ts2)

        # with Timer("_intervalSample"):
        #     ts1._intervalSample(bpList)

        with Timer("_sortedSample"):
            ts1._sortedSample(bpList)

        with Timer("addSquare"):
            ts1.addSquare(ts2)

    def test_scale2(self):
        df1 = pd.read_parquet("UnitTests/out.parquet")
        flareEmissionDF = df1[
            (df1['unitID'] == 'Site_Flare')
            & (df1['emitterID'].isin(['OPERATING', 'MALFUNCTIONING', 'UNLIT']))
        ]
        flareEmissionTS = ts.TimeseriesRLE(flareEmissionDF)
        flareEmissionTSDay = flareEmissionTS.periodicAverage(range(0, flareEmissionTS.df['timestamp'].max(), u.daysToSecs(1)))

        pass


