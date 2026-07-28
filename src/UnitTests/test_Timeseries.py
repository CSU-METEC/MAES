import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import unittest
import Timeseries as ts
import random
import pandas as pd
from Timer import Timer
import logging.config
import numpy as np
from unittest.mock import MagicMock

try:
    from TsMatplotlib import matplotlib_plot_selector
    from TsBokeh import bokeh_plotly_selector
    from TsPlotly import plotly_plot_selector
    _PLOTTERS_AVAILABLE = True
except ImportError:
    matplotlib_plot_selector = MagicMock()
    bokeh_plotly_selector = MagicMock()
    plotly_plot_selector = MagicMock()
    _PLOTTERS_AVAILABLE = False

# todo: how to run test coverage?
# todo: writeup in readme on how to run unit tests & test coverage

# logging.basicConfig(level=logging.INFO)

logging_config = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "simple": {
            "format": "%(asctime)s| %(filename)s:%(lineno)d| %(levelname)s| %(message)s",
        },
    },
    "handlers": {
        "stdout": {
            "class": "logging.StreamHandler",
            "formatter": "simple",
            "stream": "ext://sys.stdout"
                    }
        },
    "loggers":{
        "root": {
            "handlers": ["stdout"],
            "level": "INFO",
            }
    }
}

logging.config.dictConfig(config=logging_config)


TS1_TS_SPEC = [
    {'timestamp': 3,  'nextTS':  7, 'tsValue':     10},
    {'timestamp': 11, 'nextTS': 14, 'tsValue':      8},
    {'timestamp': 18, 'nextTS': 19, 'tsValue':      3},
    {'timestamp': 23, 'nextTS': 25, 'tsValue':      7},
    {'timestamp': 27, 'nextTS': 30, 'tsValue':      3},
    {'timestamp': 50, 'nextTS': 60, 'tsValue': -20.02}
]

TS2_TS_SPEC = [
    {'timestamp': 2, 'nextTS': 6, 'tsValue': 0},
    {'timestamp': 6, 'nextTS': 8, 'tsValue': 4},
    {'timestamp': 8, 'nextTS': 9, 'tsValue': 20},
    {'timestamp': 9, 'nextTS': 12, 'tsValue': 4},
    {'timestamp': 14, 'nextTS': 17, 'tsValue': 20.01},
    {'timestamp': 17, 'nextTS': 25, 'tsValue': 7},
    {'timestamp': 32, 'nextTS': 36, 'tsValue': 0.01},
    {'timestamp': 38, 'nextTS': 53, 'tsValue': 20.02},
]

TS1_CATEGORICAL_SPEC = [
    {'timestamp':  0, 'nextTS': 1589996, 'tsValue': 'OPERATING'},
    {'timestamp': 1589996, 'nextTS': 14272680, 'tsValue': 'OPERATING'},
    {'timestamp': 14272680, 'nextTS': 15417627, 'tsValue': 'OVERPRESSURE_LEAK'},
    {'timestamp': 15417627, 'nextTS': 17052528, 'tsValue': 'OPERATING'},
    {'timestamp': 17052528, 'nextTS': 31536000, 'tsValue': 'OPERATING'}
]

class TimeseriesTest(unittest.TestCase):
    pass

class BasicTSTest(unittest.TestCase):

    def genTS(self, tsName, tsUnits, dictList):
        ts1 = ts.TimeseriesRLE.fromDictList(dictList, name=tsName, units=tsUnits)
        return ts1

    def genTS1(self):
        ts1Name = "ts1"
        ts1Units = "kg/s"
        ts1 = self.genTS(ts1Name, ts1Units, TS1_TS_SPEC)
        return ts1, ts1Name, ts1Units

    def genTS2(self):
        ts2Name = "ts2"
        ts2Units = "kg/s"
        ts2 = self.genTS(ts2Name, ts2Units, TS2_TS_SPEC)
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

        tsProduct = ts1.multiplySquare(ts2)

        tsProductExpected = self.genTS("expected", "kg/s", [
                         {'timestamp':  6, 'nextTS':  7, 'tsValue':    40},
                         {'timestamp': 11, 'nextTS': 12, 'tsValue':    32},
                         {'timestamp': 18, 'nextTS': 19, 'tsValue':    21},
                         {'timestamp': 23, 'nextTS': 25, 'tsValue':    49},
                         {'timestamp': 50, 'nextTS': 53, 'tsValue': -400.8004},
        ])

        self.assertEqual(tsProduct, tsProductExpected)

    def test_mask(self):
        ts1, _, _ = self.genTS1()
        ts2, _, _ = self.genTS2()

        self.assertNotEqual(ts1, ts2)

        tsSum = ts1.addSquare(ts2)

        tsMask = self.genTS("masked", "kg/s", [
                         {'timestamp':  8, 'nextTS': 15, 'tsValue':  5},
                         {'timestamp': 24, 'nextTS': 55, 'tsValue': 10},
        ])

        with Timer("Mask") as t0:
            tsMasked1 = tsSum.mask(tsMask)
            t0.setCount(len(tsMask.df))

        # with Timer("Iterative Mask") as t1:
        #     tsMasked2 = tsSum.maskIterative(tsMask)
        #     t1.setCount(len(tsMask.df))
        pass

    def test_maskSparseTS(self):
        ts1Name = "ts1"
        ts1Units = "kg/s"
        ts1 = self.genTS(ts1Name, ts1Units, [
            {'timestamp':  0, 'nextTS':  5, 'tsValue':  5},
            {'timestamp': 10, 'nextTS': 15, 'tsValue': 10},
            {'timestamp': 20, 'nextTS': 25, 'tsValue': 20},
            {'timestamp': 30, 'nextTS': 35, 'tsValue': 30}
        ])

        tsMask1Name = "tsMask1"
        tsMask1Units = "kg/s"
        tsMask1 = self.genTS(tsMask1Name, tsMask1Units, [
            {'timestamp': 8, 'nextTS': 29, 'tsValue': 1}
        ])

        tsMasked1 = ts1.mask(tsMask1)
        ts1MaskedMean = tsMasked1.mean()

        tsMasked2 = ts1.mask(tsMask1, fillZeros=True)
        ts1MaskedMean2 = tsMasked2.mean()
        
        pass

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

    def test_zeroDurationException(self):
        df = pd.DataFrame([
            {'timestamp': 3,  'nextTS': 7,  'tsValue': 10.0},
            {'timestamp': 11, 'nextTS': 11, 'tsValue':  5.0},  # zero duration
            {'timestamp': 15, 'nextTS': 20, 'tsValue':  8.0},
        ])
        with self.assertRaises(ts.MalformedTimeseriesError):
            ts.TimeseriesRLE(df)

    def test_arithmeticPrep(self):
        ts1, _, _ = self.genTS1()
        with self.assertRaises(ts.MalformedTimeseriesError):
           res = ts1._arithmeticPrep({})

    def test_total(self):
         ts1, _, _ = self.genTS1()
         total = ts1.total()
         pass  
     
    def test_formatting(self):
        with self.assertRaises(ts.MalformedTimeseriesError):
            ts1 = self.genTS("ts1", "kg/s", [
            {'nextTS': 14, 'tsValue':      8},
            {'nextTS':  7, 'tsValue':     10},
            {'nextTS': 19, 'tsValue':      3},
            {'nextTS': 25, 'tsValue':      7},
            {'nextTS': 30, 'tsValue':      3},
            {'nextTS': 60, 'tsValue': -20.02}
            ])
        with self.assertRaises(ts.MalformedTimeseriesError):
             ts1 = self.genTS("ts1", "kg/s", [
            {'timestamp': 11, 'tsValue':      8},
            {'timestamp': 3,  'tsValue':     10},
            {'timestamp': 18, 'tsValue':      3},
            {'timestamp': 23, 'tsValue':      7},
            {'timestamp': 27, 'tsValue':      3},
            {'timestamp': 50, 'tsValue': -20.02}
            ])
        with self.assertRaises(ts.MalformedTimeseriesError):
            ts1 = self.genTS("ts1", "kg/s", [
            {'timestamp': 11, 'nextTS': 14,},
            {'timestamp': 3,  'nextTS':  7,},
            {'timestamp': 18, 'nextTS': 19,},
            {'timestamp': 23, 'nextTS': 25,},
            {'timestamp': 27, 'nextTS': 30,},
            {'timestamp': 50, 'nextTS': 60,}
            ])
    def test_mask_parse_formatt(self):
        ts1, _, _ = self.genTS1()
        with self.assertRaises(ts.MalformedTimeseriesError):
            res = ts1.mask({})

    def test_startTime_empty(self):
        tsEmpty =  ts.TimeseriesRLE(df=pd.DataFrame(columns=["timestamp", "nextTS", "tsValue"]))
        self.assertEqual(0, tsEmpty.startTime())

    def test_endtime_empty(self):
        tsEmpty =  ts.TimeseriesRLE(df=pd.DataFrame(columns=["timestamp", "nextTS", "tsValue"]))
        self.assertEqual(float('inf'), tsEmpty.endTime())

    def test_mask_starttimes(self):
        # todo: Check the result!!!
        ts1, _, _ = self.genTS1()
        ts2, _, _ = self.genTS2()
        ts1.mask(ts2)
        pass

    def test_nonzero(self):
        # todo: Check the result!!!
        ts1, _, _ = self.genTS1()
        newTs = ts1.nonzero()
        pass
    def test_mask2_(self):
        # todo: Check the result!!!
        ts1, _, _ = self.genTS1()
        ts2, _, _ = self.genTS2()

        with self.assertRaises(ts.MalformedTimeseriesError):
            res = ts1.mask2({})
        res = ts1.mask2(ts2)
        
    def test_startEndTimes(self):
        tsEmpty =  ts.TimeseriesRLE(df=pd.DataFrame(columns=["timestamp", "nextTS", "tsValue"]))
        res = tsEmpty._startEndTimes
        self.assertEqual(res,None)


    def test_maskTS(self):
        # todo: Check the result!!!
        ts1, _, _ = self.genTS1()
        tsEmpty =  ts.TimeseriesRLE(df=pd.DataFrame(columns=["timestamp", "nextTS", "tsValue"]))

        newTsempty = tsEmpty.maskTS(tStart=None, tEnd=None, fill=False)
        newTs = ts1.maskTS(tStart=None, tEnd=None, fill=False)
        pass

    def test_totalDuration(self):
        tsEmpty =  ts.TimeseriesRLE(df=pd.DataFrame(columns=["timestamp", "nextTS", "tsValue"]))
        self.assertEqual(0, tsEmpty.totalDuration())
        ts1,_,_ = self.genTS1()
        dur = ts1.totalDuration()
        self.assertEqual(dur,23)

    def test_std(self):
        # todo: Check the result!!!
        ts1,_,_ = self.genTS1()
        stdValue, meanValue, totalDur, totalValue = ts1.std()
        pass

    def test_meanAndStd(self):
        # todo: Check the result!!!
        ts1,_,_  = self.genTS1()
        meanValue, stdValue, totalDur, totalValue = ts1.meanAndStd()
        pass

    def test_min(self):
        ts1,_,_  = self.genTS1()
        minvalue = ts1.min()
        minvalue_zero = ts1.min(omitZero=True)
        self.assertEqual(minvalue_zero, -20.02)
        
    def test_max(self):
        ts1,_,_  = self.genTS1()
        maxvalue = ts1.max()
        self.assertEqual(maxvalue, 10)

    def test_max_empty(self):
        tsEmpty =  ts.TimeseriesRLE(df=pd.DataFrame(columns=["timestamp", "nextTS", "tsValue"]))
        self.assertEqual(0, tsEmpty.max())
    
    def test_median(self):
        # todo: Check the result!!!
        ts1,_,_  = self.genTS1()
        median = ts1.median()
        pass
    def test_statsTable(self):
        ts1,_,_ = self.genTS1()
        stats = ts1.statsTable()
        self.assertIsInstance(stats, dict)
        self.assertSetEqual(set(stats.keys()),
                            {'minimum', 'lower', 'mean', 'upper', 'maximum', 'stdDev', 'median', 'sum', 'onDuration'})
        self.assertFalse(any(isinstance(v, (pd.DataFrame, pd.Series, list)) for v in stats.values()))

    def test_statsTable_empty(self):
        tsEmpty = ts.TimeseriesRLE(df=pd.DataFrame(columns=["timestamp", "nextTS", "tsValue"]))
        stats = tsEmpty.statsTable()
        self.assertIsInstance(stats, dict)
        self.assertEqual(stats['mean'], 0)
        self.assertEqual(stats['sum'], 0)
        self.assertEqual(stats['onDuration'], 0)
    
    def test_equal(self):
        ts1,_,_  = self.genTS1()
        self.assertFalse(ts1.equal({}))

    # both self._durations() and self._values() are not callable
    # def test_total(self):
    #     ts1,_,_  = self.genTS1()
    #     res = ts1.total()

    # def test_secondsPerunite(self):
    #     ts1,_,_  = self.genTS1()
    #     _, ds = ts1.secondsPerUnit("day")
    #     _, hr = ts1.secondsPerUnit("hr")
    #     _, min = ts1.secondsPerUnit("MIN")
    #     _, s = ts1.secondsPerUnit("s")
    #     self.assertEqual(ds, "d")
    #     self.assertEqual(hr, "h")
    #     self.assertEqual(min, "min")
    #     self.assertEqual(s, "s")

    #     with self.assertRaises(ValueError):
    #         smt  = ts1.secondsPerUnit("Beer")


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
        tsDF = tsDF.assign(duration=tsDF['delta'].apply(lambda d: random.randint(1, max(1, d))))
        tsDF = tsDF.assign(nextTS=tsDF['timestamp']+tsDF['duration'])

        retTS = ts.TimeseriesRLE(tsDF)
        return retTS

    # def test_scale1(self):
    #     ts1 = self.genRandomTimeseries(self.BILLION, self.MILLION)
    #     ts2 = self.genRandomTimeseries(self.MILLION, self.MILLION)

    #     with Timer("_arithmeticPrep"):
    #         e1, e2, bpList = ts1._arithmeticPrep(ts2)

        # with Timer("_intervalSample"):
        #     ts1._intervalSample(bpList)

        # with Timer("_sortedSample"):
        #     ts1._sortedSample(bpList)

        # with Timer("addSquare"):
        #     ts1.addSquare(ts2)

class GraphTest(unittest.TestCase):

    def test_graphPrimitives(self):
        ts1 = ts.TimeseriesRLE.fromDictList(TS1_TS_SPEC, name='ts1', units='ts1Units')
        ts1Range = ts1.range
        self.assertTrue(ts1Range[0] <= ts1._values.min())
        self.assertTrue(ts1Range[1] >= ts1._values.max())

        tsCat1 = ts.TimeseriesCategorical.fromDictList(TS1_CATEGORICAL_SPEC, name='tsCat1')
        tsCat1Range = tsCat1.range
        tsCat1Names = map(lambda x: x['tsValue'], TS1_CATEGORICAL_SPEC)
        self.assertTrue(set(tsCat1Range), set(tsCat1Names))

class StatsTest(unittest.TestCase):
    def genTS(self, tsName, tsUnits, dictList):
        ts1 = ts.TimeseriesRLE.fromDictList(dictList, name=tsName, units=tsUnits)
        return ts1

    def genTS1(self):
        ts1 = self.genTS("ts1", "kg/s", [
        {'timestamp': 2, 'nextTS': 6, 'tsValue': 0},
        {'timestamp': 6, 'nextTS': 8, 'tsValue': 4},
        {'timestamp': 8, 'nextTS': 9, 'tsValue': 20},
        {'timestamp': 9, 'nextTS': 12, 'tsValue': 4},
        {'timestamp': 14, 'nextTS': 17, 'tsValue': 20.01},
        {'timestamp': 17, 'nextTS': 25, 'tsValue': 7},
        {'timestamp': 32, 'nextTS': 36, 'tsValue': 0.01},
        {'timestamp': 38, 'nextTS': 43, 'tsValue': 20.02},
    ])
        return ts1

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
            {'timestamp': 38, 'nextTS': 43, 'tsValue': 20.02},
        ])
        return ts2

    def test_PDFBasics(self):
        ts1 = self.genTS1()
        ts1PDF = ts1.toPDF()
        self.assertEqual(ts1._durations.sum(), ts1PDF.data['count'].sum())

        ts2 = self.genTS('ts1', 'kg/s', [
            {'timestamp':  2, 'nextTS':  6, 'tsValue':  0},
            {'timestamp':  6, 'nextTS':  8, 'tsValue':  4},
            {'timestamp':  8, 'nextTS':  9, 'tsValue': 20},
            {'timestamp':  9, 'nextTS': 12, 'tsValue':  4},
            {'timestamp': 14, 'nextTS': 17, 'tsValue': 20.01},
            {'timestamp': 17, 'nextTS': 25, 'tsValue':  7},
            {'timestamp': 32, 'nextTS': 36, 'tsValue':  0.01},
            {'timestamp': 38, 'nextTS': 43, 'tsValue': 20.02},
            {'timestamp': 50, 'nextTS': 99, 'tsValue':  7},
        ])
        ts2PDF = ts2.toPDF()
        self.assertEqual(ts2._durations.sum(), ts2PDF.data['count'].sum())

    def test_CDFInverse(self):
        CDF_INVERSE_TS = [
            {'timestamp':  3, 'nextTS':  7, 'tsValue': 10},
            {'timestamp': 11, 'nextTS': 14, 'tsValue':  8},
            {'timestamp': 18, 'nextTS': 19, 'tsValue':  3},
            {'timestamp': 23, 'nextTS': 25, 'tsValue':  7},
            {'timestamp': 27, 'nextTS': 30, 'tsValue':  3},
        ]
        ts1 = self.genTS("ts1", "kg/s", CDF_INVERSE_TS)
        ret1 = ts1.CDFInverse()
        r = np.array([7.16666667])
        self.assertTrue(np.allclose(r, ret1, atol=1e-5))

    def test_PDFStatsTable(self):
        ts1 = self.genTS1()
        ts1PDF = ts1.toPDF()
        stats = ts1PDF.statsTable()
        expected = {
            'minimum':    0.0,
            'lower':      0.0,
            'mean':       8.539,
            'upper':      20.0185,
            'maximum':    20.02,
            'stdDev':     7.936764811096942,
            'median':     4.75,
            'sum':        256.17,
            'onDuration': 30,
        }
        self.assertIsInstance(stats, dict)
        for key, expVal in expected.items():
            self.assertTrue(np.isclose(stats[key], expVal, equal_nan=True),
                            f"{key}: expected {expVal}, got {stats[key]}")

    def test_remove_zero_duration(self):
        # todo: ts2 has no zero duration intervals.  It does have zero value intervals, which I am not sure we want to remove
        ts2 = self.genTS2()
        tsoutput = ts2.removeZeroDuration()
        tsExpected = self.genTS("expected", "kg/s", [
                        {'timestamp': 6, 'nextTS': 8, 'tsValue': 4},
                        {'timestamp': 8, 'nextTS': 9, 'tsValue': 20},
                        {'timestamp': 9, 'nextTS': 12, 'tsValue': 4},
                        {'timestamp': 14, 'nextTS': 17, 'tsValue': 20.01},
                        {'timestamp': 17, 'nextTS': 25, 'tsValue': 7},
                        {'timestamp': 32, 'nextTS': 36, 'tsValue': 0.01},
                        {'timestamp': 38, 'nextTS': 43, 'tsValue': 20.02}
                    ])
        self.assertEqual(tsoutput, tsExpected)
        ts2DestructiveTest = self.genTS2()
        self.assertEqual(ts2, ts2DestructiveTest)

    def _genErrorValueTS(self):
        retTS = self.genTS("expected", "kg/s", [
            {'timestamp': 6, 'nextTS': 8, 'tsValue': np.nan},
            {'timestamp': 8, 'nextTS': 9, 'tsValue': 20},
            {'timestamp': 9, 'nextTS': 12, 'tsValue': 4},
            {'timestamp': 14, 'nextTS': 17, 'tsValue': 20.01},
            {'timestamp': 17, 'nextTS': 25, 'tsValue': 7},
            {'timestamp': 32, 'nextTS': 36, 'tsValue': 0.01},
            {'timestamp': 38, 'nextTS': 43, 'tsValue': 20.02}
        ])
        return retTS

    def test_remove_error_values(self):
        ts1 = self._genErrorValueTS()
        tsoutput = ts1.removeErrorValues([1000])
        tsExpected = self.genTS("expected", "kg/s", [
            {'timestamp': 6, 'nextTS': 8, 'tsValue': 1000},
            {'timestamp': 8, 'nextTS': 9, 'tsValue': 20},
            {'timestamp': 9, 'nextTS': 12, 'tsValue': 4},
            {'timestamp': 14, 'nextTS': 17, 'tsValue': 20.01},
            {'timestamp': 17, 'nextTS': 25, 'tsValue': 7},
            {'timestamp': 32, 'nextTS': 36, 'tsValue': 0.01},
            {'timestamp': 38, 'nextTS': 43, 'tsValue': 20.02}
        ])
        self.assertEqual(tsoutput, tsExpected)
        ts1DestructiveTest = self._genErrorValueTS()
        self.assertEqual(ts1, ts1DestructiveTest)
    
    def test_remove_error_values_no_replace(self):
        # todo: Check the result!!!
        ts1 = self.genTS("expected", "kg/s", [
                        {'timestamp': 6, 'nextTS': 8, 'tsValue': np.nan},
                        {'timestamp': 8, 'nextTS': 9, 'tsValue': 20},
                        {'timestamp': 9, 'nextTS': 12, 'tsValue': 4},
                        {'timestamp': 14, 'nextTS': 17, 'tsValue': 20.01},
                        {'timestamp': 17, 'nextTS': 25, 'tsValue': 7},
                        {'timestamp': 32, 'nextTS': 36, 'tsValue': 0.01},
                        {'timestamp': 38, 'nextTS': 43, 'tsValue': 20.02}
                    ])
        tsoutput = ts1.removeErrorValues()
        pass

    def test_zero_periods(self):
        ts1 = self.genTS1()
        tsoutput = ts1.zeroPeriods()
        tsExpected = self.genTS("expected", "kg/s", [
                         {'timestamp':  2, 'nextTS':  6, 'tsValue': 1},
                         {'timestamp': 12, 'nextTS': 14, 'tsValue': 1},
                         {'timestamp': 25, 'nextTS': 32, 'tsValue': 1},
                         {'timestamp': 36, 'nextTS': 38, 'tsValue': 1}])
        self.assertEqual(tsoutput, tsExpected)
        ts1DestructiveCheck = self.genTS1()
        self.assertEqual(ts1, ts1DestructiveCheck)

    def test_zero_periods_true(self):
        # todo: Check the result!!!
        ts1 = self.genTS1()
        tsoutput = ts1.zeroPeriods(startTime=0)
        tsExpected = self.genTS("expected", "kg/s", [
                         {'timestamp':  0, 'nextTS':  6, 'tsValue': 1},
                         {'timestamp': 12, 'nextTS': 14, 'tsValue': 1},
                         {'timestamp': 25, 'nextTS': 32, 'tsValue': 1},
                         {'timestamp': 36, 'nextTS': 38, 'tsValue': 1}])
        self.assertEqual(tsoutput, tsExpected)
        ts1DestructiveCheck = self.genTS1()
        self.assertEqual(ts1, ts1DestructiveCheck)

    def test_zero_periods_end_time(self):
        # todo: Check the result!!!
        ts1 = self.genTS1()
        tsoutput = ts1.zeroPeriods(endTime=50)
        tsExpected = self.genTS("expected", "kg/s", [
                         {'timestamp':  2, 'nextTS':  6, 'tsValue': 1},
                         {'timestamp': 12, 'nextTS': 14, 'tsValue': 1},
                         {'timestamp': 25, 'nextTS': 32, 'tsValue': 1},
                         {'timestamp': 36, 'nextTS': 38, 'tsValue': 1},
                         {'timestamp': 43, 'nextTS': 50, 'tsValue': 1}
        ])
        self.assertEqual(tsoutput, tsExpected)
        ts1DestructiveCheck = self.genTS1()
        self.assertEqual(ts1, ts1DestructiveCheck)

    def test_zero_periods_start_time_internal(self):
        # todo: Check the result!!!
        ts1 = self.genTS1()
        tsoutput = ts1.zeroPeriods(startTime=10)
        tsExpected = self.genTS("expected", "kg/s", [
                         {'timestamp': 12, 'nextTS': 14, 'tsValue': 1},
                         {'timestamp': 25, 'nextTS': 32, 'tsValue': 1},
                         {'timestamp': 36, 'nextTS': 38, 'tsValue': 1}
        ])
        self.assertEqual(tsoutput, tsExpected)
        ts1DestructiveCheck = self.genTS1()
        self.assertEqual(ts1, ts1DestructiveCheck)

    def test_zero_periods_start_time_internal_at_zero(self):
        # todo: Check the result!!!
        ts1 = self.genTS1()
        tsoutput = ts1.zeroPeriods(startTime=13)
        tsExpected = self.genTS("expected", "kg/s", [
                         {'timestamp': 13, 'nextTS': 14, 'tsValue': 1},
                         {'timestamp': 25, 'nextTS': 32, 'tsValue': 1},
                         {'timestamp': 36, 'nextTS': 38, 'tsValue': 1}
        ])
        self.assertEqual(tsoutput, tsExpected)
        ts1DestructiveCheck = self.genTS1()
        self.assertEqual(ts1, ts1DestructiveCheck)

    def test_fillZeros(self):
        ts1 = self.genTS1()
        tsoutput = ts1.fillZeros()
        tsExpected = self.genTS("expected", "kg/s", [
            {'timestamp': 2, 'nextTS': 6, 'tsValue': 0},
            {'timestamp': 6, 'nextTS': 8, 'tsValue': 4},
            {'timestamp': 8, 'nextTS': 9, 'tsValue': 20},
            {'timestamp': 9, 'nextTS': 12, 'tsValue': 4},
            {'timestamp': 12, 'nextTS': 14, 'tsValue': 0},
            {'timestamp': 14, 'nextTS': 17, 'tsValue': 20.01},
            {'timestamp': 17, 'nextTS': 25, 'tsValue': 7},
            {'timestamp': 25, 'nextTS': 32, 'tsValue': 0},
            {'timestamp': 32, 'nextTS': 36, 'tsValue': 0.01},
            {'timestamp': 36, 'nextTS': 38, 'tsValue': 0},
            {'timestamp': 38, 'nextTS': 43, 'tsValue': 20.02},
        ])
        self.assertEqual(tsoutput, tsExpected)
        ts1DestructiveCheck = self.genTS1()
        self.assertEqual(ts1, ts1DestructiveCheck)

    def test_min_empty(self):
        tsEmpty =  ts.TimeseriesRLE(df=pd.DataFrame(columns=["timestamp", "nextTS", "tsValue"]))
        self.assertEqual(0, tsEmpty.min())


    def test_threshold(self):
        ts1 = self.genTS1()
        tsoutput = ts1.threshold([20])
        tsExpected = self.genTS("expected", "kg/s", [
                         {'timestamp':  8, 'nextTS':  9, 'tsValue':   20},
                         {'timestamp': 14, 'nextTS': 17, 'tsValue':   20.01},
                         {'timestamp': 38, 'nextTS': 43, 'tsValue':   20.02}])
        self.assertEqual(tsoutput, tsExpected)

    def test_threshold_value_error(self):
        ts1 = self.genTS1()
        with self.assertRaises(ValueError):
            ts1.threshold()

        with self.assertRaises(ValueError):
            ts1.threshold([1,2,3])

    def test_threshold_multiple(self):
        # todo: Check the result!!!
        ts1 = self.genTS1()
        tsoutput = ts1.threshold([20, 5])

    def test_threshold_empty(self):
        # todo: Check the result!!!
        tsEmpty =  ts.TimeseriesRLE(df=pd.DataFrame(columns=["timestamp", "nextTS", "tsValue"]))
        tsEmpty.threshold([20])

class PDFScaleTest(unittest.TestCase):

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
        tsDF = tsDF.assign(duration=tsDF['delta'].apply(lambda d: random.randint(1, max(1, d))))
        tsDF = tsDF.assign(nextTS=tsDF['timestamp']+tsDF['duration'])

        retTS = ts.TimeseriesRLE(tsDF)
        return retTS

    def test_scale1(self):
        ts1 = self.genRandomTimeseries(self.BILLION, self.MILLION)

        with Timer("toPDF") as t0:
            ts1PDF = ts1.toPDF()
            t0.setCount(len(ts1.df))

        self.assertEqual(ts1._durations.sum(), ts1PDF.data['count'].sum())

class ResampleTest(unittest.TestCase):

    NUM_HOURS = 8760
    SECONDS_PER_HOUR = 3600
    HOURS_PER_DAY = 24
    DAYS_PER_YEAR = 365

    def alternateCalculation(self, df, sampleInterval):
        df = df.assign(sampleNum=df['timestamp'] // sampleInterval,
                       total=(df['nextTS']-df['timestamp'])*df['tsValue'])
        pt = df.pivot_table(index='sampleNum', values='total', aggfunc='sum').reset_index()
        pt = pt.assign(tsValue=pt['total'] / sampleInterval,
                       timestamp=pt['sampleNum']*sampleInterval,
                       nextTS=(pt['sampleNum']+1)*sampleInterval
                       )
        return pt

    def genEventDF(self, sequence=range(NUM_HOURS)):
        eventList = []
        hourIntervals = []
        for i, val in enumerate(sequence):
            thisTimestamp = i * self.SECONDS_PER_HOUR
            thisEvent = {'timestamp': i * self.SECONDS_PER_HOUR,
                         'nextTS': thisTimestamp + self.SECONDS_PER_HOUR / 2,
                         'tsValue': (val+1)*2
                         }
            eventList.append(thisEvent)
            hourIntervals.append(thisTimestamp)
        hourIntervals.append(self.SECONDS_PER_HOUR*self.NUM_HOURS)

        eventDF = pd.DataFrame(eventList)

        return eventDF, hourIntervals

    def test_resample1(self):
        eventDF, hourIntervals = self.genEventDF()
        eventTS = ts.TimeseriesRLE(eventDF)

        hourlyTS = eventTS.periodicAverage(hourIntervals)
        altHourlyDF = self.alternateCalculation(eventDF, 3600)
        altHourlyTS = ts.TimeseriesRLE(altHourlyDF)
        self.assertEqual(hourlyTS, altHourlyTS)

        secondsPerDay = self.SECONDS_PER_HOUR * self.HOURS_PER_DAY
        dailyIntervals = list(map(lambda x: x * secondsPerDay, range(0, self.DAYS_PER_YEAR+1)))

        dailyTS1 = eventTS.periodicAverage(dailyIntervals)
        dailyTS2 = hourlyTS.periodicAverage(dailyIntervals)
        altDailyDF = self.alternateCalculation(eventDF, secondsPerDay)
        altDailyTS = ts.TimeseriesRLE(altDailyDF)
        self.assertEqual(dailyTS1, dailyTS2)
        self.assertEqual(dailyTS1, altDailyTS)

    def test_resample_stats(self):

        eventDF, hourIntervals = self.genEventDF(sequence=np.random.normal(5000, 500, self.NUM_HOURS))
        eventTS = ts.TimeseriesRLE(eventDF)

        hourlyTS = eventTS.periodicAverage(hourIntervals)
        hourlyPDF = hourlyTS.toPDF()
        fakeHourlyDF = hourlyTS.df.assign(count=self.SECONDS_PER_HOUR)
        # self.assertTrue((fakeHourlyDF['tsValue'] == hourlyPDF.data['value']).all())

        secondsPerDay = self.SECONDS_PER_HOUR * self.HOURS_PER_DAY
        dailyIntervals = list(map(lambda x: x * secondsPerDay, range(0, self.DAYS_PER_YEAR+1)))
        dailyTS = hourlyTS.periodicAverage(dailyIntervals)
        dailyPDF = dailyTS.toPDF()
        fakeDailyDF = dailyTS.df.assign(count=self.SECONDS_PER_HOUR*self.HOURS_PER_DAY)
        # self.assertTrue((fakeDailyDF['tsValue'] == dailyPDF.data['value']).all())

        # f1, ax1, h1 = hourlyTS.plot(plottingLibrary='bokeh')
        # from bokeh.plotting import figure, save
        # from bokeh.io import output_file
        # output_file("./figure.html")
        # save(f1)

        hourlyCDF = hourlyPDF.toCDF()


        pass

class MaskTest(unittest.TestCase):

    TS1 = [
        {'timestamp':  4, 'nextTS':   10, 'tsValue': 10},
        {'timestamp':  20, 'nextTS':  30, 'tsValue': 20},
        {'timestamp':  40, 'nextTS':  45, 'tsValue': 30},
        {'timestamp':  50, 'nextTS':  80, 'tsValue': 40},
        {'timestamp':  90, 'nextTS':  99, 'tsValue': 50},
        {'timestamp': 110, 'nextTS': 115, 'tsValue': 60},
        {'timestamp': 120, 'nextTS': 125, 'tsValue': 70}

        ]

    MASK1 = [
        {'timestamp':   5, 'nextTS':  15, 'tsValue':  1},
        {'timestamp':  24, 'nextTS':  28, 'tsValue':  2},
        {'timestamp':  51, 'nextTS':  55, 'tsValue':  3},
        {'timestamp':  57, 'nextTS':  62, 'tsValue':  4},
        {'timestamp': 109, 'nextTS': 131, 'tsValue':  5}

        ]

    def test_mask_basics(self):
        # todo: Check the result!!!
        ts1 = pd.DataFrame(self.TS1)
        mask = pd.DataFrame(self.MASK1)
        res = ts.dataframeMask(ts1, mask)
        pass
    def test_value_error(self):
        # todo: Check the result!!!
        ts1 = pd.DataFrame(self.TS1)
        mask = pd.DataFrame(self.MASK1)
        with self.assertRaises(ValueError):
            res = ts.dataframeMask(self.TS1, mask)
        with self.assertRaises(ValueError):
            res = ts.dataframeMask(ts1, self.MASK1)

    def allocateDF(self, numEntries, interval, spread):
        # todo: Check the result!!!
        retDF = pd.DataFrame(index=range(numEntries))
        retDF = retDF.assign(timestamp=retDF.index*(interval+spread))
        retDF = retDF.assign(nextTS=retDF['timestamp']+interval)
        return retDF

    def test_empty_retDF(self):
        # todo: Check the result!!!
        df = self.allocateDF(numEntries=365, interval=10, spread=5)
        df = df.assign(tsValue=df.index,
                       tsOtherData=df.index.astype(int)*10)
        mask = df
        mask = mask.assign(timestamp=mask['timestamp']+5)
        mask = mask.assign(nextTS=mask['nextTS']+5)
        mask = mask[(mask.index % 10) == 0]

        with Timer("group mask w/ iter") as t1:
            divisor = 10
            for i in range(divisor):
                subDF = df[(df.index % divisor) == i]
                with Timer("mask iter") as t10:
                    res = ts.dataframeMask(subDF, mask)
                    t10.setCount(len(mask))
            t1.setCount(len(mask))

    def test_scale(self):
        # todo: Check the result!!!
        NUM_DF_ENTRIES = 100
        DF_INTERVAL = 10
        DF_SPREAD = 5
        NUM_MASK_ENTRIES = 10
        MASK_INTERVAL = 3
        MASK_SPREAD = 2

        df = self.allocateDF(NUM_DF_ENTRIES, DF_INTERVAL, DF_SPREAD)
        df = df.assign(tsValue=df.index,
                       tsOtherData=df.index.astype(int)*10)

        mask = df
        mask = mask.assign(timestamp=mask['timestamp']+1)
        mask = mask.assign(nextTS=mask['nextTS']-1)
        logging.info(f"len mask: {len(mask)}")

        with Timer("ts mask") as t00:
            dfTS = ts.TimeseriesRLE(df)
            maskTS = ts.TimeseriesRLE(mask)
            res = dfTS.mask(maskTS)
            t00.setCount(len(mask))

        with Timer("group mask") as t0:
            res = ts.dataframeMask(df, mask)
            t0.setCount(len(mask))

        pass

class TSSetTest(unittest.TestCase):

    def genTS(self, tsName, tsUnits, dictList):
        ts1 = ts.TimeseriesRLE.fromDictList(dictList, name=tsName, units=tsUnits)
        return ts1

    def genTS1(self):
        ts1Name = "ts1"
        ts1Units = "kg/s"
        ts1 = self.genTS(ts1Name, ts1Units, TS1_TS_SPEC)
        return ts1, ts1Name, ts1Units

    def genTS2(self):
        ts2Name = "ts2"
        ts2Units = "kg/s"
        ts2 = self.genTS(ts2Name, ts2Units, TS2_TS_SPEC)
        return ts2, ts2Name, ts2Units

    def test_setSummation(self):
        ts1, _, _ = self.genTS1()
        ts2, _, _ = self.genTS2()

        self.assertNotEqual(ts1, ts2)

        tsSet = ts.TimeseriesSet([ts1, ts2])

        tsSum = tsSet.sum()

        tsExpected = self.genTS("expected", "kg/s", [
            {'timestamp': 3, 'nextTS': 6, 'tsValue': 10},
            {'timestamp': 6, 'nextTS': 7, 'tsValue': 14},
            {'timestamp': 7, 'nextTS': 8, 'tsValue': 4},
            {'timestamp': 8, 'nextTS': 9, 'tsValue': 20},
            {'timestamp': 9, 'nextTS': 11, 'tsValue': 4},
            {'timestamp': 11, 'nextTS': 12, 'tsValue': 12},
            {'timestamp': 12, 'nextTS': 14, 'tsValue': 8},
            {'timestamp': 14, 'nextTS': 17, 'tsValue': 20.01},
            {'timestamp': 17, 'nextTS': 18, 'tsValue': 7},
            {'timestamp': 18, 'nextTS': 19, 'tsValue': 10},
            {'timestamp': 19, 'nextTS': 23, 'tsValue': 7},
            {'timestamp': 23, 'nextTS': 25, 'tsValue': 14},
            {'timestamp': 27, 'nextTS': 30, 'tsValue': 3},
            {'timestamp': 32, 'nextTS': 36, 'tsValue': 0.01},
            {'timestamp': 38, 'nextTS': 50, 'tsValue': 20.02},
            {'timestamp': 53, 'nextTS': 60, 'tsValue': -20.02},
        ])

        self.assertEqual(tsSum, tsExpected)

        tsSet.addTimeseries(tsExpected)

        twoTS = tsExpected.createConstant(2)
        doubleExpected = tsExpected.multiplySquare(twoTS)

        ts2Sum = tsSet.sum()
        self.assertEqual(ts2Sum, doubleExpected)


        halfExpected = tsExpected.divideSquare(twoTS)

        tsMeanSet = ts.TimeseriesSet([ts1, ts2])
        meanTS = tsMeanSet.mean()
        self.assertEqual(meanTS, halfExpected)

        pass

class LargeFullTSTest(unittest.TestCase):

    def setUp(self) -> None:
        LARGE_TSFILE = Path(__file__).resolve().parent / 'Sample_1month_maintenance.csv'
        self.largeTSDF = pd.read_csv(LARGE_TSFILE, dtype={'Timestep': int})
        self.largeTS = ts.TimeseriesFull(self.largeTSDF, startTimeColName='Timestep', rateColName='Emission (kg/s)')
        return super().setUp()  
    
    def _compareDF(self, df1, df2, testName):
        for c1, c2 in zip(df1.columns, df2.columns):
            self.assertEqual(c1, c2, f"{testName}: Equal column names")
        self.assertEqual(len(df1), len(df2), f"{testName}: Equal len")
        for c1 in df1.columns:
            self.assertTrue(all(df1[c1] == df2[c1]), f"{testName}: Column {c1} compare")

    def test_largeTS(self):
        compressedTS = self.largeTS.toTimeseriesRLE()
        compressedTSNoZeros = self.largeTS.toTimeseriesRLE(filterZeros=True)
        expandedTS = compressedTS.toCompleteTimeseries()
        self._compareDF(expandedTS.df, self.largeTSDF, "expandedTS")

        # expandedTSNoZeros = compressedTSNoZeros.toCompleteTimeseries()
        # self._compareDF(expandedTSNoZeros.df, largeTSDF, "expandedTSNoZeroes")

        pass

    # fullDF has no key "rate"
    # def test_total(self):
    #     total =  self.largeTS.total()
    #     pass

    #'TimeseriesFull' object has no attribute 'endTimeColName' so i commented out _durations
    # def test_durations(self):
        # dur = self.largeTS._durations()
        # pass

    def test_values(self):
        values = self.largeTS._values
        pass

class TestCategorical(unittest.TestCase):
    def genCat(self):
        return ts.TimeseriesCategorical.fromDictList(TS1_CATEGORICAL_SPEC, name='tsCat1')
    

    def test_properties(self):
        # todo: Check the result!!!
        cat = self.genCat()
        res  = cat._values
        res1 = cat.catval
        pass

class TestPDF(unittest.TestCase):

    # def is_cumulative(self, lst: pd.Series) -> bool:
    #     total = 0
    #     for element in lst:
    #         if element < total:
    #             return False
    #         total += element
    #     return True

    def genTS(self, tsName, tsUnits, dictList) -> "ts.TimeseriesRLE":
        ts1 = ts.TimeseriesRLE.fromDictList(dictList, name=tsName, units=tsUnits)
        return ts1
    
    def test_ts_data(self):
        # todo: Check the result!!!
        sample_data = [
                    {'timestamp': 2, 'nextTS': 6, 'tsValue': 0},
                    {'timestamp': 6, 'nextTS': 8, 'tsValue': 4},
                    {'timestamp': 8, 'nextTS': 9, 'tsValue': 20},
                    {'timestamp': 9, 'nextTS': 12, 'tsValue': 4},
                    {'timestamp': 14, 'nextTS': 17, 'tsValue': 20.01},
                    {'timestamp': 17, 'nextTS': 25, 'tsValue': 7},
                    {'timestamp': 32, 'nextTS': 36, 'tsValue': 0.01},
                    {'timestamp': 38, 'nextTS': 43, 'tsValue': 20.02},
                ]

        ts1 = self.genTS(tsName = "ts1", tsUnits = "kg/s", dictList = sample_data)
        pdf_obj = ts.TimeseriesPDF.fromTS(ts1)

    def test_addDisjointValues(self):
        pdf1 = ts.TimeseriesPDF(pd.DataFrame({'value': [1.0, 3.0], 'count': [2, 4]}))
        pdf2 = ts.TimeseriesPDF(pd.DataFrame({'value': [2.0, 5.0], 'count': [1, 3]}))
        pdf1.add(pdf2)
        self.assertEqual(list(pdf1.data['value']), [1.0, 2.0, 3.0, 5.0])
        self.assertEqual(list(pdf1.data['count']), [2, 1, 4, 3])

    def test_addOverlappingValues(self):
        # Old add() would leave duplicate rows; new add() merges by value
        pdf1 = ts.TimeseriesPDF(pd.DataFrame({'value': [1.0, 2.0, 3.0], 'count': [2, 3, 4]}))
        pdf2 = ts.TimeseriesPDF(pd.DataFrame({'value': [1.0, 2.0, 3.0], 'count': [2, 3, 4]}))
        pdf1.add(pdf2)
        self.assertEqual(len(pdf1.data), 3)
        self.assertEqual(list(pdf1.data['count']), [4, 6, 8])

    def test_CDF(self):
        df = pd.DataFrame({'value': [1, 2, 3], 'count': [2, 3, 1]})
        pdf = ts.TimeseriesPDF(df)
        result = pdf.toCDF()

        self.assertIsInstance(result, ts.TimeseriesCDF)
        expected = pd.DataFrame({'value': [1, 2, 3], 'cumulative_probability': [2/6, 5/6, 6/6]})
        self.assertTrue(result.data.reset_index(drop=True).equals(expected))

    def test_toCDFNoMutation(self):
        # toCDF() must not add columns to self.data
        df = pd.DataFrame({'value': [1, 2, 3], 'count': [2, 3, 1]})
        pdf = ts.TimeseriesPDF(df)
        pdf.toCDF()
        self.assertListEqual(list(pdf.data.columns), ['value', 'count'])

    def test_empty_timeseries(self):
        empty_ts = ts.TimeseriesRLE(df=pd.DataFrame(columns=["timestamp", "nextTS", "tsValue"]))
        pdf = ts.TimeseriesPDF.fromTS(empty_ts)

        expected_df = pd.DataFrame(columns=['value', 'count'])
        self.assertTrue(pdf.data.equals(expected_df))

    def test_cdf_emptyts(self):
        empty_ts = ts.TimeseriesRLE(df=pd.DataFrame(columns=["timestamp", "nextTS", "tsValue"]))
        pdf = ts.TimeseriesPDF.fromTS(empty_ts)
        cdf = pdf.toCDF()
        self.assertTrue(cdf.isempty())

    def test_valid_timeseries(self):
        data = {'timestamp': [1, 2, 3, 4], 'nextTS': [2, 3, 4, 5], 'tsValue': [1.2, 1.2, 1.5, 1.8]}
        ts1 = ts.TimeseriesRLE.fromDictList(data)
        pdf = ts.TimeseriesPDF.fromTS(ts1)

        expected_df = pd.DataFrame({'value': [1.2, 1.5, 1.8], 'count': [2, 1, 1]})
        self.assertTrue(pdf.data.equals(expected_df))

    def test_with_tolerance(self):
        data = {'timestamp': [1, 2, 3, 4, 5], 'nextTS': [2, 3, 4, 5, 6], 'tsValue': [1.19, 1.21, 1.52, 1.83, 1.18]}
        ts1 = ts.TimeseriesRLE.fromDictList(data)
        pdf = ts.TimeseriesPDF.fromTS(ts1, tolerance=[1])

        expected_df = pd.DataFrame({'value': [1.2, 1.5, 1.8], 'count': [3, 1, 1]})
        self.assertTrue(pdf.data.equals(expected_df))

    def test_with_datascale(self):
        data = {'timestamp': [1, 2, 3], 'nextTS': [2, 3, 4], 'tsValue': [10, 20, 30]}
        ts1 = ts.TimeseriesRLE.fromDictList(data)
        pdf = ts.TimeseriesPDF.fromTS(ts1, tolerance=[1], datascale=0.1)

        expected_df = pd.DataFrame({'value': [1.0, 2.0, 3.0], 'count': [1, 1, 1]})
        self.assertTrue(pdf.data.equals(expected_df))

    def test_stdBasic(self):
        # value=1 (count=2), value=3 (count=2) → mean=2, std=1
        pdf = ts.TimeseriesPDF(pd.DataFrame({'value': [1.0, 3.0], 'count': [2, 2]}))
        self.assertAlmostEqual(pdf.std(), 1.0)

    def test_stdEmpty(self):
        pdf = ts.TimeseriesPDF(pd.DataFrame(columns=['value', 'count']))
        self.assertTrue(np.isnan(pdf.std()))

    def test_meanTotalCounts(self):
        pdf = ts.TimeseriesPDF(pd.DataFrame({'value': [2.0, 4.0], 'count': [3, 1]}))
        self.assertAlmostEqual(pdf.mean(), 2.5)
        self.assertAlmostEqual(pdf.total(), 10.0)
        self.assertEqual(pdf.counts(), 4)

    def test_minMax(self):
        pdf = ts.TimeseriesPDF(pd.DataFrame({'value': [1.0, 5.0, 10.0], 'count': [1, 2, 1]}))
        self.assertEqual(pdf.min(), 1.0)
        self.assertEqual(pdf.max(), 10.0)

    def test_pdfInverseConvenience(self):
        pdf = ts.TimeseriesPDF(pd.DataFrame({'value': [0.0, 1.0], 'count': [1, 1]}))
        # inverse() must agree with toCDF().inverse()
        self.assertTrue(np.array_equal(pdf.inverse([0.25, 0.75]),
                                       pdf.toCDF().inverse([0.25, 0.75])))

    def test_with_empty_ts(self):
        empty_ts =  ts.TimeseriesRLE(df=pd.DataFrame(columns=["timestamp", "nextTS", "tsValue"]))
        pdf = ts.TimeseriesPDF.fromTS(empty_ts)
        total = pdf.total()
        mean = pdf.mean()
        max = pdf.max()
        min = pdf.min()
        counts = pdf.counts()

        self.assertTrue(np.isnan(total))
        self.assertTrue(np.isnan(mean))
        self.assertTrue(np.isnan(max))
        self.assertTrue(np.isnan(min))
        self.assertEqual(0, counts)

class TestTimeseriesCDF(unittest.TestCase):

    def makeCDF(self, values, cumprobs):
        df = pd.DataFrame({'value': values, 'cumulative_probability': cumprobs})
        return ts.TimeseriesCDF(df)

    def test_validInputs(self):
        cdf = self.makeCDF([0.2, 0.4, 0.6, 0.8, 1.0], [0.2, 0.6, 1.2, 2.0, 3.0])
        pts = [0.3, 0.75, 1.0]
        expected = [0.25, 0.45, 0.5333333333333333]
        self.assertTrue(np.array_equal(cdf.inverse(pts), expected))

    def test_invalidSamplePoints(self):
        cdf = self.makeCDF([0.2, 0.4, 0.6, 0.8, 1.0], [0.2, 0.6, 1.2, 2.0, 3.0])
        with self.assertRaises(ValueError):
            cdf.inverse([-0.2, 1.2, 2.0])

    def test_singleValueCDF(self):
        cdf = self.makeCDF([0.5], [0.5])
        self.assertEqual(cdf.inverse([0.25, 0.75]), [0.5, 0.5])

    def test_emptyCDF(self):
        cdf = ts.TimeseriesCDF(pd.DataFrame(columns=['value', 'cumulative_probability']))
        self.assertEqual(cdf.inverse([0.5]), [None])

    def test_missingCumulativeProbability(self):
        cdf = self.makeCDF([0.2, 0.4], [0.2, np.nan])
        self.assertEqual(cdf.inverse([0.3]), [None])

    def test_cdfStartsNonZero(self):
        cdf = self.makeCDF([0.3, 0.7, 1.0], [0.3, 1.0, 1.7])
        pts = [0.5, 0.8]
        expected = [0.41428571428571426, 0.5857142857142856]
        self.assertTrue(np.array_equal(cdf.inverse(pts), expected))

    def test_rleCDFInverse(self):
        # TimeseriesRLE.CDFInverse delegates through toPDF().inverse()
        rle = ts.TimeseriesRLE.fromDictList([
            {'timestamp':  3, 'nextTS':  7, 'tsValue': 10},
            {'timestamp': 11, 'nextTS': 14, 'tsValue':  8},
            {'timestamp': 18, 'nextTS': 19, 'tsValue':  3},
            {'timestamp': 23, 'nextTS': 25, 'tsValue':  7},
            {'timestamp': 27, 'nextTS': 30, 'tsValue':  3},
        ])
        result = rle.CDFInverse()
        self.assertTrue(np.allclose(result, [7.16666667], atol=1e-5))


def makeRLE(rows):
    df = pd.DataFrame(rows, columns=['timestamp', 'nextTS', 'tsValue'])
    return ts.TimeseriesRLE(df)

def sumOf(*tsObjects):
    return ts.TimeseriesSet(list(tsObjects)).sum()

def rleRows(tsObj):
    return list(tsObj.df[['timestamp', 'nextTS', 'tsValue']].itertuples(index=False, name=None))


class TestTimeseriesSetSum(unittest.TestCase):

    # ------------------------------------------------------------------ helpers

    def assertSumEquals(self, tsA, tsB, expected_rows):
        result = sumOf(tsA, tsB)
        self.assertEqual(rleRows(result), expected_rows,
                         msg=f"sum rows mismatch:\n  got {rleRows(result)}\n  expected {expected_rows}")

    def assertTotalsMatch(self, *tsObjects):
        result = ts.TimeseriesSet(list(tsObjects)).sum()
        expected = sum(t.total() for t in tsObjects)
        self.assertAlmostEqual(result.total(), expected, delta=max(1e-9, expected * 1e-12))

    # ------------------------------------------------------------------ disjoint / adjacent

    def test_disjoint(self):
        a = makeRLE([(0, 10, 5.0)])
        b = makeRLE([(20, 30, 3.0)])
        self.assertSumEquals(a, b, [(0, 10, 5.0), (20, 30, 3.0)])
        self.assertTotalsMatch(a, b)

    def test_adjacent(self):
        a = makeRLE([(0, 10, 5.0)])
        b = makeRLE([(10, 20, 3.0)])
        self.assertSumEquals(a, b, [(0, 10, 5.0), (10, 20, 3.0)])
        self.assertTotalsMatch(a, b)

    # ------------------------------------------------------------------ partial overlap

    def test_partialOverlap(self):
        a = makeRLE([(0, 10, 5.0)])
        b = makeRLE([(5, 15, 3.0)])
        self.assertSumEquals(a, b, [(0, 5, 5.0), (5, 10, 8.0), (10, 15, 3.0)])
        self.assertTotalsMatch(a, b)

    def test_containment(self):
        a = makeRLE([(0, 20, 2.0)])
        b = makeRLE([(5, 10, 3.0)])
        self.assertSumEquals(a, b, [(0, 5, 2.0), (5, 10, 5.0), (10, 20, 2.0)])
        self.assertTotalsMatch(a, b)

    # ------------------------------------------------------------------ breakpoint alignment

    def test_startAligned(self):
        a = makeRLE([(5, 15, 4.0)])
        b = makeRLE([(5, 20, 2.0)])
        self.assertSumEquals(a, b, [(5, 15, 6.0), (15, 20, 2.0)])
        self.assertTotalsMatch(a, b)

    def test_endAligned(self):
        a = makeRLE([(0, 10, 4.0)])
        b = makeRLE([(5, 10, 2.0)])
        self.assertSumEquals(a, b, [(0, 5, 4.0), (5, 10, 6.0)])
        self.assertTotalsMatch(a, b)

    def test_bothAligned(self):
        a = makeRLE([(5, 15, 4.0)])
        b = makeRLE([(5, 15, 2.0)])
        self.assertSumEquals(a, b, [(5, 15, 6.0)])
        self.assertTotalsMatch(a, b)

    def test_aEndIsBStart(self):
        # A ends exactly where B starts — should be treated as adjacent, not overlapping
        a = makeRLE([(0, 5, 4.0)])
        b = makeRLE([(5, 10, 2.0)])
        self.assertSumEquals(a, b, [(0, 5, 4.0), (5, 10, 2.0)])
        self.assertTotalsMatch(a, b)

    # ------------------------------------------------------------------ multi-interval

    def test_multiIntervalBothSides(self):
        a = makeRLE([(0, 5, 1.0), (10, 15, 2.0), (20, 25, 3.0)])
        b = makeRLE([(3, 12, 10.0), (18, 22, 5.0)])
        # breakpoints: 0,3,5,10,12,15,18,20,22,25
        expected = [
            (0,  3,  1.0),   # a only
            (3,  5, 11.0),   # a+b
            (5, 10, 10.0),   # b only (gap in a)
            (10, 12, 12.0),  # a+b
            (12, 15,  2.0),  # a only
            (18, 20,  5.0),  # b only (gap in a)
            (20, 22,  8.0),  # a+b
            (22, 25,  3.0),  # a only
        ]
        self.assertSumEquals(a, b, expected)
        self.assertTotalsMatch(a, b)

    def test_gapInAFilledByB(self):
        # A has a gap; B covers that gap plus beyond
        a = makeRLE([(0, 5, 4.0), (10, 15, 4.0)])
        b = makeRLE([(3, 12, 2.0)])
        expected = [
            (0,  3, 4.0),
            (3,  5, 6.0),
            (5, 10, 2.0),  # gap in A — B alone
            (10, 12, 6.0),
            (12, 15, 4.0),
        ]
        self.assertSumEquals(a, b, expected)
        self.assertTotalsMatch(a, b)

    # ------------------------------------------------------------------ three-way overlap

    def test_threeWay(self):
        a = makeRLE([(0, 10, 1.0)])
        b = makeRLE([(3, 13, 2.0)])
        c = makeRLE([(6, 16, 4.0)])
        result = ts.TimeseriesSet([a, b, c]).sum()
        expected = [
            (0,  3, 1.0),
            (3,  6, 3.0),
            (6, 10, 7.0),
            (10, 13, 6.0),
            (13, 16, 4.0),
        ]
        self.assertEqual(rleRows(result), expected)
        self.assertTotalsMatch(a, b, c)

    # ------------------------------------------------------------------ edge / degenerate

    def test_emptySet(self):
        result = ts.TimeseriesSet([]).sum()
        self.assertTrue(result.isempty())

    def test_singleTS(self):
        a = makeRLE([(0, 5, 3.0), (10, 20, 7.0)])
        result = ts.TimeseriesSet([a]).sum()
        self.assertEqual(rleRows(result), [(0, 5, 3.0), (10, 20, 7.0)])

    def test_emptyMember(self):
        a = makeRLE([(0, 10, 5.0)])
        empty = ts.TimeseriesRLE(pd.DataFrame(columns=['timestamp', 'nextTS', 'tsValue']))
        result = ts.TimeseriesSet([a, empty]).sum()
        self.assertEqual(rleRows(result), [(0, 10, 5.0)])

    def test_zeroValueInterval(self):
        # Zero-value intervals are excluded from the result
        a = makeRLE([(0, 5, 0.0), (5, 10, 3.0)])
        b = makeRLE([(0, 10, 2.0)])
        result = sumOf(a, b)
        # [0,5]: 0+2=2, [5,10]: 3+2=5
        self.assertEqual(rleRows(result), [(0, 5, 2.0), (5, 10, 5.0)])


class TSSetScaleTest(unittest.TestCase):
    """
    Timing tests for TimeseriesSet.sum().

    Two shapes of equal total-event volume (~100M events) are exercised:
      - Few wide series:    10 timeseries x 1M entries each  (~10M events)
      - Many narrow series: 1K timeseries x 10K entries each (~10M events)

    These tests assert correctness (total must match sum of individual totals)
    and print per-event throughput via Timer so regressions are visible.
    """

    def genTimeseriesVectorized(self, numEntries, timeOffset=0):
        timeSpan = numEntries * 3
        startTimes = (np.sort(np.random.choice(timeSpan, size=numEntries, replace=False))
                      .astype(np.int64) + timeOffset)
        deltas = np.empty(numEntries, dtype=np.int64)
        deltas[:-1] = startTimes[1:] - startTimes[:-1]
        deltas[-1] = timeSpan
        durations = np.maximum(1, (np.random.random(numEntries) * deltas).astype(np.int64))
        vals = np.random.uniform(0.1, 100.0, size=numEntries)
        df = pd.DataFrame({'timestamp': startTimes,
                           'nextTS':    startTimes + durations,
                           'tsValue':   vals})
        return ts.TimeseriesRLE(df)

    def genTimeseriesFixed(self, numEntries, timeOffset=0):
        startTimes = np.arange(timeOffset, timeOffset + numEntries * 2, 2, dtype=np.int64)
        vals = np.random.uniform(0.1, 100.0, size=numEntries)
        df = pd.DataFrame({'timestamp': startTimes,
                           'nextTS':    startTimes + 1,
                           'tsValue':   vals})
        return ts.TimeseriesRLE(df)

    def test_sumFewWideSeries(self):
        NUM_TIMESERIES = 10
        ENTRIES_PER_TS = 1_000_000
        TIME_STRIDE = ENTRIES_PER_TS * 3

        with Timer("Generate timeseries") as t:
            tsSet = []
            for i in range(NUM_TIMESERIES):
                tsSet.append(self.genTimeseriesVectorized(ENTRIES_PER_TS, timeOffset=i * TIME_STRIDE))
            t.setCount(NUM_TIMESERIES * ENTRIES_PER_TS)

        expectedTotal = sum(single.total() for single in tsSet)

        with Timer("TimeseriesSet.sum()") as t:
            result = ts.TimeseriesSet(tsSet).sum()
            t.setCount(NUM_TIMESERIES * ENTRIES_PER_TS)

        self.assertFalse(result.isempty())
        self.assertAlmostEqual(result.total(), expectedTotal, delta=max(1.0, expectedTotal * 1e-9))

    def test_sumManyNarrowSeries(self):
        NUM_TIMESERIES = 1_000
        ENTRIES_PER_TS = 10_000
        TIME_STRIDE = ENTRIES_PER_TS * 2

        with Timer("Generate timeseries") as t:
            tsSet = []
            for i in range(NUM_TIMESERIES):
                tsSet.append(self.genTimeseriesFixed(ENTRIES_PER_TS, timeOffset=i * TIME_STRIDE))
            t.setCount(NUM_TIMESERIES * ENTRIES_PER_TS)

        expectedTotal = sum(single.total() for single in tsSet)

        with Timer("TimeseriesSet.sum()") as t:
            result = ts.TimeseriesSet(tsSet).sum()
            t.setCount(NUM_TIMESERIES * ENTRIES_PER_TS)

        self.assertFalse(result.isempty())
        self.assertAlmostEqual(result.total(), expectedTotal, delta=max(1.0, expectedTotal * 1e-9))

    def test_sumVsOldSum(self):
        # Timing comparison: sum() (event-based) vs oldSum() (addSquare reduce).
        # 10 timeseries x 100K entries each is large enough to show a clear
        # difference (oldSum is O(K^2 * M), sum is O(K*M log(K*M))) but small
        # enough to complete in a few seconds.
        NUM_TIMESERIES = 10
        ENTRIES_PER_TS = 100_000
        TIME_STRIDE = ENTRIES_PER_TS * 3

        tsSet = []
        for i in range(NUM_TIMESERIES):
            tsSet.append(self.genTimeseriesVectorized(ENTRIES_PER_TS, timeOffset=i * TIME_STRIDE))

        expectedTotal = sum(single.total() for single in tsSet)

        with Timer("oldSum()") as t:
            oldResult = ts.TimeseriesSet(tsSet).oldSum()
            t.setCount(NUM_TIMESERIES * ENTRIES_PER_TS)

        with Timer("sum()") as t:
            newResult = ts.TimeseriesSet(tsSet).sum()
            t.setCount(NUM_TIMESERIES * ENTRIES_PER_TS)

        # Only assert correctness for the new implementation; oldSum() is known
        # to produce incorrect totals for overlapping timeseries (it is preserved
        # here solely as a timing baseline).
        self.assertAlmostEqual(newResult.total(), expectedTotal, delta=max(1.0, expectedTotal * 1e-9))


@unittest.skipUnless(_PLOTTERS_AVAILABLE, "plotting backends not available")
class TestPlotter(unittest.TestCase):
    ts1 = ts.TimeseriesRLE.fromDictList(dictList=[
                    {'timestamp': 2, 'nextTS': 6, 'tsValue': 0},
                    {'timestamp': 6, 'nextTS': 8, 'tsValue': 4},
                    {'timestamp': 8, 'nextTS': 9, 'tsValue': 20},
                    {'timestamp': 9, 'nextTS': 12, 'tsValue': 4},
                    {'timestamp': 14, 'nextTS': 17, 'tsValue': 20.01},
                    {'timestamp': 17, 'nextTS': 25, 'tsValue': 7},
                    {'timestamp': 32, 'nextTS': 36, 'tsValue': 0.01},
                    {'timestamp': 38, 'nextTS': 43, 'tsValue': 20.02},
                ],
                name="ts1",
                units="kg/s")
    pdf1 = ts1.toPDF()

    def test_TimeseriesRlePlot(self):
        # todo: How does the plot selector stuff work with saving plots to files?
        matplotlib_plot_selector(self.ts1)
        bokeh_plotly_selector(self.ts1)
        plotly_plot_selector(self.ts1)
        return None
        
    def test_pdfPlot(self):
        # todo: How does the plot selector stuff work with saving plots to files?
        matplotlib_plot_selector(self.pdf1)
        bokeh_plotly_selector(self.pdf1)
        plotly_plot_selector(self.pdf1)
        return None
        
    def test_CategoriesPlot(self):
        # todo: How does the plot selector stuff work with saving plots to files?
        tsCat1 = ts.TimeseriesCategorical.fromDictList(TS1_CATEGORICAL_SPEC, name='tsCat1')
        matplotlib_plot_selector(tsCat1)
        bokeh_plotly_selector(tsCat1)
        plotly_plot_selector(tsCat1)
        return None
