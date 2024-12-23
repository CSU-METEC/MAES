import numpy as np
import pandas as pd
from abc import ABC, abstractmethod
import scipy.integrate as si
import scipy.interpolate as sint
import logging

logger = logging.getLogger(__name__)

#
# Expanded Time Series provide for operations on complete timeseries
# Operations
#  - Add, subtract, multiply, divide
#  - Total (sum of values across an interval)
#  - Resample up / down (go from second intervals to minute, hour, day, ... and vice versa)
#  - Statistical operations
#
# Derived from Dan Z.'s Matlab timeseries code
#

class MalformedTimeseriesError(Exception):
    pass

class Timeseries(ABC):

    def __init__(self, name=None, units=None):
        self.name = name
        self.units = units

    @classmethod
    @abstractmethod
    def fromCollections(cls, startTimeCollection, endTimeCollection, valueCollection, **kwargs):
        raise NotImplementedError()

    @property
    def _name(self):
        return self.name

    @property
    def _units(self):
        return self.units

    @abstractmethod
    def _duration(self):
        raise NotImplementedError()

    @property
    @abstractmethod
    def _values(self):
        raise NotImplementedError()

    # return a set of all interval start values
    @property
    @abstractmethod
    def _startTimes(self):
        raise NotImplementedError()

    # return a set of all interval end values
    @property
    @abstractmethod
    def _endTimes(self):
        raise NotImplementedError()

    @abstractmethod
    def sampleSquare(self):
        raise NotImplementedError()

    def equal(self, ts2):
        if not isinstance(ts2, Timeseries):
            return False
        # ret = (
        #     (self._name == ts2._name)
        #     & (self._units == ts2._units)
        # )
        ret = True

        return ret

    def __eq__(self, ts2):
        return self.equal(ts2)

    def _artithmeticPrep(self, ts2):
        # all arithmetic operators need to be adjusted in the same way
        if not isinstance(ts2, Timeseries):
            raise MalformedTimeseriesError(f"{ts2} is not an ExpandedTimeseries")

        # find common breakpoints
        ts1StartSet = set(self._startTimes)
        ts1EndSet = set(self._endTimes)
        ts2StartSet = set(ts2._startTimes)
        ts2EndSet = set(ts2._endTimes)
        bpList = sorted(list(ts1StartSet.union(*[ts1EndSet, ts2StartSet, ts2EndSet])))

        # interpolate
        e1 = self.sampleSquare(bpList)
        e2 = ts2.sampleSquare(bpList)

        return e1, e2, bpList

    def total(self):
        tot = (self._duration() * self._values()).sum()
        return tot

    def addSquare(self, ts2):
        # add
        e1, e2, bpList = self._artithmeticPrep(ts2)
        e = list(e1 + e2)
        tsOut = self.__class__.fromCollections(bpList[:-1], bpList[1:], e[:-1], filterZeros=True)
        return tsOut

    def subtractSquare(self, ts2):
        e1, e2, bpList = self._artithmeticPrep(ts2)
        e = e1 - e2
        return self.__class__.fromCollections(bpList[:-1], bpList[1:], e[:-1], filterZeros=True)

    def multiplySquare(self, ts2):
        e1, e2, bpList = self._artithmeticPrep(ts2)
        e = e1 * e2
        return self.__class__.fromCollections(bpList[:-1], bpList[1:], e[:-1], filterZeros=True)

    def divideSquare(self, ts2):
        e1, e2, bpList = self._artithmeticPrep(ts2)
        e = e1 / e2
        return self.__class__.fromCollections(bpList[:-1], bpList[1:], e[:-1], filterZeros=True)

#
# RLE encoded timeseries.  Non-zero values are implemented as (startTime, endTime] intervals with a rate column
# Zero rate values can be either explicitly or implictly specified -- any interval not specified will be assumed to
# be zero
#

# Modified to not copy incoming dataframe on instance creation

class TimeseriesRLE(Timeseries):

    def _isSorted(self):
        tmpDF = pd.DataFrame({'endTime': self._endTimes, 'shiftedEndTime': self._endTimes.shift(-1)})
        tmpDF = tmpDF.assign(deltaCol=(tmpDF['endTime'] - tmpDF['shiftedEndTime']))
        isSorted = (tmpDF['deltaCol'] > 0.0).sum() == 0
        if not isSorted:
            i = 10
        return isSorted

    # column name defaults are consistent w/ MEET instantaneousEvents.csv merged with emissionTimeseries.csv files
    def __init__(self, df, startTimeColName='timestamp', endTimeColName='nextTS', valueColName='tsValue', filterZeros=False,
                 **kwargs):
        super().__init__(**kwargs)

        cols = list(df.columns)
        initError = False
        if startTimeColName not in cols:
            logger.error(f"startTimeColName {startTimeColName} not in df columns {cols}")
            initError = True
        if endTimeColName not in cols:
            logger.error(f"endTimeColName {endTimeColName} not in df columns {cols}")
            initError = True
        if valueColName not in cols:
            logger.error(f"valueColName {valueColName} not in df columns {cols}")
            initError = True

        if initError:
            raise MalformedTimeseriesError

        self.df = df
        self.startTimeColName = startTimeColName
        self.endTimeColName = endTimeColName
        self.valueColName = valueColName

        if filterZeros:
            self.df = self.df[self.df[self.valueColName] != 0.0].reset_index(drop=True)

        self.colList = [self.startTimeColName, self.endTimeColName, self.valueColName]

        self.sorted = self._isSorted()

        if not self.sorted:
            msg = f"Input dataframe for {self.name} column {self.endTimeColName} is not strictly increasing"
            logger.error(msg)
            raise MalformedTimeseriesError(msg)

        # create an interval cache for _intervalSample
        #
        # intervals = pd.IntervalIndex.from_arrays(self._startTimes, self._endTimes, closed='left')
        # self._intervalDF = pd.DataFrame(data={'values': self._values}).set_index(intervals)

    @classmethod
    def fromCollections(cls, startTimeCollection, endTimeCollection, valueCollection, **kwargs):
        df = pd.DataFrame({'timestamp': startTimeCollection, 'nextTS': endTimeCollection, 'tsValue': valueCollection})
        return cls(df, **kwargs)

    @classmethod
    def fromDictList(cls, dictList, valueColName='tsValue', **kwargs):
        df = pd.DataFrame.from_records(dictList)
        return cls(df, valueColName=valueColName, **kwargs)

    @property
    def _duration(self):
        dur = self.df[self.endTimeColName] - self.df[self.startTimeColName]
        return dur

    @property
    def _values(self):
        values = self.df[self.valueColName]
        return values

    @property
    def _startTimes(self):
        return self.df[self.startTimeColName]

    @property
    def _endTimes(self):
        return self.df[self.endTimeColName]

    def equal(self, ts2):
        ret = (
            super().equal(ts2)
            and (len(self.df) == len(ts2.df))
            and np.array_equal(self._startTimes.values, ts2._startTimes.values)
            and np.array_equal(self._endTimes, ts2._endTimes)
            and np.array_equal(self._values, ts2._values)
        )

        return ret

    def _intervalSample(self, bpList):
        ret = pd.DataFrame(data=0.0, index=bpList, columns=[self.valueColName])
        ret[self.valueColName] = ret.index.map(self._intervalDF['values']).fillna(0)
        ret = ret.squeeze()
        return ret

    def _sortedSample(self, bpList):
        indVect = self._endTimes.searchsorted(bpList, side='left')

        # filter out indices where bpList value exceeds max end time

        li2 = pd.Series(indVect)
        li1 = li2[li2.values < len(self.df)]
        bpl1 = bpList[:len(li1)]

        indDF = self.df.iloc[li1.values]
        indDF = indDF.assign(bpl1=bpl1, iv=li1.index, ov=li1.values)
        indDF = indDF.assign(useVal=(indDF[self.startTimeColName] <= indDF['bpl1'])
                                    & (indDF['bpl1'] < indDF[self.endTimeColName]))
        mapSeries = indDF.loc[indDF['useVal'], 'bpl1']

        # create new dataframe with startTime, endTime, rate corresponding to bpList

        compDF = pd.DataFrame(data={'calcRate': 0.0}, index=bpList)
        compDF.loc[self.df[self.startTimeColName], 'calcRate'] = self.df[self.valueColName].values
        compDF.loc[mapSeries.values, 'calcRate'] = self.df.loc[mapSeries.index][self.valueColName].values
        ret = compDF['calcRate'].squeeze()
        return ret


    def sampleSquare(self, bpList):
        # times are closed on the bottom end, open on the top end:
        #  self.df['startTime'] <= t < self.df['endTime']

        # ret = self._intervalSample(bpList)
        ret = self._sortedSample(bpList)

        return ret

    def periodicAverage(self, intervals):
        fullTS = self.toFullTimeseries()
        fullDF = fullTS.df
        ctz = si.cumulative_trapezoid(fullDF['rate'], fullDF['timestamp'], initial=0)
        interpVals = np.interp(intervals, fullDF['timestamp'], ctz)
        rateOut = np.diff(interpVals) / np.diff(intervals)
        outTS = TimeseriesRLE.fromCollections(intervals[:-1], intervals[1:], rateOut)
        return outTS

    def toFullTimeseries(self):

        # insert zero intervals
        stName = self.startTimeColName
        etName = self.endTimeColName
        valName = self.valueColName

        zeroIntervals = pd.DataFrame({stName: self.df[:-1][etName].to_numpy(),
                                      etName: self.df[1:][stName].to_numpy(),
                                      valName: 0})
        zeroLenIntervalMask = (zeroIntervals[stName] >= zeroIntervals[etName])
        zeroIntervals = zeroIntervals[~zeroLenIntervalMask]
        origDF = pd.concat([self.df, zeroIntervals]).sort_values(stName)

        # create the output dataframe

        p1 = pd.concat([origDF[[etName, valName]].rename(columns={etName: stName}),
                        origDF[[stName, valName]]
                        ]).reset_index(drop=True)


        extDF = (p1
                 .sort_values(stName, kind='stable')
                 .reset_index(drop=True)
                 .drop_duplicates()
                 )

        ret = TimeseriesFull.fromCollections(extDF[stName], extDF[valName],
                                             name=self.name, units=self.units)

        return ret

    def toRLETimeseries(self):
        return self

#
# Fully expanded timeseries, useful for graphing and other packages.  Only a single time column, with every rate change
# indicated by two timestamps -- one at the start of the interval and one at the end.  This implies that there will
# be duplicate timestamps
#
# Uses numpy to calculate total via trapezoidal integration
#

def calcZeroEndDF(forceZeroEnds, tmpStartDF, tmpEndDF):
    zeStartDF = pd.DataFrame(columns=['timestamp', 'rate'])
    zeEndDF = pd.DataFrame(columns=['timestamp', 'rate'])
    if forceZeroEnds:
        zeStartList = []
        zeEndList = []
        minStartIdx = tmpStartDF['timestamp'].idxmin()
        if tmpStartDF.loc[minStartIdx]['rate'] != 0:
            zeEntry0 = {'timestamp': 0, 'rate': 0}
            zeStartList.append(zeEntry0)
            zeEntry1 = {'timestamp': tmpStartDF.loc[minStartIdx]['timestamp'], 'rate': 0}
            zeStartList.append(zeEntry1)
        maxEndIdx = tmpEndDF['timestamp'].idxmax()
        if tmpEndDF.loc[maxEndIdx]['rate'] != 0:
            zeEntryMax = {'timestamp': tmpEndDF.loc[maxEndIdx]['timestamp'], 'rate': 0}
            zeEndList.append(zeEntryMax)
        zeStartDF = pd.DataFrame.from_records(zeStartList)
        zeEndDF = pd.DataFrame.from_records(zeEndList)
    return zeStartDF, zeEndDF

class TimeseriesFull(Timeseries):

    def __init__(self, df,
                 startTimeColName='timestamp', rateColName='tsValue',
                 forceZeroEnds=True,
                 **kwargs):
        super().__init__(**kwargs)

        self.df = df.rename(columns={startTimeColName: 'timestamp', rateColName: 'rate'})

    @classmethod
    def fromCollections(cls, startTimeCollection, rateCollection, **kwargs):
        df = pd.DataFrame({'timestamp': startTimeCollection, 'tsValue': rateCollection})
        return cls(df, **kwargs)

    def _duration(self):
        pass

    def _values(self):
        pass

    def _startTimes(self):
        return set(self.df['timestamp'])

    def _endTimes(self):
        return set(self.df['timestamp'])

    def sampleSquare(self, bpList):
        ret = np.interp(bpList, xp=self.df['timestamp'].to_numpy(), fp=self.df['rate'].to_numpy())
        retDF = pd.Series(index=bpList, data=ret, name='rate')
        return retDF

    def total(self):
        ret = np.trapz(self.df['rate'], x=self.df['timestamp'])
        return ret

    def cumtrapz(self):
        fullDF = self.df
        ctz = si.cumtrapz(fullDF['rate'], fullDF['timestamp'], initial=0)
        return self.__class__.fromCollections(fullDF['timestamp'], ctz)

    def periodicAverage(self, intervals):
        iSeries = pd.Series(intervals)
        ctzTS = self.cumtrapz()
        ctzDF = ctzTS.df

        # Make sure there are points defined for the beginning and end of the intervals

        preAdjustDF = pd.DataFrame()
        if iSeries.iloc[0] < ctzDF.iloc[0]['timestamp']:
            preAdjustDF = pd.DataFrame([[iSeries.iloc[0], ctzDF.iloc[0]['rate']]], columns=['timestamp', 'rate'])
        postAdjustDF = pd.DataFrame()
        if iSeries.iloc[-1] > ctzDF.iloc[-1]['timestamp']:
            postAdjustDF = pd.DataFrame([[iSeries.iloc[-1], ctzDF.iloc[-1]['rate']]], columns=['timestamp', 'rate'])

        calcDF = pd.concat([preAdjustDF, ctzDF, postAdjustDF]).drop_duplicates()
        interpObj = sint.interp1d(calcDF['timestamp'], calcDF['rate'], kind='linear')
        cumOut = pd.Series(interpObj(iSeries))

        rateOut = cumOut.diff()[1:] / iSeries.diff()[1:]

        ret = TimeseriesRLE.fromCollections(iSeries[:-1].to_numpy(), iSeries[1:].to_numpy(), rateOut,
                                            name=self.name, rateUnits=self.units)
        return ret


class TimeseriesCategorical(TimeseriesRLE):

    def __init__(self, df, **kwargs):
        super().__init__(df, **kwargs)
        categories = pd.Categorical(df[self.valueColName])
        self.df = self.df.assign(catVal=categories.codes, categories=categories)
        self.valueColName = 'catVal'

    @classmethod
    def fromCollections(cls, startTimeCollection, endTimeCollection, valueCollection, **kwargs):
        raise NotImplementedError()

    def toFullTimeseries(self):
        ret = super().toFullTimeseries()
        # the rate column in the returned full timeseries is an index into the original categorical.
        # create a new categorical column for the returned dataframe based on the new rate & the original categorical
        retCategories = pd.Categorical(self.df['categories'].cat.categories[ret.df['rate']])
        ret.df = ret.df.assign(categories=retCategories)
        return ret

    def _duration(self):
        raise NotImplementedError()

    def _values(self):
        raise NotImplementedError()

    # return a set of all interval start values
    def _startTimes(self):
        raise NotImplementedError()

    @property
    def _endTimes(self):
        return self.df[self.endTimeColName]

    def sampleSquare(self):
        raise NotImplementedError()
