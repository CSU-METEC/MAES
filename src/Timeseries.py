import numpy as np
import pandas as pd
from abc import ABC, abstractmethod
import scipy.integrate as si
import scipy.interpolate as sint
import logging
# import helper as hp
from enum import Enum

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
HOURS_PER_DAY = 24
SECONDS_PER_HOUR = 3600
SECONDS_PER_MINUTE = 60


class Method(Enum):
    SUM = "sum"
    MEAN = "mean"
    MAX = "max"
    MIN = "min"
    MEDIAN = "median"
    STD = "std"
    LOWER = "lower"
    UPPER = "upper"


class MalformedTimeseriesError(Exception):
    pass


def dataframeMask(df, mask,
                  dfStartTimeName='timestamp',
                  dfEndTimeName='nextTS',
                  dfValueName='tsValue',
                  maskStartTimeName='timestamp',
                  maskEndTimeName='nextTS',
                  maskValueName='tsValue'
                  ):
    if not isinstance(df, pd.DataFrame):
        raise ValueError(f"df parameter is not a pandas Dataframe")
    if not isinstance(mask, pd.DataFrame):
        raise ValueError(f"mask parameter is not a pandas Dataframe")

    # pd.arrays.IntervalArray also works for these.  Which is better?
    dataMap = pd.IntervalIndex.from_arrays(df[dfStartTimeName], df[dfEndTimeName], closed='both')
    # dataMap = pd.arrays.IntervalArray.from_arrays(df[dfStartTimeName], df[dfEndTimeName], closed='both')
    dmDF = pd.DataFrame({"dataIdx": df.index}, index=dataMap)
    maskMap = pd.IntervalIndex.from_arrays(mask[maskStartTimeName], mask[maskEndTimeName], closed='both')
    # maskMap = pd.arrays.IntervalArray.from_arrays(mask[maskStartTimeName], mask[maskEndTimeName], closed='both')
    maskDF = pd.DataFrame({"maskIdx": mask.index}, index=maskMap)
    om = list(map(dmDF.index.overlaps, maskDF.index))
    # len(om) == len(maskDF)
    # len(om[i]) == len(dmDF)
    if len(om) != len(maskDF):
        logging.warning(f"len(om) != len(maskDF)")

    retList = []
    for maskDFIdx, dataMask in enumerate(om):
        if not any(dataMask):
            continue
        if len(dataMask) != len(dmDF):
            logger.warning("len(datamask) != len(dmDF)")
        maskIdx = maskDF.iloc[maskDFIdx]['maskIdx']
        maskEntry = mask.loc[maskIdx:maskIdx]
        if len(maskEntry) > 1:
            logger.warning("maskEntry > 1")
        maskEntry = maskEntry.squeeze()
        dataDF = df[dataMask]

        dataDF = dataDF.assign(maskIdx=maskIdx,
                               maskStartTime=maskEntry[maskStartTimeName],
                               maskEndTime=maskEntry[maskEndTimeName],
                               maskValue=maskEntry[maskValueName])

        retList.append(dataDF)

    if not retList:
        return pd.DataFrame()

    retDF = pd.concat(retList)
    retDF = retDF.assign(maskedStartTime=retDF[[dfStartTimeName, 'maskStartTime']].max(axis='columns'),
                         maskedEndTime=retDF[[dfEndTimeName, 'maskEndTime']].min(axis='columns')
                         )
    retDF = retDF[(retDF[maskValueName] != 0)]
    return retDF


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
    def _durations(self):
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

    @property
    @abstractmethod
    def _startEndTimes(self):
        raise NotImplementedError

    # return a tuple of (minVal, maxVal) suitable for graphing (i.e. add some padding)
    @property
    def range(self):
        vals = self._values

        maxVal = vals.astype(float).max()
        minVal = vals.astype(float).min()
        # Add a little margin so graphs are not clipped
        maxVal += abs(maxVal * .1)
        minVal -= abs(minVal * .1)
        calcYRange = (minVal, maxVal if maxVal != 0.0 else 0.0001)  # Give a little margin for maximum rates
        return calcYRange

    @abstractmethod
    def sampleSquare(self):
        raise NotImplementedError()

    def equal(self, ts2):
        if not isinstance(ts2, Timeseries):
            return False
        ret = True

        return ret

    def __eq__(self, ts2):
        return self.equal(ts2)

    def _arithmeticPrep(self, ts2):
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
        tot = (self._durations * self._values).sum()
        return tot

    def addSquare(self, ts2, filterZeros):
        # add
        e1, e2, bpList = self._arithmeticPrep(ts2)
        e = list(e1 + e2)
        tsOut = self.__class__.fromCollections(bpList[:-1], bpList[1:], e[:-1], filterZeros=filterZeros)
        return tsOut

    def subtractSquare(self, ts2):
        e1, e2, bpList = self._arithmeticPrep(ts2)
        e = e1 - e2
        return self.__class__.fromCollections(bpList[:-1], bpList[1:], e[:-1], filterZeros=True)

    def multiplySquare(self, ts2):
        e1, e2, bpList = self._arithmeticPrep(ts2)
        e = e1 * e2
        return self.__class__.fromCollections(bpList[:-1], bpList[1:], list(e)[:-1], filterZeros=True)

    def divideSquare(self, ts2):
        e1, e2, bpList = self._arithmeticPrep(ts2)
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
        return isSorted

    # column name defaults are consistent w/ MEET instantaneousEvents.csv merged with emissionTimeseries.csv files
    def __init__(self, df, startTimeColName='timestamp', endTimeColName='nextTS', valueColName='tsValue',
                 filterZeros=False,
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

        self.df = df.reset_index(drop=True)
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
    def _durations(self):
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

    def isempty(self):
        if self.df.empty:
            return True
        return False

    def equal(self, ts2):
        ret = (
                super().equal(ts2)
                and (len(self.df) == len(ts2.df))
                and np.array_equal(self._startTimes.values, ts2._startTimes.values)
                and np.array_equal(self._endTimes, ts2._endTimes)
                and np.allclose(self._values, ts2._values)
        )

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

    def mask(self, ts2, fillZeros=False):
        if not isinstance(ts2, Timeseries):
            raise MalformedTimeseriesError(f"{ts2} is not an ExpandedTimeseries")

        maskDF = ts2.df.assign(maskVal=1)
        maskTS = TimeseriesRLE(maskDF,
                               startTimeColName=ts2.startTimeColName,
                               endTimeColName=ts2.endTimeColName,
                               valueColName='maskVal')

        retTS = self.multiplySquare(maskTS)
        if fillZeros:
            # currently assumes ts2 is a single interval
            retDF = retTS.df
            renameDict = {'timestamp': retTS.startTimeColName, 'nextTS': retTS.endTimeColName,
                          'tsValue': retTS.valueColName}
            startTS = maskDF.iloc[0][ts2.startTimeColName]
            endTS = maskDF.iloc[0][ts2.endTimeColName]
            zeroDF = pd.DataFrame()

            zeroDF = zeroDF.assign(nextTS=retTS._startTimes.shift(-1, fill_value=endTS),
                                   timestamp=retTS._endTimes,
                                   tsValue=0)
            zeroDF = zeroDF.rename(columns=renameDict)

            retFirstStart = retTS._startTimes[0]
            if startTS < retFirstStart:
                tsList = [
                    {'timestamp': startTS, 'nextTS': retFirstStart, 'tsValue': 0}
                ]
                firstIntervalDF = pd.DataFrame(tsList)
                firstIntervalDF = firstIntervalDF.rename(columns=renameDict)
            else:
                firstIntervalDF = pd.DataFrame()

            retDF = pd.concat([retDF, zeroDF, firstIntervalDF]).sort_values(retTS.startTimeColName).reset_index()
            retTS.df = retDF
        return retTS

    def nonzero(self):
        subDF = self.df[self.df[self.valueColName] > 0]
        subDF = subDF.assign(maskVal=1)
        newTS = TimeseriesRLE(subDF,
                              startTimeColName=self.startTimeColName,
                              endTimeColName=self.endTimeColName,
                              valueColName='maskVal')
        return newTS

    def mask2(self, ts2):
        if not isinstance(ts2, Timeseries):
            raise MalformedTimeseriesError(f"{ts2} is not an ExpandedTimeseries")

        nonZeroTS2 = ts2.nonzero()

        dataMap = pd.arrays.IntervalArray.from_arrays(self._startTimes, self._endTimes, closed='both')
        dmDF = pd.DataFrame({"dataIdx": self.df.index}, index=dataMap)
        maskMap = pd.IntervalIndex.from_arrays(nonZeroTS2._startTimes, nonZeroTS2._endTimes, closed='both')
        maskDF = pd.DataFrame({"dataIdx": nonZeroTS2.df.index}, index=maskMap)
        om = list(filter(any, map(dmDF.index.overlaps, maskDF.index)))
        # len(om) == len(maskDF)
        # len(om[i]) == len(dmDF)
        if len(om) != len(maskDF):
            logger.warning(f"len(om) != len(maskDF)")

        retList = []
        for maskDFIdx, dataMask in enumerate(om):
            if len(dataMask) != len(dmDF):
                logger.warning("len(datamask) != len(dmDF)")
            maskEntry = nonZeroTS2.df.loc[maskDF.iloc[maskDFIdx]]
            if len(maskEntry) > 1:
                logger.warning("maskEntry > 1")
            maskEntry = maskEntry.squeeze()
            dataDF = self.df[dataMask]

            dataDF = dataDF.assign(maskIdx=maskDFIdx)

            retList.append(dataDF)

        retDF = pd.concat(retList)
        return retDF

    def periodicAverage(self, intervals):
        fullTS = self.toFullTimeseries()
        fullDF = fullTS.df
        ctz = si.cumulative_trapezoid(fullDF[fullTS.valueColName], fullDF[fullTS.startTimeColName], initial=0)
        interpVals = np.interp(intervals, fullDF[fullTS.startTimeColName], ctz)
        rateOut = np.diff(interpVals) / np.diff(intervals)
        outTS = TimeseriesRLE.fromCollections(intervals[:-1], intervals[1:], rateOut,
                                              startTimeColName=self.startTimeColName,
                                              endTimeColName=self.endTimeColName,
                                              valueColName=self.valueColName
                                              )
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

    def toCompleteTimeseries(self):
        tsList = []
        for _, singleRow in self.df.iterrows():
            end = singleRow[self.endTimeColName].astype(int)
            start = singleRow[self.startTimeColName].astype(int)
            val = singleRow[self.valueColName]
            expVals = list(map(lambda x: {self.startTimeColName: x, self.valueColName: val}, range(start, end)))
            tsList.extend(expVals)
        # Need to add one final entry on the end 'cause range does not include its terminating value
        endEntry = {self.startTimeColName: end, self.valueColName: val}
        tsList.append(endEntry)
        retDF = pd.DataFrame(tsList)
        retTS = TimeseriesFull(retDF, startTimeColName=self.startTimeColName, rateColName=self.valueColName)
        return retTS

    def toPDF(self, omitZero=None):
        ret = TimeseriesPDF.fromTS(self, omitZero)
        return ret

    def CDFInverse(self, pts=[0.5]):
        p1 = self.toPDF()
        cdf = p1.toCDF()
        r = p1.cdfInverse(cdf, pts)
        return r

    def toRLETimeseries(self):
        return self

    def maskTS(self, tStart=None, tEnd=None, fill=False):
        stName = self.startTimeColName
        etName = self.endTimeColName
        valName = self.valueColName
        if self.df.empty:
            columns = [stName, etName, valName]
            df = pd.DataFrame(columns=columns)
            return self.__class__(df)

        tsIn = self.df
        if tStart is None:
            tStart = self.startTime()
        if tEnd is None:
            tEnd = self.endTime()

        # Compute the index
        idxKeep = (tsIn[etName] > tStart) & (tsIn[stName] <= tEnd)
        tsOut = tsIn[idxKeep].reset_index(drop=True)

        if not tsOut.empty:
            tsOut.loc[0, stName] = max(tsOut.loc[0, stName], tStart)  # truncate the first period
            tsOut.loc[tsOut.index[-1], etName] = min(tsOut.loc[tsOut.index[-1], etName],
                                                     tEnd)  # truncate the last period
            if fill:
                if tsOut.loc[0, stName] > tStart:
                    step = pd.DataFrame([[tStart, tsOut.loc[0, stName], 0]], columns=tsOut.columns)
                    tsOut = pd.concat([step, tsOut], ignore_index=True)
                if tsOut.loc[tsOut.index[-1], etName] < tEnd:
                    step = pd.DataFrame([[tsOut.loc[tsOut.index[-1], etName], tEnd, 0]], columns=tsOut.columns)
                    tsOut = pd.concat([tsOut, step], ignore_index=True)
        else:
            if fill:
                tsOut = pd.DataFrame(columns=tsOut.columns)
                tsOut.loc[0] = [tStart, tEnd, 0]
        if not isinstance(tsOut, pd.DataFrame):
            tsOut = pd.DataFrame(tsOut)
        return self.__class__(tsOut, startTimeColName=self.startTimeColName, endTimeColName=self.endTimeColName,
                              valueColName=self.valueColName)

    def totalDuration(self, omitZero=True):
        df1 = self.df
        if df1.empty:
            totalDur = 0
            logger.warning("empty")
        elif omitZero:
            nonZeroDF = df1[df1[self.valueColName] != 0]
            totalDur = (nonZeroDF[self.endTimeColName] - nonZeroDF[self.startTimeColName]).sum()
        else:
            totalDur = (self._endTimes - self._startTimes).sum()

        return totalDur

    def toQuantity(self):
        # Returns a vector of all time-integrated quantity for all periods in the time series
        qty = self._durations * self.df[self.valueColName]
        return qty

    def totalTS(self):
        r = sum(self.toQuantity())
        return r

    def mean(self):
        totalDur = self.totalDuration(omitZero=False)
        totalValue = self.totalTS()
        meanValue = totalValue / totalDur

        return meanValue, totalDur, totalValue

    def std(self):
        meanValue, totalDur, totalValue = self.mean()
        dur = self._durations
        stdValue = np.sqrt(np.sum(dur * (self.df[self.valueColName] - meanValue) ** 2) / (totalDur - 1))

        return stdValue, meanValue, totalDur, totalValue

    def meanAndStd(self, omitZero=False, startTime=None, endTime=None):
        # Computes the mean value and standard deviation of a time series
        # Pad out the time series if requested
        obj = self.maskTS(tStart=startTime, tEnd=endTime, fill=omitZero)

        # Compute mean value, total duration, and total value
        stdValue, meanValue, totalDur, totalValue = obj.std()
        return meanValue, stdValue, totalDur, totalValue

    def removeZeros(self):
        # Removes time periods with zero values from the time series
        if not self.df.empty:
            idx = self.df[self.valueColName] != 0
            self.df = self.df[idx]
            return self

    def startTime(self):
        # Returns the start time of the time series
        if self.df.empty:
            t1 = 0
        else:
            t1 = self.df.loc[0, self.startTimeColName]
        return t1

    def endTime(self):
        # Returns the end time of the time series
        if self.df.empty:
            t1 = float('inf')  ## jpd -- timeseries is generally considered to be an int.  Will this cause problems?
        else:
            t1 = self.df.loc[self.df.index[-1], self.endTimeColName]
        return t1

    def removeErrorValues(self, replace=[]):
        idx = ~np.isinf(self.df[self.valueColName]) & ~np.isnan(self.df[self.valueColName])
        if not replace:
            self.df = self.df[idx]
        else:
            self.df.loc[~idx, self.valueColName] = replace
        return self

    def removeZeroDuration(self):
        idx = self.df[self.valueColName] != 0
        self.df = self.df[idx]

        idx = self.df[self.startTimeColName] != self.df[self.endTimeColName]
        self.df = self.df[idx]
        return self

    def zeroPeriods(self, startatZero=False):
        if startatZero:
            all_ones = self.__class__(pd.DataFrame([(0, self.endTime(), 1)],
                                                   columns=[self.startTimeColName, self.endTimeColName,
                                                            self.valueColName]))
        else:
            all_ones = self.__class__(pd.DataFrame([(self.startTime(), self.endTime(), 1)],
                                                   columns=[self.startTimeColName, self.endTimeColName,
                                                            self.valueColName]))

        ts1 = self.removeZeros()
        ts1.df.loc[:, self.valueColName] = 1
        zeroTS = all_ones.subtractSquare(ts1)
        return zeroTS.removeZeros()

    def min(self, omitZero=False):
        if self.df.empty:
            return 0
        elif omitZero:
            obj = self.removeZeros()
            if obj.df.empty:
                minValue = []
                return minValue
            else:
                minValue = obj._values.min()
                return minValue
        elif self.hasZeroPeriods():
            minValue = 0
            return minValue
        else:
            minValue = self._values.min()
            return minValue

    def max(self):
        # Maximum value across the entire time series
        if self.df.empty:
            maxValue = 0
            return maxValue
        else:
            maxValue = self._values.max()
            return maxValue

    def median(self):
        medianValue = self.CDFInverse(pts=np.array([0.5]))
        return medianValue

    def statsTable(self, omitZero=False, startTime=None, endTime=None, CI=[0.025, 0.975], SecondsPerUnit=1):
        if self.df.empty:
            meanValue = 0
            stdValue = 0
            totalValue = 0
            totalDur = 0
            d = [0, 0, 0]
            minValue = 0
            maxValue = 0
        else:
            meanValue, stdValue, totalDur, totalValue = self.meanAndStd(omitZero=omitZero, startTime=startTime,
                                                                        endTime=endTime)

            # Correct units
            totalValue = totalValue / SecondsPerUnit

            # Create PDF & CDF to compute median and empirical CI
            d = self.CDFInverse(CI + [0.5])

            # Extrema
            minValue = self.min(omitZero=omitZero)
            maxValue = self.max()
            # Create the stats table
        statsTab = pd.DataFrame(
            {
                'Minimum': [minValue],
                'Lower': [d[0]],
                'Mean': [meanValue],
                'Upper': [d[1]],
                'Maximum': [maxValue],
                'StdDev': [stdValue],
                'Median': [d[2]],
                'Sum': [totalValue],
                'OnDuration': [totalDur],
            }
        )
        return statsTab

    def hasZeroPeriods(self):
        # returns True if there are any zero periods between the start
        # and end of the time series
        elapsed = self.endTime() - self.startTime()
        tf = self.totalDuration() < elapsed
        return tf

    def threshold(self, threshold=[]):
        # retains only periods where the values are above the threshold
        # (single value) or are between values in the threshold (2 values), inclusively.
        if len(threshold) == 0 or len(threshold) > 2:
            raise ValueError("Threshold vector must be of length 1 or 2")

        if self.isempty():
            return

        # Discard any periods with values not in the range requested
        if len(threshold) == 2:
            idx = (self.df[self.valueColName] >= min(threshold)) & (self.df[self.valueColName] <= max(threshold))
        else:
            idx = self.df[self.valueColName] >= threshold[0]
        self.df = self.df.loc[idx, :]
        return self

    @property
    def _startEndTimes(self):
        if self.df.empty:
            return None

        startTime = self.df.iloc[0][self.startTimeColName]
        endTime = self.df.iloc[-1][self.endTimeColName]

        return (startTime, endTime)

    def createConstant(self, constVal):
        times = self._startEndTimes
        newDF = pd.DataFrame(
            [{self.startTimeColName: times[0], self.endTimeColName: times[1], self.valueColName: constVal}])
        newTS = TimeseriesRLE(newDF)
        return newTS


# Fully expanded timeseries, useful for graphing and other packages.  Only a single time column, with every rate change
# indicated by two timestamps -- one at the start of the interval and one at the end.  This implies that there will
# be duplicate timestamps
#
# Uses numpy to calculate total via trapezoidal integration
#

class TimeseriesFull(Timeseries):

    def __init__(self, df,
                 startTimeColName='timestamp', rateColName='tsValue',
                 forceZeroEnds=True,
                 **kwargs):
        super().__init__(**kwargs)
        self.df = df
        self.startTimeColName = startTimeColName
        self.valueColName = rateColName

    @classmethod
    def fromCollections(cls, startTimeCollection, rateCollection, **kwargs):
        df = pd.DataFrame({'timestamp': startTimeCollection, 'tsValue': rateCollection})
        return cls(df, **kwargs)

    @property
    def _durations(self):
        raise NotImplementedError()

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

    @property
    def _startEndTimes(self):
        raise NotImplementedError

    def sampleSquare(self, bpList):
        ret = np.interp(bpList, xp=self.df['timestamp'].to_numpy(), fp=self.df['rate'].to_numpy())
        retDF = pd.Series(index=bpList, data=ret, name='rate')
        return retDF

    def total(self):
        raise NotImplementedError()

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

    def toTimeseriesRLE(self, filterZeros=False):
        vals = self._values
        workDF = self.df.assign(prevVal=vals.shift(1, fill_value=vals[0] - 1))
        changeDF = workDF[vals != workDF['prevVal']]
        lastTime = self._startTimes.iloc[-1]
        changeDF = changeDF.assign(nextTS=changeDF[self.startTimeColName].shift(-1, fill_value=lastTime).astype(int))

        retTS = TimeseriesRLE(changeDF[[self.startTimeColName, 'nextTS', self.valueColName]],
                              startTimeColName=self.startTimeColName,
                              endTimeColName='nextTS',
                              valueColName=self.valueColName,
                              filterZeros=filterZeros)

        return retTS


class TimeseriesCategorical(TimeseriesRLE):

    def __init__(self, df, **kwargs):
        super().__init__(df, **kwargs)
        categories = pd.Categorical(df[self.valueColName])
        self.df = self.df.assign(catVal=categories.codes, categories=categories)
        self.valueColName = 'categories'

    @property
    def _values(self):
        return self.categories

    @property
    def categories(self):
        return self.df["categories"]

    @property
    def catval(self):
        return self.df["catVal"]

    @classmethod
    def fromCollections(cls, startTimeCollection, endTimeCollection, valueCollection, **kwargs):
        raise NotImplementedError()

    # def toFullTimeseries(self):
    #     ret = super().toFullTimeseries()
    #     # the rate column in the returned full timeseries is an index into the original categorical.
    #     # create a new categorical column for the returned dataframe based on the new rate & the original categorical
    #     # retCategories = self.df['categories'].cat.categories[ret._values]
    #     # ret.df = ret.df.assign(categories=retCategories)
    #     # ret.valueColName = 'categories'
    #     return ret

    @property
    def range(self):
        calcYRange = list(self.df['categories'].cat.categories)
        return calcYRange

    def sampleSquare(self):
        raise NotImplementedError()

    def transitionMatrix(self):
        analysisDF = self.df.assign(duration=self._endTimes - self._startTimes, nextState=self._values.shift(-1))
        durationPT = analysisDF.pivot_table(index=self.valueColName, values='duration', aggfunc=['min', 'mean', 'max'],
                                            observed=True)
        durationPT.columns = ['min', 'mean', 'max']
        transitionPT = analysisDF.pivot_table(index='categories', values='timestamp', columns='nextState',
                                              aggfunc='count', observed=True)
        transitionProbDF = transitionPT.div(transitionPT.sum(axis='columns'), axis='index')
        transitionProbDF = transitionProbDF.fillna(0)
        ret = durationPT.merge(transitionProbDF, on='categories')
        return ret


class TimeseriesPDF():
    def __init__(self, data, tolerance=[], omitNaN=False):
        self.tolerance = tolerance
        self.OmitNaN = omitNaN

        # If data is timeseries object
        if isinstance(data, Timeseries):
            self.data = self.fromTS(data)
        else:
            self.data = data
        # Skipping functions from_vector and from_mc

    @property
    def _values(self):
        return self.data["value"]

    @property
    def _count(self):
        return self.data["count"]

    def isempty(self):
        return self.data.empty

    def add(self, pdfObj: "TimeseriesPDF"):
        self.data = pd.concat([self.data, pdfObj.data], ignore_index=True)
        return self

    @classmethod
    def fromTS(cls, ts, tolerance=[], datascale=1, omitZero=None) -> "TimeseriesPDF":
        if ts.isempty():
            return cls(pd.DataFrame(columns=['value', 'count']))

        data = ts.df[ts.valueColName]
        if tolerance:
            data = (data * datascale).round(tolerance[0]) * tolerance[0]
        counts = ts._durations.groupby(data).sum().reset_index()
        counts.columns = ['value', 'count']
        counts["probability"] = counts["count"] / ts.totalDuration(omitZero)
        return cls(counts)

    def toCDF(self) -> "pd.DataFrame":
        if self.data.empty:
            return pd.DataFrame()
        self.data['cumulative_sum'] = self.data['count'].cumsum() / self.data['count'].sum()
        r = self.data[['value', 'cumulative_sum']]
        return r

    def cdfInverse(self, cdf_df, pts=[0.5]) -> (list | list[None]):
        if any(pt > 1 or pt < 0 for pt in pts):
            raise ValueError("cdfInverse(): Sample points must lie between zero and one")

        if cdf_df.shape[0] > 1 and cdf_df['cumulative_sum'].iloc[0] != 0:
            cdf_df = pd.concat(
                [pd.DataFrame({'value': cdf_df['value'].iloc[0], 'cumulative_sum': 0}, index=[0]), cdf_df])

        if cdf_df.shape[0] == 1:
            ci = [cdf_df['value'].iloc[0]] * len(pts)
        elif cdf_df['cumulative_sum'].isna().any() or cdf_df.empty:
            ci = [None] * len(pts)
        else:
            f = sint.interp1d(cdf_df['cumulative_sum'], cdf_df['value'], bounds_error=False, fill_value=np.nan,
                              kind='linear')
            ci = f(pts)
        return ci

    def statsTable(self, params=[0.025, 0.975]):
        totalValue = self.total()
        totalDur = self.counts()
        meanValue = self.mean()
        cdf = self.toCDF()
        ci = self.cdfInverse(cdf, params)
        medianValue = self.cdfInverse(cdf, [0.5])
        minValue = self.min()
        maxValue = self.max()
        stdValue = np.nan  # no time to figure this one out at the moment

        stats = pd.DataFrame({
            'Minimum': [minValue],
            'Lower': [ci[0]],
            'Mean': [meanValue],
            'Upper': [ci[1]],
            'Maximum': [maxValue],
            'StdDev': [stdValue],
            'Median': medianValue,
            'Sum': [totalValue],
            'OnDuration': [totalDur]
        })

        return stats

    def total(self):
        if self.isempty():
            return np.nan
        else:
            data = self.data
            return np.sum(data['value'] * data['count'])

    def mean(self):
        if self.isempty():
            return np.nan
        else:
            totalValue = self.total()
            totalDur = self.counts()
            return totalValue / totalDur

    def min(self):
        if self.isempty():
            return np.nan
        else:
            return self.data['value'].iloc[0]

    def max(self):
        if self.isempty():
            return np.nan
        else:
            return self.data['value'].iloc[-1]

    def counts(self):
        if self.isempty():
            return 0
        else:
            return np.sum(self.data['count'])


#
# TimeseriesSet
#

class TimeseriesSet():

    def __init__(self, tsSetList):
        # how do we want to handle polymorphic timeseries?
        self.tsSetList = tsSetList.copy()

    def addTimeseries(self, ts):
        self.tsSetList.append(ts)

    def sum(self, filterZeros=True):
        sumTS = TimeseriesRLE(pd.DataFrame(columns=['timestamp',
                                                    'nextTS',
                                                    'tsValue']))
        for singleTS in self.tsSetList:
            sumTS = sumTS.addSquare(singleTS, filterZeros)

        return sumTS

    def mean(self):
        sumTS = self.sum()
        numTS = sumTS.createConstant(len(self.tsSetList))

        return sumTS.divideSquare(numTS)




