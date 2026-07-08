import numpy as np
import pandas as pd
from abc import ABC, abstractmethod
import scipy.integrate as si
import scipy.interpolate as sint
import logging
# import helper as hp

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
    dataMap  = pd.IntervalIndex.from_arrays(df[dfStartTimeName], df[dfEndTimeName], closed='both')
    # dataMap = pd.arrays.IntervalArray.from_arrays(df[dfStartTimeName], df[dfEndTimeName], closed='both')
    dmDF =  pd.DataFrame({"dataIdx": df.index}, index=dataMap)
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
    def  fromCollections(cls, startTimeCollection, endTimeCollection, valueCollection,
                         startTimeColName=None, endTimeColName=None, valueColName=None,
                         **kwargs):
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

    def addSquare(self, ts2):
        # add
        e1, e2, bpList = self._arithmeticPrep(ts2)
        e = list(e1 + e2)
        tsOut = self.__class__.fromCollections(bpList[:-1], bpList[1:], e[:-1],
                                               filterZeros=True,
                                               startTimeColName=self.startTimeColName,
                                               endTimeColName=self.endTimeColName,
                                               valueColName=self.valueColName)
        return tsOut

    def subtractSquare(self, ts2):
        e1, e2, bpList = self._arithmeticPrep(ts2)
        e = e1 - e2
        ret = self.__class__.fromCollections(bpList[:-1], bpList[1:], e[:-1], filterZeros=True,
                                             startTimeColName=self.startTimeColName,
                                             endTimeColName=self.endTimeColName,
                                             valueColName=self.valueColName)
        return ret

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
        shiftedTS = df[startTimeColName].shift(-1)
        overlapMask = (df[endTimeColName] > shiftedTS)
        if overlapMask.any():
            logger.error(f"Overlapping interval for {self.name} at {df[overlapMask]}")
            initError = True

        if initError:
            raise MalformedTimeseriesError

        self.df = df.reset_index(drop=True)
        self.startTimeColName = startTimeColName
        self.endTimeColName = endTimeColName
        self.valueColName = valueColName

        if filterZeros:
            self.df = self.df[self.df[self.valueColName] != 0.0].reset_index(drop=True)

        zeroDurMask = self.df[self.endTimeColName] <= self.df[self.startTimeColName]
        if zeroDurMask.any():
            msg = f"Zero-duration interval(s) in {self.name}:\n{self.df[zeroDurMask]}"
            logger.error(msg)
            raise MalformedTimeseriesError(msg)

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
    def fromCollections(cls, startTimeCollection, endTimeCollection, valueCollection,
                        startTimeColName='timestamp', endTimeColName='nextTS', valueColName='valueCollection',
                        **kwargs):
        df = pd.DataFrame({startTimeColName: startTimeCollection,
                           endTimeColName: endTimeCollection,
                           valueColName: valueCollection})
        return cls(df,
                   startTimeColName=startTimeColName,
                   endTimeColName=endTimeColName,
                   valueColName=valueColName,
                   **kwargs)

    @classmethod
    def fromDictList(cls, dictList, valueColName='tsValue', **kwargs):
        df = pd.DataFrame.from_records(dictList)
        return cls(df, valueColName=valueColName, **kwargs)

    @classmethod
    def fromValidatedArrays(cls, startTimes, endTimes, values,
                            startTimeColName='timestamp', endTimeColName='nextTS',
                            valueColName='tsValue', name=None, units=None):
        """Trusted constructor for arrays whose RLE invariants hold BY CONSTRUCTION (issue #121).

        __init__ validates every incoming frame: column presence, interval overlap
        (endTime vs the next row's startTime), zero-duration rows, and end-time sortedness —
        several pandas operations per construction. That is the right defence where data
        ENTERS the system (raw event logs, files), but pure waste for the OUTPUT of
        sumEventArrays, which guarantees strictly-increasing unique start times, contiguous
        positive-duration intervals, and no overlaps by the way it is built (sorted unique
        event times define the interval edges). Profiling (issue #121) showed this re-
        validation, repeated ~50k times per site cache build, was a significant slice of
        createPDFCache runtime.

        This constructor therefore bypasses __init__ (object.__new__ + direct attribute
        assignment) and sets every attribute __init__ would have set:
          name/units (base Timeseries state), df, the three column-name fields, colList,
          and sorted=True (asserted, not re-derived — the caller's construction guarantees it).
        Callers MUST only pass arrays produced by sumEventArrays (or arrays with the same
        proven invariants); anything else must go through the validating __init__.
        """
        obj = cls.__new__(cls)
        # Base-class state (Timeseries.__init__ sets exactly these two).
        obj.name = name
        obj.units = units
        # One DataFrame construction from columnar arrays — the single pandas object this
        # path creates. reset_index is unnecessary: a fresh frame already has a RangeIndex.
        obj.df = pd.DataFrame({startTimeColName: startTimes,
                               endTimeColName: endTimes,
                               valueColName: values})
        obj.startTimeColName = startTimeColName
        obj.endTimeColName = endTimeColName
        obj.valueColName = valueColName
        obj.colList = [startTimeColName, endTimeColName, valueColName]
        # __init__ derives this via _isSorted(); the kernel's output is sorted by construction.
        obj.sorted = True
        return obj

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
        return self.df[self.startTimeColName].astype(int)

    @property
    def _endTimes(self):
        return self.df[self.endTimeColName].astype(int)

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
            and np.allclose(self._values, ts2._values, equal_nan=True)
        )

        return ret

    def sampleSquare(self, bpList):
        startTimes = self.df[self.startTimeColName].values
        endTimes   = self.df[self.endTimeColName].values
        values     = self.df[self.valueColName].values
        bpArr  = np.asarray(bpList)
        # For each breakpoint t, find the last interval with startTime <= t.
        # searchsorted(..., side='right') - 1 gives that index (-1 means none).
        # Then check t < endTime for half-open [startTime, endTime) semantics.
        idx    = np.searchsorted(startTimes, bpArr, side='right') - 1
        valid  = idx >= 0
        vi     = idx[valid]
        vt     = bpArr[valid]
        result = np.zeros(len(bpArr))
        inside = vt < endTimes[vi]
        result[np.where(valid)[0][inside]] = values[vi[inside]]
        return pd.Series(result, index=bpArr)

    # def sampleSquare(self, bpList):
    #     # times are closed on the bottom end, open on the top end:
    #     #  self.df['startTime'] <= t < self.df['endTime']
    #
    #     # ret = self._intervalSample(bpList)
    #     ret = self._sortedSample(bpList)
    #
    #     return ret

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
            renameDict = {'timestamp': retTS.startTimeColName, 'nextTS': retTS.endTimeColName, 'tsValue': retTS.valueColName}
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

        dataMap  = pd.arrays.IntervalArray.from_arrays(self._startTimes, self._endTimes, closed='both')
        dmDF =  pd.DataFrame({"dataIdx": self.df.index}, index=dataMap)
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

        # todo: what is this doing?  Can it be replaced by a call to zeroPeriods / fillZeros?
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
        # todo: what is a complete timeseries???
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

    def toPDF(self):
        ret = TimeseriesPDF.fromTS(self)
        return ret


    def CDFInverse(self, pts=[0.5]):
        return self.toPDF().inverse(pts)

    def toRLETimeseries(self):
        return self

    def maskTS(self, tStart=None, tEnd=None, fill=False):
        # todo: can this be replaced with one of the mask functions defined above?
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
            tsOut.loc[0, stName] = max(tsOut.loc[0, stName], tStart)  #truncate the first period
            tsOut.loc[tsOut.index[-1], etName] = min(tsOut.loc[tsOut.index[-1], etName], tEnd)  #truncate the last period
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
        return self.__class__(tsOut, startTimeColName=self.startTimeColName, endTimeColName=self.endTimeColName, valueColName=self.valueColName)

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

    def removeZeros(self):
        """
        Removes time periods with zero values from the time series
        :return:
        """
        # todo: destructive
        if not self.df.empty:
            idx = self.df[self.valueColName] != 0
            self.df = self.df[idx]
            return self

    def removeZerosNonDestructive(self):
        """
        Removes time periods with zero values from the time series
        :return:
        """
        if self.df.empty:
            return self

        idx = self._values != 0
        retDF = self.df[idx]
        ret = self.__class__(retDF,
                             name=self.name,
                             startTimeColName=self.startTimeColName,
                             endTimeColName=self.endTimeColName,
                             valueColName=self.valueColName)
        return ret

    def removeErrorValues(self, replace=[]):
        idx = ~np.isinf(self.df[self.valueColName]) & ~np.isnan(self.df[self.valueColName])
        if not replace:
            newDF = self.df[idx].reset_index(drop=True)
        else:
            newDF = self.df.copy()
            newDF.loc[~idx, self.valueColName] = replace
        return self.__class__(newDF,
                              startTimeColName=self.startTimeColName,
                              endTimeColName=self.endTimeColName,
                              valueColName=self.valueColName)

    def removeZeroDuration(self):
        newDF = self.df[self.df[self.valueColName] != 0]
        newDF = newDF[newDF[self.startTimeColName] != newDF[self.endTimeColName]].reset_index(drop=True)
        return self.__class__(newDF,
                              startTimeColName=self.startTimeColName,
                              endTimeColName=self.endTimeColName,
                              valueColName=self.valueColName)

    def zeroPeriods(self, startTime=None, endTime=None, maintainOriginalZeros=False):
        """
        RLE timeseries have implicit zero periods, that is, periods that are not covered by entries with values.
        zeroPeriods constructs a new Timeseries with the zero periods (and only the zero periods) explicitly set.

        :param startTime: if set, start the result at the passed value (usually 0).  Otherwise, start the result with the start time
        of the input timeseries.  Default None (use existing start time).
        :param endTime: if set, use this value as the end time of the resulting timeseries.  If not set, use the end
        time of the input timeseries.  Default None (use existing end time).

        :returns: a "zero period" timeseries -- that is, a timeseries with zero periods of the original timeseries,
        with a vlue of 1.
        """

        # todo: why is the value of the returned timeseries 1?
        # todo: which is faster -- this method or using dataframe shift of the nextTS column?

        startTimeVal = startTime if startTime is not None else self.startTime()
        endTimeVal = endTime if endTime is not None else self.endTime()

        dataDict = {self.startTimeColName: startTimeVal,
                    self.endTimeColName:   endTimeVal,
                    self.valueColName:     1}
        allOnes = self.__class__(pd.DataFrame([dataDict]),
                                 startTimeColName = self.startTimeColName,
                                 endTimeColName = self.endTimeColName,
                                 valueColName = self.valueColName)

        # todo: removeZeros is destructive!  I do not think this does what you think it does...
        if maintainOriginalZeros:
            ts1 = self
        else:
            ts1 = self.removeZerosNonDestructive()
            ts1.df.loc[:, self.valueColName] = 1
        zeroTS = allOnes.subtractSquare(ts1)
        zeroTS.df = zeroTS.df[zeroTS.df[zeroTS.valueColName] >= 0.0]
        if maintainOriginalZeros:
            ret = zeroTS
        else:
            ret = zeroTS.removeZerosNonDestructive()
        return ret

    def fillZeros(self, **kwargs):
        zeroTS = self.zeroPeriods(**kwargs)
        zeroDF = zeroTS.df
        zeroDF[zeroTS.valueColName] = 0.0
        newDF = (pd.concat([self.df, zeroDF])
                 .reset_index(drop=True)
                 .sort_values(self.startTimeColName)
                 .drop_duplicates(subset=[self.startTimeColName, self.endTimeColName])
                 )
        newTS = self.__class__(newDF,
                               name=self.name,
                               startTimeColName=self.startTimeColName,
                               endTimeColName=self.endTimeColName,
                               valueColName=self.valueColName)
        return newTS

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
            meanValue, stdValue, totalDur, totalValue = self.meanAndStd(omitZero=omitZero, startTime=startTime, endTime=endTime)

            # Correct units
            totalValue = totalValue / SecondsPerUnit

            # Create PDF & CDF to compute median and empirical CI
            d = self.CDFInverse(CI + [0.5])

            # Extrema
            minValue = self.min(omitZero=omitZero)
            maxValue = self.max()
            # Create the stats table
        return {
            'minimum':    minValue,
            'lower':      d[0],
            'mean':       meanValue,
            'upper':      d[1],
            'maximum':    maxValue,
            'stdDev':     stdValue,
            'median':     d[2],
            'sum':        totalValue,
            'onDuration': totalDur,
        }

    def hasZeroPeriods(self):
        # returns True if there are any zero periods between the start
        # and end of the time series
        # todo: how is this used???
        elapsed = self.endTime() - self.startTime()
        tf = self.totalDuration() < elapsed
        return tf

    def threshold(self, threshold=[]):
        # retains only periods where the values are above the threshold
        # (single value) or are between values in the threshold (2 values), inclusively.
        # todo: why is threshold specified as a list?  Why not two keywords (thresholdMin, thresholdMax)?
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
        newDF = pd.DataFrame([{self.startTimeColName: times[0], self.endTimeColName: times[1], self.valueColName: constVal}])
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
        workDF = self.df.assign(prevVal=vals.shift(1, fill_value=vals[0]-1))
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
        analysisDF = self.df.assign(duration=self._endTimes-self._startTimes, nextState=self._values.shift(-1))
        durationPT = analysisDF.pivot_table(index=self.valueColName, values='duration', aggfunc=['min', 'mean', 'max'], observed=True)
        durationPT.columns = ['min', 'mean', 'max']
        transitionPT = analysisDF.pivot_table(index='categories', values='timestamp', columns='nextState', aggfunc='count', observed=True)
        transitionProbDF = transitionPT.div(transitionPT.sum(axis='columns'), axis='index')
        transitionProbDF = transitionProbDF.fillna(0)
        ret = durationPT.merge(transitionProbDF, on='categories')
        return ret

class TimeseriesPDF():
    def __init__(self, data):
        self.data = data
        #Skipping functions from_vector and from_mc

    @property
    def _values(self):
        return self.data["value"]
    
    @property
    def _count(self):
        return self.data["count"]
    
    def isempty(self):
        return self.data.empty

    def add(self, pdfObj: "TimeseriesPDF"):
        merged = pd.concat([self.data[['value', 'count']], pdfObj.data[['value', 'count']]], ignore_index=True)
        self.data = merged.groupby('value', sort=True)['count'].sum().reset_index()
        return self
    
    @classmethod
    def fromTS(cls, ts, tolerance=[], datascale=1) -> "TimeseriesPDF":
        if ts.isempty():
            return cls(pd.DataFrame(columns=['value', 'count']))

        return cls.fromDataFrame(ts.df,
                                 tolerance=tolerance, datascale=datascale,
                                 startTimeColName=ts.startTimeColName, endTimeColName=ts.endTimeColName, valueColName=ts.valueColName,)

    @classmethod
    def fromDataFrame(cls, df, tolerance=[],
                      datascale=1,
                      startTimeColName='timestamp', endTimeColName='nextTS', valueColName='tsValue'):
        data = df[valueColName]
        if tolerance:
            data = (data * datascale).round(tolerance[0]) * tolerance[0]
        durations = df[endTimeColName]-df[startTimeColName]
        counts = durations.groupby(data).sum().reset_index()
        counts.columns = ['value', 'count']
        return cls(counts)


    def toCDF(self) -> "TimeseriesCDF":
        if self.data.empty:
            return TimeseriesCDF(pd.DataFrame(columns=['value', 'cumulative_probability']))
        cumprob = self.data['count'].cumsum() / self.data['count'].sum()
        cdfDF = pd.DataFrame({'value': self.data['value'].values,
                              'cumulative_probability': cumprob.values})
        return TimeseriesCDF(cdfDF)

    def inverse(self, pts=[0.5]):
        return self.toCDF().inverse(pts)

    def std(self):
        if self.isempty():
            return np.nan
        w = self.data['count'].values
        v = self.data['value'].values
        return np.sqrt(np.sum(w * (v - self.mean()) ** 2) / np.sum(w))

    def statsTable(self, params=[0.025, 0.975]):
        totalValue = self.total()
        totalDur = self.counts()
        meanValue = self.mean()
        ci = self.inverse(params)
        medianValue = self.inverse([0.5])
        minValue = self.min()
        maxValue = self.max()
        stdValue = self.std()

        return {
            'minimum':    minValue,
            'lower':      ci[0],
            'mean':       meanValue,
            'upper':      ci[1],
            'maximum':    maxValue,
            'stdDev':     stdValue,
            'median':     medianValue[0],
            'sum':        totalValue,
            'onDuration': totalDur,
        }

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
# TimeseriesCDF
#

class TimeseriesCDF():
    def __init__(self, df):    # df columns: value, cumulative_probability
        self.data = df

    def isempty(self):
        return self.data.empty

    def inverse(self, pts=[0.5]) -> (list | list[None]):
        if any(pt > 1 or pt < 0 for pt in pts):
            raise ValueError("inverse(): Sample points must lie between zero and one")

        cdf_df = self.data

        if cdf_df.empty or cdf_df['cumulative_probability'].isna().any():
            return [None] * len(pts)

        if cdf_df.shape[0] == 1:
            return [cdf_df['value'].iloc[0]] * len(pts)

        if cdf_df['cumulative_probability'].iloc[0] != 0:
            cdf_df = pd.concat([
                pd.DataFrame({'value': [cdf_df['value'].iloc[0]], 'cumulative_probability': [0]}),
                cdf_df
            ], ignore_index=True)

        f = sint.interp1d(cdf_df['cumulative_probability'], cdf_df['value'],
                          bounds_error=False, fill_value=np.nan, kind='linear')
        return f(pts)


#
# TimeseriesSet
#

def sumEventArrays(startsList, endsList, valsList):
    """Array-native signed-event sweep summation (issue #121).

    This is EXACTLY the algorithm TimeseriesSet.sum() has always used — emit +value at each
    interval start and -value at each interval end, sort all events by time, running-sum the
    deltas to get the summed level on each inter-event interval, and clip near-zero floating-
    point residuals — implemented on raw numpy arrays instead of per-call DataFrames, concat,
    and groupby. Profiling (issue #121) showed ~99% of createPDFCache runtime was pandas
    object machinery around this algorithm; the arithmetic itself is unchanged.

    Parameters are parallel lists (one entry per input timeseries) of 1-D arrays:
    startsList[i] / endsList[i] / valsList[i] hold interval start times, end times, and values
    of input i. Intervals within one input must be non-overlapping (callers validate, exactly
    as TimeseriesRLE.__init__ always has); input ORDER does not matter because all events are
    globally sorted here.

    Returns (startTimes, endTimes, values) arrays of the summed step function. Output
    invariants, BY CONSTRUCTION (these are what let fromValidatedArrays skip re-validation):
      - startTimes is strictly increasing (unique sorted event times);
      - endTimes[i] is the next unique event time, so every duration is > 0;
      - intervals are contiguous and non-overlapping.
    """
    # Empty edge: no input timeseries at all (np.concatenate rejects an empty list of arrays).
    if not startsList:
        empty = np.array([])
        return empty, empty, empty
    # Signed event table: one +value event per interval start, one -value event per interval
    # end. np.negative allocates the negated copies (the legacy path's 'delta': -vals).
    times = np.concatenate(startsList + endsList)
    deltas = np.concatenate(valsList + list(map(np.negative, valsList)))
    if len(times) == 0:
        # Members exist but every one is empty — still an empty sum.
        empty = np.array([])
        return empty, empty, empty
    # Global sort by event time. 'stable' keeps a deterministic order for events sharing a
    # timestamp; the segment-sum below adds all same-time deltas together regardless, exactly
    # like the legacy groupby('time').sum(). (Float addition ORDER within a shared timestamp
    # can differ from pandas' internal order, so results may differ at the ~1e-16 ULP level —
    # far below both the 1e-10 residual clip here and the >=1e-6 _roundForPDF quantisation
    # applied immediately downstream in the PDF cascade.)
    order = np.argsort(times, kind="stable")
    times = times[order]
    deltas = deltas[order]
    # Segment boundaries: positions where the (sorted) event time changes. reduceat then sums
    # each same-time run of deltas — the vectorized equivalent of groupby('time')['delta'].sum().
    isNew = np.empty(len(times), dtype=bool)
    isNew[0] = True
    isNew[1:] = times[1:] != times[:-1]
    segStartIdx = np.flatnonzero(isNew)
    uniqueTimes = times[segStartIdx]
    segSums = np.add.reduceat(deltas, segStartIdx)
    # Running level: cumsum of the per-time net deltas gives the summed value on the interval
    # [uniqueTimes[i], uniqueTimes[i+1]). Identical to the legacy cumsum over the grouped table.
    cumLevels = np.cumsum(segSums)
    startTimes = uniqueTimes[:-1]
    endTimes = uniqueTimes[1:]
    values = cumLevels[:-1]
    # Near-zero residual clip — SAME semantics and SAME 1e-10 threshold as the legacy sum():
    # when several inputs share an endpoint, their +/- deltas cancel with float64 rounding
    # noise (~1e-14 for kg/h-scale rates) instead of exact zeros, creating phantom intervals
    # spanning the gaps between real events. NOTE this deliberately drops ALL near-zero
    # intervals, positive ones included — preserving the legacy behaviour exactly (see the
    # original comment retained in _sumLegacy).
    keepMask = np.abs(values) >= 1e-10
    return startTimes[keepMask], endTimes[keepMask], values[keepMask]


def durationWeightedDistribution(valuesList, durationsList):
    """Array-native duration-weighted value distribution (issue #121, round 2).

    The exact vectorized equivalent of what TimeseriesSet.toPDF() has always computed —
    concatenate the member intervals and run `durations.groupby(values).sum()` — i.e. for every
    distinct interval VALUE, the total DURATION spent at that value. That (value, totalDuration)
    table is the duration-weighted PDF the whole cascade is built on; toCDF() is then just a
    cumulative sum over it. Profiling showed the pandas groupby-per-distribution (one call per
    PDF group, ~13k groups per site) was dominated by per-call machinery, exactly like the
    summation kernel's case.

    Parameters are parallel lists of 1-D arrays (one entry per contributing timeseries, e.g. one
    per MC run): valuesList[i] holds interval values, durationsList[i] the matching durations.

    Returns (uniqueValues, totalDurations): uniqueValues strictly ascending (pandas groupby with
    sort=True ordered its keys the same way), totalDurations the per-value duration sums. Empty
    inputs return empty arrays.
    """
    if not valuesList:
        empty = np.array([])
        return empty, empty
    values = np.concatenate(valuesList)
    durations = np.concatenate(durationsList)
    if len(values) == 0:
        empty = np.array([])
        return empty, empty
    # Sort values (stable, deterministic); equal values become contiguous runs.
    order = np.argsort(values, kind="stable")
    sortedVals = values[order]
    sortedDurs = durations[order]
    # Segment boundaries where the sorted value changes; reduceat sums each run's durations —
    # the vectorized equivalent of groupby(value)['duration'].sum() with sorted keys.
    isNew = np.empty(len(sortedVals), dtype=bool)
    isNew[0] = True
    isNew[1:] = sortedVals[1:] != sortedVals[:-1]
    segStartIdx = np.flatnonzero(isNew)
    uniqueValues = sortedVals[segStartIdx]
    totalDurations = np.add.reduceat(sortedDurs, segStartIdx)
    return uniqueValues, totalDurations


class TimeseriesSet():

    def __init__(self, tsSetList):
        # how do we want to handle polymorphic timeseries?
        self.tsSetList = tsSetList.copy()

    def addTimeseries(self, ts):
        self.tsSetList.append(ts)

    def sum(self):
        """Sum the member timeseries into one summed step function (TimeseriesRLE).

        Fast path (issue #121): when every member is a PLAIN TimeseriesRLE — an exact type()
        check, so subclasses like TimeseriesCategorical (state timelines, whose values are
        category codes that must not be arithmetically summed) always take the legacy path —
        extract the raw arrays once, run the array-native kernel (sumEventArrays), and wrap
        the result via the trusted constructor. Same algorithm, same residual-clip semantics,
        none of the per-call DataFrame/concat/groupby overhead.

        The legacy frame-based implementation is retained verbatim as _sumLegacy: it is the
        fallback for mixed/subclassed member sets AND the reference implementation the unit
        tests compare the fast path against.
        """
        if not self.tsSetList:
            return TimeseriesRLE(pd.DataFrame(columns=['timestamp', 'nextTS', 'tsValue']))
        allPlainRLE = all(map(lambda singleTS: type(singleTS) is TimeseriesRLE, self.tsSetList))
        if not allPlainRLE:
            return self._sumLegacy()

        first = self.tsSetList[0]
        startsList = []
        endsList = []
        valsList = []
        for singleTS in self.tsSetList:
            df = singleTS.df
            # .to_numpy() on each column once per member — the only pandas touch on this path.
            startsList.append(df[singleTS.startTimeColName].to_numpy())
            endsList.append(df[singleTS.endTimeColName].to_numpy())
            valsList.append(df[singleTS.valueColName].to_numpy())
        startTimes, endTimes, values = sumEventArrays(startsList, endsList, valsList)
        if len(values) == 0:
            # Legacy behaviour: an empty result uses the DEFAULT column names, not the first
            # member's (see the tail of _sumLegacy) — preserved exactly.
            return TimeseriesRLE(pd.DataFrame(columns=['timestamp', 'nextTS', 'tsValue']))
        # The kernel's output invariants (strictly increasing, positive-duration, non-
        # overlapping intervals) hold by construction, so the trusted constructor may skip
        # the per-row validation TimeseriesRLE.__init__ would re-run.
        return TimeseriesRLE.fromValidatedArrays(startTimes, endTimes, values,
                                                 startTimeColName=first.startTimeColName,
                                                 endTimeColName=first.endTimeColName,
                                                 valueColName=first.valueColName)

    def _sumLegacy(self):
        if not self.tsSetList:
            return TimeseriesRLE(pd.DataFrame(columns=['timestamp', 'nextTS', 'tsValue']))

        first = self.tsSetList[0]
        startCol = first.startTimeColName
        endCol = first.endTimeColName
        valCol = first.valueColName

        # Build signed-event table: +value at interval start, -value at interval end.
        # Collecting all events then sorting once gives O(M log M) vs O(N*M) for addSquare reduce.
        dfs = []
        for singleTS in self.tsSetList:
            df = singleTS.df
            vals = df[singleTS.valueColName].values
            dfs.append(pd.DataFrame({'time': df[singleTS.startTimeColName].values, 'delta': vals}))
            dfs.append(pd.DataFrame({'time': df[singleTS.endTimeColName].values, 'delta': -vals}))

        eventsDF = pd.concat(dfs, ignore_index=True)
        grouped = eventsDF.groupby('time', sort=True)['delta'].sum()

        times = grouped.index.values
        cumsums = grouped.values.cumsum()

        # Interval [times[i], times[i+1]) has value cumsums[i]
        startTimes = times[:-1]
        endTimes = times[1:]
        intervalVals = cumsums[:-1]

        # Filter near-zero FP residuals.  When multiple timeseries share an endpoint (e.g. two
        # emitters that start and stop at the same simulation tick), their +delta and -delta events
        # land on the same time bucket.  float64 addition is not associative, so the cumsum after
        # cancellation produces tiny residuals (~1e-14 for emission rates in the 1-100 kg/h range)
        # rather than exactly 0.  Without filtering, these residuals create phantom intervals
        # spanning the large gaps between real events (e.g. 29M-second "intervals" with value
        # -1.4e-14), which corrupt downstream PDFs.  The threshold 1e-10 is 4+ orders of magnitude
        # above observed residuals and well below the smallest physically meaningful emission rate.
        nearZeroMask = np.abs(intervalVals) < 1e-10
        residualMask = (intervalVals < 0.0) & nearZeroMask
        if residualMask.any():
            logger.debug(f"TimeseriesSet.sum: {residualMask.sum()} near-zero FP residuals clipped (min={intervalVals[residualMask].min():.3e})")
        mask = ~nearZeroMask
        outDF = pd.DataFrame({startCol: startTimes[mask],
                               endCol: endTimes[mask],
                               valCol: intervalVals[mask]})

        if outDF.empty:
            return TimeseriesRLE(pd.DataFrame(columns=['timestamp', 'nextTS', 'tsValue']))

        return TimeseriesRLE(outDF.reset_index(drop=True),
                             startTimeColName=startCol,
                             endTimeColName=endCol,
                             valueColName=valCol)

    def oldSum(self):
        sumTS = TimeseriesRLE(pd.DataFrame(columns=['timestamp', 'nextTS', 'tsValue']))
        for singleTS in self.tsSetList:
            sumTS = sumTS.addSquare(singleTS)
        return sumTS

    def sumNew(self):
        startSet = set()
        endSet = set()
        for singleTS in self.tsSetList:
            startSet.update(singleTS._startTimes)
            endSet.update(singleTS._endTimes)

        startSet.update(endSet)
        bpList = sorted(list(startSet))
        sqList = []
        for singleTS in self.tsSetList:
            thisSquare = singleTS.sampleSquare(bpList)
            sqList.append(thisSquare)

        fullDF = pd.concat(sqList, axis='columns')
        sumDF = fullDF.sum(axis='columns')

        tsOut = TimeseriesRLE.fromCollections(bpList[:-1], bpList[1:], sumDF[:-1],
                                              filterZeros=True,
                                              startTimeColName='timestamp',
                                              endTimeColName='nextTS',
                                              valueColName='tsValue')
        return tsOut

    def mean(self):
        sumTS = self.sum()
        numTS = sumTS.createConstant(len(self.tsSetList))

        return sumTS.divideSquare(numTS)

    def toPDF(self):
        if not self.tsSetList:
            return TimeseriesPDF(pd.DataFrame(columns=['value', 'count']))
        first = self.tsSetList[0]
        pdfDF = pd.concat(map(lambda x: x.df, self.tsSetList))
        return TimeseriesPDF.fromDataFrame(pdfDF,
                                           startTimeColName=first.startTimeColName,
                                           endTimeColName=first.endTimeColName,
                                           valueColName=first.valueColName)





