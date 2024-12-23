import pandas as pd
from abc import ABC, abstractmethod
import SimDataManager as sdm
import logging
import MEETExceptions as me
import SDMCache as sdmc
from Timer import Timer
import numpy as np
import scipy.integrate as si
import scipy.interpolate as sint

TSTABLE_COLS = [
    'tsKey',
    'tsClassName',
    'tsOffset',
    'tsValue',
    'tsUnits',
    'tsDurationPct',
    'mcRun'
]


class TSTable():
    TS_TABLE_SINGLETON = None

    @classmethod
    def getTSTable(cls):
        if (sdm.SimDataManager.getSimDataManager() is None
                or sdm.SimDataManager.getSimDataManager().timeseriesTable is None):
            raise me.IllegalElementError("No sim data manager gc table available")
        return sdm.SimDataManager.getSimDataManager().timeseriesTable

    def __init__(self):
        self.TSByKey = {}

    def __getitem__(self, item):
        return self.TSByKey[item]

    def intern(self, ts):
        tsKey = ts.serialNum
        if tsKey not in self.TSByKey:
            self.TSByKey[tsKey] = ts
        ret = self.TSByKey[tsKey]
        return ret.serialNum

    def serialize(self, oStream, mcRunNum=None):
        tsList = []
        for tsKey, singleTS in self.TSByKey.items():
            singleTSList = singleTS.serialForm(mcRun=mcRunNum)
            tsList.extend(singleTSList)
        tsDF = pd.DataFrame(tsList)
        if tsDF.empty:
            tsDF = pd.DataFrame(columns=TSTABLE_COLS)

        tsDF[TSTABLE_COLS].to_csv(oStream, index=False)

class TimeseriesTableEntry(ABC):
    TS_SERIAL_NUM = 1

    def __init__(self, _serialNum=None, **kwargs):
        cls = TimeseriesTableEntry
        self._serialNum = cls.TS_SERIAL_NUM
        cls.TS_SERIAL_NUM += 1
        self.tsClassName = self.__class__.__name__
        tst = TSTable.getTSTable()
        tst.intern(self)

    @property
    def serialNum(self):
        return self._serialNum

    @serialNum.setter
    def serialNum(self, sNum):
        self._serialNum = sNum

    @abstractmethod
    def instantaneousEmission(self, ts):
        raise NotImplementedError


class ConstantTimeseriesTableEntry(TimeseriesTableEntry, sdmc.SDMCache):
    CONSTANT_TIMESERIES_CACHE = {}

    @classmethod
    def factory(cls, val, units):
        cls.registerCache()

        key = (val, units)
        cacheVal = cls.CONSTANT_TIMESERIES_CACHE.get(key, None)
        if cacheVal:
            return cacheVal
        ts = ConstantTimeseriesTableEntry(val, units)
        cls.CONSTANT_TIMESERIES_CACHE[key] = ts
        return ts

    @classmethod
    def resetCache(cls):
        cls.CONSTANT_TIMESERIES_CACHE = {}

    def __init__(self, val, units):
        self.val = val
        self.units = units
        super().__init__()

    def serialForm(self, **kwargs):
        ret = {
            'tsClassName': self.tsClassName,
            'tsOffset': 0,
            'tsValue': self.val,
            'tsUnits': self.units,
            'tsKey': self.serialNum,
            'tsDurationPct': 100,
            **kwargs
        }
        return [ret]

    def instantaneousEmission(self, ts):
        return self.val
