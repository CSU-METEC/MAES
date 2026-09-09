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
import tempfile
import os

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

    # Bounds how many entries stay resident in memory before the oldest half is spilled to
    # a temp CSV. Without this, TSByKey grows for the entire simulated duration (one entry
    # per relevant equipment state-change, e.g. MEETLinkedProductionEq.stateChange), never
    # shrinking, since every entry used to stay resident until the single serialize() call
    # at the very end of the run.
    #
    # Confirmed safe via a full-codebase trace of every addTimeseries/getTimeseries/
    # scaleTimeseries call site (MEETClasses.py, MEETIntermittentPneumatic.py,
    # MEETLinkedProductionEq.py, MEETSamples.py, ModelClasses.py): every caller either uses
    # the returned key immediately as a plain tag passed into an event logger call (never
    # read back), or (scaleTimeseries only) reads an entry back synchronously, in the same
    # call, right after it was created -- never later. getTimeseriesByKey, the one method
    # that would do an arbitrary read of an existing entry, is defined but never called
    # anywhere in the codebase. 2000 is a large safety margin over "read immediately after
    # creation," not a tight tuning knob -- __getitem__ deliberately still raises a plain
    # KeyError if this assumption is ever wrong for some call path this trace missed,
    # rather than silently returning stale/wrong data.
    SPILL_THRESHOLD = 2000

    @classmethod
    def getTSTable(cls):
        if (sdm.SimDataManager.getSimDataManager() is None
                or sdm.SimDataManager.getSimDataManager().timeseriesTable is None):
            raise me.IllegalElementError("No sim data manager gc table available")
        return sdm.SimDataManager.getSimDataManager().timeseriesTable

    def __init__(self, mcRunNum=None):
        self.TSByKey = {}
        self._spillPaths = []  # temp CSVs already written, oldest-entries-first
        # Captured at construction (SimDataManager.__init__ always knows its own MC run
        # number up front) so a mid-simulation spill can tag its rows correctly -- the
        # final serialize() call's own mcRunNum argument is expected to be this same value
        # (one TSTable per one MC run's lifetime), kept as serialize()'s own parameter
        # unchanged rather than silently overridden, so any future divergence would be
        # visible rather than masked.
        self._mcRunNum = mcRunNum

    def __getitem__(self, item):
        return self.TSByKey[item]

    def intern(self, ts):
        tsKey = ts.serialNum
        if tsKey not in self.TSByKey:
            self.TSByKey[tsKey] = ts
            if len(self.TSByKey) > self.SPILL_THRESHOLD:
                self._spillOldest()
        ret = self.TSByKey[tsKey]
        return ret.serialNum

    def _spillOldest(self):
        # dict iteration/insertion order is guaranteed (Python 3.7+), so the first half of
        # TSByKey's current keys are strictly the oldest-created entries.
        allKeys = list(self.TSByKey.keys())
        toSpill = allKeys[:len(allKeys) // 2]
        rows = []
        for key in toSpill:
            rows.extend(self.TSByKey[key].serialForm(mcRun=self._mcRunNum))
            del self.TSByKey[key]
        spillDF = pd.DataFrame(rows)[TSTABLE_COLS]
        fd, path = tempfile.mkstemp(suffix=".tstable_spill.csv")
        os.close(fd)
        spillDF.to_csv(path, index=False)
        self._spillPaths.append(path)

    def serialize(self, oStream, mcRunNum=None):
        # Reassembles in the same oldest-to-newest order the pre-spill code already
        # produced (dict iteration order = insertion order), so the combined output is
        # unchanged whether or not any spilling actually occurred during this run.
        wroteHeader = False
        for path in self._spillPaths:
            spillDF = pd.read_csv(path)
            spillDF[TSTABLE_COLS].to_csv(oStream, index=False, header=not wroteHeader)
            wroteHeader = True
            os.remove(path)
        self._spillPaths = []

        tsList = []
        for tsKey, singleTS in self.TSByKey.items():
            tsList.extend(singleTS.serialForm(mcRun=mcRunNum))
        tsDF = pd.DataFrame(tsList)
        if tsDF.empty:
            if wroteHeader:
                return
            tsDF = pd.DataFrame(columns=TSTABLE_COLS)
        tsDF[TSTABLE_COLS].to_csv(oStream, index=False, header=not wroteHeader)

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
