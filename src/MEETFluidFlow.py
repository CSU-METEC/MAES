from abc import ABC
import logging
import MEETExceptions as me
import DistributionProfile as dp
import TimeseriesTable as ts
import Units as u
import SimDataManager as sdm
import SDMCache as sdmc
import itertools
import pandas as pd

class SerialNumberSingleton():
    INSTANCE_SERIAL_NUMBER = 0

    @classmethod
    def getNewSerialNumber(cls):
        isn = cls.INSTANCE_SERIAL_NUMBER + 1
        cls.INSTANCE_SERIAL_NUMBER = isn
        return isn

FFTABLE_COLS = [
    'ffSerialNumber',
    'ffName',
    'ffType',
    'ffDriverUnits',
    'ffGC',
    'ffGCUnits',
    'ffSecondaryID',
]

class FFTable():
    FF_TABLE_SINGLETON = None

    @classmethod
    def getFFTable(cls):
        if (sdm.SimDataManager.getSimDataManager() is None
                or sdm.SimDataManager.getSimDataManager().ffTable is None):
            raise me.IllegalElementError("No sim data manager FluidFlow table available")
        return sdm.SimDataManager.getSimDataManager().ffTable

    def __init__(self):
        self.FFByKey = {}

    def __getitem__(self, item):
        return self.FFByKey[item]

    def intern(self, ff):
        ffKey = ff.serialNumber
        if ffKey not in self.FFByKey:
            self.FFByKey[ffKey] = ff
        ret = self.FFByKey[ffKey]
        return ret

    def serialize(self, oStream):
        ffList = []
        for ffKey, singleFF in self.FFByKey.items():
            singleTSList = singleFF.serialForm()
            ffList.extend(singleTSList)
        tsDF = pd.DataFrame(ffList)
        if tsDF.empty:
            logging.warning(f"Empty timeseries table")
        else:
            tsDF[FFTABLE_COLS].to_csv(oStream, index=False)


class FluidFlow(sdmc.SDMCache):  # todo: raise error if GCUnits and DriverUnits dont match

    def __init__(self, name,
                 driverRateInSecs,
                 driverUnits,
                 gc,
                 changeTimeAbsolute=None,
                 secondaryID=None,
                 ):

        self.name = name
        self._driverRateInSecs = 0.0
        if driverRateInSecs is not None:
            self._driverRateInSecs = driverRateInSecs
        self.driverUnits = driverUnits
        self.gc = gc
        self._changeTimeAbsolute = changeTimeAbsolute or u.getSimDuration()
        self.secondaryID = secondaryID
        self.serialNumber = SerialNumberSingleton.getNewSerialNumber()
        # self._newStateFlag = newStateFlag

        FFTable.getFFTable().intern(self)

    @property
    def driverRate(self):
        return self._driverRateInSecs

    @driverRate.setter
    def driverRate(self, value):
        self._driverRateInSecs = value

    @property
    def changeTimeAbsolute(self):
        return self._changeTimeAbsolute

    @changeTimeAbsolute.setter
    def changeTimeAbsolute(self, value):
        self._changeTimeAbsolute = value

    # @property
    # def newStateFlag(self):
    #     # if self.drive
    #     return self._driverRateInSecs
    #
    # @newStateFlag.setter
    # def newStateFlag(self, value):
    #     self._driverRateInSecs = value

    def serialForm(self):
        ret = {
            'ffSerialNumber': self.serialNumber,
            'ffName': self.name,
            'ffType': self.__class__.__name__,
            'ffDriverUnits': self.driverUnits,
            'ffGC': self.gc.serialNum,
            'ffGCUnits': self.gc.gcUnits,
            'ffSecondaryID': self.secondaryID,
        }
        return [ret]

    def unitChange(self, origFlow, gc, newUnits):
        if origFlow.driverUnits != newUnits:  # unit conversion
            newGc, conversion = gc.convert(newUnits)
            conversionDriveFactor = conversion['DriveFactor']
        else:
            conversionDriveFactor = 1
            newGc = gc
        return newGc, conversionDriveFactor


class DependentFlow(FluidFlow):
    def __init__(self, origFlow, rateTransform=None, newName=None, newUnits=None, gc=None, secondaryID=None):
        if self == origFlow:
            msg = f"Cannot be a DependentFlow of self: {self}"
            logging.warning(msg)
            raise me.IllegalArgumentError(msg)

        gc, self.conversionDriveFactor = self.unitChange(origFlow, gc, newUnits)

        super().__init__(newName or origFlow.name,
                         None,
                         newUnits or origFlow.driverUnits,
                         gc or origFlow.gc,
                         None,
                         secondaryID or origFlow.secondaryID
                         )

        self.origFlow = origFlow
        self.rateTransform = rateTransform or (lambda x: x)

    @classmethod
    def factory(cls, origFlow, rateTransform=None, newName=None, newUnits=None, gc=None, secondaryID=None):
        unitsToUse = newUnits or origFlow.driverUnits
        if isinstance(origFlow, list):
            ret = []
            for singleInstance in origFlow:
                ret.append(DependentFlow(singleInstance,
                                         rateTransform=rateTransform,
                                         newName=newName,
                                         newUnits=unitsToUse,
                                         gc=gc,
                                         secondaryID=secondaryID))
            return ret
        return DependentFlow(origFlow,
                             rateTransform=rateTransform,
                             newName=newName,
                             newUnits=unitsToUse,
                             gc=gc,
                             secondaryID=secondaryID)

    @property
    def driverRate(self):
        return self.rateTransform(self.origFlow.driverRate) * self.conversionDriveFactor

    @driverRate.setter
    def driverRate(self, value):
        raise NotImplementedError

    @property
    def changeTimeAbsolute(self):
        return self.origFlow.changeTimeAbsolute

    @changeTimeAbsolute.setter
    def changeTimeAbsolute(self, value):
        raise me.IllegalArgumentError(f"Cannot set changeTimeAbsolute on {self.__class__.__name__}")

class DependentFlowWithIndependentChangeTime(FluidFlow):
    def __init__(self, origFlow, rateTransform=None, newName=None, newUnits=None, gc=None, secondaryID=None, changeTimeAbsolute=None):
        if self == origFlow:
            msg = f"Cannot be a DependentFlow of self: {self}"
            logging.warning(msg)
            raise me.IllegalArgumentError(msg)

        gc, self.conversionDriveFactor = self.unitChange(origFlow, gc, newUnits)

        super().__init__(newName or origFlow.name,
                         None,
                         newUnits or origFlow.driverUnits,
                         gc or origFlow.gc,
                         changeTimeAbsolute,
                         secondaryID or origFlow.secondaryID,
                         )

        self.origFlow = origFlow
        self.rateTransform = rateTransform or (lambda x: x)

    @classmethod
    def factory(cls, origFlow, rateTransform=None, newName=None, newUnits=None, gc=None, secondaryID=None, changeTimeAbsolute=None):
        unitsToUse = newUnits or origFlow.driverUnits
        if isinstance(origFlow, list):
            ret = []
            for singleInstance in origFlow:
                ret.append(DependentFlowWithIndependentChangeTime(singleInstance,
                                                                  rateTransform=rateTransform,
                                                                  newName=newName,
                                                                  newUnits=unitsToUse,
                                                                  gc=gc,
                                                                  secondaryID=secondaryID,
                                                                  changeTimeAbsolute=changeTimeAbsolute
                                                                  ))
            return ret
        return DependentFlowWithIndependentChangeTime(origFlow,
                                                      rateTransform=rateTransform,
                                                      newName=newName,
                                                      newUnits=unitsToUse,
                                                      gc=gc,
                                                      secondaryID=secondaryID,
                                                      changeTimeAbsolute=changeTimeAbsolute)

    @property
    def driverRate(self):
        return self.rateTransform(self.origFlow.driverRate) * self.conversionDriveFactor

    @driverRate.setter
    def driverRate(self, value):
        raise NotImplementedError

    @property
    def changeTimeAbsolute(self):
        return self._changeTimeAbsolute

    @changeTimeAbsolute.setter
    def changeTimeAbsolute(self, value):
        self._changeTimeAbsolute = value

class AggregatedFlow(FluidFlow):
    def __init__(self, name, gc, newUnits=None, flowsToAggregate=None, secondaryID=None):
        super().__init__(name, None, newUnits, gc, secondaryID)
        self.aggregatedFlows = []

        if flowsToAggregate is None:
            flowsToAggregate = []
        for singleFlow in flowsToAggregate:
            self.addFlow(singleFlow)

    def addFlow(self, flow):
        if self == flow:
            msg = f"Cannot aggregate flow of self: {self}"
            logging.warning(msg)
            raise me.IllegalArgumentError(msg)
        if self.driverUnits != flow.driverUnits:
            msg = f"Driver unit mismatch in AggregatedFlow.addFlow.  Expected: {self.driverUnits}, got: {flow.driverUnits}"
            logging.warning(msg)
            raise me.IllegalArgumentError(msg)
        if self.gc != flow.gc:
            msg = f"Gas composition mismatch in AggregatedFlow.addFlow.  Expected: {self.gc}, got: {flow.gc}"
            logging.warning(msg)
            raise me.IllegalArgumentError(msg)

        self.aggregatedFlows.append(flow)

    @property
    def driverRate(self):
        ret = sum(map(lambda x: x.driverRate, self.aggregatedFlows))
        return ret

    @driverRate.setter
    def driverRate(self, value):
        raise NotImplementedError

    @property
    def changeTimeAbsolute(self):
        ret = min(map(lambda x: x.changeTimeAbsolute, self.aggregatedFlows))
        return ret

    @changeTimeAbsolute.setter
    def changeTimeAbsolute(self, value):
        raise me.IllegalArgumentError(f"Cannot set changeTimeAbsolute on {self.__class__.__name__}")


class CompositeFlow(FluidFlow):
    def __init__(self, name, gc, newUnits=None, flowsToCompose=None, secondaryID=None):
        super().__init__(name, None, newUnits, gc, secondaryID=secondaryID)
        self.subFlows = []

        if flowsToCompose is None:
            flowsToCompose = []
        for singleFlow in flowsToCompose:
            self.addFlow(singleFlow)

    def addFlow(self, flow):
        if self == flow:
            msg = f"Cannot aggregate flow of self: {self}"
            logging.warning(msg)
            raise me.IllegalArgumentError(msg)
        if self.name != flow.name:
            msg = f"CompositeFlow.addFlow must be the same material -- mismatch: {self.name} vs. {flow.name}"
            logging.warning(msg)
            raise me.IllegalArgumentError(msg)
        if self.driverUnits != flow.driverUnits:
            msg = f"Driver unit mismatch in CompositeFlow.addFlow.  Expected: {self.driverUnits}, got: {flow.driverUnits}"
            logging.warning(msg)
            raise me.IllegalArgumentError(msg)

        self.subFlows.append(flow)
        # self.gc.addGC(flow.gc)

    @property
    def driverRate(self):
        ret = sum(map(lambda x: x.driverRate, self.subFlows))
        return ret

    @driverRate.setter
    def driverRate(self, value):
        raise NotImplementedError

    @property
    def changeTimeAbsolute(self):
        ret = min(map(lambda x: x.changeTimeAbsolute, self.subFlows))
        return ret

    @changeTimeAbsolute.setter
    def changeTimeAbsolute(self, value):
        raise me.IllegalArgumentError(f"Cannot set changeTimeAbsolute on {self.__class__.__name__}")

class FunctionDependentFluidFlow(FluidFlow):
    def __init__(self, origFlow,
                 gc=None,
                 rateTransform=None, newName=None, newUnits=None, secondaryID=None,
                 flowFun=None,
                 ):
        super().__init__(newName or origFlow.name,
                         None,
                         newUnits or origFlow.driverUnits,
                         gc or origFlow.gc,
                         secondaryID=(secondaryID or origFlow.secondaryID))
        self.origFlow = origFlow
        self.rateTransform = rateTransform or (lambda x: x)
        self.flowFun = flowFun

    @property
    def driverRate(self):
        isFlowing = self.flowFun
        if callable(isFlowing):
            isFlowing = isFlowing()
        if isFlowing:
            return self.rateTransform(self.origFlow.driverRate)
        else:
            return 0.0

    @driverRate.setter
    def driverRate(self, value):
        raise NotImplementedError

    @property
    def changeTimeAbsolute(self):
        return self.origFlow.changeTimeAbsolute

    @changeTimeAbsolute.setter
    def changeTimeAbsolute(self, value):
        self.origFlow.changeTimeAbsolute = value

class EmpiricalFluidFlow(FluidFlow):
    def __init__(self, name, distFilename, gc, units=None, secondaryID=None):
        super().__init__(name, None, units, gc, secondaryID=secondaryID)
        self.distFilename = distFilename
        self.distributionProfile = dp.DistributionProfile.readFile(self.distFilename)
        self.pick()
        pass

    def pick(self):
        self.driverRate = self.distributionProfile.pick()
        if not self.distributionProfile.md['Units']:
            msg = (f'Units not specified for the emission factor in {self.distFilename}.'
                   f' Please put a row in the metadata with the field name "Units"')
            raise NotImplementedError(msg)
        if 'kg' in self.distributionProfile.md['Units']:
            self.driverRate = self.gc.convertKgToScf(self.driverRate)
        elif 'scf' in self.distributionProfile.md['Units']:
            self.driverRate = self.driverRate
        else:
            msg = (f'MAES does not have conversion capability from {self.distributionProfile.md["Units"]} to "scf",'
                   f'file {self.distFilename}, please use emission factors with units "scf" or "kg"')
            raise NotImplementedError(msg)
        # todo: this is wrong -- this means that only FluidFlows that define the ts can be used in EmissionManager
        self.ts = ts.ConstantTimeseriesTableEntry.factory(self.driverRate, self.gc.gcUnits)


class Volume(ABC):
    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['inletFluidFlows', 'outletFluidFlows', 'fluidFlowGC',
                                         'totalLiquidInletFlow', 'totalLiquidOutletFlow',
                                         'totalVaporInletFlow', 'totalVaporOutletFlow', 'totalHorizontalVolume']

    # todo: is there a way to make the fluid flows functional, so it is easy to link inlet flows to outlet flows?
    def __init__(self,
                 **kwargs):
        super().__init__(**kwargs)
        self.inletFluidFlows = {}
        self.outletFluidFlows = {}

    def GCForFluidFlow(self, fluidName):
        return self.fluidFlowGC[fluidName]

    def getLiquidFlows(self, flows):
        liquidFlows = []
        for key, val in flows.items():
            for var in val:
                if var.name == 'Vapor':
                    continue
                else:
                    liquidFlows.append(var)
        if len(liquidFlows) == 0:
            i = 10
        return liquidFlows

    def getInletFluidFlows(self, fluidName, secondaryID=None):
        if fluidName not in self.inletFluidFlows:
            return []
        ret = self.inletFluidFlows[fluidName]
        if secondaryID is not None:
            ret = list(filter(lambda x: x.secondaryID == secondaryID, ret))
        return ret

    def addInletFluidFlow(self, ff):
        if isinstance(ff, list):
            for singleFF in ff:
                self.addInletFluidFlow(singleFF)
            return
        fluidName = ff.name
        if fluidName not in self.inletFluidFlows:
            self.inletFluidFlows[fluidName] = []
        # todo: implement overwrite behavior???
        self.inletFluidFlows[fluidName].append(ff)

    def inletFluidFlowNames(self):
        return self.inletFluidFlows.keys()

    def getOutletFluidFlows(self, fluidName, secondaryID=None):
        if fluidName not in self.outletFluidFlows:
            return []
        ret = self.outletFluidFlows[fluidName]
        if secondaryID is not None:
            ret = list(filter(lambda x: x.secondaryID == secondaryID, ret))
        return ret

    def addOutletFluidFlow(self, ff):
        if isinstance(ff, list):
            for singleFF in ff:
                self.addOutletFluidFlow(singleFF)
            return
        fluidName = ff.name
        if fluidName not in self.outletFluidFlows:
            self.outletFluidFlows[fluidName] = []
        # todo: implement overwrite behavior???
        self.outletFluidFlows[fluidName].append(ff)

    def outletFluidFlowNames(self):
        return self.outletFluidFlows.keys()

    def updateFFChangeTime(self, changeTimeAbsolute, changeTimeDelay):
        for singleFlow in self.outletFluidFlows:
            if singleFlow in ['Condensate', 'Water', 'Vapor']:
                for i in self.outletFluidFlows[singleFlow]:
                    i.changeTimeAbsolute = changeTimeAbsolute
                    i.changeTimeDelay = changeTimeDelay
        pass

    def updateFFChangeTimeVapor(self, changeTimeAbsolute, changeTimeDelay):
        for singleFlow in self.outletFluidFlows:
            if singleFlow in ['Vapor']:
                for i in self.outletFluidFlows[singleFlow]:
                    i.changeTimeAbsolute = changeTimeAbsolute
                    # i.changeTimeDelay = changeTimeDelay
        pass

    def _logSingleFlow(self, devKey, singleFlow, currentTime, simdm, mdGroup=None, totalDriverRate=None):
        singleTS = ts.ConstantTimeseriesTableEntry.factory(singleFlow.driverRate, singleFlow.driverUnits)
        timeDelta = singleFlow.changeTimeAbsolute - currentTime
        gcKey = singleFlow.gc.serialNum
        if mdGroup:
            extraEventMD = {'mdGroup': mdGroup}
        else:
            extraEventMD = {}
        simdm.eventLogger.logFluidFlow(currentTime, timeDelta, devKey, tsKey=singleTS.serialNum, gcKey=gcKey,
                                       flowID=singleFlow.serialNumber,
                                       flowName=singleFlow.name,
                                       driverRate=singleFlow.driverRate, driverUnits=singleFlow.driverUnits,
                                       secondaryID=singleFlow.secondaryID,
                                       totalDriverRate=totalDriverRate,
                                       **extraEventMD
                                       )

    def logFluidFlowChanges(self, key, currentTS, mdGroup=None):
        simdm = sdm.SimDataManager.getSimDataManager()
        totalDriverRate = sum(map(lambda x: x.driverRate, itertools.chain(*self.outletFluidFlows.values())))
        for singleFF in itertools.chain(*self.outletFluidFlows.values()):
            self._logSingleFlow(key, singleFF, currentTS, simdm, mdGroup=mdGroup, totalDriverRate=totalDriverRate)
        pass

    def getTotalFlowRateLiquids(self, flows):
        return sum(map(lambda x: x.driverRate, itertools.chain(self.getLiquidFlows(flows))))

    def getMinChangeTimeLiquids(self, flows):
        return min(map(lambda x: x.changeTimeAbsolute, itertools.chain(self.getLiquidFlows(flows))))

    def getMinChangeTimeVapor(self, flows):
        return min(map(lambda x: x.changeTimeAbsolute, flows.get('Vapor', [])))

    def getFlowsWithSecondaryID(self, flows, secondaryIDs):
        ret = []
        for flow in flows:
            if flow.secondaryID in secondaryIDs:
                ret.append(flow)
        return ret

