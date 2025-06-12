import math

import EquipmentTable as eqt
import MEETClasses as mc
import MEETComponentLeaks as mcl
import MEET_1_Compatability as m1
import Units as u
import Distribution as d
import MEETLinkedProductionEq as meq
import MEETExceptions as me
import logging
import SimDataManager
from Distribution import Uniform
import StateManager as sm
import SimDataManager as sdm
from pathlib import Path
import AppUtils as au
import MEETFluidFlow as ff
import GasComposition3 as gc
import TimeseriesTable as ts
import EquipmentTable as et
import csv
from Chooser import UnscaledEmpiricalDistChooser
from Chooser import EmpiricalDistChooser
import random
import DistributionProfile as dp
import itertools
import pandas as pd
from abc import ABC
import json


class MEETFacility(eqt.Facility):
    def __init__(self,
                 productionGCFilename=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.productionGCFilename = productionGCFilename


class OGCIFlare(eqt.MajorEquipment):
    def __init__(self, componentCount=None, **kwargs):
        super().__init__(**kwargs)
        self.componentCount = componentCount


class MEETLeakContainer(mc.SingleStateEquipment, ff.Volume):
    def __init__(self, activityDistribution=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.activityDistribution = activityDistribution

    #
    # todo: this is here just so proxied emitters with LeakContainers work.  This should be refactored.
    #

    def getStateInfo(self):
        fakeStateInfo = {'nextTS': u.getSimDuration()}
        return fakeStateInfo

class OGCILeak(mcl.ComponentLeaks):
    def __init__(self,
                 componentCount=None,
                 totalComponents=None,
                 totalLeaks=None,
                 activityInstance=None,
                 surveyFrequency=None,
                 **kwargs):
        pLeak = kwargs['pLeak']
        MTBF = (1 * surveyFrequency * u.HOURS_PER_DAY) / pLeak
        MTTR = (MTBF * pLeak) / (1 - pLeak)
        newArgs = {**kwargs, 'MTBF': MTBF, 'MTTR': MTTR}
        super().__init__(**newArgs)
        self.componentCount = componentCount
        self.totalComponents = totalComponents
        self.totalLeaks = totalLeaks
        self.surveyFrequency = surveyFrequency

    def activityPick(self, simdm, mcRunNum=-1):
        activityCount = int(self.componentCount)
        # This is a rather convoluted process, with the effect of having the activity factor duplicate
        # all the fields of the emitter, with the exception of equipmentType, equipmentCount, implCategory, simdm, and mcRunNum
        activityDict = self.__dict__
        activityParams = dict(map(lambda x: (x, activityDict[x]),
                                  mc.EquipmentTableEntry.EQUIPMENT_TABLE_FIELDS
                                  ))
        activityParams['equipmentType'] = 'ActivityFactor'
        activityParams['equipmentCount'] = activityCount
        activityParams['implCategory'] = 'ActivityFactor'
        activityParams['simdm'] = simdm
        activityParams['mcRunNum'] = mcRunNum
        # todo: This should probably use self.activityCategory not 'ActivityFactor'

        inst = mc.ActivityFactor(**activityParams)
        return activityCount, f"{inst.unitID}_{{unitNum}}", inst

class OGCIWell(meq.LinkedWellNoLiquidUnloading):
    def __init__(self,  componentCount=None, **kwargs):
        super().__init__(**kwargs)
        self.componentCount = componentCount


class MEETFullWell(mc.MajorEquipment, mc.StateEnabledVolume):

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['condensateGCTag',
                                         'QiGasScfPerDay',
                                         'waterGCTag',
                                         'vaporGCTag',
                                         'oilDumpFlow',
                                         'waterDumpFlow',
                                         'gasDumpFlow',
                                         'productionDurationDist',
                                         'luShutInDurationDist',
                                         'unloadingDurationDist',
                                         'shutInDurationDist',
                                         'timeBetUnloadingDist',
                                         'stateMachine',
                                         'prevUnloadingTime',
                                         'planningDurationApproxSecondsDist',
                                         'completionsDist',
                                         'delayAfterPlanningApproxSecondsDist',
                                         'drillDurationApproxSecondsDist',
                                         'delayAfterDrillingApproxSecondsDist',
                                         'flowbackTimeApproxSecondsDist',
                                         'delayAfterFlowbackApproxSecondsDist',
                                         'delayAfterCompletionApproxSecondsDist',
                                         'cyclesPerDay', 'currentDelay']

    def __init__(self,
                 QiOilBblPerDay=None,
                 QiGasMcfPerDay=None,
                 QiWaterBblPerDay=None,
                 completionDurationsName=None,
                 flowTag=None,
                 flowGasComposition=None,
                 optionalUnloading=False,
                 minTimeBetUnloadingSeconds=None,
                 maxTimeBetUnloadingSeconds=None,
                 unloadingType="MANUAL",
                 weekdaysOnly=None,
                 luShutInDurationMinSeconds=None,
                 luShutInDurationMaxSeconds=None,
                 unloadingDurationMinSeconds=None,
                 unloadingDurationMaxSeconds=None,
                 shutInDurationMinSeconds=None,
                 shutInDurationMaxSeconds=None,
                 productionDurationMinSeconds=None,
                 productionDurationMaxSeconds=None,
                 unloadingGasDestination=None,
                 planningDurationApproxSeconds=None,
                 delayAfterPlanningApproxSeconds=None,
                 drillDurationApproxSeconds=None,
                 delayAfterDrillingApproxSeconds=None,
                 flowbackTimeApproxSeconds=None,
                 delayAfterFlowbackApproxSeconds=None,
                 delayAfterCompletionApproxSeconds=None,
                 start=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.QiOilBblPerDay = QiOilBblPerDay
        self.QiGasMcfPerDay = QiGasMcfPerDay
        self.QiWaterBblPerDay = QiWaterBblPerDay
        self.flowTag = flowTag
        self.flowGasComposition = flowGasComposition
        self.condensateGCTag = f"{self.flowTag}-Condensate"
        self.waterGCTag = f"{self.flowTag}-Water"
        self.vaporGCTag = 'Default-LeakGc'
        self.prevUnloadingTime = 0
        self.currentDelay = 0
        self.start = "Production" if start is None else start
        self.initializePreProductionVals(planningDurationApproxSeconds,   # initialize preproduction values and set defaults
                                         delayAfterPlanningApproxSeconds,
                                         drillDurationApproxSeconds,
                                         delayAfterDrillingApproxSeconds,
                                         completionDurationsName,
                                         flowbackTimeApproxSeconds,
                                         delayAfterFlowbackApproxSeconds,
                                         delayAfterCompletionApproxSeconds)
        self.initializeProductionVals(optionalUnloading,       # initialize Production values and set defaults
                                      unloadingType,
                                      weekdaysOnly,
                                      minTimeBetUnloadingSeconds,
                                      maxTimeBetUnloadingSeconds,
                                      luShutInDurationMinSeconds,
                                      luShutInDurationMaxSeconds,
                                      unloadingDurationMinSeconds,
                                      unloadingDurationMaxSeconds,
                                      shutInDurationMinSeconds,
                                      shutInDurationMaxSeconds,
                                      productionDurationMinSeconds,
                                      productionDurationMaxSeconds,
                                      unloadingGasDestination)
        self.stateMachine = {"PLANNED": {'stateDuration': self.getTimeForState, 'nextState': "DRILLING"},
                             "DRILLING": {'stateDuration': self.getTimeForState, 'nextState': "COMPLETION"},
                             "COMPLETION": {'stateDuration': self.getTimeForState, 'nextState': "FLOW_BACK"},
                             "FLOW_BACK": {'stateDuration': self.getTimeForState, 'nextState': "PRODUCTION"},
                             "PRODUCTION": {'stateDuration': self.getTimeForState, 'nextState': self.calcNextStateAfterProduction},
                             "LU_SHUT_IN": {'stateDuration': self.getTimeForState, 'nextState': self.choseUnloadingState},
                             "MANUAL_UNLOADING": {'stateDuration': self.getTimeForState, 'nextState': self.nextStateAfterLU},
                             "AUTOMATIC_UNLOADING": {'stateDuration': self.getTimeForState, 'nextState': self.nextStateAfterLU},
                             "SHUT_IN": {'stateDuration': self.getTimeForState, 'nextState': "PRODUCTION"},
                             }

    def initializePreProductionVals(self,                             # initialize preproduction and set default values
                                    planningDurationApproxSeconds,
                                    delayAfterPlanningApproxSeconds,
                                    drillDurationApproxSeconds,
                                    delayAfterDrillingApproxSeconds,
                                    completionDurationsName,
                                    flowbackTimeApproxSeconds,
                                    delayAfterFlowbackApproxSeconds,
                                    delayAfterCompletionApproxSeconds
                                    ):
        self.checksForPreproductionVals(planningDurationApproxSeconds,
                                        delayAfterPlanningApproxSeconds,
                                        drillDurationApproxSeconds,
                                        delayAfterDrillingApproxSeconds,
                                        completionDurationsName,
                                        flowbackTimeApproxSeconds,
                                        delayAfterFlowbackApproxSeconds,
                                        delayAfterCompletionApproxSeconds
                                        )

        self.planningDurationApproxSeconds = planningDurationApproxSeconds
        self.planningDurationApproxSecondsDist = d.Uniform({'min': planningDurationApproxSeconds - (0.2 * planningDurationApproxSeconds),  # 20 percent delta
                                                            'max': planningDurationApproxSeconds + (0.2 * planningDurationApproxSeconds)})
        self.delayAfterPlanningApproxSeconds = delayAfterPlanningApproxSeconds
        self.delayAfterPlanningApproxSecondsDist = d.Uniform({'min': delayAfterPlanningApproxSeconds - (0.2 * delayAfterPlanningApproxSeconds),  # 20 percent delta
                                                              'max': delayAfterPlanningApproxSeconds + (0.2 * delayAfterPlanningApproxSeconds)})
        self.drillDurationApproxSeconds = drillDurationApproxSeconds
        self.drillDurationApproxSecondsDist = d.Uniform({'min': drillDurationApproxSeconds - (0.2 * drillDurationApproxSeconds),  # 20 percent delta
                                                         'max': drillDurationApproxSeconds + (0.2 * drillDurationApproxSeconds)})
        self.delayAfterDrillingApproxSeconds = delayAfterDrillingApproxSeconds
        self.delayAfterDrillingApproxSecondsDist = d.Uniform({'min': delayAfterDrillingApproxSeconds - (0.2 * delayAfterDrillingApproxSeconds),  # 20 percent delta
                                                              'max': delayAfterDrillingApproxSeconds + (0.2 * delayAfterDrillingApproxSeconds)})
        if pd.isna(completionDurationsName):
            self.completionDurationsName = None
            self.completionsDist = d.Constant(0)
        else:
            self.completionDurationsName = completionDurationsName
            simdm = sdm.SimDataManager.getSimDataManager()
            completionDurationsPath = Path(au.expandFilename(simdm.config['emitterProfileDir'], simdm.config, readonly=True))
            self.completionsDist = dp.DistributionProfile.readFile(completionDurationsPath / completionDurationsName)
        self.delayAfterCompletionApproxSeconds = delayAfterCompletionApproxSeconds
        self.delayAfterCompletionApproxSecondsDist = d.Uniform({'min': delayAfterCompletionApproxSeconds - (0.2 * delayAfterCompletionApproxSeconds),  # 20 percent delta
                                                                'max': delayAfterCompletionApproxSeconds + (0.2 * delayAfterCompletionApproxSeconds)})
        self.flowbackTimeApproxSeconds = flowbackTimeApproxSeconds
        self.flowbackTimeApproxSecondsDist = d.Uniform({'min': flowbackTimeApproxSeconds - (0.2 * flowbackTimeApproxSeconds),  # 20 percent delta
                                                        'max': flowbackTimeApproxSeconds + (0.2 * flowbackTimeApproxSeconds)})
        self.delayAfterFlowbackApproxSeconds = delayAfterFlowbackApproxSeconds
        self.delayAfterFlowbackApproxSecondsDist = d.Uniform({'min': delayAfterFlowbackApproxSeconds - (0.2 * delayAfterFlowbackApproxSeconds),  # 20 percent delta
                                                              'max': delayAfterFlowbackApproxSeconds + (0.2 * delayAfterFlowbackApproxSeconds)})

    def initializeProductionVals(self,           # initialize Production values and set defaults
                                 optionalUnloading,
                                 unloadingType,
                                 weekdaysOnly,
                                 minTimeBetUnloadingSeconds,
                                 maxTimeBetUnloadingSeconds,
                                 luShutInDurationMinSeconds,
                                 luShutInDurationMaxSeconds,
                                 unloadingDurationMinSeconds,
                                 unloadingDurationMaxSeconds,
                                 shutInDurationMinSeconds,
                                 shutInDurationMaxSeconds,
                                 productionDurationMinSeconds,
                                 productionDurationMaxSeconds,
                                 unloadingGasDestination
                                 ):
        self.optionalUnloading = False if optionalUnloading is None else optionalUnloading
        self.checksForUnloadingColumns(minTimeBetUnloadingSeconds,
                                       maxTimeBetUnloadingSeconds,
                                       luShutInDurationMinSeconds,
                                       luShutInDurationMaxSeconds,
                                       unloadingDurationMinSeconds,
                                       unloadingDurationMaxSeconds
                                       )
        self.unloadingType = "MANUAL" or unloadingType
        self.weekdaysOnly = None or weekdaysOnly
        # self.start = "Production" if start is None else start
        self.minTimeBetUnloadingSeconds = minTimeBetUnloadingSeconds
        self.maxTimeBetUnloadingSeconds = maxTimeBetUnloadingSeconds
        self.timeBetUnloadingDist = d.Uniform({'min': minTimeBetUnloadingSeconds, 'max': maxTimeBetUnloadingSeconds})
        self.luShutInDurationMinSeconds = luShutInDurationMinSeconds
        self.luShutInDurationMaxSeconds = luShutInDurationMaxSeconds
        self.luShutInDurationDist = d.Uniform({'min': luShutInDurationMinSeconds, 'max': luShutInDurationMaxSeconds})
        self.unloadingDurationMinSeconds = unloadingDurationMinSeconds
        self.unloadingDurationMaxSeconds = unloadingDurationMaxSeconds
        self.unloadingDurationDist = d.Uniform({'min': unloadingDurationMinSeconds, 'max': unloadingDurationMaxSeconds})
        self.shutInDurationMinSeconds = shutInDurationMinSeconds
        self.shutInDurationMaxSeconds = shutInDurationMaxSeconds
        self.shutInDurationDist = d.Uniform({'min': shutInDurationMinSeconds, 'max': shutInDurationMaxSeconds})
        self.productionDurationMinSeconds = productionDurationMinSeconds
        self.productionDurationMaxSeconds = productionDurationMaxSeconds
        self.productionDurationDist = d.Uniform({'min': productionDurationMinSeconds, 'max': productionDurationMaxSeconds})
        self.unloadingGasDestination = unloadingGasDestination

    def checksForUnloadingColumns(self, minTimeBetUnloadingSeconds,
                                        maxTimeBetUnloadingSeconds,
                                        luShutInDurationMinSeconds,
                                        luShutInDurationMaxSeconds,
                                        unloadingDurationMinSeconds,
                                        unloadingDurationMaxSeconds):
        if self.optionalUnloading is True:
            msg = f'Please make sure there are all the Unloading columns in the study sheet,\n' \
                  f'else set the column "Optional Unloading" as False to never hit unloading states.\n' \
                  f'Main unloading Columns are "Min Time Between Unloadings", "Max Time Between Unloadings", "Unloading Type"\n' \
                  f'"LU Shut-In Duration Min", "LU Shut-In Duration Max", "Unloading Duration Min", "Unloading Duration Max"\n'
            # self.exceptionPrinting('minTimeBetUnloadingSeconds', minTimeBetUnloadingSeconds, msg)
            self.exceptionPrinting('maxTimeBetUnloadingSeconds', maxTimeBetUnloadingSeconds, msg)
            # self.exceptionPrinting('luShutInDurationMinSeconds', luShutInDurationMinSeconds, msg)
            self.exceptionPrinting('luShutInDurationMaxSeconds', luShutInDurationMaxSeconds, msg)
            # self.exceptionPrinting('unloadingDurationMinSeconds', unloadingDurationMinSeconds, msg)
            self.exceptionPrinting('unloadingDurationMaxSeconds', unloadingDurationMaxSeconds, msg)

    def checksForPreproductionVals(self, planningDurationApproxSeconds,
                                         delayAfterPlanningApproxSeconds,
                                         drillDurationApproxSeconds,
                                         delayAfterDrillingApproxSeconds,
                                         completionDurationsName,
                                         flowbackTimeApproxSeconds,
                                         delayAfterFlowbackApproxSeconds,
                                         delayAfterCompletionApproxSeconds):
        if self.start == 'PreProduction':
            msg = f'Please make sure there are all the PreProduction columns in the study sheet,\n' \
                  f'else set the column "Simulation Start Stage" as Production to start in Production Stage.\n' \
                  f'PreProduction Columns are "Approx Planning Time", "Approx Delay After Planning", "Approx Drilling Time"\n' \
                  f'"Approx Delay After Drilling", "Completion Durations Name", "Approx Delay After Completion",\n' \
                  f'"Approx Flow Back Time" and "Approx Delay After Flow Back"\n'
            self.exceptionPrinting('planningDurationApproxSeconds', planningDurationApproxSeconds, msg)
            self.exceptionPrinting('delayAfterPlanningApproxSeconds', delayAfterPlanningApproxSeconds, msg)
            self.exceptionPrinting('drillDurationApproxSeconds', drillDurationApproxSeconds, msg)
            self.exceptionPrinting('delayAfterDrillingApproxSeconds', delayAfterDrillingApproxSeconds, msg)
            self.exceptionPrinting('completionDurationsName', completionDurationsName, msg)
            self.exceptionPrinting('flowbackTimeApproxSeconds', flowbackTimeApproxSeconds, msg)
            self.exceptionPrinting('delayAfterFlowbackApproxSeconds', delayAfterFlowbackApproxSeconds, msg)
            self.exceptionPrinting('delayAfterCompletionApproxSeconds', delayAfterCompletionApproxSeconds, msg)

    def exceptionPrinting(self, valName, val, msg):
        if val == 0:
            msg2 = f'Missing parameter {valName} in unitID {self.unitID}.\n'
            raise NotImplementedError(msg2+msg)

    def calcNextStateAfterProduction(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        msg = 'No calcNextStateAfterProduction method defined for class '+str(self.__class__.__name__)
        raise NotImplementedError(msg)

    def nextStateAfterLU(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        self.prevUnloadingTime = currentTime
        nextState = "PRODUCTION"
        return nextState

    def choseUnloadingState(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        if self.unloadingType.upper() == "MANUAL":
            nextState = "MANUAL_UNLOADING"
        elif self.unloadingType.upper() == "AUTOMATIC":
            nextState = "AUTOMATIC_UNLOADING"
        else:
            msg = "No unloading of type "+self.unloadingType.upper()+" for "+str(self.unitID)
            raise AttributeError(msg)
        return nextState

    def calcStateTimes(self, **kwargs):
        stateTimes = {"PRODUCTION": self.getTimeForState(currentTime=0, currentStateInfo=sm.StateInfo(stateName="PRODUCTION")),
                      "PLANNED": 0,
                      "DRILLING": 0,
                      "COMPLETION": 0,
                      "FLOW_BACK": 0,
                      "LU_SHUT_IN": self.luShutInDurationMaxSeconds,
                      "MANUAL_UNLOADING": self.unloadingDurationMaxSeconds,
                      "AUTOMATIC_UNLOADING": self.unloadingDurationMaxSeconds,
                      "SHUT_IN": self.shutInDurationMaxSeconds
                      }
        self.stateTimes = stateTimes

    def initialStateTimes(self, **kwargs):
        if self.start == 'PreProduction':   # Start point 1
            randomState = "PLANNED"
            randomStateDelay = self.getTimeForState(currentTime=0, currentStateInfo=sm.StateInfo(stateName="PLANNED"))
        else:   # Start point 2 for Continuous, Start point 3 for Cycled
            randomState = "PRODUCTION"
            randomStateDelay = self.getTimeForState(currentTime=0, currentStateInfo=sm.StateInfo(stateName="PRODUCTION"))
        stateTimes = {randomState: randomStateDelay}
        return stateTimes

    def initialStateUpdate(self, stateName, stateDuration, currentTime):
        ret = sm.StateInfo(stateName, deltaTimeInState=stateDuration, absoluteTimeInState=currentTime + stateDuration)
        return ret

    def getTimeForState(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        cs = currentStateInfo.stateName
        if cs == "PRODUCTION":
            delay = int(self.productionDurationDist.pick())
            oilDumpFlow = u.bblPerDayToBblPerSec(self.QiOilBblPerDay)
            self.oilDumpFlow.driverRate = oilDumpFlow
            waterDumpFlow = u.bblPerDayToBblPerSec(self.QiWaterBblPerDay)
            self.waterDumpFlow.driverRate = waterDumpFlow
        elif cs == "PLANNED":
            delay = int(self.planningDurationApproxSecondsDist.pick())
            self.oilDumpFlow.driverRate = 0
            self.waterDumpFlow.driverRate = 0
        elif cs == "DRILLING":
            delay = int(self.drillDurationApproxSecondsDist.pick())
            self.oilDumpFlow.driverRate = 0
            self.waterDumpFlow.driverRate = 0
        elif cs == "COMPLETION":
            delay = int(self.completionsDist.pick())
            self.oilDumpFlow.driverRate = 0
            self.waterDumpFlow.driverRate = 0
        elif cs == "FLOW_BACK":
            delay = int(self.flowbackTimeApproxSecondsDist.pick())
            self.oilDumpFlow.driverRate = 0
            self.waterDumpFlow.driverRate = 0
            self.prevUnloadingTime = currentTime
        elif cs == "LU_SHUT_IN":
            delay = int(self.luShutInDurationDist.pick())
            self.oilDumpFlow.driverRate = 0
            self.waterDumpFlow.driverRate = 0
        elif cs == "MANUAL_UNLOADING":
            delay = int(self.unloadingDurationDist.pick())
            self.oilDumpFlow.driverRate = 0
            self.waterDumpFlow.driverRate = 0
        elif cs == "AUTOMATIC_UNLOADING":
            delay = int(self.unloadingDurationDist.pick())
            self.oilDumpFlow.driverRate = 0
            self.waterDumpFlow.driverRate = 0
        elif cs == "SHUT_IN":
            delay = int(self.shutInDurationDist.pick())
            self.oilDumpFlow.driverRate = 0
            self.waterDumpFlow.driverRate = 0
        self.nextStateTransitionTS = currentTime + delay
        self.oilDumpFlow.changeTimeAbsolute = self.nextStateTransitionTS
        self.waterDumpFlow.changeTimeAbsolute = self.nextStateTransitionTS
        return delay

    def initializeFluidFlow(self, simdm):
        condensateGC = gc.FluidFlowGC.factory(fluidFlowGCFilename=self.flowGasComposition,
                                              flow='Condensate',
                                              fluidFlowID=self.condensateGCTag,
                                              gcUnits='bbl')
        self.oilDumpFlow = ff.FluidFlow('Condensate', u.bblPerDayToBblPerSec(self.QiOilBblPerDay), 'bbl', condensateGC)
        self.addOutletFluidFlow(self.oilDumpFlow)

        waterGC = gc.FluidFlowGC.factory(fluidFlowGCFilename=self.flowGasComposition,
                                         flow='Water',
                                         fluidFlowID=self.waterGCTag,
                                         gcUnits='bbl')
        self.waterDumpFlow = ff.FluidFlow('Water', u.bblPerDayToBblPerSec(self.QiWaterBblPerDay), 'bbl', waterGC)
        self.addOutletFluidFlow(self.waterDumpFlow)


class MEETWellContinuousProduction(MEETFullWell):

    def __init__(self,
                 minTimeBetUnloading=None,
                 maxTimeBetUnloading=None,
                 unloadingType=None,
                 weekdaysOnly=None,
                 luShutInDurationMin=None,
                 luShutInDurationMax=None,
                 unloadingDurationMin=None,
                 unloadingDurationMax=None,
                 DiPerMonth=None,
                 bHyperbolic=None,
                 wellAgeMonths=None,
                 planningDurationApprox=None,
                 delayAfterPlanningApprox=None,
                 drillDurationApprox=None,
                 delayAfterDrillingApprox=None,
                 flowbackTimeApprox=None,
                 delayAfterFlowbackApprox=None,
                 delayAfterCompletionApprox=None,
                 **kwargs):
        # if bHyperbolic <= 0:
        #     msg = f"bHyperbolic must be > 0, value: {bHyperbolic}"
        #     logging.warning(msg)
        #     raise me.IllegalArgumentError(msg)
        minTimeBetUnloadingSeconds = 0 if minTimeBetUnloading is None else u.daysToSecs(minTimeBetUnloading)
        maxTimeBetUnloadingSeconds = 0 if maxTimeBetUnloading is None else u.daysToSecs(maxTimeBetUnloading)
        luShutInDurationMinSeconds = 0 if luShutInDurationMin is None else u.hoursToSecs(luShutInDurationMin)
        luShutInDurationMaxSeconds = 0 if luShutInDurationMax is None else u.hoursToSecs(luShutInDurationMax)
        unloadingDurationMinSeconds = 0 if unloadingDurationMin is None else u.hoursToSecs(unloadingDurationMin)
        unloadingDurationMaxSeconds = 0 if unloadingDurationMax is None else u.hoursToSecs(unloadingDurationMax)
        productionDurationMinSeconds = u.getSimDuration() if minTimeBetUnloading is None else u.daysToSecs(minTimeBetUnloading)
        productionDurationMaxSeconds = u.getSimDuration() if maxTimeBetUnloading is None else u.daysToSecs(maxTimeBetUnloading)
        planningDurationApproxSeconds = 0 if planningDurationApprox is None else u.daysToSecs(planningDurationApprox)
        delayAfterPlanningApproxSeconds = 0 if delayAfterPlanningApprox is None else u.daysToSecs(delayAfterPlanningApprox)
        drillDurationApproxSeconds = 0 if drillDurationApprox is None else u.daysToSecs(drillDurationApprox)
        delayAfterDrillingApproxSeconds = 0 if delayAfterDrillingApprox is None else u.daysToSecs(delayAfterDrillingApprox)
        flowbackTimeApproxSeconds = 0 if flowbackTimeApprox is None else u.daysToSecs(flowbackTimeApprox)
        delayAfterFlowbackApproxSeconds = 0 if delayAfterFlowbackApprox is None else u.daysToSecs(delayAfterFlowbackApprox)
        delayAfterCompletionApproxSeconds = 0 if delayAfterCompletionApprox is None else u.daysToSecs(delayAfterCompletionApprox)
        newArgs = {**kwargs,
                   'minTimeBetUnloadingSeconds': minTimeBetUnloadingSeconds,
                   'maxTimeBetUnloadingSeconds': maxTimeBetUnloadingSeconds,
                   'unloadingType': unloadingType,
                   'weekdaysOnly': weekdaysOnly,
                   'luShutInDurationMinSeconds': luShutInDurationMinSeconds,
                   'luShutInDurationMaxSeconds': luShutInDurationMaxSeconds,
                   'unloadingDurationMinSeconds': unloadingDurationMinSeconds,
                   'unloadingDurationMaxSeconds': unloadingDurationMaxSeconds,
                   'productionDurationMinSeconds': productionDurationMinSeconds,
                   'productionDurationMaxSeconds': productionDurationMaxSeconds,
                   'planningDurationApproxSeconds': planningDurationApproxSeconds,
                   'delayAfterPlanningApproxSeconds': delayAfterPlanningApproxSeconds,
                   'drillDurationApproxSeconds': drillDurationApproxSeconds,
                   'delayAfterDrillingApproxSeconds': delayAfterDrillingApproxSeconds,
                   'flowbackTimeApproxSeconds': flowbackTimeApproxSeconds,
                   'delayAfterFlowbackApproxSeconds': delayAfterFlowbackApproxSeconds,
                   'delayAfterCompletionApproxSeconds': delayAfterCompletionApproxSeconds
                   }
        super().__init__(**newArgs)
        self.minTimeBetUnloading = minTimeBetUnloading
        self.maxTimeBetUnloading = maxTimeBetUnloading
        self.luShutInDurationMin = luShutInDurationMin
        self.luShutInDurationMax = luShutInDurationMax
        self.unloadingDurationMin = unloadingDurationMin
        self.unloadingDurationMax = unloadingDurationMax
        self.DiPerMonth = DiPerMonth
        self.bHyperbolic = bHyperbolic
        self.wellAgeMonths = wellAgeMonths
        self.planningDurationApprox = planningDurationApprox
        self.delayAfterPlanningApprox = delayAfterPlanningApprox
        self.drillDurationApprox = drillDurationApprox
        self.delayAfterDrillingApprox = delayAfterDrillingApprox
        self.flowbackTimeApprox = flowbackTimeApprox
        self.delayAfterFlowbackApprox = delayAfterFlowbackApprox
        self.delayAfterCompletionApprox = delayAfterCompletionApprox

    def calcNextStateAfterProduction(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        if self.optionalUnloading is True:
            nextState = "LU_SHUT_IN"
        else:
            nextState = "PRODUCTION"
        return nextState

    def getTimeForState(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        cs = currentStateInfo.stateName
        if cs == "PRODUCTION":
            delay = int(self.productionDurationDist.pick())
            oilDumpFlow = u.bblPerDayToBblPerSec(self.QiOilBblPerDay)
            self.oilDumpFlow.driverRate = oilDumpFlow
            waterDumpFlow = u.bblPerDayToBblPerSec(self.QiWaterBblPerDay)
            self.waterDumpFlow.driverRate = waterDumpFlow
            self.nextStateTransitionTS = currentTime + delay
            self.oilDumpFlow.changeTimeAbsolute = self.nextStateTransitionTS
            self.waterDumpFlow.changeTimeAbsolute = self.nextStateTransitionTS
        else:
            delay = super().getTimeForState(currentStateData, currentStateInfo, currentTime)
        return delay


class MEETWellCycledProduction(MEETFullWell):
    def __init__(self,
                 QiOilBblPerDay=None,
                 QiWaterBblPerDay=None,
                 QiGasMcfPerDay=None,
                 totalLiquidsBblPerDump=None,
                 meanProductionTimeSeconds=None,
                 deltaProductionTimeSeconds=None,
                 variationShutInTime=None,
                 minTimeBetUnloading=None,
                 maxTimeBetUnloading=None,
                 unloadingType=None,
                 weekdaysOnly=None,
                 luShutInDurationMin=None,
                 luShutInDurationMax=None,
                 unloadingDurationMin=None,
                 unloadingDurationMax=None,
                 planningDurationApprox=None,
                 delayAfterPlanningApprox=None,
                 drillDurationApprox=None,
                 delayAfterDrillingApprox=None,
                 flowbackTimeApprox=None,
                 delayAfterFlowbackApprox=None,
                 delayAfterCompletionApprox=None,
                 **kwargs):
        cyclesPerDay, shutInTimeSeconds = self.calcShutInTime(QiWaterBblPerDay, QiOilBblPerDay, totalLiquidsBblPerDump, meanProductionTimeSeconds)
        if shutInTimeSeconds <= 0:
            msg = 'Oil Production and/or Water Production rates are too high or' \
                  ' Total Liquids Produced is too low for Unit ID '+str(kwargs['unitID'])
            raise ValueError(msg)
        shutInDurationMinSeconds = shutInTimeSeconds - ((variationShutInTime * shutInTimeSeconds) / 100)    # since it is percentage
        shutInDurationMaxSeconds = shutInTimeSeconds + ((variationShutInTime * shutInTimeSeconds) / 100)    # since it is percentage
        productionDurationMinSeconds = meanProductionTimeSeconds - deltaProductionTimeSeconds
        productionDurationMaxSeconds = meanProductionTimeSeconds + deltaProductionTimeSeconds
        minTimeBetUnloadingSeconds = 0 if minTimeBetUnloading is None else u.daysToSecs(minTimeBetUnloading)
        maxTimeBetUnloadingSeconds = 0 if maxTimeBetUnloading is None else u.daysToSecs(maxTimeBetUnloading)
        luShutInDurationMinSeconds = 0 if luShutInDurationMin is None else u.hoursToSecs(luShutInDurationMin)
        luShutInDurationMaxSeconds = 0 if luShutInDurationMax is None else u.hoursToSecs(luShutInDurationMax)
        unloadingDurationMinSeconds = 0 if unloadingDurationMin is None else u.hoursToSecs(unloadingDurationMin)
        unloadingDurationMaxSeconds = 0 if unloadingDurationMax is None else u.hoursToSecs(unloadingDurationMax)
        planningDurationApproxSeconds = 0 if planningDurationApprox is None else u.daysToSecs(planningDurationApprox)
        delayAfterPlanningApproxSeconds = 0 if delayAfterPlanningApprox is None else u.daysToSecs(delayAfterPlanningApprox)
        drillDurationApproxSeconds = 0 if drillDurationApprox is None else u.daysToSecs(drillDurationApprox)
        delayAfterDrillingApproxSeconds = 0 if delayAfterDrillingApprox is None else u.daysToSecs(delayAfterDrillingApprox)
        flowbackTimeApproxSeconds = 0 if flowbackTimeApprox is None else u.daysToSecs(flowbackTimeApprox)
        delayAfterFlowbackApproxSeconds = 0 if delayAfterFlowbackApprox is None else u.daysToSecs(delayAfterFlowbackApprox)
        delayAfterCompletionApproxSeconds = 0 if delayAfterCompletionApprox is None else u.daysToSecs(delayAfterCompletionApprox)
        newArgs = {**kwargs,
                   'QiWaterBblPerDay': QiWaterBblPerDay,
                   'QiOilBblPerDay': QiOilBblPerDay,
                   'QiGasMcfPerDay': QiGasMcfPerDay,
                   'shutInDurationMinSeconds': shutInDurationMinSeconds,
                   'shutInDurationMaxSeconds': shutInDurationMaxSeconds,
                   'productionDurationMinSeconds': productionDurationMinSeconds,
                   'productionDurationMaxSeconds': productionDurationMaxSeconds,
                   'minTimeBetUnloadingSeconds': minTimeBetUnloadingSeconds,
                   'maxTimeBetUnloadingSeconds': maxTimeBetUnloadingSeconds,
                   'unloadingType': unloadingType,
                   'weekdaysOnly': weekdaysOnly,
                   'luShutInDurationMinSeconds': luShutInDurationMinSeconds,
                   'luShutInDurationMaxSeconds': luShutInDurationMaxSeconds,
                   'unloadingDurationMinSeconds': unloadingDurationMinSeconds,
                   'unloadingDurationMaxSeconds': unloadingDurationMaxSeconds,
                   'planningDurationApproxSeconds': planningDurationApproxSeconds,
                   'delayAfterPlanningApproxSeconds': delayAfterPlanningApproxSeconds,
                   'drillDurationApproxSeconds': drillDurationApproxSeconds,
                   'delayAfterDrillingApproxSeconds': delayAfterDrillingApproxSeconds,
                   'flowbackTimeApproxSeconds': flowbackTimeApproxSeconds,
                   'delayAfterFlowbackApproxSeconds': delayAfterFlowbackApproxSeconds,
                   'delayAfterCompletionApproxSeconds': delayAfterCompletionApproxSeconds
                   }
        super().__init__(**newArgs)
        self.totalLiquidsBblPerDump = totalLiquidsBblPerDump
        self.meanProductionTimeSeconds = meanProductionTimeSeconds
        self.deltaProductionTimeSeconds = deltaProductionTimeSeconds
        self.variationShutInTime = variationShutInTime
        self.minTimeBetUnloading = minTimeBetUnloading
        self.maxTimeBetUnloading = maxTimeBetUnloading
        self.luShutInDurationMin = luShutInDurationMin
        self.luShutInDurationMax = luShutInDurationMax
        self.unloadingDurationMin = unloadingDurationMin
        self.unloadingDurationMax = unloadingDurationMax
        self.planningDurationApprox = planningDurationApprox
        self.delayAfterPlanningApprox = delayAfterPlanningApprox
        self.drillDurationApprox = drillDurationApprox
        self.delayAfterDrillingApprox = delayAfterDrillingApprox
        self.flowbackTimeApprox = flowbackTimeApprox
        self.delayAfterFlowbackApprox = delayAfterFlowbackApprox
        self.delayAfterCompletionApprox = delayAfterCompletionApprox
        self.cyclesPerDay = cyclesPerDay

    def calcShutInTime(self, waterBBLperDay, oilBBLperDay, liquidsBBLPerDump, productionTime):
        cyclesPerDay = (waterBBLperDay + oilBBLperDay)/liquidsBBLPerDump   # calc cycles/day
        totalCycleTime = u.SECONDS_PER_DAY / cyclesPerDay                  # calc cycle time (1/freq) in seconds
        shutInTime = totalCycleTime - productionTime                       # calc shut in time
        return cyclesPerDay, shutInTime

    def calcNextStateAfterProduction(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        if self.optionalUnloading is True:
            if self.minTimeBetUnloadingSeconds < (currentTime - self.prevUnloadingTime) < self.maxTimeBetUnloadingSeconds:
                nextState = "LU_SHUT_IN"
            else:
                nextState = "SHUT_IN"
        else:
            nextState = "SHUT_IN"
        return nextState

    def getTimeForState(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        cs = currentStateInfo.stateName
        if cs == 'PRODUCTION':
            delay = int(self.productionDurationDist.pick())
            oilDumpFlow = self.QiOilBblPerDay / self.cyclesPerDay / delay  # bblperdump/delay
            self.oilDumpFlow.driverRate = oilDumpFlow
            waterDumpFlow = self.QiWaterBblPerDay / self.cyclesPerDay / delay
            self.waterDumpFlow.driverRate = waterDumpFlow
            self.nextStateTransitionTS = currentTime + delay
            self.oilDumpFlow.changeTimeAbsolute = self.nextStateTransitionTS
            self.waterDumpFlow.changeTimeAbsolute = self.nextStateTransitionTS
        else:
             delay = super().getTimeForState(currentStateData, currentStateInfo, currentTime)
        return delay


class MEETCompressor(mc.StateEnabledVolume, et.MajorEquipment, mc.StateChangeInitiator, mc.DESStateEnabled):
    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['opCycleTimeDistSecs', 'stateMachine', 'stateNextTS',
                                         'linkedUpstreamEquipment', 'fluidName', 'overloadDurSec',
                                         'loadEquations', 'loadingDistribution', 'OPTimeProfileInst',
                                         'StatePercentageInst', 'OPTimeDistribution', 'opModeHours', 'nopModeHours',
                                         'nodModeHours', 'nopPct', 'nodPct', 'loadingKW', 'loadingPU',
                                         'nextStateTable', 'overloadDurNoFFMinSec', 'overloadDurNoFFMaxSec',
                                         'overloadDurNoFFDist', 'loadEquations', 'inletStagesDeltaH',
                                         'currentStateDelay']
    STATES_WITH_CALCULATED_TIMES = ['OPERATING', 'NOT_OPERATING_PRESSURIZED', 'NOT_OPERATING_DEPRESSURIZED']

    def __init__(self,
                 activityDistribution=None,
                 compressorType=None,
                 compressorkW=None,
                 operatingFraction=None,
                 nopFraction=None,
                 lowerLimit=None,
                 upperLimit=None,
                 compressorEfficiency=None,
                 loadCondition=None,
                 averageLoading=None,
                 stdLoading=None,
                 driverType=None,
                 type=None,
                 dataSource=None,
                 userSpecifiedDataSourceFilename=None,
                 combustionEmissionReference=None,
                 OPModeHours=None,
                 NOPModeHours=None,
                 NODModeHours=None,
                 startVentDurationSecs=None,
                 blowdownVentDurationSecs=None,
                 opCycleTimeDistSecs=None,
                 OPTimeProfile=None,
                 StatePercentages=None,
                 startDuration=0,
                 blowdownDuration=0,
                 timeRatios=None,
                 stateTimes=None,
                 OPTimeDistribution=None,
                 OPTimeHrs=None,
                 NOPTimePct=None,
                 initialState=None,
                 initialStateTime=None,
                 fuzzedInitialTime=None,
                 **kwargs):
        operatingFraction = self.resetZeroAndOne(operatingFraction)
        nopFraction = self.resetZeroAndOne(nopFraction)
        opModeHours = u.HOURS_PER_YEAR * operatingFraction
        remainingHours = u.HOURS_PER_YEAR - opModeHours
        nopModeHours = remainingHours * nopFraction
        nodModeHours = remainingHours * (1 - nopFraction)
        # if operatingFraction == 1 and nopFraction > 0:
        #     msg = (f'Operating fraction is 1, NOP fraction should be 0. Please check the study sheet, '
        #            f'unitID: {kwargs["unitID"]}')
        #     logging.warning(msg)
        super().__init__(**kwargs)
        self.activityDistribution = activityDistribution
        self.operatingFraction = operatingFraction
        self.nopFraction = nopFraction
        self.compressorType = compressorType
        self.compressorkW = compressorkW
        self.lowerLimit = lowerLimit
        self.upperLimit = upperLimit
        self.compressorEfficiency = compressorEfficiency
        self.loadCondition = loadCondition
        self.averageLoading = averageLoading
        self.stdLoading = stdLoading
        self.loadingDistribution = d.Normal({'mu': self.averageLoading, 'sigma': self.stdLoading})
        self.driverType = driverType
        self.type = type
        self.linkedUpstreamEquipment = []
        self.fluidName = 'Vapor'
        self.dataSource = dataSource
        self.userSpecifiedDataSourceFilename = userSpecifiedDataSourceFilename
        self.combustionEmissionReference = combustionEmissionReference
        self.opModeHours = opModeHours
        self.nopModeHours = nopModeHours
        self.nodModeHours = nodModeHours
        self.startVentDurationSecs = int(startVentDurationSecs)
        self.blowdownVentDurationSecs = int(blowdownVentDurationSecs)
        self.opCycleTimeDistSecs = opCycleTimeDistSecs
        self.OPTimeProfile = OPTimeProfile
        self.StatePercentages = StatePercentages
        self.startDuration = startDuration
        self.blowdownDuration = blowdownDuration

        self.timeRatios = timeRatios if timeRatios else dict.fromkeys(['OPERATING', 'NOT_OPERATING_PRESSURIZED', 'NOT_OPERATING_DEPRESSURIZED'], 0.0)
        self.stateTimes = stateTimes
        self.OPTimeDistribution = OPTimeDistribution
        self.OPTimeHrs = OPTimeHrs
        self.NOPTimePct = NOPTimePct
        self.initialState = initialState
        self.initialStateTime = initialStateTime
        self.fuzzedInitialTime = fuzzedInitialTime

        totalHours = self.opModeHours + self.nopModeHours + self.nodModeHours
        self.timeRatios = {'OPERATING': self.opModeHours / totalHours,
                           'NOT_OPERATING_PRESSURIZED': self.nopModeHours / totalHours,
                           'NOT_OPERATING_DEPRESSURIZED': self.nodModeHours / totalHours}
        totalNotOperatingHours = self.nopModeHours + self.nodModeHours
        if totalNotOperatingHours != 0:
            self.nopPct = self.nopModeHours / totalNotOperatingHours
            self.nodPct = self.nodModeHours / totalNotOperatingHours
        else:
            self.nopPct = 0.0
            self.nodPct = 0.0

        self.overloadDurSec = 0
        self.loadingKW = 0
        self.loadingPU = 0
        self.currentStateDelay = 0
        self.inletStagesDeltaH = []

        self.stateMachine = {
            "OPERATING": {
                'stateDuration': self.getTimeForState,
                'nextState': self.getNextState,
            },
            "NOT_OPERATING_PRESSURIZED": {
                'stateDuration': self.getTimeForState,
                'nextState': self.getNextState,
            },
            "NOT_OPERATING_DEPRESSURIZED": {
                'stateDuration': self.getTimeForState,
                'nextState': self.getNextState,
            },
            "STARTING": {
                'stateDuration': self.startVentDurationSecs,
                'nextState': self.getNextState,
            },
            "BLOWDOWN": {
                'stateDuration': self.blowdownVentDurationSecs,
                'nextState': self.getNextState,
            },
            "OVERLOAD": {
                'stateDuration': self.getOverloadStateTime,
                'nextState': self.getNextState,
            },
        }
        self.initialState = 'UNKNOWN'  # will get set on initialization
        self.loadEquations = self.getLoadConditions(self.loadCondition)
        # run nextStateTable through toChoser, returns instance of distribuion, do pick
        self.nextStateTable = {"OPERATING": {'nextState': {"NOT_OPERATING_PRESSURIZED": self.nopPct, "BLOWDOWN": self.nodPct}},
                               "NOT_OPERATING_PRESSURIZED": {'nextState': {"STARTING": 1.0, "BLOWDOWN": 0.0}},
                               "NOT_OPERATING_DEPRESSURIZED": {'nextState': 'STARTING'},
                               "STARTING": {'nextState': 'OPERATING'},
                               "BLOWDOWN": {'nextState': 'NOT_OPERATING_DEPRESSURIZED'},
                               "OVERLOAD": {'nextState': 'OPERATING'}}

    # todo: this is being overridden here so we can have instantiate the opCycleTimeDistHours from parameters
    # todo: specified in the intake spreadsheet.  We should modify 'readEmitterFile' so it takes distributions as
    # todo: filenames, numbers (for constant distros), or json, which will be input to distFactory

    def instantiateFromTemplate(self, simdm, **kwargs):
        newInst = self.__class__(**kwargs)
        newInst.initializeFluidFlow(simdm)
        lowerLimitSeconds = u.hoursToSecs(self.lowerLimit)
        upperLimitSeconds = u.hoursToSecs(self.upperLimit)
        dist = d.Uniform({'min': lowerLimitSeconds, 'max': upperLimitSeconds})
        newInst.startVentDurationSecs = self.startVentDurationSecs
        newInst.blowdownVentDurationSecs = self.blowdownVentDurationSecs
        newInst.opModeHours = self.opModeHours
        newInst.nopModeHours = self.nopModeHours
        newInst.nodModeHours = self.nodModeHours
        newInst.opCycleTimeDistSecs = dist
        newInst.timeRatios = self.timeRatios
        newInst.stateTimes = dict.fromkeys(self.STATES_WITH_CALCULATED_TIMES, 0)

        newInst.calculateInitialStateTimes()
        pass

    def resetZeroAndOne(self, num):
        if num == 1:
            ret = 0.999999
        elif num == 0:
            ret = 1 - 0.999999
        else:
            ret = num
        return ret

    def getOverloadStateTime(self, currentTime=None, currentStateData=None, currentStateInfo=None):
        ret = self.getOverloadDelay(currentTime)
        return ret

    def getNextState(self, currentTime=None, currentStateData=None, currentStateInfo=None):
        cs = currentStateInfo.stateName
        self.loadingKW, self.loadingPU, fuelConsump = self.calculateFuelConsumption()
        if fuelConsump > self.compressorkW:
            # self.overloadDurSec = self.getOverloadDelay()
            self.currentStateDelay = 0
            return 'OVERLOAD'
        else:
            nextState = self.checkForCurrentState(currentTime, currentStateData, currentStateInfo)
            return nextState

    def getOverloadDelay(self, currentTime):
        # overload delay = change time for vapor flows
        # if change time = simduration (ex: FixedSource), we dont know the time, so use dur from study sheet
        # if no FFs, overload dur = from study sheet
        if self.inletFluidFlows:
            overloadDurSec = abs(self.getMinChangeTimeVapor(self.inletFluidFlows) - currentTime)
            if overloadDurSec >= u.getSimDuration():
                msg = f'overload dur = simDur, please check the study sheet, unitID: {self.unitID}'
                raise NotImplementedError(msg)
                # overloadDurSec = int(self.overloadDurNoFFDist.pick())
        else:
            raise NotImplementedError
            # overloadDurSec = int(self.overloadDurNoFFDist.pick())
        return overloadDurSec

    def nextStateNoOverload(self, currentTime, currentStateData, currentStateInfo):
        cs = currentStateInfo.stateName
        nextState = self.nextStateTable.get(cs).get('nextState')
        return nextState

    def checkForCurrentState(self, currentTime, currentStateData, currentStateInfo):
        cs = currentStateInfo.stateName
        if self.currentStateDelay != 0:
            nextState = cs
        else:
            nextState = self.nextStateNoOverload2(currentTime, currentStateData, currentStateInfo)
        return nextState

    def nextStateNoOverload2(self, currentTime, currentStateData, currentStateInfo):
        cs = currentStateInfo.stateName
        if cs == 'OPERATING':
            nextState = random.choices(['NOT_OPERATING_PRESSURIZED', 'BLOWDOWN'], weights=(self.nopPct, self.nodPct), k=1)[0]
            # add OPERATING here for the sum = 1
        elif cs == 'NOT_OPERATING_PRESSURIZED':
            nextState = random.choices(['STARTING', 'BLOWDOWN'], weights=(1.0, 0), k=1)[0]
        elif cs == 'NOT_OPERATING_DEPRESSURIZED':
            nextState = 'STARTING'
        elif cs == 'STARTING':
            nextState = 'OPERATING'
        elif cs == 'BLOWDOWN':
            nextState = 'NOT_OPERATING_DEPRESSURIZED'
        elif cs == 'OVERLOAD':
            nextState = 'OPERATING'
        else:
            msg = f'state {cs} not found'
            raise NotImplementedError(msg)
        return nextState

    def getTimeForState(self, currentTime=0, currentStateData=None, currentStateInfo=None):
        cs = currentStateInfo.stateName
        if self.currentStateDelay != 0:
            retDelay = self.getUpdatedDelay(self.currentStateDelay, currentTime)
            self.currentStateDelay -= retDelay
            return retDelay
        # self.loadingKW, self.loadingPU = self.calculateFuelConsumption()
        # if self.loadingKW > self.compressorkW:
        #     ret = self.getMinChangeTimeLiquids(self.inletFluidFlows)
        if currentTime == 0:
            # if ts == 0, we have already calculated and fuzzed our initial state times
            return self.stateTimes[self.initialState]
        # otherwise, if we are back in the operating state, repick the operating time & calculate state times
        if cs == 'OPERATING':
            self.calcStateTimes(currentTime)
        delay = self.stateTimes[cs]
        if self.inletFluidFlows:
            delay = self.currentStateDelay = self.stateTimes[cs]
            updatedDelay = self.getUpdatedDelay(delay, currentTime)
            self.currentStateDelay -= updatedDelay
        else:
            updatedDelay = delay
        return updatedDelay

    def getUpdatedDelay(self, delay, currentTime):
        newDelay = abs(self.getMinChangeTimeVapor(self.inletFluidFlows) - currentTime)
        ret = min(delay, newDelay)
        return ret

    def checkForCorrectDriver(self, simdm):
        # if self.driverType not in filename.name:
        curDataPath = Path(simdm.config['emitterProfileDir']) / 'Common' / 'EnginesfuelConsumpEq'
        msg = (f'Driver Type {self.driverType} does not have a close approximation of load equations'
               f' in {curDataPath}, using rated power instead. unitID = {self.unitID}')
        logging.warning(msg)
        return None

    def getLoadConditions(self, loadCondition):
        if loadCondition is not None:
            simdm = sdm.SimDataManager.getSimDataManager()
            dataBasePath = Path(au.expandFilename(simdm.config['emitterProfileDir'], simdm.config, readonly=True)) / f'{loadCondition}'
            if self.driverType not in dataBasePath.name:
                return self.checkForCorrectDriver(simdm)
            metadata, dist = dp.readRawDistributionFile(dataBasePath)
        else:
            listOfPowers = []
            ratedPowerInHP = u.kwToHp(self.compressorkW)
            simdm = sdm.SimDataManager.getSimDataManager()
            curDataPath = Path(simdm.config['emitterProfileDir']) / 'Common' / 'EnginesfuelConsumpEq'
            for i in curDataPath.iterdir():
                for j in i.iterdir():
                    powerFromCD = int(str(j).split('_')[1].replace('HP', ''))
                    subt = abs(ratedPowerInHP - powerFromCD)
                    listOfPowers.append([j, subt, j.parent.name])
            dfPowers = pd.DataFrame(listOfPowers)
            dfPowers.columns = ['file', 'deltaP', 'dType']
            groupByType = dfPowers.groupby('dType')
            if self.driverType not in dfPowers['dType'].unique():
                return self.checkForCorrectDriver(simdm)
            else:
                correctFiles = dfPowers[dfPowers['dType'] == self.driverType]
                correctFile = correctFiles.loc[correctFiles['deltaP'].idxmin()]['file']
                metadata, dist = dp.readRawDistributionFile(correctFile)
                msg = (f'No compressor load equation in the study sheet.\n '
                       f'Using the closest load equation to the rated power {self.compressorkW}kW\n '
                       f'from {correctFile}, unitID:{self.unitID}')
                logging.info(msg)
            i = 10
        # eq = self.equationComp(dist)
        # noLoadDist = d.Histogram(dist).fromPandas(dist, probCol='NoLoad')
        # fullLoadDist = d.Histogram(dist).fromPandas(dist, probCol='FullLoad')
        return dist

    def calcConsumpFromEq(self, totalPower):
        fracRatedPower = totalPower / self.compressorkW
        # fracRatedPowerHP = u.kwToHp(fracRatedPower)
        fc = self.loadEquations['a0'] + self.loadEquations['a1']*fracRatedPower
        # fuelConsump = u.hpToKW(float(fc))
        return float(fc)

    def checkForOverload(self, totalPower):
        pass

    def calcCompressorLoad(self, vaporFlows, pu):
        simdm = sdm.SimDataManager.getSimDataManager()
        totalPower = 0
        try:
            for flow in vaporFlows:
                deltaH = flow.gc.getDeltaH(self.inletStagesDeltaH)
                if pd.isna(deltaH):
                    msg = f"flow {flow.gc.fluidFlowID} does not have a deltaH. Check the gc file," \
                          f"{flow.gc.fluidFlowGCFilename}, study sheet: {simdm.config['studyFilename']}, unitID: {self.unitID}"
                    raise NotImplementedError(msg)
                totalPower += deltaH * flow.driverRate / self.compressorEfficiency  # kJ/scf * scf/s = kW
            fuelConsump = self.calcConsumpFromEq(totalPower)
        except:
            msg = f"flow {flow.gc.fluidFlowID} does not have a deltaH. Check the gc file," \
                          f"{flow.gc.fluidFlowGCFilename}, study sheet: {simdm.config['studyFilename']}, unitID: {self.unitID}"
            raise NotImplementedError(msg)
            # totalPower = self.compressorkW*pu
        return fuelConsump, totalPower

    def calculateFuelConsumption(self):
        # if no FFs, original loading calc,
        # else, calc from FFs
        #       if deltaH exists, calc power += deltaH * mdot / compEff
        #           calc fuel consump from equation using power
        #       else, power = kW*pu
        #
        fuelConsump = 0
        if self.driverType == 'Electric motor':
            return 0, 0, 0  # Electric motor consumes no fuel and produces no combustion emissions
        loading_pu = self.loadingDistribution.pick()
        if self.loadEquations is None:
            loading_kW, loading_pu = self.calcLoadingNoFF(loading_pu)
            return loading_kW, loading_pu, 0
        if self.inletFluidFlows:
            if not self.inletFluidFlows['Vapor']:
                msg = f'No inlet vapors found for compressor {self.unitID}, switching to rated power'
                logging.warning(msg)
                loading_kW, loading_pu = self.calcLoadingNoFF(loading_pu)
            else:
                fuelConsump, compressorLoad = self.calcCompressorLoad(self.inletFluidFlows['Vapor'], loading_pu)
                loading_kW = compressorLoad
        else:
            loading_kW, loading_pu = self.calcLoadingNoFF(loading_pu)
            if loading_kW > self.compressorkW:
                msg = (f'Loading distribution > max rated load. The distribution is wrong, please check average '
                       f'and standard loading parameters'
                       f' in compressors tab, unitID: {self.unitID}, will continue using rated load')
                logging.warning(msg)
                loading_kW = self.compressorkW
        return loading_kW, loading_pu, fuelConsump

    def calcLoadingNoFF(self, loading_pu):
        if loading_pu < 0:
            loading_pu = 0
        loading_kW = loading_pu * self.compressorkW
        return loading_kW, loading_pu

    # def calcOverloadFlow(self, flow):
    #

    def linkInletFlow(self, outletME, flow):
        if flow.name != self.fluidName:
            return

        self.addInletFluidFlow(flow)
        excludeSecIDs = ['condensate_flash', 'water_flash', 'stuck_dump_valve_vapor']
        if flow.secondaryID not in excludeSecIDs:
            self.inletStagesDeltaH.append(flow.gc.fluidFlowID.split('.')[-1].split('-')[0])
        # todo: figure out how to declare these flows in the model formulation sheets
        self.addOutletFluidFlow(StateDependentFluidFlow(flow, secondaryID=f"normal_flow", majorEquipment=self, activeState="OPERATING"))
        self.addOutletFluidFlow(StateDependentFluidFlow(flow, secondaryID=f"flare_flow", majorEquipment=self, activeState="NOT_OPERATING_DEPRESSURIZED"))
        self.addOutletFluidFlow(StateDependentFluidFlow(flow, secondaryID=f"overload_flow", majorEquipment=self, activeState="OVERLOAD"))

    def ffUpdateHook(self, currentTime, stateData, **kwargs):
        self.logFluidFlowChanges(self.key, currentTime)

    def calculateInitialStateTimes(self):
        self.calcStateTimes()
        self.chooseAndFuzzInitialState()

    def getInitialState(self, currentTime=None, currentStateData=None, currentStateInfo=None):
        # cs = currentStateInfo.stateName
        self.loadingKW, self.loadingPU, fuelConsump = self.calculateFuelConsumption()
        if fuelConsump > self.compressorkW:
            return 'OVERLOAD'
        else:
            return None

    def initialStateTimes(self):
        self.calcStateTimes()
        isOverload = self.getInitialState(0, None, None)
        if isOverload:
            ret = {isOverload: self.getOverloadDelay(0)}
        else:
            ret = self.stateTimes
        return ret

    def chooseAndFuzzInitialState(self):
        initialStateChooser = EmpiricalDistChooser(self.timeRatios)
        self.initialState = initialStateChooser.randomChoice()
        self.initialStateTime = self.stateTimes[self.initialState]
        self.fuzzedInitialTime = int(self.initialStateTime * random.uniform(0, 1))
        # self.fuzzedInitialTime = self.initialStateTime
        self.stateTimes[self.initialState] = self.fuzzedInitialTime

    # def getStateMachine(self):
    #     return self.stateMachine, self.initialState, self.initialStateTime

    def calcStateTimes(self, currentTS=None):
        # todo: log OPTimeHours every change
        OPTimeSecs = self.opCycleTimeDistSecs.pick()
        for state in self.STATES_WITH_CALCULATED_TIMES:
            self.stateTimes[state] = int(OPTimeSecs * self.timeRatios[state])
        self.stateTimes['STARTING'] = self.startVentDurationSecs
        self.stateTimes['BLOWDOWN'] = self.blowdownVentDurationSecs
        if currentTS is not None:
            self.eventLogger.logRawEvent(currentTS, self.key, 'COMPRESSOR-STATE-TIMES',
                                         **{'OPTimeSecs': OPTimeSecs, **self.stateTimes})

    def logStateTimingObs(self, ts, state, op, delay=None, relatedEvent=None, initiator=None):
        self.stateInfo = {'currentState': state, 'timeUpdated': ts, 'duration': delay, 'nextTS': ts+delay}

    def logStateTiming(self, currentTime, stateData, stateInfo, delay, **kwargs):
        self.stateInfo = {'currentState': stateInfo.stateName, 'timeUpdated': currentTime, 'duration': delay, 'nextTS': currentTime+delay}

    def getStateInfo(self):
        return self.stateInfo

    def link2(self, linkedUpstreamME):
        self.linkedUpstreamEquipment.append(linkedUpstreamME)

# todo: coalesce OGCILeakContainer and OGCIBattery
# todo: coalesce OGCIBatteryLeak, OGCISpecificComponentLeak, and OGCILeak

class SpecificLeaksProduction(mc.FactorManager, mcl.ComponentLeaks):  # Fugitive Leaks, Min and Max MTTR inputs, calc MTBF min and max, Uniform dist
    def __init__(self,
                 MTTRMinDays=None,
                 MTTRMaxDays=None,
                 pLeak=None,
                 overriddenBy=None,
                 **kwargs):
        MTTRMinHour = u.daysToHours(MTTRMinDays)  # days to hours
        MTTRMaxHour = u.daysToHours(MTTRMaxDays)
        MTBFMinHour = (MTTRMinHour * (1 - pLeak)) / pLeak  # calc mtbf
        MTBFMaxHour = (MTTRMaxHour * (1 - pLeak)) / pLeak
        MTTRHourDist = d.Uniform({'min': MTTRMinHour, 'max': MTTRMaxHour})  # uniform distribution for mttr max and min
        MTBFHourDist = d.Uniform({'min': MTBFMinHour, 'max': MTBFMaxHour})
        newArgs = {**kwargs, 'pLeak': pLeak, 'MTBF': MTBFHourDist.pick(), 'MTTR': MTTRHourDist.pick()}
        super().__init__(**newArgs)  # do this to pick up default value for componentCount
        self.MTTRMinDays = MTTRMinDays
        self.MTTRMaxDays = MTTRMaxDays
        self.overriddenBy = overriddenBy #MM edit

    def activityPick(self, simdm, mcRunNum=-1):
        return 1, None, None
    
    def pickFromMTTR(self, num):
        return num

    def overrideLeaks(self, leakList, simdm,
                      mcRunNum):  # MM edit: new function to adjust current leaks based on any previous emitters that should override them
        if self.overriddenBy == None:
            return leakList
        elif self.overriddenBy == "Compressor Rod Packing Large Emitter":  # currently singling out specific cases to correct
            equipment_table = simdm.getEquipmentTable()
            equip_df = equipment_table.getMetadata()
            large_emitter_df = equip_df[(equip_df['equipmentType'] == 'SpecificLeaksProduction') & (
                        equip_df['modelReadableName'] == "Compressor Rod Packing Large Emitter") & (
                                                    equip_df['mcRunNum'] == mcRunNum)]
            override_intervals = []
            for row in large_emitter_df.itertuples(index=False, name=None):
                if row[2] == self.unitID:
                    large_emitter = equipment_table.elementLookup(facilityID=row[1], unitID=row[2], emitterID=row[3],
                                                                  mcRunNum=mcRunNum)
                    if large_emitter == None:
                        continue
                    else:
                        override_intervals.append((large_emitter.startTime, large_emitter.endTime))

            # loop through override_intervals, removing any overlap from leaks in leakList
            for individual_interval in override_intervals:
                leakcounter = 0
                updated_leakList = []
                for leak in leakList:
                    if leak['endTime'] <= individual_interval[0]:
                        leakcounter += 1
                        same_leak = {'componentLeakInstance': leakcounter, 'startTime': leak['startTime'],
                                     'endTime': leak['endTime']}
                        updated_leakList.append(same_leak)
                    elif leak['endTime'] <= individual_interval[1]:
                        if individual_interval[0] <= leak['startTime']:
                            continue  # leak removed
                        else:
                            leakcounter += 1
                            reduced_leak = {'componentLeakInstance': leakcounter, 'startTime': leak['startTime'],
                                            'endTime': individual_interval[0]}
                            updated_leakList.append(reduced_leak)
                    else:  # individual_interval[1] < leak['endTime']
                        if leak['startTime'] < individual_interval[
                            0]:  # leak splits into two, before and after large emitter
                            leakcounter += 1
                            reduced_leak1 = {'componentLeakInstance': leakcounter, 'startTime': leak['startTime'],
                                             'endTime': individual_interval[0]}
                            updated_leakList.append(reduced_leak1)
                            leakcounter += 1
                            reduced_leak2 = {'componentLeakInstance': leakcounter, 'startTime': individual_interval[1],
                                             'endTime': leak['endTime']}
                            updated_leakList.append(reduced_leak2)
                        elif leak['startTime'] < individual_interval[1]:
                            leakcounter += 1
                            reduced_leak = {'componentLeakInstance': leakcounter, 'startTime': individual_interval[1],
                                            'endTime': leak['endTime']}
                            updated_leakList.append(reduced_leak)
                        else: # individual_interval[1] <= leak['startTime']
                            leakcounter += 1
                            same_leak = {'componentLeakInstance': leakcounter, 'startTime': leak['startTime'],
                                         'endTime': leak['endTime']}
                            updated_leakList.append(same_leak)
                leakList = updated_leakList
            return leakList
        else:
            return leakList

class OGCILink(mc.LinkService):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

class OGCIPneumaticEmitter(mc.Emitter, mc.ActivityDistributionEnabled, mc.EmissionDistributionEnabled,
                           mc.DESEnabled, mc.StateChangeNotificationDestination):

    PNEUMATIC_SERIAL_NUMBER = 0
    DEFAULT_PNEUMATIC_FMT = "pneumatic_{serialNo}"

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['PNEUMATIC_SERIAL_NUMBER', 'DEFAULT_PNEUMATIC_FMT', 'gcKey']

    def __init__(self,
                 activityInstance=None,
                 instanceFormat=None,
                 statesActive=None,
                 productionGCName=None,
                 **kwargs):
        super().__init__(**kwargs)
        # todo: this is duplicated in StateBasedEmitter.  Refactor
        simdm = sdm.SimDataManager.getSimDataManager()
        self.instanceFormat = instanceFormat
        self.statesActive = statesActive


    def initializeDES(self, simdm, env, eh):
        super().initializeDES(simdm, env, eh)
        self.majorEquipment = simdm.getEquipmentTable().elementLookup(self.facilityID, self.unitID, None, self.mcRunNum)
        self.majorEquipment.registerForStateChangeNotification(self, self.stateChange)
        self.activeStatesList = self.statesActive.split(',')
        self.emissionDriverKey = simdm.getDriver4EC(self.emissionDriver)
        self.tsKey, _ = simdm.getTimeseries(self.emissionDriverKey)

    def instantiateFromTemplate(self, simdm, **kwargs):
        # todo: emitterID is being set by ActivityFactorEnabled, which is getting passed on to the instance, which is causing *all* emitterIDs based on that instance to be the same.  Fix this.
        if self.instanceFormat:
            fmtDict = {**eqt.JsonEquipmentTable._instanceDict(self.key), 'serialNo': self.PNEUMATIC_SERIAL_NUMBER}
            newEmitterID = self.instanceFormat.format(**fmtDict)
        else:
            newEmitterID = self.DEFAULT_PNEUMATIC_FMT.format(serialNo=self.PNEUMATIC_SERIAL_NUMBER)
        self.PNEUMATIC_SERIAL_NUMBER += 1

        newInst = super().instantiateFromTemplate(simdm, **{**kwargs, 'emitterID': newEmitterID})
        return newInst

    def stateChange(self, currentTime, state, op, delay=None, relatedEvent=None, initiator=None):
        if op != 'START':
            return
        if state not in self.statesActive:
            return

        self.eventLogger.logEmission(currentTime, delay, self.key, driverTSKey=self.tsKey, GCKey=self.gcKey)

class PneumaticEmitterProduction(mc.FactorManager, mc.ActivityDistributionEnabled, mc.EmissionManager):

    PNEUMATIC_SERIAL_NUMBER = 0
    DEFAULT_PNEUMATIC_FMT = "pneumatic_{serialNo}"

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['PNEUMATIC_SERIAL_NUMBER', 'DEFAULT_PNEUMATIC_FMT', 'gcKey']

    def __init__(self,
                 gasComposition=None,
                 emissionDriverUnits=None,
                 secondaryID=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.gasComposition = gasComposition
        self.emissionDriverUnits = emissionDriverUnits or 'scf'
        self.secondaryID = secondaryID

    # todo: this was cut & pasted from StateBasedEmitterProduction.  We should refactor -- perhaps into a mixin??
    def initializeFluidFlow(self, simdm):
        facility = simdm.getEquipmentTable().elementLookup(self.facilityID, None, None, self.mcRunNum)
        tmpGC = gc.FluidFlowGC.factory(fluidFlowGCFilename=facility.productionGCFilename,
                                       flow='Vapor',
                                       fluidFlowID=self.gasComposition,
                                       gcUnits=self.emissionDriverUnits
                                       )
        emissionDriverPath = Path(au.expandFilename(simdm.config['emitterProfileDir'], simdm.config)) / self.emissionDriver
        self.fluidFlow = ff.EmpiricalFluidFlow('Vapor', emissionDriverPath, tmpGC, units=self.emissionDriverUnits, secondaryID=self.secondaryID)


def genGCName(stageID, gc, process=None):
    if process is None:
        ret = f"{stageID}"
    else:
        ret = f"{stageID}-{process}"
    return ret

def genGCFlashName(gc):
    return f"{gc}-Flash"

def isNumber(inVal):
    try:
        return float(inVal)
    except:
        return None

def getFlashFrac(gasFracDistName, simdm):
    if isinstance(gasFracDistName, int) or isinstance(gasFracDistName, float):  # put to self if it is int
        if gasFracDistName > 1:
            msg = 'Gas Fraction is > 1. Please check "Fraction of Flash Released" column in Separators'
            raise NotImplementedError(msg)
        return d.Constant(gasFracDistName)
    elif isinstance(gasFracDistName, str):  # if str
        adVal = isNumber(gasFracDistName)
        if adVal is not None:  # check if it can be converted to float
            if adVal > 1:
                msg = 'Gas Fraction is > 1. Please check "Fraction of Flash Released" column in Separators'
                raise NotImplementedError(msg)
            return d.Constant(adVal)
        else:  # distribution if it is dist file
            dataBasePath = Path(au.expandFilename(simdm.config['emitterProfileDir'], simdm.config, readonly=True))
            dataPath = dataBasePath / gasFracDistName
            dist = None if gasFracDistName is None else dp.DistributionProfile.readFile(dataPath)
            return dist

# class Container
#  - add contained, figures out which contained class, delegating jobs to contained
#  - add FF
#  - combos of states + ffs to outside
#
# class Contained(mc.MajorEquipment, mc.StateEnabledVolume, Container):
#   individual state management
#   manage individual ffs


# this is multi-phase separator
class MEETContinuousSeparator2(mc.MajorEquipment, mc.StateChangeInitiator, ff.Volume):
    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['condensateGCTag', 'waterGCTag', 'stateMachine', 'trackingTime', 'delayCheck',
                                         'stuckDumpValveDurDist', 'currentGasFraction', 'trackingTimeSubtract',
                                         'operatingDurationMinSec', 'operatingDurationMaxSec', 'operatingDurationDist',
                                         'stuckDumpValveDurMinSec', 'stuckDumpValveDurMaxSec', 'gasFractionDist',
                                         'stateChangeTime', 'stuckDumpValveMTBFMinSec', 'stuckDumpValveMTBFMaxSec',
                                         'containedSeparators', 'prvSwitch', 'waterStuckDumpValveDurMinSec',
                                         'waterStuckDumpValveDurMaxSec', 'oilStuckDumpValveDurMinSec',
                                         'oilStuckDumpValveDurMaxSec', 'waterOperatingDurationDist',
                                         'oilOperatingDurationDist', 'waterStuckDumpValveDurDist',
                                         'oilStuckDumpValveDurDist', 'waterOperatingDurationMinSec',
                                         'waterOperatingDurationMaxSec', 'oilOperatingDurationMinSec',
                                         'oilOperatingDurationMaxSec']

    def __init__(self,
                 activityDistribution=None,
                 flowGCTag=None,
                 primaryWaterRatio=None,
                 productionGC=None,
                 waterFlowUnits=None,
                 vaporFlowUnits=None,
                 condensateFlowUnits=None,
                 eqComponentCount=None,
                 gasFractionDistFileName=None,
                 stuckDumpValvepLeak=None,
                 multipleInstances=None,
                 instanceFormat=None,
                 stuckDumpValveCC=None,
                 stuckDumpValvepLeakWater=None,
                 stuckDumpValvepLeakOil=None,
                 waterStuckDumpValveDurMinDays=None,
                 waterStuckDumpValveDurMaxDays=None,
                 oilStuckDumpValveDurMinDays=None,
                 oilStuckDumpValveDurMaxDays=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.stuckDumpValveCC = stuckDumpValveCC
        self.multipleInstances = multipleInstances
        self.instanceFormat = instanceFormat
        self.activityDistribution = activityDistribution
        self.flowGCTag = flowGCTag
        self.condensateGCTag = f"{self.flowGCTag}-Condensate"
        self.waterGCTag = f"{self.flowGCTag}-Water"
        self.primaryWaterRatio = primaryWaterRatio
        self.productionGC = productionGC
        self.eqComponentCount = eqComponentCount
        self.stuckDumpValvepLeak = 0 if stuckDumpValvepLeak is None else stuckDumpValvepLeak
        self.stateChangeTime = 0
        self.gasFractionDistFileName = gasFractionDistFileName
        simdm = sdm.SimDataManager.getSimDataManager()
        self.gasFractionDist = getFlashFrac(gasFractionDistFileName, simdm)

        self.stuckDumpValvepLeakWater = stuckDumpValvepLeakWater
        self.waterStuckDumpValveDurMinDays = 0 if waterStuckDumpValveDurMinDays is None else waterStuckDumpValveDurMinDays
        self.waterStuckDumpValveDurMinSec = int(u.daysToSecs(self.waterStuckDumpValveDurMinDays))
        self.waterStuckDumpValveDurMaxDays = 0 if waterStuckDumpValveDurMaxDays is None else waterStuckDumpValveDurMaxDays
        self.waterStuckDumpValveDurMaxSec = int(u.daysToSecs(self.waterStuckDumpValveDurMaxDays))
        self.waterStuckDumpValveDurDist = d.Uniform({'min': self.waterStuckDumpValveDurMinSec,
                                                     'max': self.waterStuckDumpValveDurMaxSec})

        self.stuckDumpValvepLeakOil = stuckDumpValvepLeakOil
        self.oilStuckDumpValveDurMinDays = 0 if oilStuckDumpValveDurMinDays is None else oilStuckDumpValveDurMinDays
        self.oilStuckDumpValveDurMinSec = int(u.daysToSecs(self.oilStuckDumpValveDurMinDays))
        self.oilStuckDumpValveDurMaxDays = 0 if oilStuckDumpValveDurMaxDays is None else oilStuckDumpValveDurMaxDays
        self.oilStuckDumpValveDurMaxSec = int(u.daysToSecs(self.oilStuckDumpValveDurMaxDays))
        self.oilStuckDumpValveDurDist = d.Uniform({'min': self.oilStuckDumpValveDurMinSec,
                                                   'max': self.oilStuckDumpValveDurMaxSec})

        self.waterOperatingDurationMinSec = int((self.waterStuckDumpValveDurMinSec * (1 - self.stuckDumpValvepLeakWater)) / self.stuckDumpValvepLeakWater)  # MTBF formula
        self.waterOperatingDurationMaxSec = int((self.waterStuckDumpValveDurMaxSec * (1 - self.stuckDumpValvepLeakWater)) / self.stuckDumpValvepLeakWater)  # if separator dump valve is not stuck, it is operating
        self.waterOperatingDurationDist = d.Uniform({'min': self.waterOperatingDurationMinSec,  # this defn will change if we have another state
                                                     'max': self.waterOperatingDurationMaxSec})

        self.oilOperatingDurationMinSec = int((self.oilStuckDumpValveDurMinSec * (1 - self.stuckDumpValvepLeakOil)) / self.stuckDumpValvepLeakOil)  # MTBF formula
        self.oilOperatingDurationMaxSec = int((self.oilStuckDumpValveDurMaxSec * (1 - self.stuckDumpValvepLeakOil)) / self.stuckDumpValvepLeakOil)  # if separator dump valve is not stuck, it is operating
        self.oilOperatingDurationDist = d.Uniform({'min': self.oilOperatingDurationMinSec,  # this defn will change if we have another state
                                                   'max': self.oilOperatingDurationMaxSec})

        self.currentGasFraction = 0
        self.trackingTimeSubtract = 0
        self.delayCheck = 0
        self.condensateFlowUnits = condensateFlowUnits or 'bbl'
        self.waterFlowUnits = waterFlowUnits or 'bbl'
        self.vaporFlowUnits = vaporFlowUnits or 'scf'
        self.trackingTime = 0
        self.prvSwitch = 0
        self.stateMachine = {
            'OPERATING': {'stateDuration': self.getTimeForState2, 'nextState': self.getNextState},
            'STUCK_DUMP_VALVE': {'stateDuration': self.getTimeForState2, 'nextState': self.getNextState},
            # 'OVERPRESSURE': {'stateDuration': self.getTimeForState2, 'nextState': self.getNextState}
        }
        self.initializeContainedClasses()
        i = 10
        # initialize contained classes, this class has contained instances
        # how to instantiate contained classes?

    def updateDelay(self, ogDelay, delayCheck, stateDur, trackingTime):
        if delayCheck > stateDur:
            ret = stateDur - trackingTime
        else:
            ret = ogDelay
        return ret

    def getCombinedStateTimes(self, currentStateData, currentStateInfo, currentTime):
        delayList = []
        for key, val in self.containedSeparators.items():
            if self.flowGCTag == 'Stage1':   # this is done to make sure we do not count Vapor inlets in stage1
                if isinstance(val, LiquidContained):
                    delayList.append(val.getTimeForState(currentStateData, currentStateInfo, currentTime))
            else:
                delayList.append(val.getTimeForState(currentStateData, currentStateInfo, currentTime))
        return delayList

    def updateVaporGasFrac(self, gasFrac):
        for key, val in self.containedSeparators.items():
            if key == 'Vapor':
                val.currentGasFraction = gasFrac

    def getTimeForState2(self, currentStateData, currentStateInfo, currentTime):
        cs = currentStateInfo.stateName
        delayList = self.getCombinedStateTimes(currentStateData, currentStateInfo, currentTime)
        delay = min(delayList)
        if cs == 'STUCK_DUMP_VALVE':
            self.updateVaporGasFrac(self.gasFractionDist.pick())
        self.logSubStateChanges(currentTime, delay)
        for key, val in self.containedSeparators.items():
            val.updateTrackingTime(delay)
            val.updateChangeTime(delay + currentTime, delay)
        return delay

    def logSubStateChanges(self, currentTime, delay):
        for key, val in self.containedSeparators.items():
            val.logSubStates(currentTime, self.key, delay, val.currentState, val.currentGasFraction)
            # val.logFluidFlowChanges(self.key, currentTime, None)

    def getTimeForState(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        # 1. figure out main state
        # 2. go through all contained classes to calculate sub-state
        cs = currentStateInfo.stateName
        if cs == 'OPERATING':
            # delay = min(self.getMinChangeTimeLiquids(self.inletFluidFlows) - currentTime,
            #             self.opDur)
            delay = min((min(self.containedSeparators['Condensate'].getInletChangeTimeLiquids(),
                             self.containedSeparators['Water'].getInletChangeTimeLiquids()) - currentTime),
                        self.opDur)
            # self.trackingTime += delay
            self.delayCheck = self.trackingTime + delay
            delay = self.updateDelay(delay, self.delayCheck, self.opDur, self.trackingTime)
            self.trackingTime += delay
            self.currentGasFraction = 0  # all gas goes to gas sales when in op state
            self.prvSwitch = 0
        else:   # overpressure state
            delay = min(self.containedSeparators['Condensate'].getInletChangeTimeLiquids(),
                        self.containedSeparators['Water'].getInletChangeTimeLiquids()) - currentTime
            self.delayCheck = self.trackingTime + delay
            delay = self.updateDelay(delay, self.delayCheck, delay, self.trackingTime)
            self.trackingTime += delay
            self.currentGasFraction = 0
            self.prvSwitch = 1
        # self.updateFFChangeTime(delay + currentTime, delay)
        self.containedSeparators['Condensate'].updateChangeTime(currentTime+delay, delay)
        return delay

    def getNextState(self, currentStateData, currentStateInfo, currentTime):
        nextState = []
        for key, val in self.containedSeparators.items():
            nextState.append(val.getNextState(currentStateData=currentStateData, currentStateInfo=currentStateInfo, currentTime=currentTime))
        retState = self.getMainStateFromContainedStates(nextState)
        return retState

    def getNextStateOP(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        cs = currentStateInfo.stateName
        if self.trackingTime >= self.opDur:
            self.trackingTimeSubtract = self.trackingTime
            self.trackingTime = 0
            self.sdvDur = int(self.stuckDumpValveDurDist.pick())
            nextState = 'OVERPRESSURE'
        else:
            nextState = 'OPERATING'
        return nextState

    def getNextStateOverpressure(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        cs = currentStateInfo.stateName
        if self.trackingTime >= self.sdvDur:
            self.trackingTimeSubtract = self.trackingTime
            self.trackingTime = 0
            self.opDur = int(self.operatingDurationDist.pick())
            nextState = 'OPERATING'
        else:
            nextState = 'OVERPRESSURE'
        return nextState

    def initialStateUpdate(self, stateName, stateDuration, currentTime):
        # for key, val in self.containedSeparators.items():
        #     val.allocateFF(self.inletFluidFlows, self.outletFluidFlows)

        self.currentGasFraction = 0
        try:
            # delay = random.randrange(1, min(self.getMinChangeTimeLiquids(self.inletFluidFlows),
            #                                 self.getMinChangeTimeLiquids(self.outletFluidFlows)))
            delay = random.randrange(1, min(self.containedSeparators['Condensate'].getChangeTime(),
                                            self.containedSeparators['Water'].getChangeTime()))
        except:
            delay = 1
        # self.opDur = int(self.operatingDurationDist.pick())
        # self.sdvDur = int(self.stuckDumpValveDurDist.pick())
        # if stateName == 'OPERATING':
        #     delay2 = min(self.opDur, delay)
        # elif stateName == 'STUCK_DUMP_VALVE':
        #     delay2 = min(self.sdvDur, delay)
        # else:
        #     raise ValueError(f'Cannot set initial state in {self}, unitID {self.unitID}')
        containedStates, delay3 = self.initializeInitialStatesContinedSeparators(currentTime)
        retState = self.getMainStateFromContainedStates('-'.join(containedStates))
        newDelay = min(delay, delay3)
        self.updateChangeTimeContained(currentTime+newDelay, currentTime)
        ret = sm.StateInfo(retState, newDelay, newDelay)
        self.trackingTime += newDelay
        self.updateTrackingTimeInContained(newDelay)
        return ret

    def updateChangeTimeContained(self, changeTimeAbs, changeTimeDelay):
        for key, val in self.containedSeparators.items():
            val.updateChangeTime(changeTimeAbs, changeTimeDelay)

    def updateTrackingTimeInContained(self, delay):
        for key, val in self.containedSeparators.items():
            val.updateTrackingTime(delay)

    def getMainStateFromContainedStates(self, containedStates):
        retState = 'OPERATING' if 'STUCK_DUMP_VALVE' not in containedStates else 'STUCK_DUMP_VALVE'
        return retState

    def initializeInitialStatesContinedSeparators(self, currentTime):
        # iterate through items in contained separators
        subState = []
        subDelay = []
        for key, val in self.containedSeparators.items():
            ss, sd = val.initializeState(currentTime)
            subState.append(ss)
            subDelay.append(sd)
        # retState = self.getMainStateFromContainedStates()
        newDelay = min(subDelay)
        return subState, newDelay

    def initialStateTimes(self, **kwargs):
        i = 10
        for key, val in self.containedSeparators.items():
            if isinstance(val, LiquidContained):
                if not val.inletFluidFlows:
                    pass
        return {'OPERATING': 5}

    def initializeContainedClasses(self):
        self.containedSeparators = {'Condensate': LiquidContained(fluidName='Condensate',
                                                                  flowGCTag=self.flowGCTag,
                                                                  primaryWaterRatio=None,
                                                                  currentGasFraction=self.currentGasFraction,
                                                                  opDurDist=self.oilOperatingDurationDist,
                                                                  sdvDurDist=self.oilStuckDumpValveDurDist,
                                                                  gasFractionDist=self.gasFractionDist,
                                                                  unitID=self.unitID,
                                                                  facilityID=self.facilityID
                                                                  ),
                                    'Water': LiquidContained(fluidName='Water',
                                                             flowGCTag=self.flowGCTag,
                                                             primaryWaterRatio=self.primaryWaterRatio,
                                                             currentGasFraction=self.currentGasFraction,
                                                             opDurDist=self.waterOperatingDurationDist,
                                                             sdvDurDist=self.waterStuckDumpValveDurDist,
                                                             gasFractionDist=self.gasFractionDist,
                                                             unitID=self.unitID,
                                                             facilityID=self.facilityID
                                                             ),
                                    'Vapor': VaporContained(currentGasFraction=self.currentGasFraction,
                                                            fluidName='Vapor',
                                                            opDurDist=None,
                                                            sdvDurDist=None,
                                                            unitID=self.unitID,
                                                            facilityID=self.facilityID
                                                            )}

    def linkInletFlow(self, outletME, flow):
        # delegate flows to contained classes
        self.addInletFluidFlow(flow)
        flashes, liquidFlows, vaporFlows = self.containedSeparators[flow.name].linkFlowFromContainer(outletME, flow, self)
        if liquidFlows:
            for liqFlow in liquidFlows:
                self.addOutletFluidFlow(liqFlow)
        if flashes:
            for flash in flashes:
                flashFlow = self.containedSeparators['Vapor'].addOutletFlowsFromLiquidFlashes(flash)
                self.addOutletFluidFlow(flashFlow)
        if vaporFlows:
            for vaporFlow in vaporFlows:
                self.addOutletFluidFlow(vaporFlow)
        # if flow.name in ['Condensate', 'Water']:
        #     for vapFlow in self.containedSeparators[flow.name].vaporOutletFlows:
        #         self.containedSeparators['Vapor'].addOutletFlowsFromLiquidFlashes(vapFlow)
        #     self.containedSeparators[flow.name].resetVaporOutletFlows()
        pass


# contained abstract class
class Contained(ff.Volume):
    def __init__(self,
                 unitID=None,
                 facilityID=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.trackingTime = 0
        self.unitID = unitID
        self.facilityID = facilityID
        self.mcRunNum = None
        i = 10

    def getTimeForState(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        pass

    def getNextStateOP(self, currentStateData=None, currentStateInfo=None, currentTime=None, cs=None):
        pass

    def linkFlowFromContainer(self, outletME, flow, ME):
        pass

    def getNextState(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        pass

    def getInletChangeTime(self):
        pass

    def getOutletChangeTime(self):
        pass

    def initializeState(self, currentTime):
        pass

    def updateChangeTime(self, changeTimeAbs, changeTimeDelay):
        self.updateFFChangeTime(changeTimeAbs, changeTimeDelay)

    def updateTrackingTime(self, delay):
        self.trackingTime += delay
        i = 20

    def resetTrackingTime(self):
        self.trackingTime = 0

    def updateDelay(self, ogDelay, delayCheck, stateDur, trackingTime):
        if delayCheck > stateDur:
            ret = stateDur - trackingTime
        else:
            ret = ogDelay
        return ret

    def getME(self):
        simdm = sdm.SimDataManager.getSimDataManager()
        me = simdm.getEquipmentTable().elementLookup(self.facilityID, self.unitID, None, simdm.mcRunNum)
        ret = me
        return ret

    def logSubStates(self, currentTime, key, stateDur, currentState, currentGasFraction, **kwargs):
        simdm = sdm.SimDataManager.getSimDataManager()
        return simdm.eventLogger.logSubStates(timestamp=currentTime,
                                              deviceID=key,
                                              duration=stateDur,
                                              state=currentState,
                                              nextTS=currentTime+stateDur,
                                              currentGasFraction=currentGasFraction,
                                              **kwargs)

    # def allocateFF(self, inletFlows, outletFlows):
    #     pass


# define substate machines
# water and condensate as same class - same behavior
class LiquidContained(Contained):
    def __init__(self,
                 fluidName,
                 flowGCTag,
                 primaryWaterRatio,
                 currentGasFraction,
                 opDurDist,
                 sdvDurDist,
                 gasFractionDist,
                 **kwargs
                 ):
        super().__init__(**kwargs)
        self.flowGCTag = flowGCTag
        self.primaryWaterRatio = primaryWaterRatio
        self.currentGasFraction = currentGasFraction
        self.fluidName = fluidName.upper()
        self.gasFractionDist = gasFractionDist

        # self.sdvDurMin = 86400
        # self.sdvDurMax = 259200
        self.sdvDurDist = sdvDurDist

        # self.opDurMin = 86400
        # self.opDurMax = 259200
        self.opDurDist = opDurDist

        self.trackingTime = 0
        self.currentStateDur = 0
        self.currentState = None
        self.nextState = None
        self.vaporOutletFlows = []

        # a = self.getAllContainedSepatarators()

        self.subStateMachine = {
            f'{self.fluidName}_OPERATING': {'stateDuration': self.getTimeForState, 'nextState': self.getNextStateOP},
            f'{self.fluidName}_STUCK_DUMP_VALVE': {'stateDuration': self.getTimeForState, 'nextState': self.getNextStateSDV}
        }

    def getTimeForState(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        cs = currentStateInfo.stateName
        if self.inletFluidFlows:
            delay = self.getInletChangeTime() - currentTime
        else:
            delay = u.getSimDuration()
        # delay2 = self.updateDelay(delay, self.getStateDur(currentStateInfo.stateName), )
        if delay > self.getStateDur(cs):
            delay2 = self.getStateDur(cs)
        else:
            delay2 = delay
        self.updateGasFrac(cs)
        # self.logSubStates(currentTime, self.getME().key, delay2, self.currentState)
        return delay2

    def updateGasFrac(self, cs):
        if self.currentState == f'{self.fluidName}_OPERATING':
            self.currentGasFraction = 0
        else:
            self.currentGasFraction = self.gasFractionDist.pick()

    def getStateDur(self, currentState):
        if currentState == 'OPERATING':
            return self.opDur
        else:
            return self.sdvDur

    def getCurrentState(self, cs):
        cs = cs.split('-')
        for state in cs:
            if self.fluidName in state:
                cs = state
        return cs

    def getNextState(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        # cs = self.getCurrentState(currentStateInfo.stateName)
        cs = self.currentState
        if cs == f'{self.fluidName}_OPERATING':
            ns = self.getNextStateOP(currentStateData, currentStateInfo, currentTime, cs)
        elif cs == f'{self.fluidName}_STUCK_DUMP_VALVE':
            # self.currentGasFraction = self.gasFractionDist.pick()
            ns = self.getNextStateSDV(currentStateData, currentStateInfo, currentTime)
        else:
            msg = f'No state found, {self.fluidName, cs, self.currentState}'
            raise NotImplementedError(msg)
        self.currentState = f'{self.fluidName}_{ns}'
        return ns

    def getNextStateOP(self, currentStateData=None, currentStateInfo=None, currentTime=None, cs=None):
        cs = self.currentState
        if self.trackingTime >= self.opDur:
            self.trackingTimeSubtract = self.trackingTime
            self.resetTrackingTime()
            self.sdvDur = int(self.sdvDurDist.pick())
            nextState = 'STUCK_DUMP_VALVE'
        else:
            nextState = 'OPERATING'
        return nextState

    def getNextStateSDV(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        cs = currentStateInfo.stateName
        if self.trackingTime >= self.sdvDur:
            self.trackingTimeSubtract = self.trackingTime
            self.resetTrackingTime()
            self.opDur = int(self.opDurDist.pick())
            nextState = 'OPERATING'
        else:
            nextState = 'STUCK_DUMP_VALVE'
        return nextState

    def initializeState(self, currentTime):
        initialStateTimes = {f'{self.fluidName}_OPERATING': self.opDurDist.high,
                             f'{self.fluidName}_STUCK_DUMP_VALVE': self.sdvDurDist.high}
        randomState = UnscaledEmpiricalDistChooser(initialStateTimes).randomChoice()
        randomStateTime = random.randrange(1, initialStateTimes[randomState])
        self.currentStateDur = randomStateTime
        self.currentState = randomState
        if randomState == f'{self.fluidName}_STUCK_DUMP_VALVE':
            self.nextState = f'{self.fluidName}_OPERATING'
        else:
            self.nextState = f'{self.fluidName}_STUCK_DUMP_VALVE'
        self.currentState = randomState
        self.opDur = int(self.opDurDist.pick())
        self.sdvDur = int(self.sdvDurDist.pick())
        return randomState, randomStateTime

    def getChangeTime(self):
        ret = min(self.getMinChangeTimeLiquids(self.inletFluidFlows),
                  self.getMinChangeTimeLiquids(self.outletFluidFlows))
        return ret

    def getInletChangeTime(self):
        return self.getMinChangeTimeLiquids(self.inletFluidFlows)

    def getOutletChangeTime(self):
        return self.getMinChangeTimeLiquids(self.outletFluidFlows)

    def resetVaporOutletFlows(self):
        self.vaporOutletFlows = []

    def linkFlowFromContainer(self, outletME, flow, ME):
        inletsForVapor = []
        liquidFlows = []
        self.addInletFluidFlow(flow)
        if flow.name == 'Water':
            gcName = genGCName(self.flowGCTag, flow.gc, process='Water')
            tempFlowPrimaryWater = ff.DependentFlowWithIndependentChangeTime.factory(flow,
                                                                                      rateTransform=lambda
                                                                                          x: self.primaryWaterRatio * x,
                                                                                      gc=flow.gc.derive(gcName),
                                                                                      secondaryID='primary_water',
                                                                                      newUnits=flow.driverUnits,
                                                                                      # changeTimeAbsolute=self.stateChangeTime
                                                                                      )
            self.addOutletFluidFlow(tempFlowPrimaryWater)
            liquidFlows.append(tempFlowPrimaryWater)
            # self.containedSeparators[tempFlowPrimaryWater.name].linkFlowFromContainer(outletME, tempFlowPrimaryWater, self)

            # todo: this assumes that the 98% and 2% flash GCs are the same.  Is this true?
            tempFlowSecWater = ff.DependentFlowWithIndependentChangeTime.factory(flow,
                                                                                      rateTransform=lambda x: (1.0 - self.primaryWaterRatio) * x,
                                                                                      gc=flow.gc.derive(gcName),
                                                                                      secondaryID='secondary_water',
                                                                                      newUnits=flow.driverUnits,
                                                                                      # changeTimeAbsolute=self.stateChangeTime
                                                                                      )
            self.addOutletFluidFlow(tempFlowSecWater)
            liquidFlows.append(tempFlowSecWater)
            # self.containedSeparators[tempFlowSecWater.name].linkFlowFromContainer(outletME, tempFlowSecWater, self)

            water_flash_gc_name = genGCName(self.flowGCTag, flow.gc, process='Flash')
            tempWaterFlash = ff.DependentFlowWithIndependentChangeTime.factory(flow,
                                                                                      rateTransform=lambda x: self.currentGasFraction * x,
                                                                                      gc=flow.gc.derive(
                                                                                          water_flash_gc_name,
                                                                                          flow='Vapor'),
                                                                                      secondaryID='water_flash',
                                                                                      newName='Vapor',
                                                                                      newUnits='scf',
                                                                                      # changeTimeAbsolute=self.stateChangeTime
                                                                                      )
            inletsForVapor.append((tempWaterFlash))
            self.addOutletFluidFlow(tempWaterFlash)
            # self.addOutletFluidFlowToVapor(tempWaterFlash)
            # self.containedSeparators[tempWaterFlash.name].linkFlowFromContainer(outletME, tempWaterFlash, self)

            tempWaterGasSales = ff.DependentFlowWithIndependentChangeTime.factory(flow,
                                                                                      rateTransform=lambda x: (1 - self.currentGasFraction) * x,
                                                                                      gc=flow.gc.derive(
                                                                                          water_flash_gc_name,
                                                                                          flow='Vapor'),
                                                                                      secondaryID='water_gas_sales',
                                                                                      newName='Vapor',
                                                                                      newUnits='scf',
                                                                                      # changeTimeAbsolute=self.stateChangeTime
                                                                                      )
            inletsForVapor.append(tempWaterGasSales)
            self.addOutletFluidFlow(tempWaterGasSales)
            # self.addOutletFluidFlow(tempWaterGasSales)
            # self.containedSeparators[tempWaterGasSales.name].linkFlowFromContainer(outletME, tempWaterGasSales, self)

        elif flow.name == 'Condensate':
            gcName = genGCName(self.flowGCTag, flow.gc, process='Condensate')
            tempCondensate = ff.DependentFlowWithIndependentChangeTime.factory(flow,
                                                                                      secondaryID='condensate_flow',
                                                                                      newUnits=flow.driverUnits,
                                                                                      gc=flow.gc.derive(gcName),
                                                                                      # changeTimeAbsolute=self.stateChangeTime
                                                                                      )
            self.addOutletFluidFlow(tempCondensate)
            liquidFlows.append(tempCondensate)
            # self.containedSeparators[tempCondensate.name].linkFlowFromContainer(outletME, tempCondensate, self)

            oil_flash_gc_name = genGCName(self.flowGCTag, flow.gc, process='Flash')
            tempCondensateFlash = ff.DependentFlowWithIndependentChangeTime.factory(flow,
                                                                                      rateTransform=lambda x: self.currentGasFraction * x,
                                                                                      gc=flow.gc.derive(
                                                                                          oil_flash_gc_name,
                                                                                          flow='Vapor'),
                                                                                      newName='Vapor',
                                                                                      secondaryID='condensate_flash',
                                                                                      newUnits='scf',
                                                                                      # changeTimeAbsolute=self.stateChangeTime
                                                                                      )
            inletsForVapor.append(tempCondensateFlash)
            self.addOutletFluidFlow(tempCondensateFlash)
            # self.addOutletFluidFlow(tempCondensateFlash)
            # self.containedSeparators[tempCondensateFlash.name].linkFlowFromContainer(outletME, tempCondensateFlash, self)

            tempCondensateGasSales = ff.DependentFlowWithIndependentChangeTime.factory(flow,
                                                                                      rateTransform=lambda x: (1 - self.currentGasFraction) * x,
                                                                                      gc=flow.gc.derive(
                                                                                          oil_flash_gc_name,
                                                                                          flow='Vapor'),
                                                                                      newName='Vapor',
                                                                                      secondaryID='condensate_gas_sales',
                                                                                      newUnits='scf',
                                                                                      # changeTimeAbsolute=self.stateChangeTime
                                                                                      )
            inletsForVapor.append(tempCondensateGasSales)
            self.addOutletFluidFlow(tempCondensateGasSales)
            # self.addOutletFluidFlow(tempCondensateGasSales)
            # self.containedSeparators[tempCondensateGasSales.name].linkFlowFromContainer(outletME, tempCondensateGasSales, self)
        return inletsForVapor, liquidFlows, None

    # def allocateFF(self, inletFlows, outletFlows):
    #     for key, val in inletFlows.items():
    #         if val.name == 'Condensate':
    #             self.addInletFluidFlow(val)


class VaporContained(Contained):
    def __init__(self,
                 currentGasFraction=None,
                 fluidName='VAPOR',
                 opDurDist=None,
                 sdvDurDist=None,
                 **kwargs
                 ):
        super().__init__(**kwargs)
        self.fluidName = fluidName
        self.currentGasFraction = currentGasFraction

        # self.sdvDurMin = 86400
        # self.sdvDurMax = 259200
        self.sdvDurDist = sdvDurDist

        # self.opDurMin = 86400
        # self.opDurMax = 259200
        self.opDurDist = opDurDist

        self.trackingTime = 0
        self.currentStateDur = 0
        self.currentState = None
        self.nextState = None

        self.subStateMachine = {
            'VAPOR_OPERATING': {'stateDuration': self.getTimeForState, 'nextState': self.getNextStateOP}
        }

    def getTimeForState(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        # cs = self.currentState
        delay = self.getInletChangeTime()
        # self.trackingTime += delay
        # delayCheck = self.trackingTime + delay
        # delay2 = self.updateDelay(delay, delayCheck, self.getStateDur(currentStateInfo.stateName), self.trackingTime)
        return delay

    def getStateDur(self, currentState):
        if currentState == 'OPERATING':
            return self.opDur
        else:
            return self.sdvDur

    def getNextState(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        # cs = self.getCurrentState(currentStateInfo.stateName)
        cs = self.currentState
        if cs == f'VAPOR_OPERATING':
            ns = 'OPERATING'
        # elif cs == f'VAPOR_STUCK_DUMP_VALVE':
        #     ns = self.getNextStateSDV(currentStateData, currentStateInfo, currentTime)
        else:
            msg = 'No state found'
            raise NotImplementedError(msg)
        self.currentState = f'VAPOR_{ns}'
        return ns

    def getNextStateOP(self, currentStateData=None, currentStateInfo=None, currentTime=None, cs=None):
        cs = self.currentState
        if self.trackingTime >= self.opDur:
            self.trackingTimeSubtract = self.trackingTime
            self.resetTrackingTime()
            self.sdvDur = int(self.sdvDurDist.pick())
            nextState = 'STUCK_DUMP_VALVE'
        else:
            nextState = 'OPERATING'
        return nextState

    def getNextStateSDV(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        cs = currentStateInfo.stateName
        if self.trackingTime >= self.sdvDur:
            self.trackingTimeSubtract = self.trackingTime
            self.resetTrackingTime()
            self.opDur = int(self.opDurDist.pick())
            nextState = 'OPERATING'
        else:
            nextState = 'STUCK_DUMP_VALVE'
        return nextState

    def initializeState(self, currentTime):
        # initialStateTimes = {'VAPOR_OPERATING': self.opDurDist.high,
        #                      'VAPOR_STUCK_DUMP_VALVE': self.sdvDurDist.high}
        randomState = 'VAPOR_OPERATING'
        randomStateTime = self.getInletChangeTime()
        self.currentState = randomState
        self.currentStateDur = randomStateTime
        # self.opDur = int(self.opDurDist.pick())
        # self.sdvDur = int(self.sdvDurDist.pick())
        return randomState, randomStateTime

    def getTotalInletVaporFlowRates(self):
        i = 10
        return sum(map(lambda x: x.driverRate, self.inletFluidFlows))

    def getTotalOutletVaporFlowRates(self):
        i = 10
        return sum(map(lambda x: x.driverRate, self.outletFluidFlows))

    def getInletChangeTime(self):
        return max(map(lambda x: x.changeTimeAbsolute, self.inletFluidFlows.get('Vapor', [])))
        # return self.getMinChangeTimeVapor(self.inletFluidFlows) if bool(self.inletFluidFlows) else u.getSimDuration()

    def getOutletChangeTime(self):
        return self.getMinChangeTimeVapor(self.outletFluidFlows)

    def addOutletFlowsFromLiquidFlashes(self, flow):
        self.addInletFluidFlow(flow)
        self.addOutletFluidFlow(flow)
        return flow

    def linkFlowFromContainer(self, outletME, flow, ME):
        # elif flow.name == 'Vapor':
        flowsForOut = []
        self.addInletFluidFlow(flow)
        tempSDV = ff.DependentFlowWithIndependentChangeTime.factory(flow,
                                                                    rateTransform=lambda x: self.currentGasFraction * x,
                                                                    newUnits=flow.driverUnits,
                                                                    secondaryID='stuck_dump_valve_vapor'
                                                                    )
        self.addOutletFluidFlow(tempSDV)
        flowsForOut.append(tempSDV)
        # self.containedSeparators[tempSDV.name].linkFlowFromContainer(outletME, tempSDV, self)

        tempGS = ff.DependentFlowWithIndependentChangeTime.factory(flow,
                                                                   rateTransform=lambda x: (1 - self.currentGasFraction) * x,
                                                                   newUnits=flow.driverUnits,
                                                                   secondaryID='gas_sales'
                                                                   )
        self.addOutletFluidFlow(tempGS)
        flowsForOut.append(tempGS)
        # self.containedSeparators[tempGS.name].linkFlowFromContainer(outletME, tempGS, self)
        # self.addInletFluidFlow(flow)
        return None, None, flowsForOut


class MEETContinuousSeparator(mc.MajorEquipment, mc.StateEnabledVolume):
    # class CondensateContainer(Container)

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['condensateGCTag', 'waterGCTag', 'stateMachine', 'trackingTime', 'delayCheck',
                                         'stuckDumpValveDurDist', 'currentGasFraction', 'trackingTimeSubtract',
                                         'operatingDurationMinSec', 'operatingDurationMaxSec', 'operatingDurationDist',
                                         'stuckDumpValveDurMinSec', 'stuckDumpValveDurMaxSec', 'gasFractionDist',
                                         'stateChangeTime', 'stuckDumpValveMTBFMinSec', 'stuckDumpValveMTBFMaxSec']

    def __init__(self,
                 activityDistribution=None,
                 flowGCTag=None,
                 primaryWaterRatio=None,
                 productionGC=None,
                 waterFlowUnits=None,
                 vaporFlowUnits=None,
                 condensateFlowUnits=None,
                 eqComponentCount=None,
                 stuckDumpValveMTBFMinDays=None,
                 stuckDumpValveMTBFMaxDays=None,
                 stuckDumpValveDurMinDays=None,
                 stuckDumpValveDurMaxDays=None,
                 gasFractionDistFileName=None,
                 stuckDumpValvepLeak=None,
                 multipleInstances=None,
                 instanceFormat=None,
                 stuckDumpValveCC=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.multipleInstances = multipleInstances
        self.instanceFormat = instanceFormat
        self.activityDistribution = activityDistribution
        self.flowGCTag = flowGCTag
        self.stuckDumpValveCC = stuckDumpValveCC
        self.condensateGCTag = f"{self.flowGCTag}-Condensate"
        self.waterGCTag = f"{self.flowGCTag}-Water"
        self.primaryWaterRatio = primaryWaterRatio
        self.productionGC = productionGC
        self.eqComponentCount = eqComponentCount
        self.stuckDumpValvepLeak = 0 if stuckDumpValvepLeak is None else stuckDumpValvepLeak
        self.stateChangeTime = 0
        self.gasFractionDistFileName = gasFractionDistFileName
        simdm = sdm.SimDataManager.getSimDataManager()
        self.gasFractionDist = getFlashFrac(gasFractionDistFileName, simdm)
        self.stuckDumpValveDurMinDays = 0 if stuckDumpValveDurMinDays is None else stuckDumpValveDurMinDays
        self.stuckDumpValveDurMinSec = int(u.daysToSecs(self.stuckDumpValveDurMinDays))
        self.stuckDumpValveDurMaxDays = 0 if stuckDumpValveDurMaxDays is None else stuckDumpValveDurMaxDays
        self.stuckDumpValveDurMaxSec = int(u.daysToSecs(self.stuckDumpValveDurMaxDays))
        if self.stuckDumpValvepLeak == 0:
            self.operatingDurationMinSec = 0
            self.operatingDurationMaxSec = u.getSimDuration()
            self.operatingDurationDist = d.Constant(u.getSimDuration())
        else:
            self.operatingDurationMinSec = int((self.stuckDumpValveDurMinSec * (1 - self.stuckDumpValvepLeak)) / self.stuckDumpValvepLeak)  # MTBF formula
            self.operatingDurationMaxSec = int((self.stuckDumpValveDurMaxSec * (1 - self.stuckDumpValvepLeak)) / self.stuckDumpValvepLeak)  # if separator dump valve is not stuck, it is operating
            self.operatingDurationDist = d.Uniform({'min': self.operatingDurationMinSec,   # this defn will change if we have another state
                                                    'max': self.operatingDurationMaxSec})
        self.currentGasFraction = 0
        self.trackingTimeSubtract = 0
        self.delayCheck = 0
        self.stuckDumpValveDurDist = d.Uniform({'min': self.stuckDumpValveDurMinSec,
                                                'max': self.stuckDumpValveDurMaxSec})
        self.condensateFlowUnits = condensateFlowUnits or 'bbl'
        self.waterFlowUnits = waterFlowUnits or 'bbl'
        self.vaporFlowUnits = vaporFlowUnits or 'scf'
        self.trackingTime = 0
        # OPERATING state = all valves operating normally
        # subStateMachine = phases with super state sdv
        # OVERPRESSURE = overpressure prv pops open, what happens to valves at this time?
        self.stateMachine = {
            'OPERATING': {'stateDuration': self.getTimeForState, 'nextState': self.getNextStateOP},
            'STUCK_DUMP_VALVE': {'stateDuration': self.getTimeForState, 'nextState': self.getNextStateSDV}
        }
        # self.subStateMachine = {
        #     'GAS': {'stateDuration': self.getTimeForState, 'nextState': self.getNextStateOP},
        #     'CONDENSATE': {'stateDuration': self.getTimeForState, 'nextState': self.getNextStateSDV},
        #     'PRIMARY_WATER': {'stateDuration': self.getTimeForState, 'nextState': self.getNextStateSDV},
        #     'SECONDARY_WATER': {'stateDuration': self.getTimeForState, 'nextState': self.getNextStateSDV}
        # }
        # example states: GAS-STUCK_DUMP_VALVE
        #                 CONDENSATE-STUCK_DUMP_VALVE
        #                 PRIMARY_WATER-STUCK_DUMP_VALVE:CONDENSATE-STUCK_DUMP_VALVE
        #                 CONDENSATE-STUCK_DUMP_VALVE:GAS-STUCK_DUMP_VALVE:SECONDARY_WATER-STUCK_DUMP_VALVE

    def updateDelay(self, ogDelay, delayCheck, stateDur, trackingTime):
        if delayCheck > stateDur:
            ret = stateDur - trackingTime
        else:
            ret = ogDelay
        return ret

    def getTimeForState(self,  currentStateData=None, currentStateInfo=None, currentTime=None):
        cs = currentStateInfo.stateName
        if cs == 'OPERATING':
            delay = min(self.getMinChangeTimeLiquids(self.inletFluidFlows) - currentTime,
                        self.opDur)
            # self.trackingTime += delay
            self.delayCheck = self.trackingTime+delay
            delay = self.updateDelay(delay, self.delayCheck, self.opDur, self.trackingTime)
            self.trackingTime += delay
            self.currentGasFraction = 0        # all gas goes to gas sales when in op state
        elif cs == 'STUCK_DUMP_VALVE':
            delay = min(self.getMinChangeTimeLiquids(self.inletFluidFlows) - currentTime,
                        self.sdvDur)
            # self.trackingTime += delay
            self.delayCheck = self.trackingTime+delay
            delay = self.updateDelay(delay, self.delayCheck, self.sdvDur, self.trackingTime)
            self.trackingTime += delay
            self.currentGasFraction = self.gasFractionDist.pick()
        else:
            raise ValueError(f'No state named {cs} for class {self.__class__.__name__}')
        self.updateFFChangeTime(delay+currentTime, delay)
        return delay

    def getNextStateOP(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        cs = currentStateInfo.stateName
        if self.trackingTime >= self.opDur:
            self.trackingTimeSubtract = self.trackingTime
            self.trackingTime = 0
            self.sdvDur = int(self.stuckDumpValveDurDist.pick())
            nextState = 'STUCK_DUMP_VALVE'
        else:
            nextState = 'OPERATING'
        return nextState

    def getNextStateSDV(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        cs = currentStateInfo.stateName
        if self.trackingTime >= self.sdvDur:
            self.trackingTimeSubtract = self.trackingTime
            self.trackingTime = 0
            self.opDur = int(self.operatingDurationDist.pick())
            nextState = 'OPERATING'
        else:
            nextState = 'STUCK_DUMP_VALVE'
        return nextState

    def initialStateUpdate(self, stateName, stateDuration, currentTime):
        self.currentGasFraction = 0
        try:
            delay = random.randrange(1, min(self.getMinChangeTimeLiquids(self.inletFluidFlows),
                                            self.getMinChangeTimeLiquids(self.outletFluidFlows)))
        except:
            delay = 1
        self.opDur = int(self.operatingDurationDist.pick())
        self.sdvDur = int(self.stuckDumpValveDurDist.pick())
        if stateName == 'OPERATING':
            delay2 = min(self.opDur, delay)
        elif stateName == 'STUCK_DUMP_VALVE':
            delay2 = min(self.sdvDur, delay)
            self.currentGasFraction = self.gasFractionDist.pick()
        else:
            raise ValueError(f'Cannot set initial state in {self}, unitID {self.unitID}')
        self.updateFFChangeTime(currentTime+delay2, currentTime)
        ret = sm.StateInfo(stateName, delay2, delay2)
        self.trackingTime += delay2
        return ret

    def calcStateTimes(self, **kwargs):
        stateTimes = {'OPERATING': u.getSimDuration()}
        self.stateTimes = stateTimes

    def initialStateTimes(self, **kwargs):
        return {'OPERATING': self.operatingDurationMaxSec, 'STUCK_DUMP_VALVE': self.stuckDumpValveDurMaxSec}

    def linkInletFlow(self, outletME, flow):
        self.addInletFluidFlow(flow)
        if flow.name == 'Water':
            gcName = genGCName(self.flowGCTag, flow.gc, process='Water')
            self.addOutletFluidFlow(ff.DependentFlowWithIndependentChangeTime.factory(flow,
                                                                                      rateTransform=lambda
                                                                                          x: self.primaryWaterRatio * x,
                                                                                      gc=flow.gc.derive(gcName),
                                                                                      secondaryID='primary_water',
                                                                                      newUnits=flow.driverUnits,
                                                                                      # changeTimeAbsolute=self.stateChangeTime
                                                                                      ))
            # todo: this assumes that the 98% and 2% flash GCs are the same.  Is this true?
            self.addOutletFluidFlow(ff.DependentFlowWithIndependentChangeTime.factory(flow,
                                                                                      rateTransform=lambda x: (1.0 - self.primaryWaterRatio) * x,
                                                                                      gc=flow.gc.derive(gcName),
                                                                                      secondaryID='secondary_water',
                                                                                      newUnits=flow.driverUnits,
                                                                                      # changeTimeAbsolute=self.stateChangeTime
                                                                                      ))
            water_flash_gc_name = genGCName(self.flowGCTag, flow.gc, process='Flash')
            self.addOutletFluidFlow(ff.DependentFlowWithIndependentChangeTime.factory(flow,
                                                                                      rateTransform=lambda x: self.currentGasFraction * x,
                                                                                      gc=flow.gc.derive(water_flash_gc_name, flow='Vapor'),
                                                                                      secondaryID='water_flash',
                                                                                      newName='Vapor',
                                                                                      newUnits='scf',
                                                                                      # changeTimeAbsolute=self.stateChangeTime
                                                                                      ))
            self.addOutletFluidFlow(ff.DependentFlowWithIndependentChangeTime.factory(flow,
                                                                                      rateTransform=lambda x: (1 - self.currentGasFraction) * x,
                                                                                      gc=flow.gc.derive(water_flash_gc_name, flow='Vapor'),
                                                                                      secondaryID='water_gas_sales',
                                                                                      newName='Vapor',
                                                                                      newUnits='scf',
                                                                                      # changeTimeAbsolute=self.stateChangeTime
                                                                                      ))
        elif flow.name == 'Condensate':
            gcName = genGCName(self.flowGCTag, flow.gc, process='Condensate')
            self.addOutletFluidFlow(ff.DependentFlowWithIndependentChangeTime.factory(flow,
                                                                                      secondaryID='condensate_flow',
                                                                                      newUnits=flow.driverUnits,
                                                                                      gc=flow.gc.derive(gcName),
                                                                                      # changeTimeAbsolute=self.stateChangeTime
                                                                                      ))
            oil_flash_gc_name = genGCName(self.flowGCTag, flow.gc, process='Flash')
            self.addOutletFluidFlow(ff.DependentFlowWithIndependentChangeTime.factory(flow,
                                                                                      rateTransform=lambda x: self.currentGasFraction * x,
                                                                                      gc=flow.gc.derive(oil_flash_gc_name, flow='Vapor'),
                                                                                      newName='Vapor',
                                                                                      secondaryID='condensate_flash',
                                                                                      newUnits='scf',
                                                                                      # changeTimeAbsolute=self.stateChangeTime
                                                                                      ))
            self.addOutletFluidFlow(ff.DependentFlowWithIndependentChangeTime.factory(flow,
                                                                                      rateTransform=lambda x: (1 - self.currentGasFraction) * x,
                                                                                      gc=flow.gc.derive(oil_flash_gc_name, flow='Vapor'),
                                                                                      newName='Vapor',
                                                                                      secondaryID='condensate_gas_sales',
                                                                                      newUnits='scf',
                                                                                      # changeTimeAbsolute=self.stateChangeTime
                                                                                      ))
        elif flow.name == 'Vapor':
            # gcName = self.productionGC
            self.addOutletFluidFlow(ff.DependentFlowWithIndependentChangeTime.factory(flow,
                                                             rateTransform=lambda x: self.currentGasFraction * x,
                                                             newUnits=flow.driverUnits,
                                                             secondaryID='stuck_dump_valve_vapor'
                                                             ))
            self.addOutletFluidFlow(ff.DependentFlowWithIndependentChangeTime.factory(flow,
                                                             rateTransform=lambda x: (1 - self.currentGasFraction) * x,
                                                             newUnits=flow.driverUnits,
                                                             secondaryID='gas_sales'
                                                             ))
        pass

    def calculateOperatingDuration(self, currentTime=None, **kwargs):
        return u.getSimDuration()


class MEETDumpingSeparator2(MEETContinuousSeparator2):
    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['outletDriverMultiplier', 'currentOutletDriverMultiplier',
                                         'currentRemainingVolume', 'dumpLiquidFraction',
                                         'totalWaterRatio', 'totalCondensateRatio', 'stateChangeTime',
                                         'primaryWaterFlow', 'secondaryWaterFlow', 'waterFlashFlow', 'waterGasSales',
                                         'condensateFlow', 'condensateFlash', 'condensateGasSales', 'totalVolume',
                                         'consolidatedFlowTableWater', 'consolidatedFlowTableCondensate',
                                         'consolidatedFlowTableVapor', 'volumeWater', 'volumeCondensate', 'volumeVapor',
                                         'sdvTimeTracking', 'sdvMTTRDur', 'sdvMTBFDur']

    def __init__(self,
                 dumpVolume=None,
                 dumpTime=None,
                 fillingStateDelay=None,
                 currentVolume=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.dumpVolume = dumpVolume   # in bbl
        self.dumpTime = dumpTime       # in sec
        self.condensateGCTag = f"{self.flowGCTag}-Condensate"
        self.waterGCTag = f"{self.flowGCTag}-Water"

        self.outletDriverMultiplier = self.dumpVolume / self.dumpTime
        self.currentOutletDriverMultiplier = 0
        self.currentGasFraction = 0
        self.dumpLiquidFraction = 0
        self.stateMachine = {
                             'FILLING': {'stateDuration': self.getTimeForState,
                                         'nextState': self.calcFillingNextState},
                             'DUMPING': {'stateDuration': self.getTimeForState,   # add functions for nextState?
                                         'nextState': self.calcDumpingNextState},
                             'STUCK_DUMP_VALVE': {'stateDuration': self.getTimeForState,  # add functions for nextState?
                                                  'nextState': self.getSDVNextState}
                              }

        self.fillingStateDelay = 10  # 3600
        self.currentVolume = 0
        self.totalWaterRatio = 0
        self.totalCondensateRatio = 0
        self.stateChangeTime = 0
        self.totalVolume = 0
        self.consolidatedFlowTableWater = {}
        self.volumeWater = {}                      # initialize volumes as dicts, set up gcs as index in linkInletFlows
        self.consolidatedFlowTableCondensate = {}
        self.volumeCondensate = {}
        self.consolidatedFlowTableVapor = {}
        self.volumeVapor = {}
        self.sdvTimeTracking = 0
        self.sdvMTTRDur = 0
        self.sdvMTBFDur = 0


class MEETDumpingSeparator(MEETContinuousSeparator):
    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['outletDriverMultiplier', 'currentOutletDriverMultiplier',
                                         'currentRemainingVolume', 'dumpLiquidFraction',
                                         'totalWaterRatio', 'totalCondensateRatio', 'stateChangeTime',
                                         'primaryWaterFlow', 'secondaryWaterFlow', 'waterFlashFlow', 'waterGasSales',
                                         'condensateFlow', 'condensateFlash', 'condensateGasSales', 'totalVolume',
                                         'consolidatedFlowTableWater', 'consolidatedFlowTableCondensate',
                                         'consolidatedFlowTableVapor', 'volumeWater', 'volumeCondensate', 'volumeVapor',
                                         'sdvTimeTracking', 'sdvMTTRDur', 'sdvMTBFDur']

    def __init__(self,
                 dumpVolume=None,
                 dumpTime=None,
                 fillingStateDelay=None,
                 currentVolume=None,
                 stuckDumpValveCC=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.dumpVolume = dumpVolume   # in bbl
        self.dumpTime = dumpTime       # in sec
        self.stuckDumpValveCC = stuckDumpValveCC
        self.condensateGCTag = f"{self.flowGCTag}-Condensate"
        self.waterGCTag = f"{self.flowGCTag}-Water"

        self.outletDriverMultiplier = self.dumpVolume / self.dumpTime
        self.currentOutletDriverMultiplier = 0
        self.currentGasFraction = 0
        self.dumpLiquidFraction = 0
        self.stateMachine = {
                             'FILLING': {'stateDuration': self.getTimeForState,
                                         'nextState': self.calcFillingNextState},
                             'DUMPING': {'stateDuration': self.getTimeForState,   # add functions for nextState?
                                         'nextState': self.calcDumpingNextState},
                             'STUCK_DUMP_VALVE': {'stateDuration': self.getTimeForState,  # add functions for nextState?
                                                  'nextState': self.getSDVNextState}
                              }

        self.fillingStateDelay = 10  # 3600
        self.currentVolume = 0
        self.totalWaterRatio = 0
        self.totalCondensateRatio = 0
        self.stateChangeTime = 0
        self.totalVolume = 0
        self.consolidatedFlowTableWater = {}
        self.volumeWater = {}                      # initialize volumes as dicts, set up gcs as index in linkInletFlows
        self.consolidatedFlowTableCondensate = {}
        self.volumeCondensate = {}
        self.consolidatedFlowTableVapor = {}
        self.volumeVapor = {}
        self.sdvTimeTracking = 0
        self.sdvMTTRDur = 0
        self.sdvMTBFDur = 0

    def getTimeForState(self,  currentStateData=None, currentStateInfo=None, currentTime=None):
        cs = currentStateInfo.stateName

        if cs == 'FILLING':   # todo use little functions instead of switch statements
            delay = self.getTimeToFull(currentStateData, currentStateInfo, currentTime)
            self.currentGasFraction = 0
            self.currentOutletDriverMultiplier = 0
            self.delayCheck = self.sdvTimeTracking + delay
            delay = self.updateDelay(delay, self.delayCheck, self.sdvMTBFDur, self.sdvTimeTracking)
            self.updateFlowVolumes(delay)
            self.sdvTimeTracking += delay
        elif cs == 'DUMPING':
            self.currentGasFraction = 0
            self.currentOutletDriverMultiplier = self.outletDriverMultiplier
            self.updateFlowVolumes(self.dumpTime)
            delay = self.calcDumpTime(currentTime)
            self.sdvTimeTracking += delay
        elif cs == 'STUCK_DUMP_VALVE':  # set sdv delay value in nextState methods to save that value in sdvMTTR/sdvMTBF
            delay = min((self.getMinChangeTimeLiquids(self.inletFluidFlows) - currentTime),
                        (self.sdvMTTRDur - self.sdvTimeTracking))
            self.currentGasFraction = self.gasFractionDist.pick()
            self.currentOutletDriverMultiplier = 1   # implies ffs go out as they come in
            self.delayCheck = self.sdvTimeTracking + delay
            delay = self.updateDelay(delay, self.delayCheck, self.sdvMTTRDur, self.sdvTimeTracking)
            self.updateFlowVolumesSDV(delay)
            self.sdvTimeTracking += delay
        else:
            raise AttributeError(f'no state {cs} for {self.__class__.__name__}')
        self.updateFFChangeTime(currentTime+delay, delay)   #  self.currentVolume and self.totalVolume should be the same when Dumping
        # self.updateFlowVolumes()
        if int(delay) > 5000:
            i = 10
        return int(delay)

    def updateFlowVolumes(self, delay):
        for gc in self.volumeCondensate.keys():
            self.volumeCondensate[gc] += sum(map(lambda x: x.driverRate * delay, self.consolidatedFlowTableCondensate[gc].subFlows))
            i = 10
        for gc in self.volumeWater.keys():
            self.volumeWater[gc] += sum(map(lambda x: x.driverRate * delay, self.consolidatedFlowTableWater[gc].subFlows))
        self.totalVolume += self.getTotalFlowRateLiquids(self.inletFluidFlows) * delay
        pass

    def updateFlowVolumesSDV(self, delay):
        for gc in self.volumeCondensate.keys():
            self.volumeCondensate[gc] = sum(map(lambda x: x.driverRate, self.consolidatedFlowTableCondensate[gc].subFlows))
            i = 10
        for gc in self.volumeWater.keys():
            self.volumeWater[gc] = sum(map(lambda x: x.driverRate, self.consolidatedFlowTableWater[gc].subFlows))
        self.totalVolume = 1
        pass

    def getTimeToFull(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        return self.fillingStateDelay

    def _calcFillingStateTime(self, currentTime):
        remainingVolume = self.dumpVolume - self.currentVolume

        # determine when inlet fluid flows will change their rate
        liquidFlows = self.getLiquidFlows(self.inletFluidFlows)

        # timeToRateChange = min(map(lambda x: x.changeTimeAbsolute, itertools.chain(self.getLiquidFlows(self.inletFluidFlows))))
        timeToRateChange = self.getMinChangeTimeLiquids(self.inletFluidFlows)
        rateChangeDelay = timeToRateChange - currentTime

        # calculate fill time based on current inflow rates of all incoming FF

        # totalFillRate = sum(map(lambda x: x.driverRate, itertools.chain(self.getLiquidFlows(self.inletFluidFlows))))
        totalFillRate = self.getTotalFlowRateLiquids(self.inletFluidFlows)
        if totalFillRate != 0:
            fillDuration = min(round(remainingVolume / totalFillRate), rateChangeDelay)  # Volume = driverRate * duration, (units = bbl * s / bbl = s)
            volumeFilled = totalFillRate * fillDuration
        else:
            fillDuration = rateChangeDelay
            volumeFilled = totalFillRate * fillDuration

        return fillDuration, rateChangeDelay, totalFillRate, remainingVolume

    def calcVolumeFilledWhenDumping(self, delay, currentTime):
        # fillingRateWhenDumping = sum(map(lambda x: x.driverRate, itertools.chain(self.getLiquidFlows(self.inletFluidFlows))))
        fillingRateWhenDumping = self.getTotalFlowRateLiquids(self.inletFluidFlows)
        minTime = min((self.getMinChangeTimeLiquids(self.inletFluidFlows) - currentTime), delay)
        volumeFilledWhenDumping = minTime * fillingRateWhenDumping
        return volumeFilledWhenDumping

    def calcDumpTime(self, currentTime):
        # remainingVolume = self.dumpVolume - self.currentVolume
        if self.flowGCTag == 'Stage1':
            delay = self.dumpTime
            self.currentVolume = self.calcVolumeFilledWhenDumping(delay, currentTime)  # volume added to next filling state
        else:
            # inRate = sum(map(lambda x: x.driverRate, itertools.chain(self.getLiquidFlows(self.inletFluidFlows))))
            inRate = self.getTotalFlowRateLiquids(self.inletFluidFlows)
            # outRate = sum(map(lambda x: x.driverRate, itertools.chain(self.getLiquidFlows(self.outletFluidFlows))))
            outRate = self.getTotalFlowRateLiquids(self.outletFluidFlows)
            # timeToRateChange = min(map(lambda x: x.changeTimeAbsolute, itertools.chain(self.getLiquidFlows(self.inletFluidFlows))))
            timeToRateChange = self.getMinChangeTimeLiquids(self.inletFluidFlows)
            deltaT = timeToRateChange - currentTime
            if round(inRate, 2) > round(outRate, 2):   # if this happens we'll do a warning but simulate anyway to show it in inst events
                inDumpVolume = deltaT * inRate
                dumpTimeNew = inDumpVolume / outRate
                delay = dumpTimeNew
                msg = f'INLET FLOW RATE IS FASTER THAN OUTLET FLOW RATE: equipment: {self.__class__.__name__},' \
                      f' unitID: {self.unitID}, please check the dumping separator columns "Dump Volume", "Dump Time"'
                logging.error(msg)
                # raise ValueError(msg)
            else:
                delay = self.dumpTime
                self.currentVolume = self.calcVolumeFilledWhenDumping(delay, currentTime)
        # self.resetIndividualVolumes(self.inletFluidFlows)
        return delay

    def calcFillingNextState(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        cs = currentStateInfo.stateName
        fillDuration, rateChangeDelay, totalFillRate, remainingVolume = self._calcFillingStateTime(currentTime)

        # if (self.sdvMTBFDur - self.sdvMTBFDur/10) <= self.sdvTimeTracking <= (self.sdvMTBFDur + self.sdvMTBFDur/10):
        if self.sdvTimeTracking >= self.sdvMTBFDur:
            nextState = 'STUCK_DUMP_VALVE'
            self.sdvTimeTracking = 0  # reset tracking time to 0 and start tracking MTTR in getTimeForState
            self.currentVolume = 0   # reset current volume
            self.fillingStateDelay = 0  # reset filling state delay
            self.sdvMTTRDur = int(self.stuckDumpValveDurDist.pick())
            return nextState

        if remainingVolume <= (self.dumpVolume/10):
            nextState = 'DUMPING'
            # self.currentVolume = 0
            self.currentVolume = (fillDuration * totalFillRate)   # reset volume
            self.fillingStateDelay = 0
        else:
            nextState = 'FILLING'
            self.currentVolume += (fillDuration * totalFillRate)
            self.fillingStateDelay = fillDuration
        return nextState

    def calcDumpingNextState(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        cs = currentStateInfo.stateName

        # reset ratios
        for gc in self.volumeCondensate.keys():
            self.volumeCondensate[gc] = 0
        for gc in self.volumeWater.keys():
            self.volumeWater[gc] = 0
        self.totalVolume = 0

        # set new states
        # if (self.sdvMTBFDur - self.sdvMTBFDur/10) <= self.sdvTimeTracking <= (self.sdvMTBFDur + self.sdvMTBFDur/10):
        if self.sdvTimeTracking >= self.sdvMTBFDur:
            nextState = 'STUCK_DUMP_VALVE'
            self.sdvTimeTracking = 0     # reset tracking time to 0 and start tracking MTTR in getTimeForState
            self.sdvMTTRDur = int(self.stuckDumpValveDurDist.pick())  # start tracking for SDV
        else:
            nextState = 'FILLING'
        return nextState

    def getSDVNextState(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        # if (self.sdvMTTRDur - self.sdvMTTRDur/10) <= self.sdvTimeTracking <= (self.sdvMTTRDur + self.sdvMTTRDur/10):
        if self.sdvTimeTracking >= self.sdvMTTRDur:
            nextState = 'FILLING'
            self.sdvTimeTracking = 0     # reset tracking time to 0 and start tracking MTTR in getTimeForState
            self.sdvMTBFDur = int(self.operatingDurationDist.pick())  # start tracking for filling/dump
        else:
            nextState = 'STUCK_DUMP_VALVE'
        return nextState

    def initialStateTimes(self):
        self.currentVolume = round(random.uniform(1, self.dumpVolume), 1)
        if self.currentVolume >= self.dumpVolume:
            self.stateTimes = {'DUMPING': self.dumpTime}
        else:
            delay = int(self.safeDivByZero(self.currentVolume, self.getTotalFlowRateLiquids(self.inletFluidFlows)))\
                    or self.getMinChangeTimeLiquids(self.inletFluidFlows)
            self.stateTimes = {'FILLING': delay}
        self.sdvMTTRDur = int(self.stuckDumpValveDurDist.pick())
        self.sdvMTBFDur = int(self.operatingDurationDist.pick())

        return self.stateTimes

    def initialStateUpdate(self, randomState, randomStateDelay, currentTime):
        # if we are filling, update the internal fill volume based on the randomized fill time
        if randomState == 'FILLING':
            # self.currentVolume = randomStateDelay * self.initialTotalFillRate
            # self.currentVolume = self.dumpVolume   # this is deterministic. use only for testing
            # self.fillingStateDelay = randomStateDelay
            self.currentGasFraction = 0
            if randomStateDelay <= self.getMinChangeTimeLiquids(self.inletFluidFlows):  # min between randomStateDelay or changeTimeAbsolute from prev eq
                self.updateFFChangeTime(randomStateDelay, randomStateDelay)
                self.fillingStateDelay = randomStateDelay
                # self.currentVolume = randomStateDelay * self.initialTotalFillRate
            else:
                self.updateFFChangeTime(self.getMinChangeTimeLiquids(self.inletFluidFlows), self.getMinChangeTimeLiquids(self.inletFluidFlows))
                self.fillingStateDelay = self.getMinChangeTimeLiquids(self.inletFluidFlows)
                # self.currentVolume = self.getMinChangeTimeLiquids(self.inletFluidFlows) * self.initialTotalFillRate
            delay = self.getMinChangeTimeLiquids(self.outletFluidFlows)
        else:
            self.currentGasFraction = self.gasFractionDist.pick()
            self.currentOutletDriverMultiplier = self.outletDriverMultiplier
            delay = min(self.getMinChangeTimeLiquids(self.inletFluidFlows), self.getMinChangeTimeLiquids(self.outletFluidFlows))
            delay = min(delay, self.dumpTime)
            self.updateFFChangeTime(delay, delay)
            
        self.sdvTimeTracking = self.fillingStateDelay  # start tracking time

        # self.updateIndividualVolumes(self.inletFluidFlows, self.outletFluidFlows, randomStateDelay)
        ret = sm.StateInfo(randomState,
                           deltaTimeInState=delay,
                           absoluteTimeInState=self.getMinChangeTimeLiquids(self.outletFluidFlows))
        
        if int(ret.deltaTimeInState) > 5000:
            i = 10
        return ret

    def safeDivByZero(self, a, b):   # returns 0 if div by 0
        return a/b if b else 0

    def linkInletFlow(self, outletME, flow):
        self.addInletFluidFlow(flow)
        if flow.name == 'Water':
            inboundGC = flow.gc
            gcName = genGCName(self.flowGCTag, flow.gc, process='Water')
            waterFlashGCName = genGCName(self.flowGCTag, flow.gc, process='Flash')

            if inboundGC not in self.consolidatedFlowTableWater:
                self.consolidatedFlowTableWater[inboundGC] = ff.CompositeFlow(flow.name, inboundGC, newUnits=flow.driverUnits)
                self.volumeWater[inboundGC] = 0    # set the incoming gcs as indexes, use them later to calc ratios
                # self.addOutletFluidFlow(self.consolidatedFlowTableWater[inboundGC])

                self.addOutletFluidFlow(ff.DependentFlowWithIndependentChangeTime.factory(origFlow=self.consolidatedFlowTableWater[inboundGC],
                                                                                          rateTransform=lambda x: self.primaryWaterRatio *
                                                                                             self.safeDivByZero(self.volumeWater[inboundGC], self.totalVolume) *
                                                                                             self.currentOutletDriverMultiplier,
                                                                                          gc=flow.gc.derive(gcName),
                                                                                          secondaryID='primary_water'
                                                                                          ))

                self.addOutletFluidFlow(ff.DependentFlowWithIndependentChangeTime.factory(origFlow=self.consolidatedFlowTableWater[inboundGC],
                                                                                          rateTransform=lambda x: (1.0 - self.primaryWaterRatio) *
                                                                                             self.safeDivByZero(self.volumeWater[inboundGC], self.totalVolume) *
                                                                                             self.currentOutletDriverMultiplier,
                                                                                          gc=flow.gc.derive(gcName),
                                                                                          secondaryID='secondary_water'
                                                                                          ))

                self.addOutletFluidFlow(ff.DependentFlowWithIndependentChangeTime.factory(origFlow=self.consolidatedFlowTableWater[inboundGC],
                                                                                          rateTransform=lambda x: self.currentGasFraction * x,
                                                                                          gc=flow.gc.derive(waterFlashGCName, flow='Vapor'),  # gets converted in FF class
                                                                                          newName='Vapor',
                                                                                          newUnits='scf',
                                                                                          secondaryID='water_flash'
                                                                                          ))

                self.addOutletFluidFlow(ff.DependentFlowWithIndependentChangeTime.factory(origFlow=self.consolidatedFlowTableWater[inboundGC],
                                                                                          rateTransform=lambda x: (1.0 - self.currentGasFraction) * x,
                                                                                          gc=flow.gc.derive(waterFlashGCName, flow='Vapor'),  # gets
                                                                                          newName='Vapor',
                                                                                          newUnits='scf',
                                                                                          secondaryID='water_gas_sales'
                                                                                          ))
            self.consolidatedFlowTableWater[inboundGC].addFlow(flow)
            # self.volumeWater[inboundGC] = self.consolidatedFlowTableWater['subFlows']

        elif flow.name == 'Condensate':
            inboundGC = flow.gc
            gcName = genGCName(self.flowGCTag, flow.gc, process='Condensate')
            oilFlashGCName = genGCName(self.flowGCTag, flow.gc, process='Flash')
            if inboundGC not in self.consolidatedFlowTableCondensate:
                self.consolidatedFlowTableCondensate[inboundGC] = ff.CompositeFlow(flow.name, inboundGC, newUnits=flow.driverUnits)
                self.volumeCondensate[inboundGC] = 0
                self.addOutletFluidFlow(ff.DependentFlowWithIndependentChangeTime.factory(origFlow=self.consolidatedFlowTableCondensate[inboundGC],
                                                                                          rateTransform=lambda x: self.safeDivByZero(self.volumeCondensate[inboundGC], self.totalVolume) *
                                                                                                                  self.currentOutletDriverMultiplier,
                                                                                          gc=flow.gc.derive(gcName),
                                                                                          secondaryID='condensate_flow'
                                                                                          ))
                self.addOutletFluidFlow(ff.DependentFlowWithIndependentChangeTime.factory(origFlow=self.consolidatedFlowTableCondensate[inboundGC],
                                                                                          rateTransform=lambda x: self.currentGasFraction * x,
                                                                                          gc=flow.gc.derive(oilFlashGCName, flow='Vapor'),  # convert the units
                                                                                          newUnits='scf',
                                                                                          newName='Vapor',
                                                                                          secondaryID='condensate_flash'
                                                                                          ))
                self.addOutletFluidFlow(ff.DependentFlowWithIndependentChangeTime.factory(origFlow=self.consolidatedFlowTableCondensate[inboundGC],
                                                                                          rateTransform=lambda x: (1.0 - self.currentGasFraction) * x,
                                                                                          gc=flow.gc.derive(oilFlashGCName, flow='Vapor'),
                                                                                          newUnits='scf',
                                                                                          newName='Vapor',
                                                                                          secondaryID='condensate_gas_sales'
                                                                                          ))
            self.consolidatedFlowTableCondensate[inboundGC].addFlow(flow)

        elif flow.name == 'Vapor':
            # self.addInletFluidFlow(flow)
            inboundGC = flow.gc
            gcName = self.productionGC
            if inboundGC not in self.consolidatedFlowTableVapor:
                self.consolidatedFlowTableVapor[inboundGC] = ff.CompositeFlow(flow.name, inboundGC, newUnits=flow.driverUnits)
                self.addOutletFluidFlow(ff.DependentFlowWithIndependentChangeTime.factory(origFlow=self.consolidatedFlowTableVapor[inboundGC],
                                                                 rateTransform=lambda x: self.currentGasFraction * x,
                                                                 newUnits='scf',
                                                                 secondaryID='stuck_dump_valve_vapor'
                                                                 ))
                self.addOutletFluidFlow(ff.DependentFlowWithIndependentChangeTime.factory(origFlow=self.consolidatedFlowTableVapor[inboundGC],
                                                                 rateTransform=lambda x: (1.0 - self.currentGasFraction) * x,
                                                                 newUnits='scf',
                                                                 secondaryID='gas_sales'
                                                                 ))
            self.consolidatedFlowTableVapor[inboundGC].addFlow(flow)
        pass



class MEETPneumaticController(mc.MajorEquipment, mc.StateEnabledVolume):

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['abnormalMTTRMinSec', 'abnormalMTTRMaxSec', 'abnormalDurDist', 'isPcInt',
                                         'stateMachine', 'opDurMinSec', 'opDurMaxSec', 'opDurDist']

    def __init__(self,
                 eqComponentCount=None,
                 pcType=None,
                 pLeakAbnormal=None,
                 abnormalMTTRMindays=None,
                 abnormalMTTRMaxdays=None,
                 multipleInstances=None,
                 instanceFormat=None,
                 intermittentDur=None,
                 **kwargs):

        super().__init__(**kwargs)
        self.eqComponentCount = eqComponentCount
        self.multipleInstances = multipleInstances
        self.instanceFormat = instanceFormat

        self.pcType = pcType
        if self.pcType == 'Intermittent':
            self.isPcInt = True
        else:
            self.isPcInt = False

        self.pLeakAbnormal = pLeakAbnormal
        self.abnormalMTTRMindays = abnormalMTTRMindays
        self.abnormalMTTRMaxdays = abnormalMTTRMaxdays

        self.abnormalMTTRMinSec = u.daysToSecs(self.abnormalMTTRMindays)
        self.abnormalMTTRMaxSec = u.daysToSecs(self.abnormalMTTRMaxdays)
        self.abnormalDurDist = d.Uniform({'min': self.abnormalMTTRMinSec, 'max': self.abnormalMTTRMaxSec})

        self.opDurMinSec = int((self.abnormalMTTRMinSec * (1 - self.pLeakAbnormal)) / self.pLeakAbnormal)
        self.opDurMaxSec = int((self.abnormalMTTRMaxSec * (1 - self.pLeakAbnormal)) / self.pLeakAbnormal)
        self.opDurDist = d.Uniform({'min': self.opDurMinSec, 'max': self.opDurMaxSec})

        self.intermittentDur = intermittentDur  # seconds
        # how to determine state duration?
        # States: Operating = Normal behavior with emissions within EPA limits
        #         Intermittent_Emission = Emissions during actuation of an intermittent PC, only for intermittent pcs
        #         Emitting = Abnormal emissions
        self.stateMachine = {
            'OPERATING': {'stateDuration': self.getTimeForState, 'nextState': self.getNextState},
            'INTERMITTENT_EMISSION': {'stateDuration': self.getTimeForState, 'nextState': self.getNextState},
            'EMITTING': {'stateDuration': self.getTimeForState, 'nextState': self.getNextState},
        }

    def getTimeForState(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        #  if Operating, delay = upstream FF delay
        #  if INTERMITTENT_EMISSION, delay = 15s for illustrating intermittent emissions
        #  if Emitting, delay = upstream FF delay
        cs = currentStateInfo.stateName
        if cs == 'OPERATING':
            delay = int(self.opDurDist.pick())
        elif cs == 'EMITTING':
            delay = int(self.abnormalDurDist.pick())
        elif cs == 'INTERMITTENT_EMISSION':
            delay = int(self.intermittentDur)
        else:
            msg = f'No state {cs} found'
            raise NotImplementedError(msg)
        return delay

    def getNextState(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        #  if Operating, next state = Emitting or Operating or depending on what? (assuming MTTR, MTBF)
        #  if Emitting, next state = Operating
        #  if Intermittent emission, next state = emitting or operating, how to chose? (assuming MTTR, MTBF)
        cs = currentStateInfo.stateName
        if cs == 'OPERATING':
            if self.isPcInt:
                nextState = 'INTERMITTENT_EMISSION'
            else:
                nextState = 'EMITTING'
        elif cs == 'EMITTING':
            nextState = 'OPERATING'
        elif cs == 'INTERMITTENT_EMISSION':
            nextState = 'OPERATING'
        else:
            msg = f'State {cs} does not exist'
            raise NotImplementedError(msg)
        return nextState
        pass

    def initialStateTimes(self, **kwargs):
        # initial state time same as upstream?
        # how to set up FFs?
        return {'OPERATING': self.opDurMaxSec, 'EMITTING': self.abnormalMTTRMaxSec}

    def linkInletFLow(self, outletME, flow):
        # inlet gas flows same as outlets. Only using factors for now.
        # How to set up valve switches?
        pass


# todo: add fluid flows in pneumatics
class MEETContinuousPneumatics(mc.MajorEquipment, mc.StateEnabledVolume):

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['abnormalMTTRMinSec', 'abnormalMTTRMaxSec', 'abnormalDurDist', 'isPcInt',
                                         'stateMachine', 'opDurMinSec', 'opDurMaxSec', 'opDurDist',
                                         'trackingTimeAbnormal', 'prevState', 'currentStateDelay', 'currentStateDur',
                                         'trackingTimeNormal', 'contVentDelay', 'contVentAbDelay',
                                         'notOpDelay', 'contVentDur', 'abVentDur', 'currentState1', 'nextState']

    def __init__(self,
                 eqComponentCount=None,
                 pcType=None,
                 pLeakAbnormal=None,
                 abnormalMTTRMindays=None,
                 abnormalMTTRMaxdays=None,
                 multipleInstances=None,
                 instanceFormat=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.eqComponentCount = eqComponentCount
        self.multipleInstances = multipleInstances
        self.instanceFormat = instanceFormat

        self.pcType = pcType

        self.pLeakAbnormal = pLeakAbnormal
        self.abnormalMTTRMindays = abnormalMTTRMindays
        self.abnormalMTTRMaxdays = abnormalMTTRMaxdays

        self.abnormalMTTRMinSec = u.daysToSecs(self.abnormalMTTRMindays)
        self.abnormalMTTRMaxSec = u.daysToSecs(self.abnormalMTTRMaxdays)
        self.abnormalDurDist = d.Uniform({'min': self.abnormalMTTRMinSec, 'max': self.abnormalMTTRMaxSec})

        self.opDurMinSec = int((self.abnormalMTTRMinSec * (1 - self.pLeakAbnormal)) / self.pLeakAbnormal)
        self.opDurMaxSec = int((self.abnormalMTTRMaxSec * (1 - self.pLeakAbnormal)) / self.pLeakAbnormal)
        self.opDurDist = d.Uniform({'min': self.opDurMinSec, 'max': self.opDurMaxSec})

        self.trackingTimeAbnormal = 0
        self.trackingTimeNormal = 0
        self.contVentDelay = 0
        self.contVentAbDelay = 0
        self.notOpDelay = 0
        self.contVentDur = int(self.opDurDist.pick())
        self.abVentDur = int(self.abnormalDurDist.pick())
        self.prevState = None
        self.currentState1 = None
        self.nextState = None
        self.currentStateDelay = None
        self.currentStateDur = None

        # self.intermittentDur = intermittentDur  # seconds

        self.stateMachine = {
            'CONTINUOUS_VENT': {'stateDuration': self.getTimeForState, 'nextState': self.getNextStateAfterVent},
            'CONTINUOUS_VENT_ABNORMAL': {'stateDuration': self.getTimeForState, 'nextState': self.getNextStateAfterVent},
            'NOT_OPERATING': {'stateDuration': self.getTimeForState, 'nextState': self.getNextStateAfterNO},
        }

    def getTimeForState(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        cs = currentStateInfo.stateName
        if cs == 'CONTINUOUS_VENT':
            delay = self.currentStateDelay
            # self.currentState1 = cs
            # self.prevState = cs
        elif cs == 'CONTINUOUS_VENT_ABNORMAL':
            delay = self.currentStateDelay
            # self.prevState = cs
            # self.currentState1 = cs
        elif cs == 'NOT_OPERATING':
            delay = self.notOpDelay    # only using when there are FFs
            self.currentState = cs
        else:
            msg = f'No state {cs} found'
            raise NotImplementedError(msg)
        self.trackingTimeAbnormal += delay
        return delay

    def getNextStateAfterVent(self, currentStateData, currentStateInfo, currentTime):
        if self.inletFluidFlows:
            if self.trackingTimeAbnormal > self.currentStateDur:
                nextState = self.nextState
                self.nextState = currentStateInfo.stateName
                self.getNextStateGeneral(currentStateData, currentStateInfo, currentTime, nextState)
                self.currentState1 = nextState
            else:
                nextState = self.currentState1
                self.currentStateDelay = self.getMinChangeTimeLiquids(self.inletFluidFlows) - currentTime
        else:
            nextState = self.getNextStateNoFluidFlows(currentStateData, currentStateInfo, currentTime)
        return nextState

    def getNextStateGeneral(self, currentStateData, currentStateInfo, currentTime, nextState):
        self.trackingTimeAbnormal = 0
        if nextState == 'CONTINUOUS_VENT':
            self.currentStateDur = int(self.opDurDist.pick())
        elif nextState == 'CONTINUOUS_VENT_ABNORMAL':
            self.currentStateDur = int(self.abnormalDurDist.pick())
        else:
            msg = f'{nextState} not found'
            raise NotImplementedError(msg)
        self.currentStateDelay = self.getMinChangeTimeLiquids(self.inletFluidFlows) - currentTime

    def getNextStateAfterNO(self, currentStateData, currentStateInfo, currentTime):
        pass

    # def getNextStateAfterAb(self, currentStateData, currentStateInfo, currentTime):

    def getNextStateFluidFlows(self, currentStateData, currentStateInfo, currentTime):
        liquidFlowRates = self.getLiquidFlowRates(self.inletFluidFlows)
        if sum(liquidFlowRates) > 0:
            nextState = self.getNextStateTimeTracker(currentStateData, currentStateInfo, currentTime)
            # self.contVentDelay = self.getMinChangeTimeLiquids(self.inletFluidFlows) - currentTime
        else:
            nextState = 'NOT_OPERATING'
            self.notOpDelay = self.getMinChangeTimeLiquids(self.inletFluidFlows) - currentTime
        i = 10
        return nextState

    def getNextStateAb(self, currentStateData, currentStateInfo, currentTime):
        self.trackingTimeAbnormal = 0
        self.abVentDur = int(self.abnormalDurDist.pick())
        self.contVentAbDelay = self.getMinChangeTimeLiquids(self.inletFluidFlows) - currentTime

    def getNextStateVent(self, currentStateData, currentStateInfo, currentTime):
        self.trackingTimeAbnormal = 0
        self.contVentDur = int(self.opDurDist.pick())
        self.contVentDelay = self.getMinChangeTimeLiquids(self.inletFluidFlows) - currentTime

    def getNextStateTimeTracker(self, currentStateData, currentStateInfo, currentTime):
        cs = currentStateInfo.stateName
        # nextState = 'CONTINUOUS_VENT'
        if self.prevState is None:
            nextState = random.choice(['CONTINUOUS_VENT', 'CONTINUOUS_VENT_ABNORMAL'])
            # nextState = 'CONTINUOUS_VENT'    # only for debugging
            if nextState == 'CONTINUOUS_VENT_ABNORMAL':
                self.getNextStateAb(currentStateData, currentStateInfo, currentTime)
            else:
                self.getNextStateVent(currentStateData, currentStateInfo, currentTime)
        else:
            if self.prevState == 'CONTINUOUS_VENT':
                if self.trackingTimeAbnormal > self.abVentDur:
                    nextState = 'CONTINUOUS_VENT_ABNORMAL'
                    self.getNextStateAb(currentStateData, currentStateInfo, currentTime)
                else:
                    nextState = 'CONTINUOUS_VENT'
                    self.contVentDelay = self.getMinChangeTimeLiquids(self.inletFluidFlows) - currentTime
            elif self.prevState == 'CONTINUOUS_VENT_ABNORMAL':
                if self.trackingTimeAbnormal > self.contVentDur:
                    nextState = 'CONTINUOUS_VENT'
                    self.getNextStateVent(currentStateData, currentStateInfo, currentTime)
                else:
                    nextState = 'CONTINUOUS_VENT_ABNORMAL'
                    self.contVentAbDelay = self.getMinChangeTimeLiquids(self.inletFluidFlows) - currentTime
        return nextState

    def getNextStateNoFluidFlows(self, currentStateData, currentStateInfo, currentTime):
        cs = currentStateInfo.stateName
        if cs == 'CONTINUOUS_VENT':
            nextState = 'CONTINUOUS_VENT_ABNORMAL'
            self.currentStateDelay = int(self.abnormalDurDist.pick())
        elif cs == 'CONTINUOUS_VENT_ABNORMAL':
            nextState = 'CONTINUOUS_VENT'
            self.currentStateDelay = int(self.opDurDist.pick())
        elif cs == 'NOT_OPERATING':
            nextState = 'CONTINUOUS_VENT'
        else:
            msg = f'State {cs} does not exist'
            raise NotImplementedError(msg)
        return nextState

    def getNextState(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        cs = currentStateInfo.stateName
        if self.inletFluidFlows:
            nextState = self.getNextStateFluidFlows(currentStateData, currentStateInfo, currentTime)
        else:
            nextState = self.getNextStateNoFluidFlows(currentStateData, currentStateInfo, currentTime)
        # self.prevState = nextState
        return nextState

    def initialStateTimes(self, **kwargs):
        ret = {'CONTINUOUS_VENT': 15, 'CONTINUOUS_VENT_ABNORMAL': 15}
        self.currentState1 = 'CONTINUOUS_VENT'
        self.nextState = 'CONTINUOUS_VENT_ABNORMAL'
        return ret

    def initialStateUpdate(self, stateName, stateDuration, currentTime):
        if stateName == 'CONTINUOUS_VENT':
            self.currentStateDur = int(self.opDurDist.pick())
        else:
            self.currentStateDur = int(self.abnormalDurDist.pick())
        # random.randrange(1, stateTimes[randomState])
        if self.inletFluidFlows:
            self.currentStateDelay = random.randrange(1, self.getMinChangeTimeLiquids(self.inletFluidFlows))
        else:
            self.currentStateDelay = random.randrange(1, self.currentStateDur)

        self.trackingTimeAbnormal = self.currentStateDelay
        ret = super().initialStateUpdate(stateName, stateDuration=self.currentStateDelay, currentTime=currentTime)
        return ret

    def linkInletFlow(self, outletME, flow):
        # inlet gas flows same as outlets. Only using factors for now.
        # How to set up valve switches?
        self.addInletFluidFlow(flow)
        self.addOutletFluidFlow(flow)
        pass


class MEETIntermittentPneumatics(mc.MajorEquipment, mc.StateEnabledVolume):

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['abnormalMTTRMinSec', 'abnormalMTTRMaxSec', 'abnormalDurDist', 'isPcInt',
                                         'stateMachine', 'opDurMinSec', 'opDurMaxSec', 'opDurDist',
                                         'intermittentDurCurrent', 'intermittentWaitDurCurrent', 'prevFF',
                                         'intermittentDurDist', 'intermittentWaitDurDist', 'intermittentDurNormalDist',
                                         'abDurMinSec', 'abDurMaxSec', 'abDurDist', 'trackingTime', 'isNewState',
                                         'normalDurMinSec', 'normalDurMaxSec', 'opDurDist', 'currentBigState',
                                         'nextBigState', 'intermittentWaitDur', 'abDur', 'opDur', 'currentBigStateTime']

    def __init__(self,
                 eqComponentCount=None,
                 pcType=None,
                 multipleInstances=None,
                 instanceFormat=None,
                 intermittentDurMin=None,
                 intermittentDurMax=None,
                 intermittentWaitDurMin=None,
                 intermittentWaitDurMax=None,
                 pLeakAb=None,
                 abDurMinDays=None,
                 abDurMaxDays=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.eqComponentCount = eqComponentCount
        self.multipleInstances = multipleInstances
        self.instanceFormat = instanceFormat
        # self.pAbnormal = pAbnormal

        self.pcType = pcType

        self.intermittentDurMin = intermittentDurMin  # seconds
        self.intermittentDurMax = intermittentDurMax
        self.intermittentDurDist = d.Uniform({'min': self.intermittentDurMin, 'max': self.intermittentDurMax})
        self.intermittentDurCurrent = 0
        self.intermittentDurNormalDist = d.Uniform({'min': 15, 'max': 60})

        self.intermittentWaitDurMin = intermittentWaitDurMin
        self.intermittentWaitDurMax = intermittentWaitDurMax
        self.intermittentWaitDurDist = d.Uniform({'min': self.intermittentWaitDurMin, 'max': self.intermittentWaitDurMax})
        self.intermittentWaitDurCurrent = 0

        self.pLeakAb = pLeakAb
        self.abDurMinDays = abDurMinDays
        self.abDurMaxDays = abDurMaxDays
        self.abDurMinSec = int(u.daysToSecs(self.abDurMinDays))
        self.abDurMaxSec = int(u.daysToSecs(self.abDurMaxDays))
        self.abDurDist = d.Uniform({'min': self.abDurMinSec, 'max': self.abDurMaxSec})

        self.normalDurMinSec = int((self.abDurMinSec * (1 - self.pLeakAb)) / self.pLeakAb)
        self.normalDurMaxSec = int((self.abDurMaxSec * (1 - self.pLeakAb)) / self.pLeakAb)
        self.opDurDist = d.Uniform({'min': self.normalDurMinSec, 'max': self.normalDurMaxSec})

        self.trackingTime = 0
        self.currentBigState = None
        self.nextBigState = None
        self.intermittentWaitDur = 0
        self.abDur = 0
        self.opDur = 0
        self.currentBigStateTime = 0
        self.isNewState = True
        self.prevFF = 0

        self.stateMachine = {
            'INTERMITTENT_VENT': {'stateDuration': self.getTimeForState,
                                  'nextState': self.getNextStateVent},
            'INTERMITTENT_VENT_ABNORMAL': {'stateDuration': self.getTimeForState,
                                           'nextState': self.getNextStateAbVent},
            'INTERMITTENT_VENT_WAIT': {'stateDuration': self.getTimeForState,
                                       'nextState': self.getNextStateIntWait},
            'NOT_OPERATING': {'stateDuration': self.getTimeForState,
                              'nextState': 'INTERMITTENT_VENT_WAIT'},
        }

    def getTimeForState(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        cs = currentStateInfo.stateName
        # self.updateBigStateTime(cs)
        if cs == 'INTERMITTENT_VENT':
            delay = int(self.intermittentDurNormalDist.pick())
        elif cs == 'INTERMITTENT_VENT_ABNORMAL':
            delay = int(self.intermittentDurDist.pick())
        elif cs == 'INTERMITTENT_VENT_WAIT':
            delay = self.intermittentWaitDur
        elif cs == 'NOT_OPERATING':
            delay = 20
        else:
            msg = f'No state {cs} found'
            raise NotImplementedError(msg)
        self.trackingTime += delay
        return delay

    def updateIntWaitDur(self, currentTime):
        if self.inletFluidFlows:
            if ('Water' or 'Condensate') in self.inletFluidFlows:
                ret = self.getMinChangeTimeLiquids(self.inletFluidFlows) - currentTime
            elif 'Vapor' in self.inletFluidFlows:
                ret = self.getMinChangeTimeVapor(self.inletFluidFlows) - currentTime    
        else:
            ret = int(self.intermittentWaitDurDist.pick())
        return ret

    def getNextStateVent(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        cs = currentStateInfo.stateName
        self.currentBigState = cs
        self.nextBigState = 'INTERMITTENT_VENT_ABNORMAL'
        self.intermittentWaitDur = self.updateIntWaitDur(currentTime)
        self.prevFF = self.getTotalFlowRateLiquids(self.inletFluidFlows)
        return 'INTERMITTENT_VENT_WAIT'

    def getNextStateAbVent(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        cs = currentStateInfo.stateName
        self.currentBigState = cs
        self.nextBigState = 'INTERMITTENT_VENT'
        self.intermittentWaitDur = self.updateIntWaitDur(currentTime)
        self.prevFF = self.getTotalFlowRateLiquids(self.inletFluidFlows)
        return 'INTERMITTENT_VENT_WAIT'

    def getNextStateIntWait(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        cs = currentStateInfo.stateName
        nextState = self.getNextStateFluidFlows(currentStateData, currentStateInfo, currentTime)
        return nextState

    def getNextStateFluidFlows(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        cs = currentStateInfo.stateName
        if self.trackingTime > self.currentBigStateTime:
            nextState = self.nextBigState
            self.currentBigStateTime = self.resetBigStateTime(nextState)
            self.trackingTime = 0
            nextState = self.checkPseudoStateChange(nextState, currentTime)
        else:
            nextState = self.currentBigState
            nextState = self.checkPseudoStateChange(nextState, currentTime)
        return nextState

    def checkPseudoStateChange(self, ns, currentTime=None):
        if self.inletFluidFlows:
            nextFF = self.getTotalFlowRateLiquids(self.inletFluidFlows)
            if nextFF == self.prevFF:
                nextState = 'INTERMITTENT_VENT_WAIT'
                self.intermittentWaitDur = self.updateIntWaitDur(currentTime)
                self.prevFF = nextFF
            else:
                nextState = ns
        else:
            nextState = ns
        return nextState

    def resetBigStateTime(self, nextState):
        if nextState == 'INTERMITTENT_VENT_ABNORMAL':
            ret = int(self.abDurDist.pick())
        else:
            ret = int(self.opDurDist.pick())
        return ret

    def nextSmallState(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        pass

    def initialStateTimes(self, **kwargs):
        # initial state time same as upstream?
        # how to set up FFs?
        self.abDur = int(self.abDurDist.pick())
        self.opDur = int(self.opDurDist.pick())
        bigStateChoice = {'INTERMITTENT_VENT': self.opDur, 'INTERMITTENT_VENT_ABNORMAL': self.abDur}
        retNew = UnscaledEmpiricalDistChooser(bigStateChoice).randomChoice()
        if retNew == 'INTERMITTENT_VENT':
            ret = {'INTERMITTENT_VENT': int(self.intermittentDurNormalDist.pick())}
        else:
            ret = {'INTERMITTENT_VENT_ABNORMAL': int(self.intermittentDurDist.pick())}
        # ret = {'INTERMITTENT_VENT': int(self.intermittentDurDist.pick()),
        #        'INTERMITTENT_VENT_ABNORMAL': int(self.intermittentDurDist.pick())}
        return ret

    def initialStateUpdate(self, stateName, stateDuration, currentTime):
        if stateName == 'INTERMITTENT_VENT':
            self.currentBigStateTime = self.opDur
        else:
            self.currentBigStateTime = self.abDur
        ret = super().initialStateUpdate(stateName, stateDuration, currentTime)
        self.currentBigState = stateName
        self.prevFF = self.getTotalFlowRateLiquids(self.inletFluidFlows)
        return ret

    def linkInletFlow(self, outletME, flow):
        pass
        # inlet gas flows same as outlets. Only using factors for now.
        # How to set up valve switches?
        self.addInletFluidFlow(flow)
        self.addOutletFluidFlow(flow)




class MEETHeater(mc.MajorEquipment, mc.StateEnabledVolume):

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['stateMachine', 'fuelConsumption', 'opMinSec', 'opMaxSec', 'opDurDist',
                                         'malfMinSec', 'malfMaxSec', 'malfDurDist', 'totalFF',
                                         'shutInMinSec', 'shutInMaxSec', 'shutInDurDist']

    def __init__(self,
                 heaterPowerKW=None,
                 opDE=None,
                 opMinDays=None,
                 opMaxDays=None,
                 pMalf=None,
                 malfMinDays=None,
                 malfMaxDays=None,
                 malfDE=None,
                 pShutIn=None,
                 shutInMinDays=None,
                 shutInMaxDays=None,
                 **kwargs
                 ):
        super().__init__(**kwargs)
        self.heaterPowerKW = heaterPowerKW
        self.fuelConsumption = 0  # initialize

        self.opDE = opDE
        self.opMinDays = opMinDays
        self.opMaxDays = opMaxDays
        self.opMinSec = u.daysToSecs(self.opMinDays)
        self.opMaxSec = u.daysToSecs(self.opMaxDays)
        self.opDurDist = d.Uniform({'min': self.opMinSec, 'max': self.opMaxSec})

        self.pMalf = pMalf
        self.malfMinDays = malfMinDays
        self.malfMaxDays = malfMaxDays
        self.malfDE = malfDE
        self.malfMinSec = u.daysToSecs(self.malfMinDays)
        self.malfMaxSec = u.daysToSecs(self.malfMaxDays)
        self.malfDurDist = d.Uniform({'min': self.malfMinSec, 'max': self.malfMaxSec})

        self.pShutIn = pShutIn
        self.shutInMinDays = shutInMinDays
        self.shutInMaxDays = shutInMaxDays
        self.shutInMinSec = u.daysToSecs(self.shutInMinDays)
        self.shutInMaxSec = u.daysToSecs(self.shutInMaxDays)
        self.shutInDurDist = d.Uniform({'min': self.shutInMinSec, 'max': self.shutInMaxSec})

        self.totalFF = 0

        self.stateMachine = {
            'OPERATING': {'stateDuration': self.getTimeForState,
                          'nextState': {"MALFUNCTIONING": self.pMalf, "SHUT_IN": self.pShutIn}},
            'MALFUNCTIONING': {'stateDuration': self.getTimeForState,
                               'nextState': 'OPERATING'},
            'SHUT_IN': {'stateDuration': self.getTimeForState,
                        'nextState': 'OPERATING'}
        }
        
    def getTimeForState(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        cs = currentStateInfo.stateName
        self.totalFF = sum(map(lambda x: x.driverRate, self.inletFluidFlows['Vapor']))  # assume no mixing of gcs
        if cs == 'OPERATING':
            delay = int(self.opDurDist.pick())
        elif cs == 'MALFUNCTIONING':
            delay = int(self.malfDurDist.pick())
        else:
            delay = int(self.shutInDurDist.pick())
        return delay

    def initialStateTimes(self, **kwargs):
        self.totalFF = sum(map(lambda x: x.driverRate, self.inletFluidFlows['Vapor']))
        ret = {'OPERATING': int(self.opDurDist.pick()),
               'MALFUNCTIONING': int(self.malfDurDist.pick()),
               'SHUT_IN': int(self.shutInDurDist.pick())}
        return ret

    def safeDivByZero(self, a, b):   # returns 0 if div by 0
        return a/b if b else 0

    def createEmitterFlow(self, tag, flow, destructionEfficiency, activeState=None):
        destGC = gc.DestructionGC.destructionEfficiencyFactory(inSpec=destructionEfficiency, origGC=flow.gc)
        lhv = destGC.getLhvVals()
        # lhv = float(flow.gc.gcMetadata['LHV - Stage 1 (kJ/scf)'])
        fuelConsumption = self.heaterPowerKW / lhv    # fuel consumption not in self because we can have multiple gcs
        if flow.driverRate == 0:
            fuelConsumption = 0
        fc = fuelConsumption * self.safeDivByZero(flow.driverRate, self.totalFF)
        flowToEmit = ff.FluidFlow('Vapor', fuelConsumption, 'scf', destGC)
        newFlow = StateDependentFluidFlow(flowToEmit,
                                          gc=destGC,
                                          majorEquipment=self,
                                          activeState=activeState,
                                          rateTransform=lambda x: x * self.safeDivByZero(flow.driverRate, self.totalFF),
                                          secondaryID=tag)
        return newFlow

    def linkInletFlow(self, outletME, flow):
        self.addInletFluidFlow(flow)
        if flow.name == 'Vapor':
            # self.addOutletFluidFlow(flow)
            newFlowOp = self.createEmitterFlow('HEATER-OPERATING', flow, self.opDE, activeState='OPERATING')
            newFlowMalf = self.createEmitterFlow('HEATER-MALFUNCTIONING', flow, self.malfDE, activeState='MALFUNCTIONING')
            self.addOutletFluidFlow(newFlowOp)
            self.addOutletFluidFlow(newFlowMalf)
            self.addOutletFluidFlow(ff.DependentFlow.factory(flow,
                                                             secondaryID=flow.secondaryID,
                                                             rateTransform=lambda x: x -
                                                                                     newFlowOp.driverRate -
                                                                                     newFlowMalf.driverRate,
                                                             newUnits=flow.driverUnits,
                                                             gc=flow.gc
                                                             ))
        else:
            self.addOutletFluidFlow(flow)
        pass


class EmpiricalFluidFlow(mc.EmissionManager):
    def __init__(self,
                 gasComposition=None,
                 emissionDriverUnits=None,
                 secondaryID=None,
                 **kwargs
                 ):
        super().__init__(**kwargs)
        self.gasComposition = gasComposition
        self.emissionDriverUnits = emissionDriverUnits or 'scf'
        self.secondaryID = secondaryID

    def initializeFluidFlow(self, simdm):
        facility = simdm.getEquipmentTable().elementLookup(self.facilityID, None, None, self.mcRunNum)
        tmpGC = gc.FluidFlowGC.factory(fluidFlowGCFilename=facility.productionGCFilename,
                                       flow='Vapor',
                                       fluidFlowID=self.gasComposition,
                                       gcUnits=self.emissionDriverUnits
                                       )
        self.fluidFlow = self._calcFlow(simdm, tmpGC)
        # we have the fluid flow -- now set it as an outlet flow from the major equipment
        majorEquipment = simdm.getEquipmentTable().elementLookup(self.facilityID, self.unitID, None, self.mcRunNum)
        majorEquipment.addOutletFluidFlow(self.fluidFlow)

class EmpiricalFlowProduction(mc.FactorManager, EmpiricalFluidFlow):
    def __init__(self,
                 **kwargs
                 ):
        super().__init__(**kwargs)

    def _calcFlow(self, simdm, tmpGC):
        emissionDriverPath = Path(au.expandFilename(simdm.config['emitterProfileDir'], simdm.config)) / self.emissionDriver
        fluidFlow = ff.EmpiricalFluidFlow('Vapor', emissionDriverPath, tmpGC, units=self.emissionDriverUnits, secondaryID=self.secondaryID)
        return fluidFlow

class EmpiricalFlowImmediateProduction(EmpiricalFluidFlow):
    def __init__(self,
                 emissionVolume=None,
                 emissionDuration=None,
                 **kwargs
                 ):
        super().__init__(**kwargs)
        self.emissionVolume = emissionVolume
        self.emissionDuration = emissionDuration

    def _calcFlow(self, simdm, tmpGC):
        driverRateInSecs = self.emissionVolume / self.emissionDuration

        fluidFlow = ff.FluidFlow('Vapor', driverRateInSecs, self.emissionDriverUnits, tmpGC, secondaryID=self.secondaryID)
        fluidFlow.ts = ts.ConstantTimeseriesTableEntry.factory(driverRateInSecs, self.emissionDriverUnits)
        return fluidFlow

def strToBool(value):
    if isinstance(value, bool):
        return value
    elif isinstance(value, str):
        if value.lower() in ('true', 'yes', '1'):
            return True
        elif value.lower() in ('false', 'no', '0'):
            return False
    raise ValueError(f'Cannot convert {value} to boolean')


class EmpiricalFlowFromMajorEquipment(EmpiricalFluidFlow):
    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['crankcaseDist']
    def __init__(self,
                 crankcaseDistrib=None, 
                 cceeFlag=None,
                 **kwargs
                ):
        super().__init__(**kwargs)
        simdm = sdm.SimDataManager.getSimDataManager()
        self.crankcaseDistrib = crankcaseDistrib
        self.cceeFlag = strToBool(cceeFlag)
        self.crankcaseDist = getCrankcaseDist(crankcaseDistrib, simdm)
        i = 10
    
    def initializeFluidFlow(self, simdm):
        pass
    
    def stateChange(self, currentTime, stateInfo, op, delay=0, relatedEvent=0, initiator=None):
        i = 10
        if self.cceeFlag == False:
            pass
        else:
            crankcaseFraction = self.crankcaseDist.pick()
            self.fluidFlow = self.majorEquipment.exhaustFF
            self.fluidFlow.ts = ts.ConstantTimeseriesTableEntry.factory(self.majorEquipment.loadingKW*crankcaseFraction, 'kW')  
            # 14.4% of exhaust emissions calculated exactly after exhaust
            super().stateChange(currentTime, stateInfo, op, delay, relatedEvent, initiator)


def getCrankcaseDist(crankDist, simdm):
    if isinstance(crankDist, int) or isinstance(crankDist, float):  # put to self if it is int
        return d.Constant(crankDist)
    elif isinstance(crankDist, str):  # if str
        adVal = isNumber(crankDist)
        if adVal is not None:  # check if it can be converted to float
            return d.Constant(adVal)
        else:  # distribution if it is dist file
            dataBasePath = Path(au.expandFilename(simdm.config['emitterProfileDir'], simdm.config, readonly=True))
            dataPath = dataBasePath / crankDist
            dist = None if crankDist is None else dp.DistributionProfile.readFile(dataPath)
            return dist
        

class SimpleUpstreamFlowStateEnabled(ABC):
    def __init__(self,
                 transitionTimeForCurrentState=0,
                 **kwargs):
        super().__init__(**kwargs)
        self.transitionTimeForCurrentState = 0

    def nextState(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        if currentTime < self.transitionTimeForCurrentState:
            return currentStateInfo.stateName
        return currentStateData['nextStateProbabilities'].randomChoice()

    def stateDuration(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        if self.transitionTimeForCurrentState == currentTime:
            self.transitionTimeForCurrentState = int(currentStateData['durationForState'].pick()) + currentTime
        upstreamStateTransitionTimeDelta = self.getMinChangeTimeVapor(self.inletFluidFlows) - currentTime
        ttFCS = self.transitionTimeForCurrentState - currentTime
        nextTransitionDelta = min(upstreamStateTransitionTimeDelta, ttFCS)
        ret = nextTransitionDelta
        return ret


class MEETFlare(mc.MajorEquipment, ff.Volume, SimpleUpstreamFlowStateEnabled, mc.StateChangeInitiator):

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['stateMachine',
                                         'opDurDist', 'malfDurDist', 'unlitDurDist']

    def __init__(self,
                 pMalfunction=None,
                 pUnlit=None,
                 malfDurMax=None,
                 malfDurMin=None,
                 malfDestEff=None,
                 unlitDurMin=None,
                 unlitDurMax=None,
                 unlitDestEff=None,
                 opDurMin=None,
                 opDurMax=None,
                 opDestEff=None,
                 fluid=None,
                 **kwargs):
        super().__init__(**kwargs)

        self.fluid = 'Vapor'

        self.pMalfunction = pMalfunction
        self.pUnlit = pUnlit
        self.malfDurMax = malfDurMax
        self.malfDurMin = malfDurMin
        self.malfDestEff = malfDestEff
        self.unlitDurMin = unlitDurMin
        self.unlitDurMax = unlitDurMax
        self.unlitDestEff = unlitDestEff
        self.opDurMin = opDurMin
        self.opDurMax = opDurMax
        self.opDestEff = opDestEff

        self.opDurDist = Uniform({'min': u.daysToSecs(self.opDurMin), 'max': u.daysToSecs(self.opDurMax)})
        self.malfDurDist = Uniform({'min': u.daysToSecs(self.malfDurMin), 'max': u.daysToSecs(self.malfDurMax)})
        self.unlitDurDist = Uniform({'min': u.daysToSecs(self.unlitDurMin), 'max': u.daysToSecs(self.unlitDurMax)})
        self.stateMachine = {
            'OPERATING': {'nextState':     self.nextState,
                          'stateDuration': self.stateDuration,
                          'nextStateProbabilities': sm.toChooser({'UNLIT': self.pUnlit, 'MALFUNCTIONING': self.pMalfunction}),
                          'durationForState': self.opDurDist},
            'UNLIT': {'nextState':     self.nextState,
                      'stateDuration': self.stateDuration,
                      'nextStateProbabilities': sm.toChooser('OPERATING'),
                      'durationForState': self.unlitDurDist},
            'MALFUNCTIONING': {'nextState':     self.nextState,
                               'stateDuration': self.stateDuration,
                               'nextStateProbabilities': sm.toChooser('OPERATING'),
                               'durationForState': self.malfDurDist},
        }

    def initialStateTimes(self):
        stateTimes = {
            'OPERATING':      u.daysToSecs(self.opDurMax),
            'UNLIT':          u.daysToSecs(self.unlitDurMax),
            'MALFUNCTIONING': u.daysToSecs(self.malfDurMax)
        }
        return stateTimes

    def initialStateUpdate(self, randomState, randomStateDelay, currentTime):
        self.transitionTimeForCurrentState = randomStateDelay
        calcStateDuration = self.stateDuration(None, None, currentTime)
        ret = super().initialStateUpdate(randomState, calcStateDuration, currentTime)
        return ret

    def createEmitterFlow(self, tag, flow, destructionEfficiency, activeState='OPERATING'):
        destGC = gc.DestructionGC.destructionEfficiencyFactory(inSpec=destructionEfficiency, origGC=flow.gc)
        newFlow = StateDependentFluidFlow(flow,
                                          gc=destGC,
                                          majorEquipment=self,
                                          activeState=activeState,
                                          secondaryID=tag)
        return newFlow

    def linkInletFlow(self, outletME, flow):
        if self.fluid != flow.name:
            return

        self.addInletFluidFlow(flow)
        self.addOutletFluidFlow(flow)
        # todo: this looks wrong -- it is using the old-style gas comp reference
        self.addOutletFluidFlow(self.createEmitterFlow('Flare-OPERATING',      flow, self.opDestEff,    activeState='OPERATING'))
        self.addOutletFluidFlow(self.createEmitterFlow('Flare-UNLIT', flow, self.unlitDestEff, activeState='UNLIT'))
        self.addOutletFluidFlow(self.createEmitterFlow('Flare-MALFUNCTIONING', flow, self.malfDestEff,  activeState='MALFUNCTIONING'))


class StateDependentFluidFlow(ff.FunctionDependentFluidFlow):
    def __init__(self,
                 origFlow,
                 activeState=None,
                 majorEquipment=None,
                 **kwargs):
        newKwargs = {**kwargs, 'flowFun': self.flowFun}
        super().__init__(origFlow, **newKwargs)
        self.activeState = activeState
        self.majorEquipment = majorEquipment

    def flowFun(self):   # updated flowFun so that initial state is considered for FFs
        if self.majorEquipment.initialStateTransient:
            currentState = self.majorEquipment.initialStateTransient.stateName
        else:
            currentState = self.majorEquipment.stateManager.currentState.stateName
        ret = (currentState == self.activeState)
        return ret

class MEETCommonHeader(mc.MajorEquipment, ff.Volume, mc.DESEnabled):

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['stateMachine',
                                         'linkedUpstreamEquipment',
                                         'stateChangeNotificationRecipients',
                                         'consolidatedFlowTable']

    def __init__(self,
                 componentCount=None,
                 fluid=None,
                 flashGC=None,
                 flowGCTag=None,
                 gcTag=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.componentCount = componentCount
        self.fluid = fluid
        self.linkedUpstreamEquipment = []
        self.stateMachine = {
            'OPERATING': {'stateDuration': self.calculateOperatingDuration, 'nextState': 'OPERATING'},
        }
        self.consolidatedFlowTable = {}

    def getStateMachine(self):
        delay = self.calculateOperatingDuration(0)
        return self.stateMachine, 'OPERATING', delay

    def calcStateTimes(self, **kwargs):
        stateTimes = {}
        for state in self.stateMachine:
            if state == 'OPERATING':
                stateTimes[state] = u.getSimDuration()
        self.stateTimes = stateTimes

    def link2(self, linkedME1):
        self.linkedUpstreamEquipment.append(linkedME1)

    def linkInletFlow(self, outletME, flow):
        if self.fluid != flow.name:
            return

        self.addInletFluidFlow(flow)

        inboundGC = flow.gc
        # Common Headers don't produce flash

        if inboundGC not in self.consolidatedFlowTable:
            self.consolidatedFlowTable[inboundGC] = ff.AggregatedFlow(flow.name, inboundGC, newUnits=flow.driverUnits)
            self.addOutletFluidFlow(self.consolidatedFlowTable[inboundGC])
            # simdm.getGasCompositionFF(outflowFlashGC, self.flashGC)
        self.consolidatedFlowTable[inboundGC].addFlow(flow)
        pass


    # def calculateOperatingDuration(self, currentTime):
    #     return u.getSimDuration()

    def calculateOperatingDuration(self, currentTime=None, **kwargs):
        # todo: Hardcoded to just one upstream device.  Fix this!
        if len(self.linkedUpstreamEquipment) == 0:
            return u.getSimDuration()
        nextUpstreamTS = min(map(lambda x: x.nextStateTransitionTS, self.linkedUpstreamEquipment))
        delay = nextUpstreamTS - currentTime
        self.nextStateTransitionTS = nextUpstreamTS
        return delay

    def instantiateFromTemplate(self, simdm, **kwargs):
        return super().instantiateFromTemplate(simdm, **kwargs)


class MEETBattery2(mc.MajorEquipment, mc.LinkedEquipmentMixin, mc.FFLoggingVolume):

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['stateMachine', 'slope', 'yIntercept', 'currentYIntercept',
                                         'upstreamEquipment', 'sumOfVaporOutletFlows', 'prvSwitch',
                                         'stateChangeNotificationRecipients', 'gasRatio',
                                         'consolidatedFlowTable', 'totalGasVolume', 'flowGCTag', 'gcTag',
                                         'vaporFF', 'consolidatedFlowTableVapor', 'opDur', 'consolidatedTankFlash',
                                         'tankThiefHatchMTTRMinSec', 'tankThiefHatchMTTRMaxSec',
                                         'tankThiefHatchDurDist', 'tankThiefHatchGasFrac',
                                         'tankOverpressureMTTRMinSec', 'tankOverpressureMTTRMaxSec',
                                         'tankOverpressureDurDist', 'tankOverpressureGasFrac', 'currentGasFraction',
                                         'tankOverpressureThresholdScfs', 'inletFlowAtMaxPrimryFlowScfs',
                                         'maxPrimaryOutletFlowScfs', 'primaryEqRatio', 'currentPrimaryEqRatio',
                                         'tankOverpressureMTBFMinSec', 'tankOverpressureMTBFMaxSec',
                                         'tankOverpressureMTBFDurDist', 'overpressureTimeTracker', 'tankVol', 'tankCapacityScf', 'volumeGas']

    def __init__(self,
                 activityDistribution=None,
                 fluid=None,
                 tankOverpressurePLeak=None,
                 tankOverpressureMTTRMinDays=None,
                 tankOverpressureMTTRMaxDays=None,
                 tankOverpressureThresholdScfh=None,
                 inletFlowAtMaxPrimryFlowScfh=None,
                 maxPrimaryOutletFlowScfh=None,
                 tankMode=None,
                 tankControlled=None,
                 tankCapacity=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.activityDistribution = activityDistribution
        self.fluid = fluid
        self.vaporFF = 'Vapor'
        self.flowGCTag = 'Tank'
        self.gcTag = f"{self.flowGCTag}"
        self.upstreamEquipment = []
        self.tankOverpressurePLeak = 0 if tankOverpressurePLeak is None else tankOverpressurePLeak
        self.tankOverpressureMTTRMinDays = 0 if tankOverpressureMTTRMinDays is None else tankOverpressureMTTRMinDays
        self.tankOverpressureMTTRMaxDays = 0 if tankOverpressureMTTRMaxDays is None else tankOverpressureMTTRMaxDays
        self.tankOverpressureMTTRMinSec = u.daysToSecs(self.tankOverpressureMTTRMinDays)
        self.tankOverpressureMTTRMaxSec = u.daysToSecs(self.tankOverpressureMTTRMaxDays)
        self.tankOverpressureDurDist = d.Uniform({'min': self.tankOverpressureMTTRMinSec,
                                                  'max': self.tankOverpressureMTTRMaxSec})
        # MTBFMinHour = (MTTRMinHour * (1 - pLeak)) / pLeak  # calc mtbf
        self.tankOverpressureMTBFMinSec = (self.tankOverpressureMTTRMinSec * (1 - self.tankOverpressurePLeak)) / self.tankOverpressurePLeak
        self.tankOverpressureMTBFMaxSec = (self.tankOverpressureMTTRMaxSec * (1 - self.tankOverpressurePLeak)) / self.tankOverpressurePLeak
        self.tankOverpressureMTBFDurDist = d.Uniform({'min': self.tankOverpressureMTBFMinSec,
                                                     'max': self.tankOverpressureMTBFMaxSec})

        self.tankOverpressureThresholdScfh = 999999 if tankOverpressureThresholdScfh is None else tankOverpressureThresholdScfh
        self.tankOverpressureThresholdScfs = u.scfPerHourToScfPerSec(self.tankOverpressureThresholdScfh)
        self.inletFlowAtMaxPrimryFlowScfh = inletFlowAtMaxPrimryFlowScfh
        self.inletFlowAtMaxPrimryFlowScfs = u.scfPerHourToScfPerSec(self.inletFlowAtMaxPrimryFlowScfh)
        self.maxPrimaryOutletFlowScfh = maxPrimaryOutletFlowScfh
        self.maxPrimaryOutletFlowScfs = u.scfPerHourToScfPerSec(self.maxPrimaryOutletFlowScfh)
        if self.maxPrimaryOutletFlowScfh > self.inletFlowAtMaxPrimryFlowScfh:
            msg = "Maximum Primary Outlet Flow is greater than Inlet Flow at Max Primary Flow, please check mechanistic columns in tanks"
            raise NotImplementedError(msg)
        self.stateMachine = {
            'OPERATING': {'stateDuration': self.getTimeForState, 'nextState': self.getNextState},
            'MECHANISTIC_THIEF_HATCH': {'stateDuration': self.getTimeForState, 'nextState': self.getNextState},
            'PRV': {'stateDuration': self.getTimeForState, 'nextState': self.getNextState}
        }
        self.consolidatedFlowTable = {}
        self.consolidatedFlowTableVapor = {}
        self.consolidatedTankFlash = {}
        self.opDur = 40
        self.primaryEqRatio = self.maxPrimaryOutletFlowScfs / self.tankOverpressureThresholdScfs
        self.currentPrimaryEqRatio = 0
        self.slope = self.getSlope()
        self.yIntercept = self.getYIntercept(self.slope)
        self.currentYIntercept = 0
        self.sumOfVaporOutletFlows = 0
        self.prvSwitch = 0
        self.gasRatio = {}
        self.volumeGas = {}
        self.totalGasVolume = 0
        self.overpressureTimeTracker = 0
        self.tankMode = tankMode
        self.tankControlled = tankControlled
        self.tankVol = 0
        self.tankCapacity = tankCapacity # convert to scf
        self.tankCapacityScf = self.tankCapacity * 1000
        i = 10

    def getSlope(self):
        return 1
    
    def getYIntercept(self, slope):
        return 0

    def getNextState(self, currentStateData=None, currentStateInfo=None, currentTime=None):

        # to set rateTransform equations, use prvSwitch, currentPrimaryEqRatio, currentYIntercept
        # m = currentPrimaryEqRatio
        # x = sumOfVaporOutletFlows
        # c = currentYIntercept
        # check when rate changes in inlet flows to keep track of ff driverRates
        cs = currentStateInfo.stateName
        # check if we want emission factor emissions or mechanistic
        if not self.tankMode:
            self.opDur = u.getSimDuration()
            self.currentPrimaryEqRatio = 1
            self.currentYIntercept = 0
            self.prvSwitch = 0
            return 'OPERATING'

        self.sumOfVaporOutletFlows = 0
        # reset sum of outlet flows for next state.
        # We can use direct ratios here because sep flows come out with change time delay
        if self.vaporFF in self.inletFluidFlows:
            # timeToRateChange = min(map(lambda x: x.changeTimeAbsolute,
            #                            itertools.chain(self.inletFluidFlows[self.vaporFF],
            #                                            self.inletFluidFlows['Water'],
            #                                            self.inletFluidFlows['Condensate'])))
            timeToRateChange = min(map(lambda x: x.changeTimeAbsolute, itertools.chain(self.inletFluidFlows[self.vaporFF],
                                                                                       self.inletFluidFlows[self.fluid])))
            rateChangeDelay = timeToRateChange - currentTime
            self.sumOfVaporOutletFlows = self.sumOfVapors()
            # self.sumOfVaporOutletFlows = sum(map(lambda x: x.driverRate, itertools.chain(self.inletFluidFlows[self.vaporFF])))
            i = 10           # sum of inlet vapor plus flashes = sum of outlet vapors
        else:
            nextState = 'OPERATING'
            self.opDur = u.getSimDuration()
            self.currentPrimaryEqRatio = 1
            return nextState

        # ThiefHatch/Vent opens randomly based on pLeak/MTTR. Keep a check of this when we check for threshold
        # if np.random.random() < self.tankOverpressurePLeak:
        if self.tankControlled:
            nextState = self.getNextStateControlled(currentStateData, currentStateInfo, currentTime, rateChangeDelay)
        else:
            nextState = self.getNextStateUncontrolled(currentStateData, currentStateInfo, currentTime, rateChangeDelay)
        return nextState
    
    def getNextStateUncontrolled(self, currentStateData=None, currentStateInfo=None, currentTime=None, rateChangeDelay=0):
        # nextState = "OPERATING"
        # self.tankVol += self.sumOfVaporOutletFlows * rateChangeDelay
        if self.tankVol > self.tankCapacityScf:
            self.overpressureDur = rateChangeDelay
            self.sumOfVaporOutletFlows = 999999   # implies very little will go to flares, most flows will go to prvs (1/total<<0.1)
            # self.tankVol = 0  # reset tank volume to 0 since we are sending all flows to prvs
            nextState = self.prv(currentStateData, currentStateInfo, currentTime) 
            i = 10

        else:
            self.opDur = rateChangeDelay
            self.tankVol += self.sumOfVaporOutletFlows * rateChangeDelay
            nextState = self.operatingUncontrolled(currentStateData, currentStateInfo, currentTime)
        return nextState

    def getNextStateControlled(self, currentStateData=None, currentStateInfo=None, currentTime=None, rateChangeDelay=0):
        if self.sumOfVaporOutletFlows > self.tankOverpressureThresholdScfs:
            self.overpressureDur = rateChangeDelay
            nextState = self.prv(currentStateData, currentStateInfo, currentTime)
        
        elif self.sumOfVaporOutletFlows < self.tankOverpressureThresholdScfs:
            self.opDur = rateChangeDelay
            nextState = self.operating(currentStateData, currentStateInfo, currentTime)
        return nextState 

    def prv(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        # this is a state change to prv
        # the prv is open and we send all flows to prvs
        nextState = 'PRV'
        self.currentPrimaryEqRatio = 0
        self.currentYIntercept = self.tankOverpressureThresholdScfs # = outlet to flares is at max flow
        self.prvSwitch = 1
        self.slope = 0  
        self.yIntercept = self.maxPrimaryOutletFlowScfs
        # delay = self.getMinChangeTimeLiquids(self.inletFluidFlows)
        return nextState

    def operatingUncontrolled(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        # this is a state change to operating
        # the prv is closed and accumulate gas in the tank, no flows to flares, 
        # OP state in uncontrolled mode just accumulates gas.
        nextState = 'OPERATING'
        self.currentYIntercept = 0
        self.currentPrimaryEqRatio = 0 
        self.prvSwitch = 0  # prv is closed
        # delay = self.getMinChangeTimeLiquids(self.inletFluidFlows)
        return nextState

    def operating(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        # this is a state change to operating
        # the prv is closed and we send all flows to flares
        nextState = 'OPERATING'
        self.currentPrimaryEqRatio = 1
        self.currentYIntercept = 0
        self.prvSwitch = 0
        self.slope = 1
        self.yIntercept = 0
        # delay = self.getMinChangeTimeLiquids(self.inletFluidFlows)
        return nextState

    def calcCurrentMultiplier(self, x):
        # if self.tankOverpressureThresholdScfs < self.sumOfVaporOutletFlows <= self.inletFlowAtMaxPrimryFlowScfs:
        #     currentMultiplier = self.sumOfVaporOutletFlows
        # elif self.sumOfVaporOutletFlows > self.inletFlowAtMaxPrimryFlowScfs:
        #     currentMultiplier = self.sumOfVaporOutletFlows
        # elif self.sumOfVaporOutletFlows < self.tankOverpressureThresholdScfs:
        #     currentMultiplier = self.sumOfVaporOutletFlows
        ret = self.sumOfVaporOutletFlows
        return ret

    def getTimeForState(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        cs = currentStateInfo.stateName
        if cs == 'OPERATING':
            delay = self.opDur
            self.overpressureTimeTracker += delay
        elif cs == 'MECHANISTIC_THIEF_HATCH':
            delay = int(self.tankOverpressureDurDist.pick())
            self.overpressureTimeTracker = 0  # reset time tracking to start mtbf tracking after this state
        elif cs == 'PRV':
            delay = self.overpressureDur
            self.overpressureTimeTracker += delay
        else:
            raise ValueError(f'No state {cs} for class {self.__class__.__name__}')
        # self.updateFlowVolumes(delay)
        return delay

    def getGasFrac(self, distFileName):
        simdm = sdm.SimDataManager.getSimDataManager()
        distPath = Path(au.expandFilename(simdm.config['emitterProfileDir'], simdm.config, readonly=True))
        dist = dp.DistributionProfile.readFile(distPath / distFileName)
        return dist

    def calcStateTimes(self, **kwargs):
        stateTimes = {}
        for state in self.stateMachine:
            if state == "OPERATING":
                stateTimes[state] = u.getSimDuration()
        self.stateTimes = stateTimes

    def sumOfVapors(self):
        # we use consolidatedTankFlash here to get sumOfVaporOutletFlows
        sumOfInletVapors = sum(map(lambda x: x.driverRate, itertools.chain(self.inletFluidFlows[self.vaporFF])))
        sumOfFlashes = sum(self.consolidatedTankFlash[gc].driverRate for gc in self.consolidatedTankFlash)
        return sumOfInletVapors+sumOfFlashes

    def funcInsideThreshold(self, x):
        if self.sumOfVaporOutletFlows > self.tankOverpressureThresholdScfs:
            ret = 1
        return ret

    def sumOfVaporsWaterTank(self):
        sumOfFlashes = sum(self.consolidatedTankFlash[gc].driverRate for gc in self.consolidatedTankFlash)
        return sumOfFlashes
    

    def initialStateTimes(self):
        # ret = {'OPERATING': self.opDur}  # todo randomize this
        # to set rateTransform equations, use prvSwitch, currentPrimaryEqRatio, currentYIntercept
        # m = currentPrimaryEqRatio
        # x = sumOfVaporOutletFlows
        # c = currentYIntercept
        if self.vaporFF in self.inletFluidFlows:
            self.sumOfVaporOutletFlows = self.sumOfVapors()
            if self.sumOfVaporOutletFlows > self.tankOverpressureThresholdScfs:
                nextState = self.prv(currentStateData=None, currentStateInfo=None, currentTime=0)
                delay = self.getMinChangeTimeLiquids(self.inletFluidFlows)
                ret = {nextState: delay}
            elif self.sumOfVaporOutletFlows < self.tankOverpressureThresholdScfs:
                nextState = self.operating(currentStateData=None, currentStateInfo=None, currentTime=0)
                delay = self.getMinChangeTimeLiquids(self.inletFluidFlows)
                ret = {nextState: delay}
            else:
                delay = u.getSimDuration()
        else:
            delay = u.getSimDuration()
            self.sumOfVaporOutletFlows = self.sumOfVaporsWaterTank()
            self.currentYIntercept = 0
            self.currentPrimaryEqRatio = 1
            self.prvSwitch = 0
            ret = {'OPERATING': delay}
        self.tankOverpressureInitDist = d.Uniform({'min': 0,
                                                     'max': self.tankOverpressureMTBFMinSec})
        self.overpressureTimeTracker = self.tankOverpressureInitDist.pick()
        return ret

    def safeDivByZero(self, a, b):   # returns 0 if div by 0
        return a/b if b else 0

    def zeroShift(self, out, val):   # outlet must be 0 if inlet is zero. This is to avoid +- values in FFs
        return out if val else 0
    
    def getSlopeNew(self, x):
        return 1

    def getXForFF(self, x, inboundGC):
        if self.tankControlled:
            # newRateTransform = x
            newRateTransform = self.sumOfVaporOutletFlows
        else:
            newRateTransform = self.safeDivByZero(self.volumeGas[inboundGC], self.totalVolume)
        return newRateTransform

    def getRatio(self, x):
        ratio = 1
        # if self.tankControlled: 
        #     ratio = 1
        return ratio

    def updateRateTransform(self, x, inboundGC):
        # slope = self.getSlopeNew(x)
        # ratio = self.getRatio(x)
        newRateTransform = self.getXForFF(x, inboundGC)
        ret = self.slope*newRateTransform + self.yIntercept
        return ret
    

    # def updateFlowVolumes(self, delay):
    #     for gc in self.volumeCondensate.keys():
    #         self.volumeCondensate[gc] += sum(map(lambda x: x.driverRate * delay, self.consolidatedFlowTableCondensate[gc].subFlows))
    #         i = 10
    #     for gc in self.volumeWater.keys():
    #         self.volumeWater[gc] += sum(map(lambda x: x.driverRate * delay, self.consolidatedFlowTableWater[gc].subFlows))
    #     self.totalVolume += self.getTotalFlowRateLiquids(self.inletFluidFlows) * delay
    #     pass


    # self.consolidatedFlowTableCondensate[inboundGC] = ff.CompositeFlow(flow.name, inboundGC, newUnits=flow.driverUnits)
    # self.volumeCondensate[inboundGC] = 0
    # self.addOutletFluidFlow(ff.DependentFlowWithIndependentChangeTime.factory(origFlow=self.consolidatedFlowTableCondensate[inboundGC],
    #                                                                           rateTransform=lambda x: self.safeDivByZero(self.volumeCondensate[inboundGC], self.totalVolume) *
    #                                                                                                   self.currentOutletDriverMultiplier,
    #                                                                           gc=flow.gc.derive(gcName),
    #                                                                           secondaryID='condensate_flow'
    #                                                                           ))
    # bool for flare vs prv

    def linkInletFlow(self, outletME, flow):
        if self.fluid == flow.name:
            self.addInletFluidFlow(flow)
            inboundGC = flow.gc
            outflowGCName = inboundGC
            outflowFlashGC = genGCName('Tank', inboundGC, process='Flash')

            # define outlet flows based on the inlet flow GC.  If we have seen this GC before, reuse it
            # otherwise:
            #   define an Aggregated flow to receive this (and other flows of this GC)
            #   define an output flow corresponding to the inlet flow
            #   define a flash flow
            if inboundGC not in self.consolidatedFlowTable:
                self.consolidatedFlowTable[inboundGC] = ff.AggregatedFlow(flow.name, inboundGC, newUnits=flow.driverUnits)
                self.addOutletFluidFlow(self.consolidatedFlowTable[inboundGC])
                self.gasRatio[inboundGC] = 0   # todo do not assume all flashes go to flares
                self.volumeGas[inboundGC] = 0
                self.addOutletFluidFlow(ff.DependentFlow.factory(self.consolidatedFlowTable[inboundGC],
                                                                 rateTransform=lambda x: self.zeroShift(self.updateRateTransform(x, inboundGC) *
                                                                                         self.safeDivByZero(x,
                                                                                         self.calcCurrentMultiplier(x)),
                                                                                         x),
                                                                 newName='Vapor',
                                                                 gc=inboundGC.derive(outflowFlashGC, flow='Vapor'),
                                                                 newUnits='scf',
                                                                 secondaryID='tank_flash'
                                                                 ))

                self.addOutletFluidFlow(ff.DependentFlow.factory(self.consolidatedFlowTable[inboundGC],
                                                                 rateTransform=lambda x: self.zeroShift(x - (self.updateRateTransform(x, inboundGC) *
                                                                                         self.safeDivByZero(x,
                                                                                         self.calcCurrentMultiplier(x))),
                                                                                         x) * self.prvSwitch,
                                                                 newName='Vapor',
                                                                 gc=inboundGC.derive(outflowFlashGC, flow='Vapor'),
                                                                 newUnits='scf',
                                                                 secondaryID='tank_flash_emitted_gas'
                                                                 ))
                # add tank flash in Aggregated Flow to use later when we decide what goes to flare and prv
                tempFlash = ff.DependentFlow.factory(self.consolidatedFlowTable[inboundGC],
                                                          rateTransform=lambda x: x,
                                                          newName='Vapor',
                                                          gc=inboundGC.derive(outflowFlashGC, flow='Vapor'),
                                                          newUnits='scf',
                                                          secondaryID='tf'
                                                          )
                self.consolidatedTankFlash[inboundGC] = ff.AggregatedFlow(tempFlash.name, tempFlash.gc, newUnits=tempFlash.driverUnits)
                # use tempFlash/consolidatedTankFlash for storing the tank flash value and use it to determine
                # self.sumOfVaporOutletFlows in the tank. Ratios are calculated later by x/sumOfVaporOutletFlows
                self.consolidatedTankFlash[inboundGC].addFlow(tempFlash)
            self.consolidatedFlowTable[inboundGC].addFlow(flow)

        elif flow.name == 'Vapor':
            self.addInletFluidFlow(flow)
            inboundGC = flow.gc
            outflowGCName = inboundGC
            outflowFlashGC = flow.gc.fluidFlowID
            if inboundGC not in self.consolidatedFlowTableVapor:
                self.consolidatedFlowTableVapor[inboundGC] = ff.AggregatedFlow(flow.name, inboundGC, newUnits=flow.driverUnits)
                # outletFlows should be zeros for zero inletFlows, since we're adding a y-intercept
                # multiply by ratio, we can do this since the timing is handled by dumping separator class
                self.gasRatio[inboundGC] = 0   # set up volume ratios
                self.addOutletFluidFlow(ff.DependentFlow.factory(self.consolidatedFlowTableVapor[inboundGC],
                                                                 rateTransform=lambda x: self.zeroShift(self.updateRateTransform(x, inboundGC) *
                                                                                         self.safeDivByZero(x,
                                                                                         self.calcCurrentMultiplier(x)),
                                                                                         x),
                                                                 secondaryID='tank_gas_outlet',
                                                                 gc=inboundGC,
                                                                 newUnits='scf'
                                                                 ))
                self.addOutletFluidFlow(ff.DependentFlow.factory(self.consolidatedFlowTableVapor[inboundGC],
                                                                 # rateTransform=lambda x: x * (1 - self.currentPrimaryEqRatio) + self.yIntercept,
                                                                 rateTransform=lambda x: self.zeroShift(x - (self.updateRateTransform(x, inboundGC) *
                                                                                         self.safeDivByZero(x,
                                                                                         self.calcCurrentMultiplier(x))),
                                                                                         x) * self.prvSwitch,
                                                                 secondaryID='emitted_gas',
                                                                 gc=inboundGC,
                                                                 newUnits='scf'
                                                                 ))
            self.consolidatedFlowTableVapor[inboundGC].addFlow(flow)
            i = 10
        pass

    def calculateOperatingDuration(self, currentTime=None, **kwargs):
        return u.getSimDuration()

    def instantiateFromTemplate(self, simdm, **kwargs):
        inst = super().instantiateFromTemplate(simdm, **kwargs)
        if inst is None:
            return None

        return inst

class MEETBattery3(mc.MajorEquipment, mc.LinkedEquipmentMixin, mc.FFLoggingVolume):

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['stateMachine', 'slope', 'yIntercept', 'currentYIntercept',
                                         'upstreamEquipment', 'sumOfVaporOutletFlows', 'prvSwitch',
                                         'stateChangeNotificationRecipients', 'gasRatio',
                                         'consolidatedFlowTable', 'totalGasVolume', 'flowGCTag', 'gcTag',
                                         'vaporFF', 'consolidatedFlowTableVapor', 'opDur', 'consolidatedTankFlash',
                                         'tankThiefHatchMTTRMinSec', 'tankThiefHatchMTTRMaxSec',
                                         'tankThiefHatchDurDist', 'tankThiefHatchGasFrac',
                                         'tankOverpressureMTTRMinSec', 'tankOverpressureMTTRMaxSec',
                                         'tankOverpressureDurDist', 'tankOverpressureGasFrac', 'currentGasFraction',
                                         'tankOverpressureThresholdScfs', 'inletFlowAtMaxPrimryFlowScfs',
                                         'maxPrimaryOutletFlowScfs', 'primaryEqRatio', 'currentPrimaryEqRatio',
                                         'tankOverpressureMTBFMinSec', 'tankOverpressureMTBFMaxSec',
                                         'tankOverpressureMTBFDurDist', 'overpressureTimeTracker', 'maxPrimaryOutletFlowScfh']

    def __init__(self,
                 activityDistribution=None,
                 fluid=None,
                 tankOverpressurePLeak=None,
                 tankOverpressureMTTRMinDays=None,
                 tankOverpressureMTTRMaxDays=None,
                 tankOverpressureThresholdScfh=None,
                 tankMode=None,
                 tankControlled=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.activityDistribution = activityDistribution
        self.fluid = fluid
        self.vaporFF = 'Vapor'
        self.flowGCTag = 'Tank'
        self.gcTag = f"{self.flowGCTag}"
        self.upstreamEquipment = []
        self.tankOverpressurePLeak = 0 if tankOverpressurePLeak is None else tankOverpressurePLeak
        self.tankOverpressureMTTRMinDays = 0 if tankOverpressureMTTRMinDays is None else tankOverpressureMTTRMinDays
        self.tankOverpressureMTTRMaxDays = 0 if tankOverpressureMTTRMaxDays is None else tankOverpressureMTTRMaxDays
        self.tankOverpressureMTTRMinSec = u.daysToSecs(self.tankOverpressureMTTRMinDays)
        self.tankOverpressureMTTRMaxSec = u.daysToSecs(self.tankOverpressureMTTRMaxDays)
        self.tankOverpressureDurDist = d.Uniform({'min': self.tankOverpressureMTTRMinSec,
                                                  'max': self.tankOverpressureMTTRMaxSec})
        # MTBFMinHour = (MTTRMinHour * (1 - pLeak)) / pLeak  # calc mtbf
        self.tankOverpressureMTBFMinSec = (self.tankOverpressureMTTRMinSec * (1 - self.tankOverpressurePLeak)) / self.tankOverpressurePLeak
        self.tankOverpressureMTBFMaxSec = (self.tankOverpressureMTTRMaxSec * (1 - self.tankOverpressurePLeak)) / self.tankOverpressurePLeak
        self.tankOverpressureMTBFDurDist = d.Uniform({'min': self.tankOverpressureMTBFMinSec,
                                                     'max': self.tankOverpressureMTBFMaxSec})

        self.tankOverpressureThresholdScfh = 999999 if tankOverpressureThresholdScfh is None else tankOverpressureThresholdScfh
        self.tankOverpressureThresholdScfs = u.scfPerHourToScfPerSec(self.tankOverpressureThresholdScfh)
        # self.inletFlowAtMaxPrimryFlowScfh = inletFlowAtMaxPrimryFlowScfh
        # self.inletFlowAtMaxPrimryFlowScfs = u.scfPerHourToScfPerSec(self.inletFlowAtMaxPrimryFlowScfh)
        self.maxPrimaryOutletFlowScfh = tankOverpressureThresholdScfh 
        self.maxPrimaryOutletFlowScfs = u.scfPerHourToScfPerSec(self.maxPrimaryOutletFlowScfh)
        # if self.maxPrimaryOutletFlowScfh > self.inletFlowAtMaxPrimryFlowScfh:
        #     msg = "Maximum Primary Outlet Flow is greater than Inlet Flow at Max Primary Flow, please check mechanistic columns in tanks"
        #     raise NotImplementedError(msg)
        self.stateMachine = {
            'OPERATING': {'stateDuration': self.getTimeForState, 'nextState': self.getNextState},
            'VENT_ACC': {'stateDuration': self.getTimeForState, 'nextState': self.getNextState},
            'VENT_REL': {'stateDuration': self.getTimeForState, 'nextState': self.getNextState}
        }
        self.consolidatedFlowTable = {}
        self.consolidatedFlowTableVapor = {}
        self.consolidatedTankFlash = {}
        self.opDur = 40
        self.primaryEqRatio = self.maxPrimaryOutletFlowScfs / self.tankOverpressureThresholdScfs
        self.currentPrimaryEqRatio = 0
        # self.slope = self.getSlope()
        self.yIntercept = 0
        self.currentYIntercept = 0
        self.sumOfVaporOutletFlows = 0
        self.prvSwitch = 0
        self.gasRatio = {}
        self.totalGasVolume = 0
        self.overpressureTimeTracker = 0
        self.tankMode = tankMode
        self.tankControlled = tankControlled

    def getYIntercept(self, slope):   # eq of line y=mx+c
        yInt = self.maxPrimaryOutletFlowScfs - slope * self.inletFlowAtMaxPrimryFlowScfs
        return yInt

    def getSlope(self):  # slope = (y2-y1)/(x2-x1)
        slope = (self.maxPrimaryOutletFlowScfs - self.tankOverpressureThresholdScfs) \
                / (self.inletFlowAtMaxPrimryFlowScfs - self.tankOverpressureThresholdScfs)
        return slope

    def overPressureVars(self, rateChangeDelay):
        nextState = 'VENT_REL'
        self.currentPrimaryEqRatio = 0
        self.currentYIntercept = self.maxPrimaryOutletFlowScfs  # implies outlet to flares is at max flow
        self.overpressureDur = rateChangeDelay
        self.prvSwitch = 1  # everything else goes to prv
        return nextState
    
    # someone left that thing open for no reason / malfunction
    def overPressureVarsAccidental(self, rateChangeDelay):
        nextState = 'VENT_ACC'
        self.currentPrimaryEqRatio = 0   # slope = 1. So all input gas goes to output as it is (y=x+c)
        self.prvSwitch = 1  # prv is malfunctioning so we keep the prv open
        self.currentYIntercept = 0  # no y intercept since all input is going to prvs and not flares (y=x)
        self.sumOfVaporOutletFlows = 999999   # implies very little will go to flares, most flows will go to prvs (1/total<<0.1)
        return nextState
    
    def operatingState(self, rateChangeDelay):
        nextState = 'OPERATING'
        self.currentYIntercept = 0    # all outlet vapor flows go to the flares
        self.currentPrimaryEqRatio = 1
        self.prvSwitch = 0   # prv is closed
        self.opDur = rateChangeDelay
        return nextState

    def getNextState(self, currentStateData=None, currentStateInfo=None, currentTime=None):

        # check when rate changes in inlet flows to keep track of ff driverRates
        cs = currentStateInfo.stateName
        # check if we want emission factor emissions or mechanistic
        if not self.tankMode:
            self.opDur = u.getSimDuration()
            self.currentPrimaryEqRatio = 1
            self.currentYIntercept = 0
            self.prvSwitch = 0
            return 'OPERATING'

        self.sumOfVaporOutletFlows = 0
        # reset sum of outlet flows for next state.
        # We can use direct ratios here because sep flows come out with change time delay
        if self.vaporFF in self.inletFluidFlows:
            timeToRateChange = min(map(lambda x: x.changeTimeAbsolute, itertools.chain(self.inletFluidFlows[self.vaporFF], self.inletFluidFlows[self.fluid])))
            rateChangeDelay = timeToRateChange - currentTime
            self.sumOfVaporOutletFlows = self.sumOfVapors()
        else:
            nextState = 'OPERATING'
            self.opDur = u.getSimDuration()
            self.currentPrimaryEqRatio = 1
            return nextState

        if not self.tankControlled:
            nextState = self.overPressureVarsAccidental(rateChangeDelay)
            return nextState

        # ThiefHatch/Vent opens randomly based on pLeak/MTTR. Keep a check of this when we check for threshold
        # if np.random.random() < self.tankOverpressurePLeak:
        if (self.tankOverpressureMTBFMinSec - self.tankOverpressureMTBFMinSec/10) <= \
                self.overpressureTimeTracker <= \
                (self.tankOverpressureMTBFMaxSec + self.tankOverpressureMTBFMaxSec/10):
            nextState = self.overPressureVarsAccidental(rateChangeDelay)
        elif self.overpressureTimeTracker > self.tankOverpressureMTBFMaxSec:
            nextState = self.overPressureVarsAccidental(rateChangeDelay)
        elif self.sumOfVaporOutletFlows > self.tankOverpressureThresholdScfs:
            nextState = self.overPressureVars(rateChangeDelay)
        elif self.sumOfVaporOutletFlows < self.tankOverpressureThresholdScfs:
            nextState = self.operatingState(rateChangeDelay)
        else:
            msg = 'Cannot set next State in MEETBattery'
            raise NotImplementedError(msg)
        return nextState

    def calcCurrentMultiplier(self, x):
        if self.sumOfVaporOutletFlows < self.tankOverpressureThresholdScfs:
            currentMultiplier = self.sumOfVaporOutletFlows
        else:
            currentMultiplier = self.sumOfVaporOutletFlows
        return currentMultiplier

    def getTimeForState(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        cs = currentStateInfo.stateName
        if cs == 'OPERATING':
            delay = self.opDur
            self.overpressureTimeTracker += delay
        elif cs == 'VENT_ACC':
            delay = int(self.tankOverpressureDurDist.pick())
            self.overpressureTimeTracker = 0  # reset time tracking to start mtbf tracking after this state
        elif cs == 'VENT_REL':
            delay = self.overpressureDur
            self.overpressureTimeTracker += delay
        else:
            raise ValueError(f'No state {cs} for class {self.__class__.__name__}')
        # self.updateFlowVolumes(delay)
        return delay

    def getGasFrac(self, distFileName):
        simdm = sdm.SimDataManager.getSimDataManager()
        distPath = Path(au.expandFilename(simdm.config['emitterProfileDir'], simdm.config, readonly=True))
        dist = dp.DistributionProfile.readFile(distPath / distFileName)
        return dist

    def calcStateTimes(self, **kwargs):
        stateTimes = {}
        for state in self.stateMachine:
            if state == "OPERATING":
                stateTimes[state] = u.getSimDuration()
        self.stateTimes = stateTimes

    def sumOfVapors(self):
        # we use consolidatedTankFlash here to get sumOfVaporOutletFlows
        sumOfInletVapors = sum(map(lambda x: x.driverRate, itertools.chain(self.inletFluidFlows[self.vaporFF])))
        sumOfFlashes = sum(self.consolidatedTankFlash[gc].driverRate for gc in self.consolidatedTankFlash)
        return sumOfInletVapors+sumOfFlashes

    def sumOfVaporsWaterTank(self):
        sumOfFlashes = sum(self.consolidatedTankFlash[gc].driverRate for gc in self.consolidatedTankFlash)
        return sumOfFlashes

    def initialStateTimes(self):
        if self.vaporFF in self.inletFluidFlows:
            self.sumOfVaporOutletFlows = self.sumOfVapors()

            if not self.tankControlled:
                delay = self.getMinChangeTimeLiquids(self.inletFluidFlows)
                nextState = self.overPressureVarsAccidental(delay)
                ret = {nextState: delay}
            else:
                if self.sumOfVaporOutletFlows > self.tankOverpressureThresholdScfs:
                    delay = self.getMinChangeTimeLiquids(self.inletFluidFlows)
                    nextState = self.overPressureVars(delay)
                    ret = {nextState: delay}
                else:
                    delay = self.getMinChangeTimeLiquids(self.inletFluidFlows)
                    nextState = self.operatingState(delay)
                    ret = {nextState: delay}
        else:
            delay = u.getSimDuration()
            self.sumOfVaporOutletFlows = self.sumOfVaporsWaterTank()
            self.currentYIntercept = 0
            self.currentPrimaryEqRatio = 1
            self.prvSwitch = 0
            ret = {'OPERATING': delay}
        self.tankOverpressureInitDist = d.Uniform({'min': 0,
                                                     'max': self.tankOverpressureMTBFMinSec})
        self.overpressureTimeTracker = self.tankOverpressureInitDist.pick()
        return ret

    def safeDivByZero(self, a, b):   # returns 0 if div by 0
        return a/b if b else 0

    def zeroShift(self, out, val):   # outlet must be 0 if inlet is zero. This is to avoid +- values in FFs
        return out if val else 0

    def linkInletFlow(self, outletME, flow):
        if self.fluid == flow.name:
            self.addInletFluidFlow(flow)
            inboundGC = flow.gc
            outflowGCName = inboundGC
            outflowFlashGC = genGCName('Tank', inboundGC, process='Flash')

            # define outlet flows based on the inlet flow GC.  If we have seen this GC before, reuse it
            # otherwise:
            #   define an Aggregated flow to receive this (and other flows of this GC)
            #   define an output flow corresponding to the inlet flow
            #   define a flash flow
            if inboundGC not in self.consolidatedFlowTable:
                self.consolidatedFlowTable[inboundGC] = ff.AggregatedFlow(flow.name, inboundGC, newUnits=flow.driverUnits)
                self.addOutletFluidFlow(self.consolidatedFlowTable[inboundGC])
                self.gasRatio[inboundGC] = 0   # todo do not assume all flashes go to flares
                self.addOutletFluidFlow(ff.DependentFlow.factory(self.consolidatedFlowTable[inboundGC],
                                                                 rateTransform=lambda x: self.zeroShift((self.sumOfVaporOutletFlows *
                                                                                         self.currentPrimaryEqRatio +
                                                                                         self.currentYIntercept) *
                                                                                         self.safeDivByZero(x,
                                                                                         self.calcCurrentMultiplier(x)),
                                                                                         x),
                                                                 newName='Vapor',
                                                                 gc=inboundGC.derive(outflowFlashGC, flow='Vapor'),
                                                                 newUnits='scf',
                                                                 secondaryID='tank_flash'
                                                                 ))
                self.addOutletFluidFlow(ff.DependentFlow.factory(self.consolidatedFlowTable[inboundGC],
                                                                 rateTransform=lambda x: self.zeroShift(x - ((self.sumOfVaporOutletFlows *
                                                                                         self.currentPrimaryEqRatio +
                                                                                         self.currentYIntercept) *
                                                                                         self.safeDivByZero(x,
                                                                                         self.calcCurrentMultiplier(x))),
                                                                                         x) * self.prvSwitch,
                                                                 newName='Vapor',
                                                                 gc=inboundGC.derive(outflowFlashGC, flow='Vapor'),
                                                                 newUnits='scf',
                                                                 secondaryID='tank_flash_emitted_gas'
                                                                 ))
                # add tank flash in Aggregated Flow to use later when we decide what goes to flare and prv
                tempFlash = ff.DependentFlow.factory(self.consolidatedFlowTable[inboundGC],
                                                          rateTransform=lambda x: x,
                                                          newName='Vapor',
                                                          gc=inboundGC.derive(outflowFlashGC, flow='Vapor'),
                                                          newUnits='scf',
                                                          secondaryID='tf'
                                                          )
                self.consolidatedTankFlash[inboundGC] = ff.AggregatedFlow(tempFlash.name, tempFlash.gc, newUnits=tempFlash.driverUnits)
                # use tempFlash/consolidatedTankFlash for storing the tank flash value and use it to determine
                # self.sumOfVaporOutletFlows in the tank. Ratios are calculated later by x/sumOfVaporOutletFlows
                self.consolidatedTankFlash[inboundGC].addFlow(tempFlash)
            self.consolidatedFlowTable[inboundGC].addFlow(flow)

        elif flow.name == 'Vapor':
            self.addInletFluidFlow(flow)
            inboundGC = flow.gc
            outflowGCName = inboundGC
            outflowFlashGC = flow.gc.fluidFlowID
            if inboundGC not in self.consolidatedFlowTableVapor:
                self.consolidatedFlowTableVapor[inboundGC] = ff.AggregatedFlow(flow.name, inboundGC, newUnits=flow.driverUnits)
                # outletFlows should be zeros for zero inletFlows, since we're adding a y-intercept
                # multiply by ratio, we can do this since the timing is handled by dumping separator class
                self.gasRatio[inboundGC] = 0   # set up volume ratios
                self.addOutletFluidFlow(ff.DependentFlow.factory(self.consolidatedFlowTableVapor[inboundGC],
                                                                 rateTransform=lambda x: self.zeroShift((self.sumOfVaporOutletFlows *
                                                                                         self.currentPrimaryEqRatio +
                                                                                         self.currentYIntercept) *
                                                                                         self.safeDivByZero(x,
                                                                                         self.calcCurrentMultiplier(x)),
                                                                                         x),
                                                                 secondaryID='tank_gas_outlet',
                                                                 gc=inboundGC,
                                                                 newUnits='scf'
                                                                 ))
                self.addOutletFluidFlow(ff.DependentFlow.factory(self.consolidatedFlowTableVapor[inboundGC],
                                                                 # rateTransform=lambda x: x * (1 - self.currentPrimaryEqRatio) + self.yIntercept,
                                                                 rateTransform=lambda x: self.zeroShift(x - ((self.sumOfVaporOutletFlows *
                                                                                         self.currentPrimaryEqRatio +
                                                                                         self.currentYIntercept) *
                                                                                         self.safeDivByZero(x,
                                                                                            self.calcCurrentMultiplier(x))),
                                                                                         x) * self.prvSwitch,
                                                                 secondaryID='emitted_gas',
                                                                 gc=inboundGC,
                                                                 newUnits='scf'
                                                                 ))
            self.consolidatedFlowTableVapor[inboundGC].addFlow(flow)
            i = 10
        pass

    def calculateOperatingDuration(self, currentTime=None, **kwargs):
        return u.getSimDuration()

    def instantiateFromTemplate(self, simdm, **kwargs):
        inst = super().instantiateFromTemplate(simdm, **kwargs)
        if inst is None:
            return None

        return inst


class MEETBattery(mc.MajorEquipment, mc.LinkedEquipmentMixin, mc.FFLoggingVolume):

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['stateMachine', 'slope', 'yIntercept', 'currentYIntercept',
                                         'upstreamEquipment', 'sumOfVaporOutletFlows', 'prvSwitch',
                                         'stateChangeNotificationRecipients', 'gasRatio',
                                         'consolidatedFlowTable', 'totalGasVolume', 'flowGCTag', 'gcTag',
                                         'vaporFF', 'consolidatedFlowTableVapor', 'opDur', 'consolidatedTankFlash',
                                         'tankThiefHatchMTTRMinSec', 'tankThiefHatchMTTRMaxSec',
                                         'tankThiefHatchDurDist', 'tankThiefHatchGasFrac',
                                         'tankOverpressureMTTRMinSec', 'tankOverpressureMTTRMaxSec',
                                         'tankOverpressureDurDist', 'tankOverpressureGasFrac', 'currentGasFraction',
                                         'tankOverpressureThresholdScfs', 'inletFlowAtMaxPrimryFlowScfs',
                                         'maxPrimaryOutletFlowScfs', 'primaryEqRatio', 'currentPrimaryEqRatio',
                                         'tankOverpressureMTBFMinSec', 'tankOverpressureMTBFMaxSec',
                                         'tankOverpressureMTBFDurDist', 'overpressureTimeTracker']

    def __init__(self,
                 activityDistribution=None,
                 fluid=None,
                 tankOverpressurePLeak=None,
                 tankOverpressureMTTRMinDays=None,
                 tankOverpressureMTTRMaxDays=None,
                 tankOverpressureThresholdScfh=None,
                 inletFlowAtMaxPrimryFlowScfh=None,
                 maxPrimaryOutletFlowScfh=None,
                 tankMode=None,
                 tankControlled=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.activityDistribution = activityDistribution
        self.fluid = fluid
        self.vaporFF = 'Vapor'
        self.flowGCTag = 'Tank'
        self.gcTag = f"{self.flowGCTag}"
        self.upstreamEquipment = []
        self.tankOverpressurePLeak = 0 if tankOverpressurePLeak is None else tankOverpressurePLeak
        self.tankOverpressureMTTRMinDays = 0 if tankOverpressureMTTRMinDays is None else tankOverpressureMTTRMinDays
        self.tankOverpressureMTTRMaxDays = 0 if tankOverpressureMTTRMaxDays is None else tankOverpressureMTTRMaxDays
        self.tankOverpressureMTTRMinSec = u.daysToSecs(self.tankOverpressureMTTRMinDays)
        self.tankOverpressureMTTRMaxSec = u.daysToSecs(self.tankOverpressureMTTRMaxDays)
        self.tankOverpressureDurDist = d.Uniform({'min': self.tankOverpressureMTTRMinSec,
                                                  'max': self.tankOverpressureMTTRMaxSec})
        # MTBFMinHour = (MTTRMinHour * (1 - pLeak)) / pLeak  # calc mtbf
        self.tankOverpressureMTBFMinSec = (self.tankOverpressureMTTRMinSec * (1 - self.tankOverpressurePLeak)) / self.tankOverpressurePLeak
        self.tankOverpressureMTBFMaxSec = (self.tankOverpressureMTTRMaxSec * (1 - self.tankOverpressurePLeak)) / self.tankOverpressurePLeak
        self.tankOverpressureMTBFDurDist = d.Uniform({'min': self.tankOverpressureMTBFMinSec,
                                                     'max': self.tankOverpressureMTBFMaxSec})

        self.tankOverpressureThresholdScfh = 999999 if tankOverpressureThresholdScfh is None else tankOverpressureThresholdScfh
        self.tankOverpressureThresholdScfs = u.scfPerHourToScfPerSec(self.tankOverpressureThresholdScfh)
        self.inletFlowAtMaxPrimryFlowScfh = inletFlowAtMaxPrimryFlowScfh
        self.inletFlowAtMaxPrimryFlowScfs = u.scfPerHourToScfPerSec(self.inletFlowAtMaxPrimryFlowScfh)
        self.maxPrimaryOutletFlowScfh = maxPrimaryOutletFlowScfh
        self.maxPrimaryOutletFlowScfs = u.scfPerHourToScfPerSec(self.maxPrimaryOutletFlowScfh)
        if self.maxPrimaryOutletFlowScfh > self.inletFlowAtMaxPrimryFlowScfh:
            msg = "Maximum Primary Outlet Flow is greater than Inlet Flow at Max Primary Flow, please check mechanistic columns in tanks"
            raise NotImplementedError(msg)
        self.stateMachine = {
            'OPERATING': {'stateDuration': self.getTimeForState, 'nextState': self.getNextState},
            'MECHANISTIC_THIEF_HATCH': {'stateDuration': self.getTimeForState, 'nextState': self.getNextState},
            'PRV': {'stateDuration': self.getTimeForState, 'nextState': self.getNextState}
        }
        self.consolidatedFlowTable = {}
        self.consolidatedFlowTableVapor = {}
        self.consolidatedTankFlash = {}
        self.opDur = 40
        self.primaryEqRatio = self.maxPrimaryOutletFlowScfs / self.tankOverpressureThresholdScfs
        self.currentPrimaryEqRatio = 0
        self.slope = self.getSlope()
        self.yIntercept = self.getYIntercept(self.slope)
        self.currentYIntercept = 0
        self.sumOfVaporOutletFlows = 0
        self.prvSwitch = 0
        self.gasRatio = {}
        self.totalGasVolume = 0
        self.overpressureTimeTracker = 0
        self.tankMode = tankMode
        self.tankControlled = tankControlled

    def getYIntercept(self, slope):   # eq of line y=mx+c
        yInt = self.maxPrimaryOutletFlowScfs - slope * self.inletFlowAtMaxPrimryFlowScfs
        return yInt

    def getSlope(self):  # slope = (y2-y1)/(x2-x1)
        slope = (self.maxPrimaryOutletFlowScfs - self.tankOverpressureThresholdScfs) \
                / (self.inletFlowAtMaxPrimryFlowScfs - self.tankOverpressureThresholdScfs)
        return slope

    def getNextState(self, currentStateData=None, currentStateInfo=None, currentTime=None):

        # check when rate changes in inlet flows to keep track of ff driverRates
        cs = currentStateInfo.stateName
        # check if we want emission factor emissions or mechanistic
        if not self.tankMode:
            self.opDur = u.getSimDuration()
            self.currentPrimaryEqRatio = 1
            self.currentYIntercept = 0
            self.prvSwitch = 0
            return 'OPERATING'

        self.sumOfVaporOutletFlows = 0
        # reset sum of outlet flows for next state.
        # We can use direct ratios here because sep flows come out with change time delay
        if self.vaporFF in self.inletFluidFlows:
            # timeToRateChange = min(map(lambda x: x.changeTimeAbsolute,
            #                            itertools.chain(self.inletFluidFlows[self.vaporFF],
            #                                            self.inletFluidFlows['Water'],
            #                                            self.inletFluidFlows['Condensate'])))
            timeToRateChange = min(map(lambda x: x.changeTimeAbsolute, itertools.chain(self.inletFluidFlows[self.vaporFF],
                                                                                       self.inletFluidFlows[self.fluid])))
            rateChangeDelay = timeToRateChange - currentTime
            self.sumOfVaporOutletFlows = self.sumOfVapors()
            # self.sumOfVaporOutletFlows = sum(map(lambda x: x.driverRate, itertools.chain(self.inletFluidFlows[self.vaporFF])))
            i = 10           # sum of inlet vapor plus flashes = sum of outlet vapors
        else:
            nextState = 'OPERATING'
            self.opDur = u.getSimDuration()
            self.currentPrimaryEqRatio = 1
            return nextState

        # ThiefHatch/Vent opens randomly based on pLeak/MTTR. Keep a check of this when we check for threshold
        # if np.random.random() < self.tankOverpressurePLeak:
        if (self.tankOverpressureMTBFMinSec - self.tankOverpressureMTBFMinSec/10) <= \
                self.overpressureTimeTracker <= \
                (self.tankOverpressureMTBFMaxSec + self.tankOverpressureMTBFMaxSec/10):
            nextState = 'MECHANISTIC_THIEF_HATCH'
            self.currentPrimaryEqRatio = 0   # slope = 1. So all input gas goes to output as it is (y=x+c)
            self.prvSwitch = 1  # prv is malfunctioning so we keep the prv open
            self.currentYIntercept = 0  # no y intercept since all input is going to prvs and not flares (y=x)
            self.sumOfVaporOutletFlows = 999999   # implies very little will go to flares, most flows will go to prvs (1/total<<0.1)
        elif self.overpressureTimeTracker > self.tankOverpressureMTBFMaxSec:
            nextState = 'MECHANISTIC_THIEF_HATCH'
            self.currentPrimaryEqRatio = 0
            self.prvSwitch = 1  # prv is malfunctioning so we keep the prv open
            self.currentYIntercept = 0  # no y intercept since all input is going to prvs and not flares (y=x)
            self.sumOfVaporOutletFlows = 999999   # implies very little will go to flares, most flows will go to prvs (1/total<<0.1)
        # check if AggregateFlow (outlets) < threshold at current time. ThiefHatch/Vent opens when AggregatedFlow > threshold
        elif self.sumOfVaporOutletFlows > self.tankOverpressureThresholdScfs:
            if self.tankOverpressureThresholdScfs < self.sumOfVaporOutletFlows <= self.inletFlowAtMaxPrimryFlowScfs:
                nextState = 'PRV'
                self.overpressureDur = rateChangeDelay
                self.currentPrimaryEqRatio = self.getSlope()  # set the slope of out/in, flare flow gets slower
                self.currentYIntercept = self.getYIntercept(self.slope)
                self.prvSwitch = 1  # prv starts to open, outrate of prv = sum of total vapor flows - flare flow
            else:  # sumOfVaporOutletFlows > inletFlowAtMaxPrimryFlowScfs
                nextState = 'PRV'
                self.currentPrimaryEqRatio = 0
                self.currentYIntercept = self.maxPrimaryOutletFlowScfs  # implies outlet to flares is at max flow
                self.overpressureDur = rateChangeDelay
                self.prvSwitch = 1  # everything else goes to prv
        elif self.sumOfVaporOutletFlows < self.tankOverpressureThresholdScfs:
            nextState = 'OPERATING'
            self.currentYIntercept = 0    # all outlet vapor flows go to the flares
            self.currentPrimaryEqRatio = 1
            self.prvSwitch = 0   # prv is closed
            self.opDur = rateChangeDelay
        else:
            msg = 'Cannot set next State in MEETBattery'
            raise NotImplementedError(msg)
        return nextState

    def calcCurrentMultiplier(self, x):
        if self.tankOverpressureThresholdScfs < self.sumOfVaporOutletFlows <= self.inletFlowAtMaxPrimryFlowScfs:
            currentMultiplier = self.sumOfVaporOutletFlows
        elif self.sumOfVaporOutletFlows > self.inletFlowAtMaxPrimryFlowScfs:
            currentMultiplier = self.sumOfVaporOutletFlows
        elif self.sumOfVaporOutletFlows < self.tankOverpressureThresholdScfs:
            currentMultiplier = self.sumOfVaporOutletFlows
        return currentMultiplier

    def getTimeForState(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        cs = currentStateInfo.stateName
        if cs == 'OPERATING':
            delay = self.opDur
            self.overpressureTimeTracker += delay
        elif cs == 'MECHANISTIC_THIEF_HATCH':
            delay = int(self.tankOverpressureDurDist.pick())
            self.overpressureTimeTracker = 0  # reset time tracking to start mtbf tracking after this state
        elif cs == 'PRV':
            delay = self.overpressureDur
            self.overpressureTimeTracker += delay
        else:
            raise ValueError(f'No state {cs} for class {self.__class__.__name__}')
        # self.updateFlowVolumes(delay)
        return delay

    def getGasFrac(self, distFileName):
        simdm = sdm.SimDataManager.getSimDataManager()
        distPath = Path(au.expandFilename(simdm.config['emitterProfileDir'], simdm.config, readonly=True))
        dist = dp.DistributionProfile.readFile(distPath / distFileName)
        return dist

    def calcStateTimes(self, **kwargs):
        stateTimes = {}
        for state in self.stateMachine:
            if state == "OPERATING":
                stateTimes[state] = u.getSimDuration()
        self.stateTimes = stateTimes

    def sumOfVapors(self):
        # we use consolidatedTankFlash here to get sumOfVaporOutletFlows
        sumOfInletVapors = sum(map(lambda x: x.driverRate, itertools.chain(self.inletFluidFlows[self.vaporFF])))
        sumOfFlashes = sum(self.consolidatedTankFlash[gc].driverRate for gc in self.consolidatedTankFlash)
        return sumOfInletVapors+sumOfFlashes

    def sumOfVaporsWaterTank(self):
        sumOfFlashes = sum(self.consolidatedTankFlash[gc].driverRate for gc in self.consolidatedTankFlash)
        return sumOfFlashes

    def initialStateTimes(self):
        # ret = {'OPERATING': self.opDur}  # todo randomize this
        if self.vaporFF in self.inletFluidFlows:
            self.sumOfVaporOutletFlows = self.sumOfVapors()
            if self.tankOverpressureThresholdScfs < self.sumOfVaporOutletFlows <= self.inletFlowAtMaxPrimryFlowScfs:
                nextState = 'PRV'
                delay = self.getMinChangeTimeLiquids(self.inletFluidFlows)
                self.currentPrimaryEqRatio = self.getSlope()  # set the slope of out/in, flare flow gets slower
                self.currentYIntercept = self.getYIntercept(self.slope)
                self.prvSwitch = 1  # prv starts to open, outrate of prv = sum of total vapor flows - flare flow
                ret = {nextState: delay}
            elif self.sumOfVaporOutletFlows > self.inletFlowAtMaxPrimryFlowScfs:
                nextState = 'PRV'
                self.currentPrimaryEqRatio = 0
                self.currentYIntercept = self.maxPrimaryOutletFlowScfs  # implies outlet to flares is at max flow
                delay = self.getMinChangeTimeLiquids(self.inletFluidFlows)
                self.prvSwitch = 1  # everything else goes to prv
                ret = {nextState: delay}
            elif self.sumOfVaporOutletFlows < self.tankOverpressureThresholdScfs:
                nextState = 'OPERATING'
                self.currentYIntercept = 0  # all outlet vapor flows go to the flares
                self.currentPrimaryEqRatio = 1
                self.prvSwitch = 0  # prv is closed
                delay = self.getMinChangeTimeLiquids(self.inletFluidFlows)
                ret = {nextState: delay}
            else:
                delay = u.getSimDuration()
        else:
            delay = u.getSimDuration()
            self.sumOfVaporOutletFlows = self.sumOfVaporsWaterTank()
            self.currentYIntercept = 0
            self.currentPrimaryEqRatio = 1
            self.prvSwitch = 0
            ret = {'OPERATING': delay}
        self.tankOverpressureInitDist = d.Uniform({'min': 0,
                                                     'max': self.tankOverpressureMTBFMinSec})
        self.overpressureTimeTracker = self.tankOverpressureInitDist.pick()
        return ret

    def safeDivByZero(self, a, b):   # returns 0 if div by 0
        return a/b if b else 0

    def zeroShift(self, out, val):   # outlet must be 0 if inlet is zero. This is to avoid +- values in FFs
        return out if val else 0

    def linkInletFlow(self, outletME, flow):
        if self.fluid == flow.name:
            self.addInletFluidFlow(flow)
            inboundGC = flow.gc
            outflowGCName = inboundGC
            outflowFlashGC = genGCName('Tank', inboundGC, process='Flash')

            # define outlet flows based on the inlet flow GC.  If we have seen this GC before, reuse it
            # otherwise:
            #   define an Aggregated flow to receive this (and other flows of this GC)
            #   define an output flow corresponding to the inlet flow
            #   define a flash flow
            if inboundGC not in self.consolidatedFlowTable:
                self.consolidatedFlowTable[inboundGC] = ff.AggregatedFlow(flow.name, inboundGC, newUnits=flow.driverUnits)
                self.addOutletFluidFlow(self.consolidatedFlowTable[inboundGC])
                self.gasRatio[inboundGC] = 0   # todo do not assume all flashes go to flares
                self.addOutletFluidFlow(ff.DependentFlow.factory(self.consolidatedFlowTable[inboundGC],
                                                                 rateTransform=lambda x: self.zeroShift((self.sumOfVaporOutletFlows *
                                                                                         self.currentPrimaryEqRatio +
                                                                                         self.currentYIntercept) *
                                                                                         self.safeDivByZero(x,
                                                                                         self.calcCurrentMultiplier(x)),
                                                                                         x),
                                                                 newName='Vapor',
                                                                 gc=inboundGC.derive(outflowFlashGC, flow='Vapor'),
                                                                 newUnits='scf',
                                                                 secondaryID='tank_flash'
                                                                 ))
                self.addOutletFluidFlow(ff.DependentFlow.factory(self.consolidatedFlowTable[inboundGC],
                                                                 rateTransform=lambda x: self.zeroShift(x - ((self.sumOfVaporOutletFlows *
                                                                                         self.currentPrimaryEqRatio +
                                                                                         self.currentYIntercept) *
                                                                                         self.safeDivByZero(x,
                                                                                         self.calcCurrentMultiplier(x))),
                                                                                         x) * self.prvSwitch,
                                                                 newName='Vapor',
                                                                 gc=inboundGC.derive(outflowFlashGC, flow='Vapor'),
                                                                 newUnits='scf',
                                                                 secondaryID='tank_flash_emitted_gas'
                                                                 ))
                # add tank flash in Aggregated Flow to use later when we decide what goes to flare and prv
                tempFlash = ff.DependentFlow.factory(self.consolidatedFlowTable[inboundGC],
                                                          rateTransform=lambda x: x,
                                                          newName='Vapor',
                                                          gc=inboundGC.derive(outflowFlashGC, flow='Vapor'),
                                                          newUnits='scf',
                                                          secondaryID='tf'
                                                          )
                self.consolidatedTankFlash[inboundGC] = ff.AggregatedFlow(tempFlash.name, tempFlash.gc, newUnits=tempFlash.driverUnits)
                # use tempFlash/consolidatedTankFlash for storing the tank flash value and use it to determine
                # self.sumOfVaporOutletFlows in the tank. Ratios are calculated later by x/sumOfVaporOutletFlows
                self.consolidatedTankFlash[inboundGC].addFlow(tempFlash)
            self.consolidatedFlowTable[inboundGC].addFlow(flow)

        elif flow.name == 'Vapor':
            self.addInletFluidFlow(flow)
            inboundGC = flow.gc
            outflowGCName = inboundGC
            outflowFlashGC = flow.gc.fluidFlowID
            if inboundGC not in self.consolidatedFlowTableVapor:
                self.consolidatedFlowTableVapor[inboundGC] = ff.AggregatedFlow(flow.name, inboundGC, newUnits=flow.driverUnits)
                # outletFlows should be zeros for zero inletFlows, since we're adding a y-intercept
                # multiply by ratio, we can do this since the timing is handled by dumping separator class
                self.gasRatio[inboundGC] = 0   # set up volume ratios
                self.addOutletFluidFlow(ff.DependentFlow.factory(self.consolidatedFlowTableVapor[inboundGC],
                                                                 rateTransform=lambda x: self.zeroShift((self.sumOfVaporOutletFlows *
                                                                                         self.currentPrimaryEqRatio +
                                                                                         self.currentYIntercept) *
                                                                                         self.safeDivByZero(x,
                                                                                         self.calcCurrentMultiplier(x)),
                                                                                         x),
                                                                 secondaryID='tank_gas_outlet',
                                                                 gc=inboundGC,
                                                                 newUnits='scf'
                                                                 ))
                self.addOutletFluidFlow(ff.DependentFlow.factory(self.consolidatedFlowTableVapor[inboundGC],
                                                                 # rateTransform=lambda x: x * (1 - self.currentPrimaryEqRatio) + self.yIntercept,
                                                                 rateTransform=lambda x: self.zeroShift(x - ((self.sumOfVaporOutletFlows *
                                                                                         self.currentPrimaryEqRatio +
                                                                                         self.currentYIntercept) *
                                                                                         self.safeDivByZero(x,
                                                                                                            self.calcCurrentMultiplier(x))),
                                                                                         x) * self.prvSwitch,
                                                                 secondaryID='emitted_gas',
                                                                 gc=inboundGC,
                                                                 newUnits='scf'
                                                                 ))
            self.consolidatedFlowTableVapor[inboundGC].addFlow(flow)
            i = 10
        pass

    def calculateOperatingDuration(self, currentTime=None, **kwargs):
        return u.getSimDuration()

    def instantiateFromTemplate(self, simdm, **kwargs):
        inst = super().instantiateFromTemplate(simdm, **kwargs)
        if inst is None:
            return None

        return inst


class MEETFFDumpService(et.MEETService, mc.DESOneShot):
    def __init__(self,
                 outFilename=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.outFilename = outFilename

    def timeoutCallback(self, simtime):
        simdm = sdm.SimDataManager.getSimDataManager()
        dumpFile = Path(simdm.config['resultsRoot']) / f"{self.facilityID}.csv"
        # dumpFile = Path(simdm.mcScenarioDir) / f"{self.facilityID}.csv"
        with open(dumpFile, "w", newline='') as oFile:
            dw = csv.DictWriter(oFile, fieldnames=['facilityID', 'unitID',
                                                   'dir', 'type',
                                                   'name', 'secondaryID', 'serialNumber',
                                                   'driverRate', 'driverUnits', 'gc', 'changeTime'])
            dw.writeheader()
            for singleInst in simdm.equipmentTable.equipmentMap.values():
                if not isinstance(singleInst, ff.Volume):
                    continue

                if simdm.mcRunNum != singleInst.mcRunNum:     # check if current mcRunNum is the same as eq mcRunNum
                    continue                                  # skip current loop if they're not equal

                for dir in ['inletFluidFlows', 'outletFluidFlows']:
                    singleFlow = singleInst.__dict__[dir]
                    for flowKey, flows in singleFlow.items():
                        for singleFlow in flows:
                            try:
                                record = {
                                    'facilityID': singleInst.facilityID,
                                    'unitID': singleInst.unitID,
                                    'dir': dir,
                                    'type': singleFlow.__class__.__name__,
                                    'name': singleFlow.name,
                                    'secondaryID': singleFlow.secondaryID,
                                    'serialNumber': singleFlow.serialNumber,
                                    'driverRate': singleFlow.driverRate,
                                    'driverUnits': singleFlow.driverUnits,
                                    'gc': singleFlow.gc.fluidFlowID,
                                    'changeTime': singleFlow.changeTimeAbsolute
                                }
                                dw.writerow(record)
                            except Exception as e:
                                i = 10
                                raise e
            pass


class MEETFFDumpServiceAutomatic(MEETFFDumpService):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class MEETCyclingWell(mc.MajorEquipment, mc.DESStateEnabled, ff.Volume):  # Note that this does *not* inherit from MEETWell, so we can define our own fluid flows

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['stateMachine',
                                         'upstreamEquipment',
                                         'shutInDistribution',
                                         'dumpDistribution',
                                         'oilDumpFlow',
                                         'waterDumpFlow',
                                         'flashDumpFlow',
                                         'condensateGCTag',
                                         'waterGCTag',
                                         'vaporGCTag'
                                         ]

    def __init__(self,
                 flowTag=None,
                 oilBblPerDump=None,
                 gasMcfPerDay=None,
                 waterBblPerDump=None,
                 shutInTimeMin=None,
                 shutInTimeMax=None,
                 dumpTimeMin=None,
                 dumpTimeMax=None,
                 flowGasComposition=None,
                 **kwargs):

        super().__init__(**kwargs)
        self.flowTag = flowTag
        self.oilBblPerDump = oilBblPerDump
        self.gasMcfPerDay = gasMcfPerDay
        self.waterBblPerDump = waterBblPerDump
        self.shutInTimeMin = shutInTimeMin
        self.shutInTimeMax = shutInTimeMax
        self.dumpTimeMin = dumpTimeMin
        self.dumpTimeMax = dumpTimeMax
        self.flowGasComposition = flowGasComposition

        self.condensateGCTag = f"{self.flowTag}-Condensate"
        self.waterGCTag = f"{self.flowTag}-Water"
        self.vaporGCTag = f"{self.flowTag}-Flash"

        self.shutInDistribution = d.Uniform({'min': shutInTimeMin, 'max': shutInTimeMax})
        self.dumpDistribution = d.Uniform({'min': dumpTimeMin, 'max': dumpTimeMax})

        self.stateMachine = {
            'PRODUCING': {'stateDuration': self.calcStateDuration, 'nextState': 'SHUT-IN'},
            'SHUT-IN':   {'stateDuration': self.calcStateDuration, 'nextState': 'PRODUCING'}
        }

    # def getStateMachine(self):
    #     return self.stateMachine, 'SHUT-IN', int(self.shutInDistribution.pick())

    def initializeFluidFlow(self, simdm):
        condensateGC = gc.FluidFlowGC.factory(fluidFlowGCFilename=self.flowGasComposition,
                                              flow='Condensate',
                                              fluidFlowID=self.condensateGCTag,
                                              gcUnits='bbl')
        waterGC = gc.FluidFlowGC.factory(fluidFlowGCFilename=self.flowGasComposition,
                                         flow='Water',
                                         fluidFlowID=self.waterGCTag,
                                         gcUnits='bbl')
        # gasGC = gc.FluidFlowGC(fluidFlowGCFilename=self.flowGasComposition,
        #                        flow='Gas',
        #                        fluidFlowID=self.vaporGCTag,
        #                        units='bbl')

        self.oilDumpFlow = ff.FluidFlow('Condensate', 0, 'bbl', condensateGC) # rate is set to 0 -- will be updated in calcStateDuration
        self.addOutletFluidFlow(self.oilDumpFlow)
        self.waterDumpFlow = ff.FluidFlow('Water', 0, 'bbl', waterGC)
        self.addOutletFluidFlow(self.waterDumpFlow)
        # self.flashDumpFlow = ff.FluidFlow('Gas', u.scfPerDayToScfPerSec(self.gasMcfPerDay * 1000), 'scf', gasGC)
        # self.addOutletFluidFlow(self.flashDumpFlow)

    def calcStateTimes(self, **kwargs):
        stateTimes = {}
        stateTimes = {'PRODUCING': int(self.dumpTimeMax),
                      'SHUT-IN': int(self.shutInTimeMax)}
        self.stateTimes = stateTimes

    def initialStateTimes(self):
        self.calcStateTimes()
        return self.stateTimes

    def initialStateUpdate(self, randomState, randomStateDelay, currentTime):
        si = super().initialStateUpdate(randomState, randomStateDelay, currentTime)
        self.oilDumpFlow.changeTimeAbsolute = si.absoluteTimeInState
        self.waterDumpFlow.changeTimeAbsolute = si.absoluteTimeInState
        return si

    def calcStateDuration(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        state = currentStateInfo.stateName
        if state == 'PRODUCING':
            duration = int(self.dumpDistribution.pick())
            oilDumpRate = self.oilBblPerDump / duration
            self.oilDumpFlow.driverRate = oilDumpRate
            waterDumpRate = self.waterBblPerDump / duration
            self.waterDumpFlow.driverRate = waterDumpRate
        else:
            duration = int(self.shutInDistribution.pick())
            self.oilDumpFlow.driverRate = 0
            self.waterDumpFlow.driverRate = 0
        self.nextStateTransitionTS = currentTime + duration
        self.oilDumpFlow.changeTimeAbsolute = self.nextStateTransitionTS
        self.waterDumpFlow.changeTimeAbsolute = self.nextStateTransitionTS
        return duration

    def registerForStateChangeNotification(self, emitter, stateChange):
        pass


class MEETCyclingWell2(MEETCyclingWell):

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['stateMachine',
                                         'upstreamEquipment',
                                         'shutInDistribution',
                                         'productionDistribution',
                                         'oilDumpFlow',
                                         'waterDumpFlow',
                                         'flashDumpFlow',
                                         'condensateGCTag',
                                         'waterGCTag',
                                         'vaporGCTag'
                                         ]

    def __init__(self,
                 flowTag=None,
                 oilBblPerDay=None,
                 waterBblPerDay=None,
                 gasMcfPerDay=None,
                 totalLiquidsBblPerDump=None,
                 meanDumpTime=None,
                 deltaDumpTime=None,
                 variationShutInTime=None,
                 flowGasComposition=None,
                 **kwargs):

        flowTag = flowTag
        cyclesPerDay, shutInTime = self.calcShutInTime(waterBblPerDay, oilBblPerDay, totalLiquidsBblPerDump, meanDumpTime)
        if shutInTime <= 0:
            msg = 'Oil Production and/or Water Production rates are too high or' \
                  ' Total Liquids Produced is too low for Unit ID '+str(kwargs['unitID'])
            raise ValueError(msg)
        oilBblPerDump = oilBblPerDay / cyclesPerDay
        gasMcfPerDay = gasMcfPerDay
        waterBblPerDump = waterBblPerDay / cyclesPerDay
        shutInTimeMin = shutInTime - ((variationShutInTime * shutInTime) / 100)    # since it is percentage
        shutInTimeMax = shutInTime + ((variationShutInTime * shutInTime) / 100)    # since it is percentage
        dumpTimeMin = meanDumpTime - deltaDumpTime
        dumpTimeMax = meanDumpTime + deltaDumpTime
        flowGasComposition = flowGasComposition
        newArgs = {**kwargs,
                   'flowTag': flowTag,
                   'oilBblPerDump': oilBblPerDump,
                   'gasMcfPerDay': gasMcfPerDay,
                   'waterBblPerDump': waterBblPerDump,
                   'shutInTimeMin': shutInTimeMin,
                   'shutInTimeMax': shutInTimeMax,
                   'dumpTimeMin': dumpTimeMin,
                   'dumpTimeMax': dumpTimeMax,
                   'flowGasComposition': flowGasComposition}
        super().__init__(**newArgs)
        self.flowTag = flowTag
        self.oilBblPerDay = oilBblPerDay
        self.waterBblPerDay = waterBblPerDay
        self.gasMcfPerDay = gasMcfPerDay
        self.totalLiquidsBblPerDump = totalLiquidsBblPerDump
        self.meanDumpTime = meanDumpTime
        self.deltaDumpTime = deltaDumpTime
        self.variationShutInTime = variationShutInTime
        self.flowGasComposition = flowGasComposition

    def calcShutInTime(self, waterBBLperDay, oilBBLperDay, liquidsBBLPerDump, productionTime):
        cyclesPerDay = (waterBBLperDay + oilBBLperDay)/liquidsBBLPerDump   # calc cycles/day
        totalCycleTime = u.SECONDS_PER_DAY / cyclesPerDay                  # calc cycle time (1/freq) in seconds
        shutInTime = totalCycleTime - productionTime                       # calc shut in time
        return cyclesPerDay, shutInTime


class MEETSynchedSeparator(MEETContinuousSeparator):

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['stateMachine',
                                         'linkedUpstreamEquipment',
                                         'nextStateTransitionTS'
                                         ]

    def __init__(self,
                 **kwargs):
        super().__init__(**kwargs)

        self.nextStateTransitionTS = 0

        self.stateMachine = {
            'OPERATING': {'stateDuration': self.calculateOperatingDuration, 'nextState': 'OPERATING'},
            'DUMPING': {'stateDuration': self.dumpDuration, 'nextState': 'OPERATING'},
        }

    def stateFromUpstreamME(self, upstreamME, currentTime):
        if upstreamME.stateManager.currentState in ['PRODUCING', 'DUMPING']:
            retState = 'DUMPING'
        else:
            retState = 'OPERATING'
        if hasattr(upstreamME, 'nextStateTransitionTS'):
            delay = upstreamME.nextStateTransitionTS - currentTime
        else:
            delay = 0
        return retState, delay

    def getStateMachine(self):
        return self.stateMachine

    # def initialStateTimes(self, **kwargs):
    #     stateTimes =

    def calculateOperatingDuration(self, currentTime=None, **kwargs):
        self.nextStateTransitionTS = u.getSimDuration()
        delay = self.nextStateTransitionTS - currentTime
        return delay

    def dumpDuration(self, currentTime=None, **kwargs):
        self.nextStateTransitionTS = u.getSimDuration()
        delay = self.nextStateTransitionTS - currentTime
        return delay

    def instantiateFromTemplate(self, simdm, **kwargs):
        inst = super().instantiateFromTemplate(simdm, **kwargs)
        if inst is None:
            return inst

        inst.linkedUpstreamEquipment = []
        return inst

    def link2(self, linkedME1):
        self.linkedUpstreamEquipment.append(linkedME1)

class MEETProbe(mc.MajorEquipment, mc.FFLoggingVolume):

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['stateMachine']

    def __init__(self,
                 **kwargs):
        super().__init__(**kwargs)
        self.stateMachine = {
            'OPERATING':      {'origNextState': 'OPERATING', 'stateDuration': self.pickOpDuration, 'nextState': 'OPERATING'},
        }

    def linkInletFlow(self, outletME, flow):
        self.addInletFluidFlow(flow)
        self.addOutletFluidFlow(flow)

    def initialStateTimes(self):
        delay = self.pickOpDuration(None, None, 0)
        return {'OPERATING': delay}

    def pickOpDuration(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        ff = list(map(lambda x: x.changeTimeAbsolute, itertools.chain(self.inletFluidFlows.get('Water', []),
                                                                      self.inletFluidFlows.get('Condensate', []),
                                                                      self.inletFluidFlows.get('Vapor', []))))
        opDuration = min(ff) if ff else 0

        ret = opDuration - currentTime
        return ret


class MEETFFEmitter(mc.EmissionManager):

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['majorEquipment']

    def __init__(self,
                 fluid=None,
                 secondaryID=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.statesActive = self.statesActive or 'OPERATING'
        self.fluid = 'Vapor'
        self.fluidFlow = None
        self.secondaryID = secondaryID.split(',') if isinstance(secondaryID, str) else secondaryID
        i = 10

    def initializeFluidFlow(self, simdm):
        super().initializeFluidFlow(simdm)
        self.majorEquipment = simdm.getEquipmentTable().elementLookup(self.facilityID, self.unitID, None, self.mcRunNum)

    def stateChange(self, currentTime, stateInfo, op, delay=0, relatedEvent=0, initiator=None):
        if op != 'START':
            return
        if stateInfo.stateName not in self.activeStatesList:
            return

        deltat = min(map(lambda x: x.changeTimeAbsolute, self.majorEquipment.outletFluidFlows['Vapor'])) - currentTime

        for singleFlow in self.majorEquipment.outletFluidFlows.get('Vapor', []):
            # deltat = singleFlow.changeTimeAbsolute - currentTime
            if singleFlow.secondaryID not in self.secondaryID:
                continue
            singleTS = ts.ConstantTimeseriesTableEntry.factory(singleFlow.driverRate, singleFlow.driverUnits)
            self.eventLogger.logEmission(currentTime, deltat, self.key,
                                         driverTSKey=singleTS.serialNum,
                                         GCKey=singleFlow.gc.serialNum,
                                         flowID=singleFlow.serialNumber,
                                         )

class MEETSimpleEquipment(mc.MajorEquipment, mc.StateEnabledVolume):
    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['stateMachine', 'opDist', 'nopDist']

    def __init__(self,
                 opMin=None,
                 opMax=None,
                 nopMin=None,
                 nopMax=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.opMin = opMin,
        self.opMax = opMax,
        self.nopMin = nopMin,
        self.nopMax = nopMax,

        self.opDist = d.Uniform({'min': opMin, 'max': opMax})
        self.nopDist = d.Uniform({'min': nopMin, 'max': nopMax})
        self.stateMachine = {
            'OPERATING': {'stateDuration': self.pickOpDuration, 'nextState': 'NOTOPERATING'},
            'NOTOPERATING': {'stateDuration': self.pickNopDuration, 'nextState': 'OPERATING'},
        }

    def pickOpDuration(self, *args, **kwargs):
        pass

    def pickNopDuration(self, *args, **kwargs):
        pass

    def linkInletFlow(self, outletME, singleFlow):
        pass

    def initialStateTimes(self):
        raise me.UnknownElementError(f"DESStateEnabled instance ({self.__class__.__name__}, key: {self.key} has no initialStateTimes function defined")

    # Override this if your major equipment needs to do some initialization of its internal state based on
    # the chosen initial state & duration
    def initialStateUpdate(self, stateName, stateDuration, currentTime):
        pass

class MEETGenericStateMachine(mc.MajorEquipment, mc.StateChangeInitiator):

    def __init__(self,
                 stateMachineFile=None,
                 gasComposition=None,
                 factorTag=None,
                 **kwargs
                 ):
        super().__init__(**kwargs)
        self.stateMachineFile = stateMachineFile

    def _transformSM(self, inSM):
        newSM = {}
        for singleState, singleTP in inSM['stateTransitionProbabilities'].items():
            newSM[singleState] = {'nextState': singleTP}

        for singleState, stateTimes in inSM['samples'].items():
            dist = d.Sampled(stateTimes)
            newSM[singleState]['stateDuration'] = dist
        return newSM

    def getStateMachine(self):
        with open(self.stateMachineFile, "r") as iFile:
            sm = json.load(iFile)
        self.stateMachine = self._transformSM(sm)
        self.initialState = sm['firstState']
        return self.stateMachine

    def initialStateTimes(self):
        smData = self.stateMachine[self.initialState]
        return {self.initialState: smData['stateDuration'].pick()}


class MEETDehydrator(mc.MajorEquipment, mc.StateEnabledVolume):

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['stateMachine', 'dryGasFraction', 'flashTankFlashesFraction', 'stillVentEmissionsFraction', 'glycolPumpInjectionRate']

    def __init__(self,
                 glycolType=None,
                 glycolPump=None,
                 flashTank=None,
                 # reboilerCombustionEfficiency=None,
                 # strippingGas=None,
                 # strippingGasType=None,
                 operatingHours=None,
                 leanGlycolCirculationRatio=None,
                 wetGasFlowRate=None,
                 # flareEfficiency=None,
                 wetGasTemperature=None,
                 wetGasPressure=None,
                 leanGlycolCirculationRate=None,
                 tankFlashControlledFlag=None,
                 stillVentControlledFlag=None,
                 strippingGasFlowRate=None,
                 glycolPumpInjectionRatio=None,
                 # glycolPumpInjectionRate=None,
                 # dryGasFraction=None,
                 # flashTankFlashesFraction=None,
                 # stillVentFraction=None,
                 **kwargs
                 ):
        super().__init__(**kwargs)
        self.glycolType = glycolType
        self.glycolPump = glycolPump
        self.flashTank = flashTank if flashTank in [True, False]\
            else (lambda: (_ for _ in ())
                  .throw(ValueError(f"Flash Tank value must be 'TRUE' or 'FALSE' but provided as '{flashTank}'")))()
        # self.reboilerCombustionEfficiency = reboilerCombustionEfficiency
        # self.strippingGasType = strippingGasType
        self.leanGlycolCirculationRatio = leanGlycolCirculationRatio
        self.wetGasFlowRate = wetGasFlowRate
        self.wetGasTemperature = wetGasTemperature
        self.wetGasPressure = wetGasPressure
        self.operatingHours = operatingHours
        self.leanGlycolCirculationRate = leanGlycolCirculationRate
        self.stateMachine = {
            "OPERATING": {
                'stateDuration': self.getTimeForState,
                'nextState': "OPERATING",
            }
        }
        self.dryGasFraction = self.getFFFractions()[0]
        # If flash tank is present
        if flashTank:
            self.flashTankFlashesFraction = self.getFFFractions()[1]
            self.stillVentEmissionsFraction = self.getFFFractions()[2]
        # If no flash tank
        else:
            self.flashTankFlashesFraction = 0
            self.stillVentEmissionsFraction = self.getFFFractions()[1] + self.getFFFractions()[2]

        self.tankFlashControlledFlag = tankFlashControlledFlag
        self.stillVentControlledFlag = stillVentControlledFlag
        self.strippingGasFlowRate = strippingGasFlowRate if strippingGasFlowRate else 0 # convert to scf/sec form scfm
        self.glycolPumpInjectionRatio = glycolPumpInjectionRatio
        self.glycolPumpInjectionRate = glycolPumpInjectionRatio * leanGlycolCirculationRate * self.operatingFraction() / 60 if glycolPumpInjectionRatio else 0  # convert to scf/s from scfm

    # MAES scales emissions per sec to annual, therefore if
    def operatingFraction(self):
        opFrac = self.operatingHours/8760
        return opFrac

    def getFFFractions(self):
        r = self.leanGlycolCirculationRatio
        t = self.wetGasTemperature
        p = self.wetGasPressure
        dryGasCoeff = [99.87968790818823,  1.31338717e-02, 2.73786953e-03, -3.00387534e-05, 1.35887593e-04,
                       -2.84347704e-04, 3.87505361e-06, -1.47187253e-05, 2.40871045e-07, 4.18028576e-09]
        flashTankFlashesCoeff = [0.09695588774199027, -1.21273753e-02, -2.03201528e-03, 9.14327677e-06, -4.98201789e-05,
                                 1.89172288e-04, 7.95605438e-07, 9.54657923e-06, 1.71405401e-07, -1.63953128e-08]
        # can result to a negative for very small fractions
        stillVentEmissionsCoeff = [0.017856850085667668, -7.24314720e-04, -5.83275116e-04, 1.78994646e-05,
                                   -8.23467332e-05, 9.05913783e-05, -4.91216602e-06, 4.78741081e-06,
                                   -4.55718451e-07, 1.65505619e-08]

        terms = [1, r, t, p, r**2, r*t, r*p, t**2, t*p, p**2]
        # terms = [1, r, t, p]
        dryGasFraction = sum(c * t for c, t in zip(dryGasCoeff, terms))/100
        flashTankFlashesFraction = sum(c * t for c, t in zip(flashTankFlashesCoeff, terms))/100
        stillVentEmissionsFraction = sum(c * t for c, t in zip(stillVentEmissionsCoeff, terms))/100
        # stillVentEmissionsFraction = 1 - dryGasFraction - flashTankFlashesFraction
        # if stillVentEmissionsFraction < 0:
        #     stillVentEmissionsFraction = 0.1 * flashTankFlashesFraction
        #     flashTankFlashesFraction = 0.9 * flashTankFlashesFraction

        #  Using average absorption values from research data when any of the fractions is negative
        #  Improve this section, high circulation ratio > 7 gal/lb of H2O
        #  (Run ProMax with high Circulation ratios to see what's the impact on emissions)
        if any(fraction < 0 for fraction in [dryGasFraction, flashTankFlashesFraction, stillVentEmissionsFraction]):
            dryGasFraction = 0.9995568
            flashTankFlashesFraction = 0.0002856
            stillVentEmissionsFraction = 0.0001450

        return [dryGasFraction, flashTankFlashesFraction, stillVentEmissionsFraction]

    #  add a condition to stop if doesn't sum to 1; update this

    # def getNextState(self, currentTime=None, currentStateData=None, currentStateInfo=None):
    #     cs = currentStateInfo.stateName
    #     return cs

    def getTimeForState(self, currentTime=0, currentStateData=None, currentStateInfo=None):
        timeToStateChange = min(map(lambda x: x.changeTimeAbsolute, self.inletFluidFlows['Vapor']))
        delay = timeToStateChange - currentTime

        return delay

    def initialStateTimes(self):
        ret = {'OPERATING': min(map(lambda x: x.changeTimeAbsolute, self.inletFluidFlows['Vapor']))}
        return ret

    def initialStateUpdate(self, stateName, stateDuration, currentTime):
        outflowCounts = len(self.outletFluidFlows['Vapor'])
        # self.strippingGasFlowRate = self.strippingGasFlowRate * 3/outflowCounts  # distribute the stripping gas to the still vent outlet flows
        # self.glycolPumpInjectionRate = self.glycolPumpInjectionRate * 3/outflowCounts  # distribute the pump injection rate to the flash tank outlet flows
        ret = sm.StateInfo(stateName, deltaTimeInState=stateDuration, absoluteTimeInState=currentTime + stateDuration)
        return ret

    def linkInletFlow(self, outletME, flow):
        self.addInletFluidFlow(flow)
        if flow.name == 'Vapor':
            gas_sales_flow = flow if flow.secondaryID == 'gas_sales' else None
            # gas glcol pump emsssions
            # if no flash tank, this emissions will be lost: update this section to emit at the still vent if no flash tank
            if len(self.outletFluidFlows) == 0 and self.glycolPumpInjectionRate > 0:
                if self.flashTank:
                    self.addOutletFluidFlow(ff.DependentFlow.factory(flow,
                                                                     # rateTransform=lambda x: 0.056361544/100 * x,
                                                                     rateTransform=lambda x: self.glycolPumpInjectionRate,
                                                                     newUnits=flow.driverUnits,
                                                                     secondaryID='gycol_pump_flash_tank_emissions',
                                                                     gc=flow.gc
                                                                     ))
                else:
                    self.addOutletFluidFlow(ff.DependentFlow.factory(flow,
                                                                     # rateTransform=lambda x: 0.056361544/100 * x,
                                                                     rateTransform=lambda
                                                                         x: self.glycolPumpInjectionRate,
                                                                     newUnits=flow.driverUnits,
                                                                     secondaryID='gycol_pump_still_vent_emissions',
                                                                     gc=flow.gc
                                                                     ))
                if self.strippingGasFlowRate > 0:
                    self.addOutletFluidFlow(ff.DependentFlow.factory(flow,
                                                                     # rateTransform=lambda x: 0.056361544/100 * x,
                                                                     rateTransform=lambda
                                                                         x: self.strippingGasFlowRate / 60,
                                                                     newUnits=flow.driverUnits,
                                                                     secondaryID='stripping_gas_emissions',
                                                                     gc=flow.gc
                                                                     ))
            # add a stripping gas emissions flow if available
            if len(self.outletFluidFlows) == 0 and self.glycolPumpInjectionRate == 0 and self.strippingGasFlowRate > 0:
                    self.addOutletFluidFlow(ff.DependentFlow.factory(flow,
                                                                     # rateTransform=lambda x: 0.056361544/100 * x,
                                                                     rateTransform=lambda
                                                                         x: self.strippingGasFlowRate / 60,
                                                                     newUnits=flow.driverUnits,
                                                                     secondaryID='stripping_gas_emissions',
                                                                     gc=flow.gc
                                                                     ))
            # dry gas from contactor
            self.addOutletFluidFlow(ff.DependentFlow.factory(flow,
                                                             # rateTransform=lambda x: 99.87764066/100 * x,
                                                             rateTransform=lambda x: self.dryGasFraction * (x - self.glycolPumpInjectionRate if gas_sales_flow else 0),
                                                             newUnits=flow.driverUnits,
                                                             secondaryID='gas_sales',
                                                             gc=flow.gc.derive('DehyContactor-Flash')
                                                             ))
            # flashed gas at the flash tank
            self.addOutletFluidFlow(ff.DependentFlow.factory(flow,
                                                             # rateTransform=lambda x: 0.06511518/100 * x,
                                                             rateTransform=lambda x: self.flashTankFlashesFraction * (x - self.glycolPumpInjectionRate if gas_sales_flow else 0),
                                                             newUnits=flow.driverUnits,
                                                             secondaryID='flash_tank_flashes',
                                                             gc=flow.gc.derive('DehyFlashTank-Flash')
                                                             ))
            # vented gas at the still vent column
            self.addOutletFluidFlow(ff.DependentFlow.factory(flow,
                                                             # rateTransform=lambda x: 0.056361544/100 * x,
                                                             # rateTransform=lambda x: self.stillVentEmissionsFraction * (x - self.glycolPumpInjectionRate if gas_sales_flow else 0) + self.strippingGasFlowRate,
                                                             rateTransform=lambda x: self.stillVentEmissionsFraction * (x - self.glycolPumpInjectionRate if gas_sales_flow else 0),
                                                             newUnits=flow.driverUnits,
                                                             secondaryID='still_vent_emissions',
                                                             gc=flow.gc.derive('DehyStillVent-Flash')
                                                             ))

        pass