import simpy.exceptions

from EquipmentTable import MajorEquipment, Emitter, MEETTemplate, ActivityFactor, EquipmentTableEntry, MEETService
from abc import ABC, abstractmethod
from pathlib import Path
import AppUtils as au
import logging
from StateManager import StateManager, StateInfo
import Units as u
from Chooser import EmpiricalDistChooser, UnscaledEmpiricalDistChooser
import random
import StoredProfile as sp
from EmitterProfile import EmitterProfile
import SimDataManager as sdm
import EmissionDriver as ed
import MEETExceptions as me
import math
import GasComposition3 as gc
import MEETFluidFlow as ff
import MEETExceptions as me
import Distribution as d
import TimeseriesTable as ts
import pandas as pd
import json
import DistributionProfile as dp
import warnings
import operator

#
# Picker Mixins
#


def toProfile(profileFilename):
    simdm = sdm.SimDataManager.getSimDataManager()
    config = simdm.config
    emitterBasePath = Path(au.expandFilename(config['emitterProfileDir'], config))
    emitterProfilePath = emitterBasePath / profileFilename
    profile = sp.ActivityProfile.readProfile(emitterProfilePath)
    return profile

def toDriver4ECProfile(profileFilename):
    simdm = sdm.SimDataManager.getSimDataManager()
    config = simdm.config
    emitterBasePath = Path(au.expandFilename(config['emitterProfileDir'], config))
    emitterProfilePath = emitterBasePath / profileFilename
    profile = sp.Driver4ECProfile.readProfile(emitterProfilePath)
    return profile

def toEmissionDriver(profileFilename):
    simdm = sdm.SimDataManager.getSimDataManager()
    config = simdm.config
    emitterBasePath = Path(au.expandFilename(config['emitterProfileDir'], config))
    emitterProfilePath = emitterBasePath / profileFilename
    driver = ed.EmissionDriver(filename=emitterProfilePath)
    return driver

def isNumber(inVal):
    try:
        return int(float(inVal))
    except:
        return None

def parseActivityDistribution(activityDistribution, simdm, factorTag, unitID):
    if isinstance(activityDistribution, int) or isinstance(activityDistribution, float):  # put to self if it is int
        return d.Constant(int(activityDistribution))
    elif isinstance(activityDistribution, str):  # if str
        adVal = isNumber(activityDistribution)
        if adVal is not None:  # check if it can be converted to int
            return d.Constant(int(adVal))
        else:  # distribution if it is dist file
            dataBasePath = Path(au.expandFilename(simdm.config['emitterProfileDir'], simdm.config, readonly=True))
            dataPath = dataBasePath / activityDistribution
            areFactorsCorrect(simdm, activityDistribution, factorTag, unitID)
            return ed.ActivityProfile(activityDistribution, dataPath).distProfile.distribution
            # activityProfileKey = simdm.getActivityProfile(activityFactor)   # distribution if it is dist file
            # self.activityFactor = simdm.activityProfilePick(activityProfileKey)


def areFactorsCorrect(simdm, factorFilename, factorTag, unitID):
    prodFacType = ['Common', 'Production']
    midFacType = ['Common', 'Midstream']
    if 'facilityType' not in simdm.config:
        msg = (f'Please put a new row in Global Simulation Parameters called "Facility Type". The three values to \n '
               f'be used are [Common, Midstream or Production] depending on the location of the factors used. \n'
               f'Continuing to use "Common"')
        logging.warning(msg)
        facType = 'Common'
    else:
        facType = simdm.config['facilityType'].capitalize().replace('-', '')
    if pd.isna(factorFilename):
        pass
    else:
        facTypeList = [Path(factorFilename).parts[0], 'Common']
        if facType not in facTypeList:
            msg = (f'Factors do not match facility type. Facility is {facType} and factors are for {Path(factorFilename).parts[0]}. '
                   f'\nPlease update the study sheet or factors.csv at {factorFilename}, factorTag = {factorTag}, unitID = {unitID}')
            raise NotImplementedError(msg)
    pass


class FactorManager():
    # filter by factor, modelID, emitter class name in that order
    # select 'Default' factorTag for selecting factors from master emitter list
    # chose a specific factorTag for selecting specific factors. Make sure to add the specific factors in Factors.csv
    # no blanks/optional fields
    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['parsedActivityDistribution']

    def __init__(self,
                 factorTag=None,
                 activityDistribution=None,
                 emitterModelFactorTag=None,
                 emissionDriver=None,
                 **kwargs):
        super().__init__(**kwargs)
        if factorTag in [None, 'Default']:
            msg = f'{self.unitID} does not have a factor tag. Do not use "Default" factor tag'
            logging.error(msg)
            raise NotImplementedError(msg)
        self.factorTag = factorTag
        self.emitterModelFactorTag = emitterModelFactorTag
        simdm = sdm.SimDataManager.getSimDataManager()
        # self.crosscheckEmissions(simdm)
        self.activityDistribution, self.parsedActivityDistribution = self.lookupActivityFactor(self.factorTag, self.emitterModelFactorTag, activityDistribution, simdm, **kwargs)
        self.emissionDriver = self.lookupEmissionFactor(self.factorTag, self.emitterModelFactorTag, emissionDriver, simdm, **kwargs)
        areFactorsCorrect(simdm, self.emissionDriver, factorTag, self.unitID)
        i = 10

    def lookupActivityFactor(self, factorTag, emitterModelFactorTag, activityDistribution, simdm, unitID=None, **kwargs):
        if activityDistribution is not None:  # for component count in study sheet, will also be used in instantiateFromTemplate/instantiateMultiple
            return activityDistribution, parseActivityDistribution(activityDistribution, simdm, factorTag, unitID)
        adMatch2 = self.filterFactor(factorTag, emitterModelFactorTag, simdm, unitID)
        activityDistribution = adMatch2['activityDistribution'].values[0]
        activityDistribution = None if pd.isna(activityDistribution) else activityDistribution
        parsedActivityDistribution = parseActivityDistribution(activityDistribution, simdm, factorTag, unitID)
        return activityDistribution, parsedActivityDistribution

    def lookupEmissionFactor(self, factorTag, emitterModelFactorTag, emissionDriver, simdm,
                             unitID=None,
                             **kwargs):
        if emissionDriver is not None:
            return emissionDriver
        # todo: make au.expandfilename, put file name in errors
        edMatch2 = self.filterFactor(factorTag, emitterModelFactorTag, simdm, unitID)
        emissionDriver = None if pd.isna(edMatch2['emissionDriver'].values[0]) else edMatch2['emissionDriver'].values[0]
        return emissionDriver

    def filterFactor(self, factorTag, emitterModelFactorTag, simdm, unitID):
        filename = simdm.config['factorName']
        factorsDF = pd.read_csv(simdm.config['factorName']).dropna(how='all')
        # self.areFactorsCorrect(simdm, filename, factorTag, unitID)
        match1 = factorsDF[(factorsDF['emitterModelFactorTag'] == emitterModelFactorTag)]
        match2 = match1[(match1['factorTag'] == factorTag)]
        if match2.shape[0] != 1:
            msg = f'No match for emitterModelFactorTag: {emitterModelFactorTag}, factorTag: {factorTag}.' \
                  f'Check emission and activity factor matching for {unitID}, filename: {filename}'
            raise NotImplementedError(msg)
        return match2


class ActivityDistributionEnabled(MEETTemplate):

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['parsedActivityDistribution']

    AD_SERIAL_NUMBER = 0

    def __init__(self,
                 activityInstanceKey=None,
                 instanceSerial=None,
                 activityCategory=None,
                 **kwargs):
        super().__init__(**kwargs)
        # self.activityDistribution = activityDistribution
        # self.parsedActivityDistribution = parseActivityDistribution(self.activityDistribution)
        self.activityCategory = activityCategory
        self.activityInstanceKey = activityInstanceKey
        self.instanceSerial = instanceSerial

    def createActivityFactorInstance(self, activityCount, mcRunNum):
        activityDict = self.__dict__
        activityParams = dict(map(lambda x: (x, activityDict[x]),
                                  EquipmentTableEntry.EQUIPMENT_TABLE_FIELDS
                                  ))
        activityParams['equipmentType'] = 'ActivityFactor'
        activityParams['facilityID'] = activityDict['facilityID']
        activityParams['unitID'] = activityDict['unitID']
        activityParams['emitterID'] = f"AF_{ActivityDistributionEnabled.AD_SERIAL_NUMBER}"
        activityParams['equipmentCount'] = activityCount
        activityParams['implCategory'] = 'ActivityFactor'
        activityParams['mcRunNum'] = mcRunNum
        inst = ActivityFactor(**activityParams)
        ActivityDistributionEnabled.AD_SERIAL_NUMBER += 1

        return inst

    def instantiateMultiple(self,
                            simdm,
                            mcRunNum=-1,
                            **kwargs):
        if self.parsedActivityDistribution:
            activityCount = int(self.parsedActivityDistribution.pick())
        else:
            msg = f"Parameter activityDistribution must be specified for ActivityDistributionEnabled instance"
            logging.error(msg)
            raise me.IllegalArgumentError(msg)

        activityInst = self.createActivityFactorInstance(activityCount, mcRunNum)

        newKwargs = {**kwargs,
                     'activityInstanceKey': activityInst.key,
                     'mcRunNum': mcRunNum,
                     'instanceSerial': 0}
        for instSerial in range(activityCount):
            newKwargs['instanceSerial'] = instSerial
            newKwargs['emitterID'] = None
            inst = self.instantiateFromTemplate(simdm, **newKwargs)
        return activityCount

    def activityPick(self, simdm, mcRunNum=-1):
        activityProfileKey = simdm.getActivityProfile(self.activityProfileName)
        activityCount = simdm.activityProfilePick(activityProfileKey)
        activityDict = self.__dict__
        activityParams = dict(map(lambda x: (x, activityDict[x]),
                                  EquipmentTableEntry.EQUIPMENT_TABLE_FIELDS
                                  ))
        activityParams['equipmentType'] = 'ActivityFactor'
        activityParams['facilityID'] = activityDict['facilityID']
        activityParams['unitID'] = activityDict['unitID']
        activityParams['emitterID'] = activityDict['emitterID']
        activityParams['equipmentCount'] = activityCount
        activityParams['implCategory'] = 'ActivityFactor'
        activityParams['simdm'] = simdm
        activityParams['mcRunNum'] = mcRunNum
        # todo: This should probably use self.activityCategory not 'ActivityFactor'

        inst = ActivityFactor(**activityParams)
        return activityCount, f"{inst.unitID}_{{unitNum}}", inst

    def instantiateFromTemplate(self, simdm,
                                activityInstance=None,
                                instanceCount=None,
                                **kwargs):
        ret = super().instantiateFromTemplate(simdm, **kwargs)
        return ret

class EmissionDistributionEnabled(MEETTemplate):

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['driver4ECProfile']

    def __init__(self, emissionDriver=None, gasComposition = None, driver4EC=None, driver4ECProfile=None, **kwargs):
        super().__init__(**kwargs)
        self.driver4EC = driver4EC
        self.driver4ECProfile = driver4ECProfile
        self.emissionDriver = emissionDriver
        self.gasComposition = gasComposition

    def instantiateFromTemplate(self, simdm, **kwargs):
        if self.emissionDriver:
            profile = simdm.addDriver4EC(name=self.emissionDriver, driver=toEmissionDriver(self.emissionDriver))
            instParams = {**kwargs,
                          'driver4ECProfile': profile}
        else:
            instParams = kwargs
        ret = super().instantiateFromTemplate(simdm, **instParams)
        return ret

#
# DES Mixins
#

class DESEnabled(ABC):
    def __init__(self, numEvents=None, **kwargs):
        super().__init__(**kwargs)
        self.numEvents = 0

    def initializeDES(self, simdm, env, eh):
        self.eventLogger = eh
        self.env = env

class DESStateEnabled(DESEnabled):
    def __init__(self,
                 stateMachine=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.stateMachine = stateMachine

    def getStateMachine(self):
        stateMachine = self.stateMachine
        # initialState = self.stateMachine[0].key()
        # initialDelay = 0
        return stateMachine

    # This needs to be overridden to return a dictionary of states & stateTimes.  This dictionary will
    # be used to randomize startup by calculateInitialStateAndDuration

    # Throw an error if this class is called
    def initialStateTimes(self):
        raise me.UnknownElementError(f"DESStateEnabled instance ({self.__class__.__name__}, key: {self.key} has no initialStateTimes function defined")

    # Override this if your major equipment needs to do some initialization of its internal state based on
    # the chosen initial state & duration
    def initialStateUpdate(self, stateName, stateDuration, currentTime):
        ret = StateInfo(stateName, deltaTimeInState=stateDuration, absoluteTimeInState=currentTime + stateDuration)
        return ret

    def calculateInitialStateAndDuration(self):
        stateTimes = self.initialStateTimes()
        randomState = UnscaledEmpiricalDistChooser(stateTimes).randomChoice()
        if len(stateTimes) == 1:
            randomStateDelay = stateTimes[randomState]  # sim time for 1-state equipment or user specified state (ex: wells)
        else:
            randomStateDelay = random.randrange(1, stateTimes[randomState])
            # randomStateDelay = stateTimes[randomState]
        initialState = self.initialStateUpdate(randomState, randomStateDelay, 0)
        return initialState

    def initializeStateManager(self):
        # stateParams, initialState, initialDelay = self.getStateMachine()
        stateParams = self.getStateMachine()
        self.stateManager = StateManager(self.key, stateParams, self.eventLogger)
        initialState = self.calculateInitialStateAndDuration()
        self.initialState = initialState
        self.initialStateTransient = initialState

    def calcNextState(self, nowInSecs, currentStateInfo):
        ret = self.stateManager.calcNextState(nowInSecs, currentStateInfo)
        return ret

    def initializeDES(self, simdm, env, eh):
        super().initializeDES(simdm, env, eh)
        self.initializeStateManager()
        self.action = env.process(self.run())

    GLOBAL_EVENT_COUNT = 0

    def run(self):
        currentStateInfo = None
        while True:
            try:
                self.numEvents += 1
                DESStateEnabled.GLOBAL_EVENT_COUNT += 1
                nowInSecs = self.env.now
                nextStateInfo = self.initialStateTransient or self.calcNextState(nowInSecs, currentStateInfo)
                self.initialStateTransient = None
                currentStateInfo = self.stateManager.enterState(nowInSecs, nextStateInfo)
                delay = currentStateInfo.deltaTimeInState
                if delay < 0:
                    raise ValueError(f'Negative delay {delay}, unitID {self.unitID}')
                yield self.env.timeout(delay)
                nowInSecs = self.env.now
                self.stateManager.exitState(nowInSecs, currentStateInfo)
            except Exception as e:
                eventCount = DESStateEnabled.GLOBAL_EVENT_COUNT
                # raise simpy.exceptions.SimPyException
                logging.exception(e)
                raise e

def toValue(inVal, defVal):
    if inVal is None:
        return defVal
    if math.isnan(inVal):
        return defVal
    return inVal


class StateChangeNotificationDestination():
    pass


class EmissionManager(DESEnabled, StateChangeNotificationDestination, Emitter):

    def __init__(self,
                 statesActive=[],
                 startTime=None,
                 endTime=None,

                 ffName=None,
                 **kwargs
                 ):
        super().__init__(**kwargs)
        self.statesActive = statesActive
        self.startTime = startTime
        self.endTime = endTime

        self.ffName = ffName

    # set up the event handling configuration in initializeDES, and not in __init__,
    # so we can avoid serialization issues
    def initializeDES(self, simdm, env, eh):
        super().initializeDES(simdm, env, eh)  # this will initialize local varibles self.env and self.eventLogger
        self.simdm = simdm
        # calculate functionality
        self.stateDriven = self.statesActive != 'ALL'
        startSpecified = not((self.startTime is None) or math.isnan(self.startTime))
        endSpecified = not ((self.endTime is None) or math.isnan(self.endTime))
        self.timed = startSpecified or endSpecified

        # check combinations of functionality
        if not self.stateDriven and not self.timed:  # we must be either state driven or timed (or both)
            msg = f"Emitter {self.key} is neither state driver nor timed"
            logging.error(msg)
            raise me.IllegalArgumentError(msg)
        ## clean up default values
        self.startTime = toValue(self.startTime, 0)
        self.endTime = toValue(self.endTime, u.getSimDuration())

        if not self.stateDriven and self.timed:     # if we are just timed, we need to set up a run loop of our own
            self.doOneshot()

        if self.stateDriven:
            self.doStateDriven()

        # self.initializeFluidFlow(simdm)
        # if self.fluidFlow is None or self.fluidFlow.ts is None:
        #     i = 10

    # def initializeFluidFlow(self, _):
    #     self.fluidFlow = None  # this will cause an exception if not overridden

    def doStateDriven(self):
        self.activeStatesList = list(self.statesActive.split(','))
        self.majorEquipment = self.simdm.getEquipmentTable().elementLookup(self.facilityID, self.unitID, None, self.mcRunNum)
        self.majorEquipment.registerForStateChangeNotification(self, self.stateChange)

    def stateChange(self, currentTime, stateInfo, op, delay=0, relatedEvent=0, initiator=None):
        if op != 'START':
            return
        if stateInfo.stateName not in self.activeStatesList:
            return

        # check for intervals that are outside the boundaries of interest

        stateEndTime = stateInfo.absoluteTimeInState
        if currentTime > self.endTime:
            return
        if stateEndTime < self.startTime:
            return

        # we know we have an overlapping state event

        t1 = max(currentTime, self.startTime)
        t2 = min(stateEndTime, self.endTime)

        deltat = t2 - t1

        try:
            self.eventLogger.logEmission(t1, deltat, self.key,
                                         driverTSKey=self.fluidFlow.ts.serialNum,
                                         GCKey=self.fluidFlow.gc.serialNum,
                                         flowID=self.fluidFlow.serialNumber
                                         )
        except Exception as e:
            logging.error(e)
            i = 10

    def doOneshot(self):
        self.action = self.env.process(self.run())

    def run(self):
        logging.debug(f"One shot emitter run {self.key} -- emit at {self.startTime}")
        self.numEvents += 1
        yield self.env.timeout(self.startTime)
        self.timeoutCallback(self.env.now)  # keep this as a callback so we can specialize in a subclass

    def timeoutCallback(self, simtime):
        deltat = self.endTime - simtime
        self.eventLogger.logEmission(simtime, deltat, self.key, self.fluidFlow.df.serialNum, self.fluidFlow.gc.serialNum)

class DESOneShot(DESEnabled):
    def __init__(self, oneShotStartTime=0, numEvents=None, **kwargs):
        super().__init__(**kwargs)
        self.oneShotStartTime = oneShotStartTime
        self.numEvents = 0

    def initializeDES(self, simdm, env, eh):
        if self.oneShotStartTime is not None:
            super().initializeDES(simdm, env, eh)
            self.action = env.process(self.run())

    @abstractmethod
    def timeoutCallback(self, simtime):
        raise NotImplementedError

    def run(self):
        logging.debug(f"One shot emitter run {self.key} -- emit at {self.oneShotStartTime}")
        self.numEvents += 1
        yield self.env.timeout(self.oneShotStartTime)
        self.timeoutCallback(self.env.now)

class StateChangeInitiator(DESStateEnabled):

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['stateChangeNotifierTable']

    def __init__(self, stateChangeNotifierTable=None, **kwargs):
        super().__init__(**kwargs)
        self.stateChangeNotifierTable = {}

    def initializeStateManager(self):
        super().initializeStateManager()
        self.hookStateMachine()
        # self.stateManager = StateManager(self.key, hookedStateParams, self.eventLogger, initialState=initialState)

    def registerForStateChangeNotification(self, dest, notifier):
        if not isinstance(dest, StateChangeNotificationDestination):
            raise NotImplementedError

        destKey = dest.key
        if destKey not in self.stateChangeNotifierTable:
            self.stateChangeNotifierTable[destKey] = list()
        if notifier not in self.stateChangeNotifierTable[destKey]:  # can't use a set here because StateChangeNotificationEnabled is not hashable
            self.stateChangeNotifierTable[destKey].append(notifier)

    # previous versions of these functions (entryHook, exitHook, hookStateMachine) allowed for a single-level
    # stack of entry hooks.  Do we still need this?

    def siEntryHook(self, currentTime, stateData, delay=0, relatedEvent=0, stateInfo=None):
        for singleDest, notifierSet in self.stateChangeNotifierTable.items():
            for singleNotifier in notifierSet:
                singleNotifier(currentTime, stateInfo, 'START', relatedEvent=relatedEvent, initiator=self)

    def siExitHook(self, currentTime, stateData, delay=0, relatedEvent=0, stateInfo=None):
        for singleDest, notifierSet in self.stateChangeNotifierTable.items():
            for singleNotifier in notifierSet:
                singleNotifier(currentTime, stateInfo, 'STOP', relatedEvent=relatedEvent, initiator=self)

    def hookStateMachine(self):
        sm = self.stateManager
        for singleState in sm.stateNames:
            sm.hookEntry(singleState, self.siEntryHook)
            sm.hookExit(singleState, self.siExitHook)

class StateEnabledVolume(StateChangeInitiator, ff.Volume):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

class FFLoggingVolume(StateEnabledVolume):
    def __init__(self,
                 mdGroup=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.mdGroup = mdGroup

    def ffUpdateHook(self, currentTime, stateData, **kwargs):
        self.logFluidFlowChanges(self.key, currentTime, mdGroup=self.mdGroup)

    def initializeStateManager(self):
        super().initializeStateManager()
        sm = self.stateManager
        for singleState in sm.stateNames:
            sm.hookEntry(singleState, self.ffUpdateHook)
        pass

#
# Class definitions usable by model formulation -- classes inheriting from basic Emitter, picker, DES, and Serializer
# mixin classes
#

class StateBasedEmitterProduction(FactorManager, EmissionManager):

    def __init__(self,
                 emissionDriverUnits=None,
                 gasComposition=None,
                 secondaryID=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.gasComposition = gasComposition
        self.emissionDriverUnits = emissionDriverUnits or 'scf'
        self.secondaryID = secondaryID

    def initializeFluidFlow(self, simdm):
        super().initializeFluidFlow(simdm)
        facility = simdm.getEquipmentTable().elementLookup(self.facilityID, None, None, self.mcRunNum)
        tmpGC = gc.FluidFlowGC.factory(fluidFlowGCFilename=facility.productionGCFilename,
                                       flow='Vapor',
                                       fluidFlowID=self.gasComposition,
                                       gcUnits=self.emissionDriverUnits
                                       )
        emissionDriverPath = Path(au.expandFilename(simdm.config['emitterProfileDir'], simdm.config)) / self.emissionDriver
        self.fluidFlow = ff.EmpiricalFluidFlow('Vapor', emissionDriverPath, tmpGC,
                                               units=self.emissionDriverUnits, secondaryID=self.secondaryID)
        pass

class StateBasedEmitter(Emitter, DESEnabled, EmissionDistributionEnabled, StateChangeNotificationDestination):

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['gcKey']

    def __init__(self,
                 productionGCName=None,
                 statesActive=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.statesActive = statesActive
        self.productionGCName = productionGCName
        # simdm = sdm.SimDataManager.getSimDataManager()
        # self.gcKey = gcKey

    def initializeDES(self, simdm, env, eh):
        super().initializeDES(simdm, env, eh)
        self.majorEquipment = simdm.getEquipmentTable().elementLookup(self.facilityID, self.unitID, None, self.mcRunNum)
        self.majorEquipment.registerForStateChangeNotification(self, self.stateChange)
        if self.emissionDriver:
            self.emissionDriverKey = simdm.getDriver4EC(self.emissionDriver)
            self.tsKey, _ = simdm.getTimeseries(self.emissionDriverKey)
        self.activeStatesList = self.statesActive.split(',')

    def stateChange(self, currentTime, state, op, delay=0, relatedEvent=0, initiator=None):
        if op != 'START':
            return
        if state not in self.activeStatesList:
            return
        self.eventLogger.logEmission(currentTime, delay, self.key, self.tsKey, self.gcKey)
        pass


# todo: directory content picking needs to be "promoted" to be a standard feature of reading a distribution
def pickOPCycleTimes(epPath, epDir):
    epDirPath = epPath / epDir
    distToUse = random.choice(list(epDirPath.glob("*.csv")))
    dist = EmitterProfile.readEmitterFile(distToUse)
    return dist, str(distToUse)

# todo: move this to someplace more appropriate (units?)

def getSimEndTime():
    simdm = sdm.SimDataManager.getSimDataManager()
    simEndTime = simdm.config['simDurationSeconds']
    return simEndTime

class TimedStateBasedEmitter(StateBasedEmitter):

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['gcKey']

    def __init__(self,
                 startTime=None,
                 endTime=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.startTime = startTime if startTime is not None else 0
        self.endTime = endTime if endTime is not None else getSimEndTime()
        pass

    def stateChange(self, currentTime, state, op, delay=0, relatedEvent=0, initiator=None):
        if op != 'START':
            return
        if state not in self.activeStatesList:
            return

        # check for intervals that are outside the boundaries of interest

        stateEndTime = currentTime + delay
        if currentTime > self.endTime:
            return
        if stateEndTime < self.startTime:
            return

        # we know we have an overlapping state event

        t1 = max(currentTime, self.startTime)
        t2 = min(stateEndTime, self.endTime)

        deltat = t2 - t1

        self.eventLogger.logEmission(t1, deltat, self.key, self.tsKey, self.gcKey)


class SingleStateEquipment(MajorEquipment, StateChangeInitiator, DESStateEnabled):
    """Major equipment model locked in a single state: "OPERATING" for the duration of the sim"""

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['stateMachine']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.stateMachine = {"OPERATING": {'stateDuration': self.getOperatingStateDelay, 'nextState': "OPERATING"}}

    def instantiateFromTemplate(self, simdm, **kwargs):
        newInst = super().instantiateFromTemplate(simdm, **kwargs)
        if newInst is None:
            return None
        newInst.initialState = "OPERATING"
        return newInst

    def getOperatingStateDelay(self, currentStateData=None, currentStateInfo=None, currentTime=None):
        ret = u.getSimDuration() - currentTime
        return ret

    def getStateMachine(self):
        return self.stateMachine

    def initialStateTimes(self):
        ret = {"OPERATING": u.getSimDuration()}
        return ret

    def calcStateTimes(self, **kwargs):
        delay = u.getSimDuration()
        stateTimes = {}
        sm = {"OPERATING": {'stateDuration': delay, 'nextState': "OPERATING"}}
        stateTimes["OPERATING"] = sm["OPERATING"]["stateDuration"]
        self.stateTimes = stateTimes

#
# Link service & mixins
#

# link1 is upstream, link2 is downstream
class LinkedEquipmentMixin():
    def link1(self, linkedME2):
        """Link downstream linkedME2 to self"""
        pass

    def link2(self, linkedME1):
        """Link upstream equipment linkedME1 to self"""
        pass

class LinkService(MEETService):
    # todo: Current implementation requires both me1 and me2 to have LinkedEquipmentMixin.  We may not require that,
    #  e.g. separators listen to wells, but wells don't listen back.

    def __init__(self, me1Facility=None, me1UnitID=None, me2Facility=None, me2UnitID=None, **kwargs):
        super().__init__(**kwargs)
        self.me1Facility = me1Facility
        self.me1UnitID = me1UnitID
        self.me2Facility = me2Facility
        self.me2UnitID = me2UnitID

    def instantiateFromTemplate(self, simdm, **kwargs):
        newInst = super().instantiateFromTemplate(simdm, **kwargs)
        if newInst is None:
            return None

        et = simdm.getEquipmentTable()
        mcRunNum = newInst.mcRunNum

        me1 = et.elementLookup(facilityID=self.me1Facility, unitID=self.me1UnitID, mcRunNum=mcRunNum)
        if me1 is None:
            err = f"Unknown link1 equipment: {self.me1Facility}, {self.me1UnitID}"
            logging.error(err)
            raise me.IllegalArgumentError(err)

        me2 = et.elementLookup(facilityID=self.me2Facility, unitID=self.me2UnitID, mcRunNum=mcRunNum)
        if me2 is None:
            err = f"Unknown link2 equipment: {self.me2Facility}, {self.me2UnitID}"
            logging.error(err)
            raise me.IllegalArgumentError(err)

        if isinstance(me1, LinkedEquipmentMixin):
            me1.link1(me2)
        if isinstance(me2, LinkedEquipmentMixin):
            me2.link2(me1)
        return newInst

    def run(self):
        super().run()

#
# Update of old MEET1 Combustion Emitter
#

class MEETCombustionEmitter(EmissionManager):

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['loadingDistribution', 'exhaustParams', 'exhaustEffDF',
                                         'noLoadDist', 'fullLoadDist', 'loadEquations']

    def __init__(self,
                 # emissionReference=None,
                 driverType=None,
                 driverSizekW=None,
                 sealType=None,
                 averageLoading=None,
                 stdLoading=None,
                 combustionGCName=None,
                 engineEfficiency=None,
                 exhaustFactors=None,
                 compressorEfficiency=None,
                 loadCondition=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.driverType = driverType
        self.driverSizekW = driverSizekW
        self.sealType = sealType
        self.averageLoading = averageLoading
        self.stdLoading = stdLoading
        self.combustionGCName = combustionGCName
        self.loadingDistribution = d.Normal({'mu': self.averageLoading, 'sigma': self.stdLoading})

        self.engineEfficiency = engineEfficiency or 0.3
        self.exhaustFactors = exhaustFactors or "Default"
        self.compressorEfficiency = compressorEfficiency
        self.loadCondition = loadCondition
        # self.loadEquations = self.getLoadConditions(self.loadCondition)

    # def getLoadConditions(self, loadCondition):
    #     simdm = sdm.SimDataManager.getSimDataManager()
    #     dataBasePath = Path(au.expandFilename(simdm.config['emitterProfileDir'], simdm.config, readonly=True)) / f'{loadCondition}'
    #     metadata, dist = dp.readRawDistributionFile(dataBasePath)
    #     return dist

    def getDestEfficiencies(self, exhaustFactors):
        # todo: this method of accessing the destruction efficiency file is different than other file access methods.
        # todo: normalize them.
        simdm = sdm.SimDataManager.getSimDataManager()
        dataBasePath = Path(au.expandFilename(simdm.config['emitterProfileDir'], simdm.config, readonly=True)) / 'CompressorDestructionEfficiencies/fake.csv'
        dataPath = dataBasePath.with_stem(exhaustFactors)
        metadata, destEffDF = dp.readRawDistributionFile(dataPath)
        destEffDF = destEffDF.rename(columns={'Species': 'destSpecies', 'Destruction Efficiency': 'destEfficiency'})
        return metadata, destEffDF

    def initializeFluidFlow(self, simdm):
        super().initializeFluidFlow(simdm)
        ffID = self.combustionGCName
        facility = simdm.getEquipmentTable().elementLookup(self.facilityID, None, None, self.mcRunNum)
        # Look up the base combustion GC
        fuelGC = gc.FluidFlowGC.factory(fluidFlowGCFilename=facility.productionGCFilename,
                                        flow='Vapor',
                                        fluidFlowID=ffID,
                                        gcUnits='scf'
                                        )
        # compose base combustion GC with engine GC and destruction GC
        combustGC = gc.composeEngineExhaustGC(origGC=fuelGC,
                                              driverType=self.driverType,
                                              engineEfficiency=self.engineEfficiency,
                                              destructionEfficiency=self.exhaustFactors)
        initialLoadingkW = 0.0
        self.fluidFlow = ff.FluidFlow('', initialLoadingkW, 'kW', combustGC)
        self.fluidFlow.ts = ts.ConstantTimeseriesTableEntry.factory(initialLoadingkW, 'kW')

    def stateChange(self, currentTime, state, op, delay=0, relatedEvent=0, initiator=None, **kwargs):
        simdm = sdm.SimDataManager.getSimDataManager()
        me = simdm.getEquipmentTable().elementLookup(self.facilityID, self.unitID, None, self.mcRunNum)
        if op != 'START':
            return
        if state.stateName in self.activeStatesList:
            loading_kW, loading_pu = self.majorEquipment.loadingKW, self.majorEquipment.loadingPU
        else:
            loading_kW = 0.0
            loading_pu = 0.0
        self.eventLogger.logRawEvent(currentTime, self.key, 'COMPRESSOR-LOADING-CHANGE',
                                     duration=state.deltaTimeInState, nextTS=state.absoluteTimeInState,
                                     loading=loading_pu, kW=loading_kW)
        self.fluidFlow.driverRate = loading_kW
        self.fluidFlow.ts = ts.ConstantTimeseriesTableEntry.factory(loading_kW, 'kW')
        me.exhaustFF = self.fluidFlow
        super().stateChange(currentTime, state, op, delay=delay, relatedEvent=relatedEvent, initiator=initiator)

class MEETFixedSource(SingleStateEquipment, StateEnabledVolume):

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['facility', 'condensateFlow', 'gasFlow', 'waterFlow']

    def __init__(self,
                 QiOilBblPerDay=None,
                 QiGasMcfPerDay=None,
                 QiWaterBblPerDay=None,
                 flowTag=None,
                 flowGasComposition=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.QiOilBblPerDay = QiOilBblPerDay
        self.QiGasMcfPerDay = QiGasMcfPerDay
        self.QiWaterBblPerDay = QiWaterBblPerDay
        self.flowTag = flowTag
        self.flowGasComposition = flowGasComposition

    def _initFluidFlow(self, flowType, flowRate, gcUnits):
        ffid = f"{self.flowTag}-{flowType}"
        # if flowType == 'Vapor':
        #     ffid = 'Default-LeakGc'
        gcForFlow = gc.FluidFlowGC.factory(fluidFlowGCFilename=self.flowGasComposition,
                                           flow=flowType,
                                           fluidFlowID=ffid,
                                           gcUnits=gcUnits
                                           )
        fluidFlow = ff.FluidFlow(flowType, flowRate, gcUnits, gcForFlow)
        fluidFlow.ts = ts.ConstantTimeseriesTableEntry.factory(flowRate, gcUnits)
        return fluidFlow


    def initializeFluidFlow(self, simdm):
        super().initializeFluidFlow(simdm)
        self.facility = simdm.getEquipmentTable().elementLookup(self.facilityID, None, None, self.mcRunNum)
        condensateFlow = self._initFluidFlow('Condensate', u.bblPerDayToBblPerSec(self.QiOilBblPerDay), 'bbl')
        self.addOutletFluidFlow(condensateFlow)
        gasFlow = self._initFluidFlow('Vapor', u.scfPerDayToScfPerSec(self.QiGasMcfPerDay*1000), 'scf')
        self.addOutletFluidFlow(gasFlow)
        waterFlow = self._initFluidFlow('Water', u.bblPerDayToBblPerSec(self.QiWaterBblPerDay), 'bbl')
        self.addOutletFluidFlow(waterFlow)

def fixupStateMachine(inSM):
    retSM = {}
    for singleStateName, singleStateData in inSM.items():
        newStateData = {
            'nextState': singleStateData['nextState'],
            'stateDuration': d.Uniform({'min': singleStateData['min'], 'max': singleStateData['max']}),
            'stateDuration': d.BoundedNormal({'mu': singleStateData['mean'],
                                              'sigma': singleStateData['std'],
                                              'min': singleStateData['min']}),
            'max': singleStateData['max']
        }
        retSM = {**retSM, singleStateName: newStateData}
    return retSM
