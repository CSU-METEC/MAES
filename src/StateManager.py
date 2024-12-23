import logging
from Chooser import EmpiricalDistChooser, SingletonChooser, FunctionalChooser
from enum import Enum
import random
from Distribution import Distribution
import MEETExceptions as me

logging.getLogger(__name__).addHandler(logging.NullHandler())


STATE_CASES = {
    str: SingletonChooser,
    Enum: SingletonChooser,
    dict: EmpiricalDistChooser
}

def toChooser(sInfo):
    if isinstance(sInfo, dict):
        stateChooser = EmpiricalDistChooser(sInfo)
    elif isinstance(sInfo, str) or isinstance(sInfo, Enum):
        stateChooser = SingletonChooser(sInfo)
    elif callable(sInfo):
        stateChooser = FunctionalChooser(sInfo)
    else:
        msg = f"Unknown chooser for {sInfo}"
        logging.error(msg)
        raise Exception(msg)
    return stateChooser

def fixupState(singleState):
    ret = {'entryHook': [], 'exitHook': []}
    for sKey, sInfo in singleState.items():
        if sKey == 'nextState':
            stateChooser = toChooser(sInfo)
            ret['nextState'] = stateChooser
        elif sKey == 'entryHook':
            ret['entryHook'].append(sInfo)
        elif sKey == 'exitHook':
            ret['entryHook'].append(sInfo)
        else:
            ret[sKey] = sInfo
    return ret

def fixupStateTable(sTable):
    ret = {}
    for stateKey, stateInfo in sTable.items():
        ret[stateKey] = fixupState(stateInfo)
    return ret

class StateInfo():
    def __init__(self, stateName, absoluteTimeInState=None, deltaTimeInState=None):
        self.stateName = stateName
        self.absoluteTimeInState = absoluteTimeInState
        self.deltaTimeInState = deltaTimeInState

class StateManager():
    def __init__(self, deviceID, stateTable, eh):
        self.deviceID = deviceID
        self.stateTable = fixupStateTable(stateTable)
        self.eh = eh
        # It is convienient to keep a copy of the current state for access by other classes (state dependent
        # fluid flows, etc).  This will keep the copy.  Note, however, that it is otherwise not used by the
        # state manager
        self.cachedCurrentState = None

    @property
    def stateNames(self):
        return self.stateTable.keys()

    @property
    def currentState(self):
        return self.cachedCurrentState

    def logState(self, stateName, stateData, ev, currentTime, delay, hookData={}):
        debugData = {'duration': delay, 'nextTS': currentTime+delay}
        extraData = {**stateData.get('stateEventData', {}), **debugData, **hookData}
        return self.eh.logEvent(currentTime, self.deviceID, stateName, ev, **extraData)

    def callHooks(self, hookName, stateData, currentTime, **kwargs):
        hookList = stateData.get(hookName, [])
        for singleHook in hookList:
            if callable(singleHook):
                singleHook(currentTime, stateData, **kwargs)

    def enterState(self, currentTime, newStateInfo):
        newStateName = newStateInfo.stateName
        newStateData = self.stateTable.get(newStateName, None)
        if not newStateData:
            msg = f"StateManager enterState -- no state named {newStateData}"
            logging.error(msg)
            raise me.IllegalElementError(msg)
        delay = newStateInfo.deltaTimeInState
        eventID = self.logState(newStateName, newStateData, "START", currentTime, delay)
        self.cachedCurrentState = newStateInfo
        self.callHooks('entryHook', newStateData, currentTime, delay=delay, relatedEvent=eventID, stateInfo=newStateInfo)
        return newStateInfo

    def saveVolumeInfo(self, currentTime, outletFFs):
        i = 10

    def exitState(self, currentTime, currentStateInfo):
        currentStateName = currentStateInfo.stateName
        currentStateData = self.stateTable.get(currentStateName, None)
        if not currentStateData:
            msg = f"StateManager exitState -- no state named {currentStateData}"
            logging.error(msg)
            raise msg
        eventID = self.logState(currentStateName, currentStateData, "STOP", currentTime, 0)
        self.callHooks('exitHook', currentStateData, currentTime, delay=0, relatedEvent=eventID, stateInfo=currentStateInfo)

    def calcNextState(self, currentTime, currentStateInfo):
        if currentStateInfo is None:
            i = 10
        currentStateName = currentStateInfo.stateName
        currentStateData = self.stateTable[currentStateName]
        newStateName = currentStateData['nextState'].randomChoice(currentStateData=currentStateData,
                                                                  currentStateInfo=currentStateInfo,
                                                                  currentTime=currentTime)
        newStateData = self.stateTable[newStateName]
        si = StateInfo(newStateName)
        delay = self.calcDelay(newStateData, si, currentTime)
        si.deltaTimeInState = delay
        si.absoluteTimeInState = currentTime+delay
        return si

    def calcDelay(self, newStateData, newStateInfo, currentTime):
        stateDuration = newStateData['stateDuration']
        if isinstance(stateDuration, int) or isinstance(stateDuration, float):
            delay = int(stateDuration)
        elif isinstance(stateDuration, Distribution):
            delay = int(stateDuration.pick())
        elif callable(stateDuration):
            delay = stateDuration(currentStateData=newStateData,
                                  currentStateInfo=newStateInfo,
                                  currentTime=currentTime)
        else:
            logging.warning(f"Unknown duration: {newStateData['stateDuration']}")
            raise ValueError

        return delay

    def hookEntry(self, stateName, hook):
        self.stateTable[stateName]['entryHook'].append(hook)

    def hookExit(self, stateName, hook):
        self.stateTable[stateName]['exitHook'].append(hook)



