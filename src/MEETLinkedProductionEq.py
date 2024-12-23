import math
import MEETClasses as mc
import Units as u
from Distribution import Uniform
import random
import EmissionDriver as ed
import SimDataManager as sdm
import MEETProductionWells as mp
import MEETExceptions as me
import logging


class LinkedWellNoLiquidUnloading(mp.WellNoLiquidUnloading, mc.LinkedEquipmentMixin, mc.StateChangeInitiator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class LinkedWellAutomatedLiquidUnloading(mp.WellAutomatedLiquidUnloading, mc.LinkedEquipmentMixin,
                                         mc.StateChangeInitiator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class LinkedWellManualLiquidUnloading(mp.WellManualLiquidUnloading, mc.LinkedEquipmentMixin, mc.StateChangeInitiator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class LinkedWellCyclic(mp.WellCyclic, mc.LinkedEquipmentMixin, mc.StateChangeInitiator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class LinkedProductionSeparator(mc.MajorEquipment, mc.LinkedEquipmentMixin, mc.StateChangeNotificationDestination,
                                mc.StateChangeInitiator):
    """Separator state machine transitions between 'FILLING' and 'DUMPING' states.
    Uses nextTS logic from upstream well(s) to determine state durations and next state for separator.
    Maintains self.nextTS and self.dumpRateBblPerSec for downstream tank(s).
    """

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['stateMachine', 'connectedWells', 'bblPerDumpDist', 'secPerDumpDist',
                                         'stateChangeNotificationRecipients']

    def __init__(self, bblPerDumpMin=None, bblPerDumpMax=None,
                 secPerDumpMin=None, secPerDumpMax=None, fluid=None,
                 **kwargs):

        super().__init__(**kwargs)
        # dump volume (each dump a volume will be selected from a uniform distribution between min and max)
        self.bblPerDumpMin = bblPerDumpMin
        self.bblPerDumpMax = bblPerDumpMax
        # dump duration (each dump a duration will be selected from a uniform distribution between min and max)
        self.secPerDumpMin = secPerDumpMin
        self.secPerDumpMax = secPerDumpMax
        # Fluid should be either 'OIL' or 'WATER'.  Used to query correct production value from upstream wells.
        self.fluid = fluid
        self.connectedWells = {}
        self.stateMachine = {
            'FILLING': {'stateDuration': self.fillingDurHook, 'nextState': self.getNextState},
            'DUMPING': {'exitHook': self.dumpingExitHook, 'stateDuration': self.dumpingDurHook,
                        'nextState': self.getNextState}
        }

    def instantiateFromTemplate(self, simdm, **kwargs):
        newInst = super().instantiateFromTemplate(simdm, **kwargs)
        if newInst is None:
            return newInst
        newInst.bblPerDumpDist = Uniform({'min': self.bblPerDumpMin, 'max': self.bblPerDumpMax})
        newInst.secPerDumpDist = Uniform({'min': self.secPerDumpMin, 'max': self.secPerDumpMax})
        newInst.lastDumpVolBbl = 0
        # select initial level (start at random fraction full)
        newInst.resetDumpVolBbl(0)
        fractionFull = random.random()
        newInst.volUntilDump = newInst.nextDumpVolBbl * (1 - fractionFull)
        newInst.lastVolAdjTime = 0
        # set initial fillrate
        newInst.fillRateBblPerSec = 0
        newInst.resetNextDumpTS(0)
        newInst.delay = 1

    def link2(self, linkedWell):
        """Link current separator to upstream well"""
        # add linked well to dict of connected wells
        self.connectedWells[linkedWell.key] = linkedWell

    def fillingDurHook(self, currentTime):
        """What to execute each time we enter the filling state
        Note: this is called in state manager next state to set the duration in the current state, not actually in the
        entry hook"""
        volAdj = self.fillRateBblPerSec * (currentTime - self.lastVolAdjTime)  # calc fluid produced since last adj time
        self.adjustVolUntilDump(volAdj, currentTime)  # adjust volume until next dump
        self.resetFillRate(currentTime)  # reset total fill rate from wells
        self.resetNextDumpTS(currentTime)  # reset timing for next dump based on new vol and fillrate
        self.resetNextWellTS()  # reset timing of next well state change
        if self.nextDumpTS < self.nextWellTS:  # if dump occurs before well state change
            self.nextState = "DUMPING"  # next state will be dumping
            self.nextTS = self.nextDumpTS  # we want to enter that state at this timestamp
        else:  # well state change occurs before dump
            self.nextState = "FILLING"  # next state will be filling
            self.nextTS = self.nextWellTS  # we want to enter that state at this timestamp
        self.delay = self.nextTS - currentTime  # delay (time in the filling state we're currently entering)
        return self.delay

    def dumpingDurHook(self, currentTime):
        """What to execute each time we enter the dumping state
        Note: this is called in state manager next state to set the duration in the current state, not actually in the
        entry hook"""
        volAdj = self.fillRateBblPerSec * (currentTime - self.lastVolAdjTime)  # calc fluid produced since last adj time
        self.adjustVolUntilDump(volAdj, currentTime)  # adjust volume until next dump (volUntilDump should be near 0 since we're entering dumping, but need to reset self.lastVolAdjTime through this method)
        self.nextState = "FILLING"  # next state will be filling
        self.delay = self.selectDumpDuration()  # select the dump duration from the distribution (time in dumping state we're currently entering)
        self.nextTS = currentTime + self.delay  # we want to enter that state at this timestamp
        self.dumpRateBblPerSec = self.nextDumpVolBbl / self.delay  # set dumpRate for tanks to query downstream
        return self.delay

    def dumpingExitHook(self, currentTime, state, stateData, delay=None, relatedEvent=None, initiator=None):
        """What to execute each time we exit the dumping state"""
        volAdj = self.fillRateBblPerSec * (currentTime - self.lastVolAdjTime)  # calc fluid produced since entering dumping state
        self.resetDumpVolBbl(currentTime)
        self.adjustVolUntilDump(volAdj, currentTime)  # account for volume produced while in dumping state

    def resetDumpVolBbl(self, currentTime):
        """Pick volume for next dump from distribution, reset separator to empty, and log time of vol adjustment"""
        self.nextDumpVolBbl = self.bblPerDumpDist.pick()  # pick next dump vol from distribution
        self.volUntilDump = self.nextDumpVolBbl  # set separator to empty
        self.lastVolAdjTime = currentTime  # reset time of last vol adj

    def adjustVolUntilDump(self, volAdj, currentTime):
        """Adjust the volume remaining until the next dump to account for liquid produced, volAdj, and log the time of the adjustment"""
        self.volUntilDump = self.volUntilDump - volAdj  # apply adjustment
        self.lastVolAdjTime = currentTime  # reset time of last vol adj

    def resetNextDumpTS(self, currentTime):
        """Reset the time of the next dump based on the time it will take to produce the volUntilDump at the current
        fillRateBblPerSec"""
        if self.fillRateBblPerSec == 0:
            self.secUntilDump = u.getSimDuration()
        else:
            self.secUntilDump = math.floor(self.volUntilDump / self.fillRateBblPerSec)
            # Use math floor() instead of int() to have predictable rounding errors.  Separator must dump before it's
            # overfull to avoid negative delay in state machines.
        if self.secUntilDump < 1:
            logging.warning(f"in separator {self.key}, sec until dump < 1: {self.secUntilDump}")
            self.secUntilDump = 1
        self.nextDumpTS = currentTime + self.secUntilDump

    def resetNextWellTS(self):
        """Reset the time of the next well state change"""
        if not self.connectedWells:
            self.nextWellTS = u.getSimDuration()
            return
        self.nextWellTS = min(map(lambda x: x.nextTS, self.connectedWells.values()))

    def resetFillRate(self, currentTime):
        """Reset fill rate from connected upstream wells"""
        # todo: generalize and extend to other fluids?
        fillRateBblPerDay = 0
        if self.fluid == "OIL":
            fillRateBblPerDay = sum(map(lambda x: x.getOilBblPerDay(currentTime), self.connectedWells.values()))
        elif self.fluid == 'WATER':
            fillRateBblPerDay = sum(map(lambda x: x.getWaterBblPerDay(currentTime), self.connectedWells.values()))
        self.fillRateBblPerSec = fillRateBblPerDay / u.daysToSecs(1)

    def selectDumpDuration(self):
        duration = int(self.secPerDumpDist.pick())
        return duration

    def getDumpRateBblPerSec(self):
        if self.getCurrentState() == 'DUMPING':
            return self.dumpRateBblPerSec
        return 0

    def getStateMachine(self):
        return self.stateMachine, 'FILLING', 0

    def getNextState(self, currentTime=None):
        return self.nextState


class LinkedProductionTank(mc.MajorEquipment, mc.LinkedEquipmentMixin, mc.StateChangeNotificationDestination,
                           mc.StateChangeInitiator):
    """Liquid storage tank state machine includes only "FILLING" state. THis state restarts each time the seaprator(s)
    upstream transition state. The fillratebblpersec is reset each time the "FILLING" state is restarted."""

    # todo: revise to account for "instant flash" vs " working and breathing" separately.
    #  Instant flash is coincident with separator dump. Working and breathing is over more extended timeframe.

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['stateMachine', 'connectedSeparators', 'stateChangeNotificationRecipients']

    def __init__(self, fluid=None,
                 **kwargs):

        super().__init__(**kwargs)
        # Fluid should be either 'OIL' or 'WATER'.  Used to verify eq chain, and handle emitter properly at end.
        self.fluid = fluid
        self.connectedSeparators = {}
        self.stateMachine = {
            'FILLING': {'stateDuration': self.fillingDurEntryHook, 'nextState': 'FILLING'},
        }

    def instantiateFromTemplate(self, simdm, **kwargs):
        newInst = super().instantiateFromTemplate(simdm, **kwargs)
        if newInst is None:
            return newInst
        newInst.fillRateBblPerSec = 0
        newInst.nextTS = 0

    def link2(self, linkedSeparator):
        """Link current tank to upstream separator"""
        # add linked separator to dict of connected separators
        self.connectedSeparators[linkedSeparator.key] = linkedSeparator
        # # register self (the tank) with the linked separator to receive state change notifications
        if linkedSeparator.fluid != self.fluid:
            msg = f"fluid must be the same for linked separator, {linkedSeparator.fluid}, and tank {self.fluid}"
            logging.warning(msg)
            raise me.IllegalArgumentError(msg)

    def fillingDurEntryHook(self, currentTime):
        """Reset delay when entering filling state..."""
        oldFillRate = self.fillRateBblPerSec
        self.resetFillRate()
        if self.fillRateBblPerSec != oldFillRate:  # if fill rate changed log rate change for debug
            self.logRateChange(currentTime, oldFillRate, self.fillRateBblPerSec)  # logRateChange for debug
        self.resetNextTS()
        self.delay = self.nextTS - currentTime
        return self.delay

    def resetFillRate(self):
        """query all upstream separators and set the current filing rate"""
        self.fillRateBblPerSec = sum(map(lambda x: x.getDumpRateBblPerSec(), self.connectedSeparators.values()))

    def resetNextTS(self):
        if not self.connectedSeparators:
            self.nextTS = u.getSimDuration()
        else:
            self.nextTS = min(map(lambda x: x.nextTS, self.connectedSeparators.values()))

    def getFillRateBblPerSec(self):
        """return the filling rate for "instant" flash calculations"""
        if self.getCurrentState() == "FILLING":
            return self.fillRateBblPerSec
        return 0

    def getStateMachine(self):
        return self.stateMachine, 'FILLING', 0

    def logRateChange(self, ts, oldRate, newRate):
        simdm = sdm.SimDataManager.getSimDataManager()
        simdm.eventLogger.logRawEvent(ts, self.key, 'RATE-CHANGE', oldRate=oldRate, newRate=newRate)
        pass


class LinkedProductionTankFlare(mc.MajorEquipment, mc.LinkedEquipmentMixin, mc.StateChangeNotificationDestination,
                                mc.StateChangeInitiator):
    """Flare state machine transitions between 'OPERATING','MALFUNCTIONING', and 'UNLIT' states.
    Receives state change notifications from upstream tanks(s)."""

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['stateMachine', 'stateChangeNotificationRecipients', 'connectedEq',
                                         'opDurDist', 'malfDurDist', 'unlitDurDist',
                                         'nextTS', 'condensateFlashDriverRateBblPerSec',
                                         'waterFlashDriverRateBblPerSec']

    def __init__(self, pMalfunction=None, pUnlit=None,
                 opDurMin=None, opDurMax=None,
                 malfDurMin=None, malfDurMax=None,
                 unlitDurMin=None, unlitDurMax=None,
                 **kwargs):

        super().__init__(**kwargs)
        self.pMalfunction = pMalfunction  # probability of transitioning from operating to malfunctioning
        self.pUnlit = pUnlit  # probability of transitioning from operating to unlit
        # min and max state durations for op, malfunctioning, and unlit state uniform distributions
        self.opDurMin = opDurMin
        self.opDurMax = opDurMax
        self.malfDurMin = malfDurMin
        self.malfDurMax = malfDurMax
        self.unlitDurMin = unlitDurMin
        self.unlitDurMax = unlitDurMax
        self.opDurDist = Uniform({'min': self.opDurMin, 'max': self.opDurMax})
        self.malfDurDist = Uniform({'min': self.malfDurMin, 'max': self.malfDurMax})
        self.unlitDurDist = Uniform({'min': self.unlitDurMin, 'max': self.unlitDurMax})
        self.stateMachine = {
            'OPERATING': {'nextState': {'UNLIT': self.pUnlit, 'MALFUNCTIONING': self.pMalfunction},
                          'stateDuration': self.opDurDist, 'entryHook': self.scEntryHook},
            'UNLIT': {'nextState': 'OPERATING', 'stateDuration': self.unlitDurDist, 'entryHook': self.scEntryHook},
            'MALFUNCTIONING': {'nextState': 'OPERATING', 'stateDuration': self.malfDurDist,
                               'entryHook': self.scEntryHook}
        }
        self.nextTS = 0
        self.connectedEq = {}
        self.condensateFlashDriverRateBblPerSec = 0
        self.waterFlashDriverRateBblPerSec = 0

    def instantiateFromTemplate(self, simdm, **kwargs):
        newInst = super().instantiateFromTemplate(simdm, **kwargs)
        if newInst is None:
            return newInst
        newInst.initialState = 'OPERATING'
        newInst.initialDelay = int(self.opDurDist.pick())
        newInst.nextTS = newInst.initialDelay

    def link2(self, linkedTank):
        """Link current flare to upstream tank"""
        # add linked tank to dict of connected tanks
        self.connectedEq[linkedTank.key] = linkedTank
        # register self (the flare) with the linked tank to receive state change notifications
        linkedTank.registerForStateChangeNotification(self, self.tankStateChange)

    def tankStateChange(self, currentTime, tankState, op, delay=None, relatedEvent=None, initiator=None):
        """What to do when an upstream tank changes state..."""
        if op != 'START':  # only need to reset driver rates when tank enters new state
            return
        self.resetFlareDriverRates(currentTime)

    def scEntryHook(self, currentTime, state, stateData, delay=None, relatedEvent=None, initiator=None):
        """What to do when flare state changes..."""
        self.nextTS = currentTime + delay  # need to update nextTS for the flare

    def resetFlareDriverRates(self, currentTime):
        """query all upstream tanks amd set the current flare driver rates"""
        # oldConRate = self.condensateFlashDriverRateBblPerSec
        # oldWaterRate = self.waterFlashDriverRateBblPerSec
        condensateFlashDriverRateBblPerSec = 0
        waterFlashDriverRateBblPerSec = 0
        for singleTank in self.connectedEq.values():
            if singleTank.fluid == "OIL":
                condensateFlashDriverRateBblPerSec += singleTank.getFillRateBblPerSec()
            elif singleTank.fluid == "WATER":
                waterFlashDriverRateBblPerSec += singleTank.getFillRateBblPerSec()
        self.condensateFlashDriverRateBblPerSec = condensateFlashDriverRateBblPerSec
        self.waterFlashDriverRateBblPerSec = waterFlashDriverRateBblPerSec
        # # log rate changes for debug.
        # if oldConRate != self.condensateFlashDriverRateBblPerSec:
        #     self.logRateChange('COND-FLARE-RATE-CHANGE', currentTime, oldConRate, self.condensateFlashDriverRateBblPerSec)
        # if oldWaterRate != self.waterFlashDriverRateBblPerSec:
        #     self.logRateChange('WATER-FLARE-RATE-CHANGE', currentTime, oldWaterRate, self.waterFlashDriverRateBblPerSec)

    # def logRateChange(self, command, ts, oldRate, newRate):
    #     simdm = sdm.SimDataManager.getSimDataManager()
    #     simdm.eventLogger.logRawEvent(ts, self.key, command, oldRate=oldRate, newRate=newRate)
    #     pass

    def getStateMachine(self):
        return self.stateMachine, self.initialState, self.initialDelay


class LinkedProductionVRU(mc.MajorEquipment, mc.LinkedEquipmentMixin, mc.StateChangeNotificationDestination,
                          mc.StateChangeInitiator):
    """Vapor Recovery Unit state machine transitions between 'OPERATING', 'IDLE' and 'MALFUNCTIONING' states.
    Transitions from IDLE state each time a separator dumps to upstream tanks. Malfunctions occur based on fixed
    probability at each transition from IDLE state. Frequency of malfunctions is therefore dependent on production rates
    and separator sizing. Receives state change notifications from upstream tanks(s) to reset driver rates."""

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['stateMachine', 'stateChangeNotificationRecipients', 'connectedTanks',
                                         'opDurDist', 'malfDurDist'
                                         ]

    def __init__(self, pMalf=None, malfDurMin=None, malfDurMax=None,
                 **kwargs):

        super().__init__(**kwargs)
        # min and max state durations malfunctioning state uniform distributions
        self.pMalf = pMalf  # Probability of transitioning from idle to malfunctioning instead of idle to operating
        self.malfDurMin = malfDurMin
        self.malfDurMax = malfDurMax
        self.malfDurDist = Uniform({'min': self.malfDurMin, 'max': self.malfDurMax})
        self.connectedTanks = {}
        self.stateMachine = {
            'OPERATING': {'nextState': self.getNextState, 'stateDuration': self.opDurHook, 'exitHook': self.opExitHook},
            'IDLE': {'nextState': self.getNextState, 'stateDuration': self.idleDurHook},
            'MALFUNCTIONING': {'nextState': self.getNextState, 'stateDuration': self.malfDurHook, 'exitHook': self.malfExitHook}
        }

    def instantiateFromTemplate(self, simdm, **kwargs):
        newInst = super().instantiateFromTemplate(simdm, **kwargs)
        if newInst is None:
            return newInst
        newInst.nextTS = 0
        newInst.nextTankTS = 0
        newInst.nextRepairTS = 0  # timestamp of next repair
        newInst.condensateFlashDriverRateBblPerSec = 0
        newInst.waterFlashDriverRateBblPerSec = 0
        newInst.initialState = 'IDLE'
        newInst.initialDelay = 0
        newInst.getNextState = 'OPERATING'

    def link2(self, linkedTank):
        """Link current VRU to upstream tank"""
        # add linked tank to dict of connected tanks
        self.connectedTanks[linkedTank.key] = linkedTank

    def opDurHook(self, currentTime):
        """What to execute each time we enter the operating state
        Note: this is called in state manager next state to set the duration in the current state, not actually in the
        entry hook"""
        self.resetDriverRates()
        self.resetNextTankTS()
        self.nextTS = self.nextTankTS  # will remain in this state until next state change at tanks
        self.delay = self.nextTS - currentTime  # delay (time in the operating state we're currently entering)
        return self.delay

    def opExitHook(self, currentTime, state, stateData, delay=None, relatedEvent=None, initiator=None):
        """What to execute each time we exit the operating state"""
        self.resetDriverRates()
        if self.waterFlashDriverRateBblPerSec > 0 or self.condensateFlashDriverRateBblPerSec > 0:
            self.nextState = "OPERATING"  # if a tank is still flashing VRU will reenter op state at new driver rate
        else:
            self.nextState = "IDLE"  # if flash rates are both zero, VRU will shut down to idle state

    def idleDurHook(self, currentTime):
        """What to execute each time we enter the operating state
        Note: this is called in state manager next state to set the duration in the current state, not actually in the
        entry hook"""
        self.resetNextTankTS()
        self.nextTS = self.nextTankTS  # will try to restart VRU next time tank(s) state change
        self.delay = self.nextTS - currentTime  # delay (time in the idle state we're currently entering)
        if random.random() <= self.pMalf:
            self.nextState = 'MALFUNCTIONING'
            self.nextRepairTS = currentTime + self.delay + int(self.malfDurDist.pick())
        else:
            self.nextState = 'OPERATING'
        return self.delay

    def malfDurHook(self, currentTime):
        """What to execute each time we enter the malfunctioning state
        Note: this is called in state manager next state to set the duration in the current state, not actually in the
        entry hook"""
        self.resetDriverRates()
        self.resetNextTankTS()
        if self.nextRepairTS < self.nextTankTS:
            self.nextTS = self.nextRepairTS  # will need repair at this time, but next state (IDLE or OP) must be selected in exit hook
        else:
            self.nextTS = self.nextTankTS  # will need to restart malfunctioning state at new driver rates
        self.delay = self.nextTS - currentTime  # delay (time in the malfunctioning state we're currently entering)
        return self.delay

    def malfExitHook(self, currentTime, state, stateData, delay=None, relatedEvent=None, initiator=None):
        """What to execute each time we exit the malfunctioning state"""
        if currentTime == self.nextRepairTS:
            self.resetDriverRates()
            if self.waterFlashDriverRateBblPerSec > 0 or self.condensateFlashDriverRateBblPerSec > 0:
                self.nextState = "OPERATING"  # if a tank is still flashing VRU will reenter op state at current driver rate
            else:
                self.nextState = "IDLE"  # if flash rates are both zero, VRU will shut down to idle state
        else:
            self.nextState = 'MALFUNCTIONING'

    def resetDriverRates(self):
        """query all upstream tanks and set the current driver rates"""
        condensateFlashDriverRateBblPerSec = 0
        waterFlashDriverRateBblPerSec = 0
        for singleTank in self.connectedTanks.values():
            if singleTank.fluid == "OIL":
                condensateFlashDriverRateBblPerSec += singleTank.getFillRateBblPerSec()
            elif singleTank.fluid == "WATER":
                waterFlashDriverRateBblPerSec += singleTank.getFillRateBblPerSec()
        self.condensateFlashDriverRateBblPerSec = condensateFlashDriverRateBblPerSec
        self.waterFlashDriverRateBblPerSec = waterFlashDriverRateBblPerSec

    def getNextState(self):
        return self.nextState

    def resetNextTankTS(self):
        self.nextTankTS = min(map(lambda x: x.nextTS, self.connectedTanks.values()))

    def getCondensateDriverRate(self):
        """Get the driver rate for the VRU from condensate tanks. If vru is operating or idle, tank flash is being
        handled properly by VRU and pumped into sales line (no flash emissions). If VRU is malfunctioning flash driver
        should be passed to flare or emitter."""
        cs = self.getCurrentState()
        if cs == "OPERATING":
            return 0.0
        if cs == "IDLE":
            return 0.0
        if cs == "MALFUNCTIONING":
            return self.condensateFlashDriverRateBblPerSec

    def getWaterDriverRate(self):
        """Get the driver rate for the VRU from water tanks. If vru is operating or idle, tank flash is being handled
        properly by VRU and pumped into sales line (no flash emissions). If VRU is malfunctioning flash driver should be
        passed to flare or emitter."""
        cs = self.getCurrentState()
        if cs == "OPERATING":
            return 0.0
        if cs == "IDLE":
            return 0.0
        if cs == "MALFUNCTIONING":
            return self.waterFlashDriverRateBblPerSec

    def getStateMachine(self):
        return self.stateMachine, self.initialState, self.initialDelay

class LinkedProductionVRU_VariationA(mc.MajorEquipment, mc.LinkedEquipmentMixin, mc.StateChangeNotificationDestination,
                          mc.StateChangeInitiator):
    """Vapor Recovery Unit state machine transitions between 'OPERATING' and 'MALFUNCTIONING' states.
    Transitions from Operating to Malfunctioning based on duration distributions.
    Receives state change notifications from upstream tanks(s) to reset driver rates."""

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['stateMachine', 'stateChangeNotificationRecipients', 'connectedTanks',
                                         'opDurDist', 'malfDurDist'
                                         ]

    def __init__(self,  opDurMin=None, opDurMax=None, malfDurMin=None, malfDurMax=None,
                 **kwargs):

        super().__init__(**kwargs)
        # min and max state durations for op and malfunctioning state uniform distributions
        self.opDurMin = opDurMin
        self.opDurMax = opDurMax
        self.opDurDist = Uniform({'min': self.opDurMin, 'max': self.opDurMax})
        self.malfDurMin = malfDurMin
        self.malfDurMax = malfDurMax
        self.malfDurDist = Uniform({'min': self.malfDurMin, 'max': self.malfDurMax})
        self.connectedTanks = {}
        self.stateMachine = {
            'OPERATING': {'nextState': self.getNextState, 'stateDuration': self.opDurHook, 'exitHook': self.opExitHook},
            'MALFUNCTIONING': {'nextState': self.getNextState, 'stateDuration': self.malfDurHook,
                               'exitHook': self.malfExitHook}
        }

    def instantiateFromTemplate(self, simdm, **kwargs):
        newInst = super().instantiateFromTemplate(simdm, **kwargs)
        if newInst is None:
            return newInst
        newInst.nextTS = 0
        newInst.nextTankTS = 0
        newInst.nextRepairTS = 0  # timestamp of next repair
        newInst.nextMalfTS = int(random.random()*self.opDurDist.pick())  # timestamp of next malfunciton (start a random point into cycle)
        newInst.condensateFlashDriverRateBblPerSec = 0
        newInst.waterFlashDriverRateBblPerSec = 0
        newInst.initialState = 'OPERATING'
        newInst.initialDelay = 0
        newInst.getNextState = 'OPERATING'

    def link2(self, linkedTank):
        """Link current VRU to upstream tank"""
        # add linked tank to dict of connected tanks
        self.connectedTanks[linkedTank.key] = linkedTank

    def opDurHook(self, currentTime):
        """What to execute each time we enter the operating state"""
        # Note: this is called in StateManager nextState() to set the duration in the current state
        self.resetDriverRates()
        self.resetNextTankTS()
        self.nextTS = min(self.nextTankTS, self.nextMalfTS)  # will remain in this state until next state change at tanks or next malfunction (whichever comes first)
        self.delay = self.nextTS - currentTime  # delay (time in the operating state we're currently entering)
        return self.delay

    def opExitHook(self, currentTime, state, stateData, delay=None, relatedEvent=None, initiator=None):
        """What to execute each time we exit the operating state"""
        if currentTime == self.nextMalfTS:
            self.nextState = "MALFUNCTIONING"
            self.resetNextRepairTS(currentTime)
        else:
            self.nextState = 'OPERATING'

    def malfDurHook(self, currentTime):
        """What to execute each time we enter the malfunctioning state
        Note: this is called in state manager next state to set the duration in the current state, not actually in the
        entry hook"""
        self.resetDriverRates()
        self.resetNextTankTS()
        self.nextTS = min(self.nextTankTS, self.nextRepairTS)  # will remain in this state until next state change at tanks or next repair (whichever comes first)
        self.delay = self.nextTS - currentTime  # delay (time in the malfuncitoning state we're currently entering)
        return self.delay

    def malfExitHook(self, currentTime, state, stateData, delay=None, relatedEvent=None, initiator=None):
        """What to execute each time we exit the malfunctioning state"""
        if currentTime == self.nextRepairTS:
            self.nextState = "OPERATING"  # if a tank is still flashing VRU will reenter op state at current driver rate
            self.resetNextMalfTS(currentTime)
        else:
            self.nextState = 'MALFUNCTIONING'

    def resetDriverRates(self):
        """query all upstream tanks and set the current driver rates"""
        condensateFlashDriverRateBblPerSec = 0
        waterFlashDriverRateBblPerSec = 0
        for singleTank in self.connectedTanks.values():
            if singleTank.fluid == "OIL":
                condensateFlashDriverRateBblPerSec += singleTank.getFillRateBblPerSec()
            elif singleTank.fluid == "WATER":
                waterFlashDriverRateBblPerSec += singleTank.getFillRateBblPerSec()
        self.condensateFlashDriverRateBblPerSec = condensateFlashDriverRateBblPerSec
        self.waterFlashDriverRateBblPerSec = waterFlashDriverRateBblPerSec

    def getNextState(self, currentTime=None):
        return self.nextState

    def resetNextMalfTS(self, currentTime):
        # reset next malf TS when transitioning from malf to op state
        self.nextMalfTS = currentTime + int(self.opDurDist.pick())

    def resetNextRepairTS(self, currentTime):
        # reset next repair TS when transitioning from op state to malf state
        self.nextRepairTS = currentTime + int(self.malfDurDist.pick())

    def resetNextTankTS(self):
        if not self.connectedTanks:
            self.nextTankTS = u.getSimDuration()
        else:
            self.nextTankTS = min(map(lambda x: x.nextTS, self.connectedTanks.values()))

    def getCondensateDriverRate(self):
        """Get the driver rate for the VRU from condensate tanks. If vru is operating or idle, tank flash is being
        handled properly by VRU and pumped into sales line (no flash emissions). If VRU is malfunctioning flash driver
        should be passed to flare or emitter."""
        cs = self.getCurrentState()
        if cs == "OPERATING":
            return 0.0
        if cs == "MALFUNCTIONING":
            return self.condensateFlashDriverRateBblPerSec

    def getWaterDriverRate(self):
        """Get the driver rate for the VRU from water tanks. If vru is operating or idle, tank flash is being handled
        properly by VRU and pumped into sales line (no flash emissions). If VRU is malfunctioning flash driver should be
        passed to flare or emitter."""
        cs = self.getCurrentState()
        if cs == "OPERATING":
            return 0.0
        if cs == "MALFUNCTIONING":
            return self.waterFlashDriverRateBblPerSec

    def getStateMachine(self):
        return self.stateMachine, self.initialState, self.initialDelay


class LinkedProductionVRUFlare(LinkedProductionTankFlare):

    def __init__(self,
                 **kwargs):
        super().__init__(**kwargs)

    def link2(self, linkedVRU):
        """Link current flare to upstream VRU"""
        # add linked VRU to dict of connected VRUs
        self.connectedEq[linkedVRU.key] = linkedVRU
        # register self (the flare) with the linked VRU to receive state change notifications
        linkedVRU.registerForStateChangeNotification(self, self.VRUStateChange)

    def VRUStateChange(self, currentTime, sepState, op, delay=None, relatedEvent=None, initiator=None):
        """What to do when an upstream VRU changes state..."""
        if op != 'START':
            return
        self.resetFlareDriverRates(currentTime)

    def resetFlareDriverRates(self, currentTime):
        """query all upstream VRUs amd set the current flare driver rates"""
        condensateFlashDriverRateBblPerSec = 0
        waterFlashDriverRateBblPerSec = 0
        for singleVRU in self.connectedEq.values():
            condensateFlashDriverRateBblPerSec += singleVRU.getCondensateDriverRate()
            waterFlashDriverRateBblPerSec += singleVRU.getWaterDriverRate()
        self.condensateFlashDriverRateBblPerSec = condensateFlashDriverRateBblPerSec
        self.waterFlashDriverRateBblPerSec = waterFlashDriverRateBblPerSec


class Emitter_UncontrolledTankFlash(mc.Emitter, mc.DESEnabled, mc.StateChangeNotificationDestination):
    """Emitter for uncontrolled tank flash emissions.  Designed to be connected directly to a tank in the model
    formulation.  Emits at steady rate when tank is filling.  Driver is liquid fill rate (bbl) and composition should
    be in units of kg species/bbl. """

    def __init__(self, gasComposition=None, **kwargs):
        super().__init__(**kwargs)
        self.gasComposition = gasComposition

    def initializeDES(self, simdm, env, eh):
        super().initializeDES(simdm, env, eh)
        self.majorEquipment = simdm.getEquipmentTable().elementLookup(self.facilityID, self.unitID, None, self.mcRunNum)
        self.majorEquipment.registerForStateChangeNotification(self, self.stateChange)
        self.gcKey, _ = simdm.getGasComposition(self.gasComposition)
        self.simdm = simdm

    def stateChange(self, currentTime, state, op, delay=0, relatedEvent=0, initiator=None):
        if op != 'START':
            return
        emissionRate = self.majorEquipment.fillRateBblPerSec
        if emissionRate > 0:
            ts = ed.ConstantEmissionTimeseries(emissionRate, 'bbl')
            tsKey, _ = self.simdm.addTimeseries(ts)
            self.eventLogger.logEmission(currentTime, delay, self.key, tsKey, self.gcKey)
        pass


class Emitter_FlaredTankFlash(mc.Emitter, mc.DESEnabled, mc.StateChangeNotificationDestination):
    """This flash emitter models only emits in a single state of the flare. It is intended to have a different emitter
    to apply a different gas composition and emission category in each state"""
    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['nextTS', 'nextFlareTS']

    def __init__(self, fluid, gasComposition=None, statesActive=None, **kwargs):
        super().__init__(**kwargs)
        self.fluid = fluid
        self.gasComposition = gasComposition
        self.statesActive = statesActive

    def initializeDES(self, simdm, env, eh):
        super().initializeDES(simdm, env, eh)
        self.majorEquipment = simdm.getEquipmentTable().elementLookup(self.facilityID, self.unitID, None, self.mcRunNum)
        # register for state change notifications from the flare this emitter is located on, and the tanks feeding that flare.
        self.majorEquipment.registerForStateChangeNotification(self, self.stateChange)
        for tank in self.majorEquipment.connectedEq.values():
            if tank.fluid == self.fluid:
                # only register for notifications from the tanks handling the same type of fluid as this emitter
                tank.registerForStateChangeNotification(self, self.stateChange)
        # set gaa compositions for each
        self.gcKey, _ = simdm.getGasComposition(self.gasComposition)
        self.simdm = simdm
        self.driverRate = 0  # bbl/sec
        self.nextTankTS = 0
        self.nextFlareTS = 0
        self.nextTS = 0
        self.delay = 0

    def stateChange(self, currentTime, state, op, delay=0, relatedEvent=0, initiator=None):
        if op != 'START':
            return
        # todo: why is this not simply a reference to the state parameter?
        if self.majorEquipment.getCurrentState() in self.statesActive:
            self.resetDriverRate()
            self.resetNextTSandDelay(currentTime)
            if self.driverRate > 0:
                ts = ed.ConstantEmissionTimeseries(self.driverRate, 'bbl')
                tsKey, _ = self.simdm.addTimeseries(ts)
                self.eventLogger.logEmission(currentTime, self.delay, self.key, tsKey, self.gcKey)
        pass

    def resetNextTankTS(self):
        self.nextTankTS = min(map(lambda x: x.nextTS, self.majorEquipment.connectedEq.values()))

    def resetNextFlareTS(self):
        self.nextFlareTS = self.majorEquipment.nextTS

    def resetNextTSandDelay(self, currentTime):
        self.resetNextTankTS()
        self.resetNextFlareTS()
        self.nextTS = min(self.nextTankTS, self.nextFlareTS)
        self.delay = max(1, self.nextTS - currentTime)

    def resetDriverRate(self):
        if self.fluid == "WATER":
            self.driverRate = self.majorEquipment.waterFlashDriverRateBblPerSec
        elif self.fluid == "OIL":
            self.driverRate = self.majorEquipment.condensateFlashDriverRateBblPerSec


class Emitter_FlaredVRUFlash(mc.Emitter, mc.DESEnabled, mc.StateChangeNotificationDestination):
    """This flash emitter models only emits in a single state of the flare. It is intended to have a different emitter
    to apply a different gas composition and emission category in each state. THis flare """
    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['nextTS', 'nextFlareTS']

    def __init__(self, fluid, gasComposition=None, statesActive=None, **kwargs):
        super().__init__(**kwargs)
        self.fluid = fluid
        self.gasComposition = gasComposition
        self.statesActive = statesActive

    def initializeDES(self, simdm, env, eh):
        super().initializeDES(simdm, env, eh)
        self.majorEquipment = simdm.getEquipmentTable().elementLookup(self.facilityID, self.unitID, None, self.mcRunNum)
        # register for state change notifications from the flare this emitter is located on, and the tanks feeding that flare.
        self.majorEquipment.registerForStateChangeNotification(self, self.stateChange)
        for vru in self.majorEquipment.connectedEq.values():
            vru.registerForStateChangeNotification(self, self.stateChange)
        # set gaa compositions for each
        self.gcKey, _ = simdm.getGasComposition(self.gasComposition)
        self.simdm = simdm
        self.driverRate = 0  # bbl/sec
        self.nextTankTS = 0
        self.nextFlareTS = 0
        self.nextTS = 0
        self.delay = 0

    def stateChange(self, currentTime, state, op, delay=0, relatedEvent=0, initiator=None):
        if op != 'START':
            return
        if self.majorEquipment.getCurrentState() in self.statesActive:
            self.resetDriverRate()
            self.resetNextTSandDelay(currentTime)
            if self.driverRate > 0:
                ts = ed.ConstantEmissionTimeseries(self.driverRate, 'bbl')
                tsKey, _ = self.simdm.addTimeseries(ts)
                self.eventLogger.logEmission(currentTime, self.delay, self.key, tsKey, self.gcKey)
        pass

    def resetNextTankTS(self):
        if not self.majorEquipment.connectedEq:
            ts = u.getSimDuration()
        else:
            ts = min(map(lambda x: x.nextTS, self.majorEquipment.connectedEq.values()))

        self.nextTankTS = ts

    def resetNextFlareTS(self):
        self.nextFlareTS = self.majorEquipment.nextTS

    def resetNextTSandDelay(self, currentTime):
        self.resetNextTankTS()
        self.resetNextFlareTS()
        self.nextTS = min(self.nextTankTS, self.nextFlareTS)
        self.delay = max(1, self.nextTS - currentTime)

    def resetDriverRate(self):
        if self.fluid == "WATER":
            self.driverRate = self.majorEquipment.waterFlashDriverRateBblPerSec
        elif self.fluid == "OIL":
            self.driverRate = self.majorEquipment.condensateFlashDriverRateBblPerSec

class LinkedProductionContinuousSeparator(mc.MajorEquipment, mc.LinkedEquipmentMixin,
                                          mc.StateChangeNotificationDestination, mc.StateChangeInitiator):

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['stateMachine', 'connectedWells', 'stateChangeNotificationRecipients']

    def __init__(self, fluid=None,
                 **kwargs):

        super().__init__(**kwargs)
        # Fluid should be either 'OIL' or 'WATER'.  Used to verify eq chain, and handle emitter properly at end.
        self.fluid = fluid
        self.connectedWells = {}
        self.stateMachine = {
            'OPERATING': {'stateDuration': self.calculateOperatingDuration, 'nextState': 'OPERATING'},
        }

    def instantiateFromTemplate(self, simdm, **kwargs):
        newInst = super().instantiateFromTemplate(simdm, **kwargs)
        if newInst is None:
            return newInst
        newInst.fillRateBblPerSec = 0
        newInst.dumpRateBblPerSec = 0
        newInst.nextTS = 0

    def link2(self, linkedWell):
        """Link current separator to upstream well"""
        self.connectedWells[linkedWell.key] = linkedWell


    def calculateOperatingDuration(self, currentTime=None, **kwargs):
        oldFillRate = self.fillRateBblPerSec
        self.resetFillRate(currentTime)
        self.logRateChange(currentTime, oldFillRate, self.fillRateBblPerSec)  # logRateChange for debug
        self.calcNextTS()
        self.delay = self.nextTS - currentTime
        return self.delay

    def resetFillRate(self, currentTime):
        """Reset fill rate from connected upstream wells"""
        # todo: generalize and extend to other fluids?
        fillRateBblPerDay = 0
        if self.fluid == "OIL":
            fillRateBblPerDay = sum(map(lambda x: x.getOilBblPerDay(currentTime), self.connectedWells.values()))
        elif self.fluid == 'WATER':
            fillRateBblPerDay = sum(map(lambda x: x.getWaterBblPerDay(currentTime), self.connectedWells.values()))
        self.fillRateBblPerSec = fillRateBblPerDay / u.daysToSecs(1)
        self.dumpRateBblPerSec = self.fillRateBblPerSec

    def calcNextTS(self):
        """Reset the time of the next well state change"""
        if not self.connectedWells:
            self.nextTS = u.getSimDuration()
            return
        self.nextTS = min(map(lambda x: x.nextTS, self.connectedWells.values()))

    def getFillRateBblPerSec(self):
        return self.fillRateBblPerSec

    def getDumpRateBblPerSec(self):
        return self.dumpRateBblPerSec

    def getStateMachine(self):
        return self.stateMachine, 'OPERATING', 0

    def logRateChange(self, ts, oldRate, newRate):
        simdm = sdm.SimDataManager.getSimDataManager()
        simdm.eventLogger.logRawEvent(ts, self.key, 'RATE-CHANGE', oldRate=oldRate, newRate=newRate)
        pass

