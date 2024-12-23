from EquipmentTable import Facility, MajorEquipment, Emitter, MEETTemplate, ActivityFactor, EquipmentTableEntry
from abc import ABC, abstractmethod
import MEETGlobals as mg
import EmitterProfile as ep
from pathlib import Path
import AppUtils as au
import logging
from StateManager import StateManager
import Units as u
from EmitterProfile import EmitterProfile
from Chooser import EmpiricalDistChooser
from Distribution import Uniform
import random
import MEETClasses as mc
import MEETExceptions as me


class WellStates(mc.DESStateEnabled):
    # MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['wellAgeSec']

    def __init__(self, QiGasMcfPerDay=None, QiOilBblPerDay=None, QiWaterBblPerDay=None,
                 DiPerMonth=None, bHyperbolic=None, wellAgeMonths=None,
                 completionDurationsName=None,
                 **kwargs):
        """Standard well major equipment model incorporating hyperbolic production decline curve.
        :param QiGasMcfPerDay - Initial gas production rate of well (MCF/day)
        :param QiOilBblPerDay - Initial oil or condensate production rate of well (bbl/day)
        :param QiWaterBblPerDay - Initial water production rate of well (bbl/day)
        :param DiPerMonth - Initial decline rate
        :param bHyperbolic - Hyperbolic decline parameter
        :param wellAgeMonths - Age of well at start of simulation, t = 0 (months)
        :param completionDurationsName - File path name for data file containing distribution of completion durations in (sec).
        """''

        if bHyperbolic <= 0:
            msg = f"bHyperbolic must be > 0, value: {bHyperbolic}"
            logging.warning(msg)
            raise me.IllegalArgumentError(msg)

        super().__init__(**kwargs)
        # production decline curve parameters
        self.QiOilBblPerDay = QiOilBblPerDay  # initial oil production rate (bbl/day)
        self.QiGasMcfPerDay = QiGasMcfPerDay  # initial gas production rate (Mcf/day)
        self.QiWaterBblPerDay = QiWaterBblPerDay  # initial water production rate (bbl/day)
        self.DiPerMonth = DiPerMonth  # initial decline rate
        self.bHyperbolic = bHyperbolic  # hyperbolic parameter
        self.wellAgeMonths = wellAgeMonths  # age of well (months) at start of simulation (t = 0 sec)

        # Completion duration profiles
        self.completionDurationsName = completionDurationsName
        # Note: Completion emission rates are handled by StateBasedEmitter model. Well model only needs the duration for
        # the state machine implementation.

    def instantiateFromTemplate(self, simdm, **kwargs):
        newInst = super().instantiateFromTemplate(simdm, **kwargs)

        # pick completion duration
        epDir = au.expandFilename(simdm.config['emitterProfileDir'], simdm.config)
        epPath = Path(epDir)
        if self.completionDurationsName:
            completionDurations = EmitterProfile.readEmitterFile(epPath / self.completionDurationsName)
            newInst.completionDuration = int(completionDurations.pick())
        else:
            newInst.completionDuration = 0

        newInst.stateTimes = dict.fromkeys(
            ['PREPRODUCTION', 'COMPLETION', 'STEADY_PRODUCTION', 'LU_PRODUCTION', 'LU_SHUTIN', 'LU_VENT'], 0)
        newInst.nextTS = 0  # time stamp of next scheduled state change
        return newInst

    def resetNextTS(self, currentTime, delay):
        self.nextTS = currentTime + delay

    def getNextTS(self):
        return self.nextTS

    def getWellAge(self, t):
        """calculate well age (months) at time t (secs) into simulation"""
        return u.secsToMonths(t + u.monthsToSecs(self.wellAgeMonths))

    def HyperbolicFraction(self, t):
        """Return ratio of the production rate of the well at time t in the simulation relative to the initial
        production rate of the well assuming a hyperbolic decline curve"""
        # t = time into simulation in sec (sec)
        currentAge = self.getWellAge(t)  # current age of well in months
        if currentAge < 0:
            return 0
        return 1. / (1 + self.DiPerMonth * self.bHyperbolic * currentAge) ** (1.0 / self.bHyperbolic)

    def getGasMcfPerDay(self, t):
        """Return the gas production rate (Mcf/d) at simulation time t assuming a hyperbolic decline curve"""
        cs = self.getCurrentState()
        if cs in ['STEADY_PRODUCTION', 'LU_PRODUCTION']:  # if current state is a producing state
            return self.QiGasMcfPerDay * self.HyperbolicFraction(t)
        else:  # if current state is not a producing state
            return 0.0

    def getOilBblPerDay(self, t):
        """Return the oil production rate (bbl/d) at simulation time t assuming a hyperbolic decline curve"""
        cs = self.getCurrentState()
        if cs in ['STEADY_PRODUCTION', 'LU_PRODUCTION']:  # if current state is a producing state
            return self.QiOilBblPerDay * self.HyperbolicFraction(t)
        else:  # if current state is not a producing state
            return 0.0

    def getWaterBblPerDay(self, t):
        """Return the water production rate (bbl/d) at simulation time t assuming a hyperbolic decline curve"""
        cs = self.getCurrentState()
        if cs in ['STEADY_PRODUCTION', 'LU_PRODUCTION']:  # if current state is a producing state
            return self.QiWaterBblPerDay * self.HyperbolicFraction(t)
        else:  # if current state is not a producing state
            return 0.0

    # def CumulativeHyperbolicFraction(self, t):
    #     """Return integral of decline curve at time t in the simulation.  Returned value is in units of (days) and when
    #     multiplied by the initial production rate, q_i in (bbl/day) for liquids or (MCF/day) for gas will provide the
    #     total number of bbl or MCF produced between the start of production (wellAge = 0) and time t"""
    #     # t = time into simulation in sec (sec)
    #     currentAge = self.getWellAge(t)  # current age of well in months
    #     if currentAge < 0:
    #         return 0
    #     return u.DAYS_PER_MONTH / (self.DiPerMonth * self.bHyperbolic * (1 - 1 / self.bHyperbolic)) * \
    #            (1 / (1 + self.DiPerMonth * self.bHyperbolic * currentAge) ** (1 / self.bHyperbolic - 1) - 1)
    #
    # def getCumulativeGasMcf(self, t):
    #     """Return the cumulative gas production (Mcf) at simulation time t assuming a hyperbolic decline curve"""
    #     return self.QiGasMcfPerDay * self.CumulativeHyperbolicFraction(t)
    #
    # def getCumulativeOilBbl(self, t):
    #     """Return the cumulative oil production (bbl) at simulation time t assuming a hyperbolic decline curve"""
    #     return self.QiOilBblPerDay * self.CumulativeHyperbolicFraction(t)
    #
    # def getCumulativeWaterBbl(self, t):
    #     """Return the cumulative water production (bbl) at simulation time t assuming a hyperbolic decline curve"""
    #     return self.QiWaterBblPerDay * self.CumulativeHyperbolicFraction(t)

    def getTimeForState(self, currentTime=0):
        """Return duration in current state"""
        # overridden by each well model (WellNoLiquidUnloading, WellManualLiquidUnloading, WellAutomatedLiquidUnloading
        return

    def getStateMachine(self):
        """ Major equipment states are:
            PREPRODUCTION - Before well is completed;
            COMPLETION - During well completion;
            STEADY_PRODUCTION - Well producing prior to onset of liquid unloading;
            LU_PRODUCTION - Well producing after onset of liquid unloading;
            LU_SHUTIN - Well shut in prior to vented liquid unloading;
            LU_VENT - Well venting to atmosphere during vented liquid unloading. """

        sm = {"PREPRODUCTION": {'stateDuration': self.getTimeForState, 'nextState': "COMPLETION"},
              "COMPLETION": {'stateDuration': self.getTimeForState, 'nextState': "STEADY_PRODUCTION"},
              "STEADY_PRODUCTION": {'stateDuration': self.getTimeForState, 'nextState': "LU_PRODUCTION"},
              "LU_PRODUCTION": {'stateDuration': self.getTimeForState, 'nextState': "LU_SHUTIN"},
              "LU_SHUTIN": {'stateDuration': self.getTimeForState, 'nextState': "LU_VENT"},
              "LU_VENT": {'stateDuration': self.getTimeForState, 'nextState': "LU_PRODUCTION"},
              }

        return sm, self.initialState, self.stateTimes[self.initialState]


class WellNoLiquidUnloading(mc.MajorEquipment, mc.StateChangeInitiator, WellStates):

    def __init__(self, **kwargs):
        """ Well model with no liquid unloading. """
        super().__init__(**kwargs)
        self.stateMachine = {"PREPRODUCTION": {'stateDuration': 0, 'nextState': "COMPLETION"},
                             "COMPLETION": {'stateDuration': 0, 'nextState': "STEADY_PRODUCTION"},
                             "STEADY_PRODUCTION": {'stateDuration': self.getTimeForState, 'nextState': "STEADY_PRODUCTION"},
                             }

    def instantiateFromTemplate(self, simdm, **kwargs):
        newInst = super().instantiateFromTemplate(simdm, **kwargs)
        if newInst is None:
            return newInst
        # newInst.stateTimes = dict.fromkeys(['PREPRODUCTION', 'COMPLETION', 'STEADY_PRODUCTION'], 0.0)

        # Determine initial state
        if u.monthsToSecs(self.wellAgeMonths) < - newInst.completionDuration:
            newInst.initialState = "PREPRODUCTION"
            newInst.initialStateTime = int(-1 * (u.monthsToSecs(self.wellAgeMonths) + newInst.completionDuration))
        elif u.monthsToSecs(self.wellAgeMonths) < 0:
            newInst.initialState = "COMPLETION"
            newInst.initialStateTime = int(-1 * u.monthsToSecs(self.wellAgeMonths))
        else:
            newInst.initialState = "STEADY_PRODUCTION"
            newInst.initialStateTime = u.getSimDuration()

        newInst.stateTimes[newInst.initialState] = newInst.initialStateTime
        return newInst

    def getTimeForState(self, currentTime=0, currentStateData=None, currentStateInfo=None):
        # override getTimeForState for special case with no liquid unloadings
        cs = currentStateInfo.stateName
        if cs == 'PREPRODUCTION':
            delay = self.stateTimes[cs]
            # if ts == 0, we have already calculated our initial state times
            # delay = self.stateTimes[self.initialState]
        elif cs == 'COMPLETION':
            delay = self.stateTimes[cs]
            # delay = self.completionDuration
        elif cs == 'STEADY_PRODUCTION':
            delay = u.getSimDuration()
        else:
            raise ValueError('unknown current state'.format(cs))
        self.resetNextTS(currentTime, delay)
        return delay

    def getStateMachine(self):
        """ Well state machine for special case with no liquid unloadings. Major equipment states are:
                    PREPRODUCTION - Before well is completed;
                    COMPLETION - During well completion;
                    STEADY_PRODUCTION - Well producing prior to onset of liquid unloading."""

        # sm = {"PREPRODUCTION": {'stateDuration': self.getTimeForState, 'nextState': "COMPLETION"},
        #       "COMPLETION": {'stateDuration': self.getTimeForState, 'nextState': "STEADY_PRODUCTION"},
        #       "STEADY_PRODUCTION": {'stateDuration': self.getTimeForState, 'nextState': "STEADY_PRODUCTION"},
        #       }
        return self.stateMachine, self.initialState, self.stateTimes[self.initialState]


class WellAutomatedLiquidUnloadingDeterministic(mc.MajorEquipment, mc.StateChangeInitiator, WellStates):
    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['LUProductionDuration']

    def __init__(self, LUFrequency=None, LUShutInDuration=None,
                 LUVentDuration=None, LUOnsetAge=None,
                 **kwargs):
        """ Well including automated liquid unloading. Unloading timing in this model is deterministic (there is no
            jitter between events or between monte carlo iterations).
            :param LUFrequency - frequency of vented unloading events (events/year)
            :param LUShutInDuration - duration well is shut in before each vent event (sec)
            :param LUVentDuration - duration of each vent event (sec)
            :param LUOnsetAge - age of well when LU cycles first begin (months)
            """
        super().__init__(**kwargs)

        # check parameter values
        if LUOnsetAge < 0:
            msg = "LUOnsetAge must be >= 0"
            logging.warning(msg)
            raise ValueError(msg)
        if LUFrequency <= 0:
            msg = "LUFrequency must be > 0"
            logging.warning(msg)
            raise ValueError(msg)
        if LUShutInDuration < 1:
            msg = "LUShutInDuration must be >= 1"
            logging.warning(msg)
            raise ValueError(msg)
        if LUVentDuration < 1:
            msg = "LUVentDuration must be >= 1"
            logging.warning(msg)
            raise ValueError(msg)

        self.LUFrequency = LUFrequency  # events per year
        self.LUShutInDuration = LUShutInDuration  # seconds
        self.LUVentDuration = LUVentDuration  # seconds
        self.LUOnsetAge = LUOnsetAge  # months
        LUProdDur = int((u.SECONDS_PER_YEAR / self.LUFrequency) - self.LUVentDuration - self.LUShutInDuration)
        if LUProdDur < 1:
            LUProdDur = 1
            logging.warning('LUFrequency, LUShutInDuration, and LUVentDuration poorly defined such that state duration '
                            'LU_PRODUCTION < 1. Overriding LUFrequency such that state duration LU_PRODUCTION = 1')
        self.LUProductionDuration = LUProdDur

    def instantiateFromTemplate(self, simdm, **kwargs):
        newInst = super().instantiateFromTemplate(simdm, **kwargs)
        if newInst is None:
            return newInst

        # Determine initial state
        if u.monthsToSecs(self.wellAgeMonths) < - newInst.completionDuration:
            newInst.initialState = "PREPRODUCTION"
            newInst.initialStateTime = int(-1 * (u.monthsToSecs(self.wellAgeMonths) + newInst.completionDuration))
        elif u.monthsToSecs(self.wellAgeMonths) < 0:
            newInst.initialState = "COMPLETION"
            newInst.initialStateTime = int(-1 * u.monthsToSecs(self.wellAgeMonths))
        elif u.monthsToSecs(self.wellAgeMonths) < u.monthsToSecs(self.LUOnsetAge):
            newInst.initialState = "STEADY_PRODUCTION"
            newInst.initialStateTime = int(u.monthsToSecs(self.LUOnsetAge) - u.monthsToSecs(self.wellAgeMonths))
        else:
            cycleDuration = int(u.SECONDS_PER_YEAR / self.LUFrequency)
            timeAfterLUOnset = int(u.monthsToSecs(self.wellAgeMonths) - u.monthsToSecs(self.LUOnsetAge))
            _, timeIntoCycle = divmod(cycleDuration, timeAfterLUOnset)
            if timeIntoCycle < cycleDuration - self.LUShutInDuration - self.LUVentDuration:
                newInst.initialState = "LU_PRODUCTION"
                newInst.initialStateTime = int(
                    cycleDuration - self.LUShutInDuration - self.LUVentDuration - timeIntoCycle)
            elif timeIntoCycle < cycleDuration - self.LUVentDuration:
                newInst.initialState = "LU_SHUTIN"
                newInst.initialStateTime = int(cycleDuration - self.LUVentDuration - timeIntoCycle)
            else:
                newInst.initialState = "LU_VENT"
                newInst.initialStateTime = int(cycleDuration - timeIntoCycle)

        newInst.stateTimes[newInst.initialState] = newInst.initialStateTime
        return newInst

    def getTimeForState(self, currentTime=0):
        # override getTimeForState for special case with automated liquid unloadings
        cs = self.getCurrentState()
        if currentTime == 0:
            # if ts == 0, we have already calculated our initial state time
            delay = self.stateTimes[self.initialState]
        elif cs == 'COMPLETION':  # otherwise, if we are transitioning to:
            delay = self.completionDuration
        elif cs == 'STEADY_PRODUCTION':
            delay = int(u.monthsToSecs(self.LUOnsetAge))
        elif cs == 'LU_PRODUCTION':
            delay = self.LUProductionDuration
        elif cs == 'LU_SHUTIN':
            delay = self.LUShutInDuration
        elif cs == 'LU_VENT':
            delay = self.LUVentDuration
        else:
            raise ValueError('unknown current state'.format(cs))
        self.resetNextTS(currentTime, delay)
        return delay


class WellAutomatedLiquidUnloading(mc.MajorEquipment, mc.StateChangeInitiator, WellStates):
    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['LUProdDurDist', 'LUShutInDurDist', 'LUVentDurDist']

    def __init__(self, LUOnsetAge=None, LUFrequency=None,
                 LUShutInDurationMin=None, LUShutInDurationMax=None,
                 LUVentDurationMin=None, LUVentDurationMax=None,
                 **kwargs):
        """ Well including automated liquid unloading. Unloading timing is stochastic in this model. Duration of each
            liquid unloading state is selected each unloading event from uniform distributions between the min and max
            parameters. Duration of LU_Production state is drawn from uniform distribution where min and max duration
            are derived from LU frequency and the max and min durations in the other LU states.
            :param LUOnsetAge - Age of well when LU cycles first begin (months)
            :param LUFrequency - Frequency of vented unloading events (events/year)
            :param LUShutInDurationMin - Minimum duration well is shut in before each vent event (sec)
            :param LUShutInDurationMax - Maximum duration well is shut in before each vent event (sec)
            :param LUVentDurationMin - Minimum duration of each vent event (sec)
            :param LUVentDurationMax - Maximum duration of each vent event (sec)
            """

        super().__init__(**kwargs)

        # check parameter values
        if LUFrequency <= 0:
            msg = "LUFrequency must be > 0"
            logging.warning(msg)
            raise ValueError(msg)
        if LUOnsetAge < 0:
            msg = "LUOnsetAge must be >= 0"
            logging.warning(msg)
            raise ValueError(msg)
        if LUShutInDurationMin < 1:
            msg = "LUShutInDurationMin must be >= 1"
            logging.warning(msg)
            raise ValueError(msg)
        if LUShutInDurationMax <= LUShutInDurationMin:
            msg = "LUShutInDurationMax must be > LUShutInDurationMin"
            logging.warning(msg)
            raise ValueError(msg)
        if LUVentDurationMin < 1:
            msg = "LUVentDurationMin must be >= 1"
            logging.warning(msg)
            raise ValueError(msg)
        if LUVentDurationMax <= LUVentDurationMin:
            msg = "LUVentDurationMax must be > LUVentDurationMin"
            logging.warning(msg)
            raise ValueError(msg)

        self.LUFrequency = LUFrequency  # events per year
        self.LUOnsetAge = LUOnsetAge  # months
        self.LUShutInDurationMin = LUShutInDurationMin  # seconds
        self.LUShutInDurationMax = LUShutInDurationMax  # seconds
        self.LUVentDurationMin = LUVentDurationMin  # seconds
        self.LUVentDurationMax = LUVentDurationMax  # seconds

        # Build uniform distributions to draw from for cycle timing parameters
        LUProdDurMin = u.SECONDS_PER_YEAR * 1 / self.LUFrequency - self.LUShutInDurationMax - self.LUVentDurationMax
        LUProdDurMax = u.SECONDS_PER_YEAR * 1 / self.LUFrequency - self.LUShutInDurationMin - self.LUVentDurationMin
        if LUProdDurMin < 1:
            LUProdDurMin = 1
            logging.warning(
                'Calculated minimum state duration for state LU_PRODUCTION < 1. Setting minimum duration = 1')
        if LUProdDurMax < LUProdDurMin:
            LUProdDurMax = LUProdDurMin + 1
            logging.warning(
                'Calculated maximum state duration for state LU_PRODUCTION < LUProdDurMin. Setting maximum duration = LUProdDurMin+1')
        self.LUProdDurDist = Uniform({'min': LUProdDurMin, 'max': LUProdDurMax})
        self.LUShutInDurDist = Uniform({'min': self.LUShutInDurationMin, 'max': self.LUShutInDurationMax})
        self.LUVentDurDist = Uniform({'min': self.LUVentDurationMin, 'max': self.LUVentDurationMax})

    def instantiateFromTemplate(self, simdm, **kwargs):
        newInst = super().instantiateFromTemplate(simdm, **kwargs)
        if newInst is None:
            return newInst

        # Determine initial state
        if u.monthsToSecs(self.wellAgeMonths) < - newInst.completionDuration:
            newInst.initialState = "PREPRODUCTION"
            newInst.initialStateTime = int(-1 * (u.monthsToSecs(self.wellAgeMonths) + newInst.completionDuration))
        elif u.monthsToSecs(self.wellAgeMonths) < 0:
            newInst.initialState = "COMPLETION"
            newInst.initialStateTime = int(-1 * u.monthsToSecs(self.wellAgeMonths))
        elif u.monthsToSecs(self.wellAgeMonths) < u.monthsToSecs(self.LUOnsetAge):
            newInst.initialState = "STEADY_PRODUCTION"
            newInst.initialStateTime = int(u.monthsToSecs(self.LUOnsetAge) - u.monthsToSecs(self.wellAgeMonths))
        else:
            newInst.stateTimes = {}
            newInst.calcALUCycleTimes()  # calculate state times for cycle
            cycleDuration = newInst.stateTimes['LU_PRODUCTION'] + newInst.stateTimes['LU_SHUTIN'] + newInst.stateTimes[
                'LU_VENT']  # calculate duration of full cycle
            timeIntoCycle = random.randint(1,
                                           cycleDuration - 1)  # start at a random time into cycle (minus 1 to prevent time into cycle = cycle duration)
            if timeIntoCycle < newInst.stateTimes['LU_PRODUCTION']:
                newInst.initialState = "LU_PRODUCTION"
                newInst.initialStateTime = int(newInst.stateTimes['LU_PRODUCTION'] - timeIntoCycle)
            elif timeIntoCycle < newInst.stateTimes['LU_PRODUCTION'] + newInst.stateTimes['LU_SHUTIN']:
                newInst.initialState = "LU_SHUTIN"
                newInst.initialStateTime = int(
                    newInst.stateTimes['LU_PRODUCTION'] + newInst.stateTimes['LU_SHUTIN'] - timeIntoCycle)
            else:
                newInst.initialState = "LU_VENT"
                newInst.initialStateTime = int(cycleDuration - timeIntoCycle)

        newInst.stateTimes[newInst.initialState] = newInst.initialStateTime
        return newInst

    def getTimeForState(self, currentTime=0):
        # override getTimeForState for special case with automated liquid unloadings
        cs = self.getCurrentState()
        if currentTime == 0:
            # if ts == 0, we have already calculated our initial state time
            delay = int(self.stateTimes[self.initialState])
        # otherwise, if we are transitioning to:
        elif cs == 'COMPLETION':
            delay = int(self.completionDuration)
        elif cs == 'STEADY_PRODUCTION':
            delay = int(u.monthsToSecs(self.LUOnsetAge))
        elif cs == 'LU_PRODUCTION':
            self.calcALUCycleTimes()  # reset cycle times for this cycle
            delay = int(self.stateTimes[cs])
        else:
            delay = int(self.stateTimes[cs])

        self.resetNextTS(currentTime, delay)
        return delay

    def calcALUCycleTimes(self):
        """Select cycle times for automated liquid unloading from uniform distributions and set state durations for this
        cycle."""
        self.stateTimes['LU_PRODUCTION'] = int(self.LUProdDurDist.pick())
        self.stateTimes['LU_SHUTIN'] = int(self.LUShutInDurDist.pick())
        self.stateTimes['LU_VENT'] = int(self.LUVentDurDist.pick())


class WellManualLiquidUnloadingDeterministic(mc.MajorEquipment, mc.StateChangeInitiator, WellStates):
    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['LUProductionDuration']

    def __init__(self, LUOnsetAge=None, LUFrequency=None,
                 LUShutInDuration=None, LUVentDuration=None,
                 **kwargs):
        """Well including manual liquid unloading. Unloading timing in this model is deterministic
            (there is no jitter between monte carlo iterations). Timing of state changes in MLU cycle must occur during
            "working hours" which leads to some deviation from specified frequency and durations of individual events.
            :param LUOnsetAge - age of well when LU cycles first begin (months)
            :param LUFrequency - frequency of vented unloading events (events/year)
            :param LUShutInDuration - duration well is shut in before each vent event (sec)
            :param LUVentDuration - duration of each vent event (sec)
            """
        super().__init__(**kwargs)
        # check parameter values
        if LUOnsetAge < 0:
            msg = "LUOnsetAge must be >= 0"
            logging.warning(msg)
            raise ValueError(msg)
        if LUFrequency <= 0:
            msg = "LUFrequency must be > 0"
            logging.warning(msg)
            raise ValueError(msg)
        if LUShutInDuration < 1:
            msg = "LUShutInDuration must be >= 1"
            logging.warning(msg)
            raise ValueError(msg)
        if LUVentDuration < 1:
            msg = "LUVentDuration must be >= 1"
            logging.warning(msg)
            raise ValueError(msg)
        self.LUOnsetAge = LUOnsetAge  # Months
        self.LUFrequency = LUFrequency  # events per year
        self.LUShutInDuration = LUShutInDuration  # seconds
        self.LUVentDuration = LUVentDuration  # seconds

        # calc production duration from Freq, shut in duration, and vent duration
        LUProdDur = int(u.SECONDS_PER_YEAR * 1 / self.LUFrequency - self.LUShutInDuration - self.LUVentDuration)
        if LUProdDur < 1:
            LUProdDur = 1
            logging.warning('Minimum state duration for state LU_PRODUCTION < 1. Setting minimum duration = 1')
        self.LUProductionDuration = LUProdDur

    def instantiateFromTemplate(self, simdm, **kwargs):
        newInst = super().instantiateFromTemplate(simdm, **kwargs)
        if newInst is None:
            return newInst

        # Determine initial state
        if u.monthsToSecs(self.wellAgeMonths) < - newInst.completionDuration:
            newInst.initialState = "PREPRODUCTION"
            newInst.initialStateTime = int(-1 * (u.monthsToSecs(self.wellAgeMonths) + newInst.completionDuration))
        elif u.monthsToSecs(self.wellAgeMonths) < 0:
            newInst.initialState = "COMPLETION"
            newInst.initialStateTime = int(-1 * u.monthsToSecs(self.wellAgeMonths))
        elif u.monthsToSecs(self.wellAgeMonths) < u.monthsToSecs(self.LUOnsetAge):
            newInst.initialState = "STEADY_PRODUCTION"
            newInst.initialStateTime = int(u.monthsToSecs(self.LUOnsetAge) - u.monthsToSecs(self.wellAgeMonths))
        else:
            newInst.stateTimes = dict.fromkeys(['LU_PRODUCTION', 'LU_SHUTIN', 'LU_VENT'], 0.0)
            newInst.calcMLUCycleTimes(0)
            newInst.initialState = "LU_PRODUCTION"
            newInst.initialStateTime = newInst.stateTimes["LU_PRODUCTION"]

        newInst.stateTimes[newInst.initialState] = newInst.initialStateTime
        return newInst

    def getTimeForState(self, currentTime=0):
        """ Returns duration (secs) in current state."""
        # override getTimeForState for special case with manual liquid unloadings
        cs = self.getCurrentState()
        if currentTime == 0:
            # if ts == 0, we have already calculated our initial state time
            delay = self.stateTimes[self.initialState]
        # otherwise, if we are transitioning to:
        elif cs == 'COMPLETION':
            delay = self.completionDuration
        elif cs == 'STEADY_PRODUCTION':
            delay = int(u.monthsToSecs(self.LUOnsetAge))
        elif cs == 'LU_PRODUCTION':
            self.calcMLUCycleTimes(currentTime)  # reset cycle times for this cycle
            delay = self.stateTimes[cs]
        else:
            delay = self.stateTimes[cs]

        self.resetNextTS(currentTime, delay)
        return delay

    def calcMLUCycleTimes(self, currentTime):
        """ Calculate manual liquid unloading (MLU) cycle times. State durations are specified. All MLU state changes
        must occur during working hours for operator to manually adjust valve positions. This leads to some deviations
        from specified durations and frequency. """
        LUProducingTime = u.nextWorkingSec(currentTime + self.LUProductionDuration) - currentTime
        self.stateTimes['LU_PRODUCTION'] = LUProducingTime
        LUShutInTime = u.nextWorkingSec(currentTime + LUProducingTime + self.LUShutInDuration) - (
                    currentTime + LUProducingTime)
        self.stateTimes['LU_SHUTIN'] = LUShutInTime
        LUVentTime = u.nextWorkingSec(currentTime + LUProducingTime + LUShutInTime + self.LUVentDuration) - (
                    currentTime + LUProducingTime + LUShutInTime)
        self.stateTimes['LU_VENT'] = LUVentTime


class WellManualLiquidUnloading(mc.MajorEquipment, mc.StateChangeInitiator, WellStates):
    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['LUProdDurDist', 'LUShutInDurDist', 'LUVentDurDist']

    def __init__(self, LUOnsetAge=None, LUFrequency=None,
                 LUShutInDurationMin=None, LUShutInDurationMax=None,
                 LUVentDurationMin=None, LUVentDurationMax=None,
                 **kwargs):
        """ Well including manual liquid unloading. Unloading timing is stochastic in this model. Duration of each
            liquid unloading state is selected each unloading event from uniform distributions between the min and max
            parameters. Timing of state changes in MLU cycle must occur during "working hours".
            :param LUOnsetAge - Age of well when LU cycles first begin (months)
            :param LUFrequency - Frequency of vented unloading events (events/year)
            :param LUShutInDurationMin - Minimum duration well is shut in before each vent event (sec)
            :param LUShutInDurationMax - Maximum duration well is shut in before each vent event (sec)
            :param LUVentDurationMin - Minimum duration of each vent event (sec)
            :param LUVentDurationMax - Maximum duration of each vent event (sec)"""

        super().__init__(**kwargs)

        # check parameter values
        if LUOnsetAge < 0:
            msg = "LUOnsetAge must be >= 0"
            logging.warning(msg)
            raise ValueError(msg)
        if LUFrequency <= 0:
            msg = "LUFrequency must be > 0"
            logging.warning(msg)
            raise ValueError(msg)
        if LUShutInDurationMin < 1:
            msg = "LUShutInDurationMin must be >= 1"
            logging.warning(msg)
            raise ValueError(msg)
        if LUShutInDurationMax <= LUShutInDurationMin:
            msg = "LUShutInDurationMax must be > LUShutInDurationMin"
            logging.warning(msg)
            raise ValueError(msg)
        if LUVentDurationMin < 1:
            msg = "LUVentDurationMin must be >= 1"
            logging.warning(msg)
            raise ValueError(msg)
        if LUVentDurationMax <= LUVentDurationMin:
            msg = "LUVentDurationMax must be > LUVentDurationMin"
            logging.warning(msg)
            raise ValueError(msg)

        self.LUFrequency = LUFrequency  # events per year
        self.LUOnsetAge = LUOnsetAge  # Months
        self.LUShutInDurationMin = LUShutInDurationMin  # seconds
        self.LUShutInDurationMax = LUShutInDurationMax  # seconds
        self.LUVentDurationMin = LUVentDurationMin  # seconds
        self.LUVentDurationMax = LUVentDurationMax  # seconds

        # Build uniform distributions to draw from for cycle timing parameters
        LUProdDurMin = u.SECONDS_PER_YEAR * 1 / self.LUFrequency - self.LUShutInDurationMax - self.LUVentDurationMax
        LUProdDurMax = u.SECONDS_PER_YEAR * 1 / self.LUFrequency - self.LUShutInDurationMin - self.LUVentDurationMin
        if LUProdDurMin < 1:
            LUProdDurMin = 1
            logging.warning('Minimum state duration for state LU_PRODUCTION < 1. Setting minimum duration = 1')
        if LUProdDurMax < LUProdDurMin:
            LUProdDurMax = LUProdDurMin + 1
            logging.warning(
                'Calculated maximum state duration for state LU_PRODUCTION < LUProdDurMin. Setting maximum duration = LUProdDurMin+1')
        self.LUProdDurDist = Uniform({'min': LUProdDurMin, 'max': LUProdDurMax})
        self.LUShutInDurDist = Uniform({'min': self.LUShutInDurationMin, 'max': self.LUShutInDurationMax})
        self.LUVentDurDist = Uniform({'min': self.LUVentDurationMin, 'max': self.LUVentDurationMax})

    def instantiateFromTemplate(self, simdm, **kwargs):
        newInst = super().instantiateFromTemplate(simdm, **kwargs)
        if newInst is None:
            return newInst

        # Determine initial state
        if u.monthsToSecs(self.wellAgeMonths) < - newInst.completionDuration:
            newInst.initialState = "PREPRODUCTION"
            newInst.initialStateTime = int(-1 * (u.monthsToSecs(self.wellAgeMonths) + newInst.completionDuration))
        elif u.monthsToSecs(self.wellAgeMonths) < 0:
            newInst.initialState = "COMPLETION"
            newInst.initialStateTime = int(-1 * u.monthsToSecs(self.wellAgeMonths))
        elif u.monthsToSecs(self.wellAgeMonths) < u.monthsToSecs(self.LUOnsetAge):
            newInst.initialState = "STEADY_PRODUCTION"
            newInst.initialStateTime = int(u.monthsToSecs(self.LUOnsetAge) - u.monthsToSecs(self.wellAgeMonths))
        else:
            newInst.stateTimes = {}
            newInst.calcMLUCycleTimes()
            cycleDuration = newInst.stateTimes['LU_PRODUCTION'] + newInst.stateTimes['LU_SHUTIN'] + newInst.stateTimes[
                'LU_VENT']
            timeIntoCycle = random.randint(1, cycleDuration)
            if timeIntoCycle < newInst.stateTimes['LU_PRODUCTION']:
                newInst.initialState = "LU_PRODUCTION"
                newInst.initialStateTime = int(newInst.stateTimes['LU_PRODUCTION'] - timeIntoCycle)
            elif timeIntoCycle < newInst.stateTimes['LU_PRODUCTION'] + newInst.stateTimes['LU_SHUTIN']:
                newInst.initialState = "LU_SHUTIN"
                newInst.initialStateTime = int(
                    newInst.stateTimes['LU_PRODUCTION'] + newInst.stateTimes['LU_SHUTIN'] - timeIntoCycle)
            else:
                newInst.initialState = "LU_VENT"
                newInst.initialStateTime = int(cycleDuration - timeIntoCycle)

        newInst.stateTimes[newInst.initialState] = newInst.initialStateTime
        return newInst

    def getTimeForState(self, currentTime=0):
        """Returns duration(secs) in current state."""
        # override getTimeForState for special case with manual liquid unloadings
        cs = self.getCurrentState()
        if currentTime == 0:
            # if ts == 0, we have already calculated our initial state time
            delay = self.stateTimes[self.initialState]
        # otherwise, if we are transitioning to:
        elif cs == 'COMPLETION':
            delay = self.completionDuration
        elif cs == 'STEADY_PRODUCTION':
            delay = int(u.monthsToSecs(self.LUOnsetAge))
        elif cs == 'LU_PRODUCTION':
            self.calcMLUCycleTimes(currentTime)  # reset cycle times for this cycle
            delay = self.stateTimes[cs]
        else:
            delay = self.stateTimes[cs]

        self.resetNextTS(currentTime, delay)
        return delay

    def calcMLUCycleTimes(self, currentTime):
        """ Calculate manual liquid unloading (MLU) cycle times. State durations are drawn from uniform distributions.
        All MLU state changes must occur during working hours for operator to manually adjust valve positions. """
        # todo: get current time from simdm instead?
        LUProducingTime = int(u.nextWorkingSec(currentTime + self.LUProdDurDist.pick()) - currentTime)
        self.stateTimes['LU_PRODUCTION'] = LUProducingTime
        LUShutInTime = int(u.nextWorkingSec(currentTime + LUProducingTime + self.LUShutInDurDist.pick()) - (
                    currentTime + LUProducingTime))
        self.stateTimes['LU_SHUTIN'] = LUShutInTime
        LUVentTime = int(u.nextWorkingSec(currentTime + LUProducingTime + LUShutInTime + self.LUVentDurDist.pick()) - (
                    currentTime + LUProducingTime + LUShutInTime))
        self.stateTimes['LU_VENT'] = LUVentTime


class WellCyclic(mc.MajorEquipment, mc.StateChangeInitiator):

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['stateMachine', 'ShutInDurDist', 'LiqProdDurDist', 'GasProdDurDist']

    def __init__(self, QiGasMcfPerDay=None, QiOilBblPerDay=None, QiWaterBblPerDay=None,
                 CyclesPerDay=None, LiqProdTimeMinSec=None, LiqProdTimeMaxSec=None,
                 GasProdTimeMinSec=None, GasProdTimeMaxSec=None, ShutInTimeVar=None,
                 **kwargs):
        """Well which operates cyclically - ShutIn -> Liquid Production -> Gas Production -> ShutIn.
        Production rates are constant (no decline) at values of Qi.  Model assumes Qi values are based on monthly
        production numbers, and since well only produces intermittently the actual flow rate are larger during producing
        states.
        :param QiGasMcfPerDay - Gas production rate of well (MCF/day)
        :param QiOilBblPerDay - Oil or condensate production rate of well (bbl/day)
        :param QiWaterBblPerDay - Water production rate of well (bbl/day)
        :param CyclesPerDay - Nominal cycle frequency
        :param LiqProdTimeMinSec - Minimum time in liquid production state (sec)
        :param LiqProdTimeMaxSec - Maximum time in liquid production state (sec)
        :param GasProdTimeMinSec - Minimum time in gas production state (sec)
        :param GasProdTimeMaxSec - Maximum time in gas production state (sec)
        :param ShutInTimeVar - variability in shut in time (fraction) applied as +/- nominal shutin time.

        """
        super().__init__(**kwargs)

        # check parameter values
        if CyclesPerDay <= 0:
            msg = "CyclesPerDay must be > 0"
            logging.warning(msg)
            raise ValueError(msg)

        if LiqProdTimeMinSec <= 0:
            msg = "LiqProdTimeMinSec must be > 0"
            logging.warning(msg)
            raise ValueError(msg)
        if LiqProdTimeMaxSec <= LiqProdTimeMinSec:
            msg = "LiqProdTimeMaxSec must be > LiqProdTimeMinSec"
            logging.warning(msg)
            raise ValueError(msg)

        if GasProdTimeMinSec <= 0:
            msg = "GasProdTimeMinSec must be > 0"
            logging.warning(msg)
            raise ValueError(msg)
        if GasProdTimeMaxSec <= GasProdTimeMinSec:
            msg = "GasProdTimeMaxSec must be > GasProdTimeMinSec"
            logging.warning(msg)
            raise ValueError(msg)

        if ShutInTimeVar < 0:
            msg = "ShutInTimeVar must be >= 0"
            logging.warning(msg)
            raise ValueError(msg)

        # production decline curve parameters
        self.QiOilBblPerDay = QiOilBblPerDay  # initial oil production rate (bbl/day)
        self.QiGasMcfPerDay = QiGasMcfPerDay  # initial gas production rate (Mcf/day)
        self.QiWaterBblPerDay = QiWaterBblPerDay  # initial water production rate (bbl/day)
        # cycle timing parameters
        self.CyclesPerDay = CyclesPerDay
        self.LiqProdTimeMinSec = LiqProdTimeMinSec
        self.LiqProdTimeMaxSec = LiqProdTimeMaxSec
        self.GasProdTimeMinSec = GasProdTimeMinSec
        self.GasProdTimeMaxSec = GasProdTimeMaxSec
        self.ShutInTimeVar = ShutInTimeVar

        # Build uniform distributions to draw from for state times
        NominalShutInDur = u.SECONDS_PER_DAY / self.CyclesPerDay - \
                           (self.LiqProdTimeMinSec + self.LiqProdTimeMaxSec) / 2 - \
                           (self.GasProdTimeMinSec + self.GasProdTimeMaxSec) / 2
        ShutInTimeMin = int((1 - self.ShutInTimeVar) * NominalShutInDur)
        ShutInTimeMax = int((1 + self.ShutInTimeVar) * NominalShutInDur)
        if ShutInTimeMin < 1:
            ShutInTimeMin = 1
            logging.warning('Calculated minimum state duration for state SHUT-IN < 1. Setting minimum duration = 1')
        if ShutInTimeMax < ShutInTimeMin:
            ShutInTimeMax = ShutInTimeMin + 1
            logging.warning('Calculated maximum state duration for state SHUT-IN < ShutInTimeMin. Setting maximum duration = ShutInTimeMin + 1')
        self.ShutInDurDist = Uniform({'min': ShutInTimeMin, 'max': ShutInTimeMax})
        self.LiqProdDurDist = Uniform({'min': self.LiqProdTimeMinSec, 'max': self.LiqProdTimeMaxSec})
        self.GasProdDurDist = Uniform({'min': self.GasProdTimeMinSec, 'max': self.GasProdTimeMaxSec})

        # state machine
        self.stateMachine = {
            'SHUTIN': {'stateDuration': self.ShutInDurHook, 'nextState': 'LIQUID PRODUCTION'},
            'LIQUID PRODUCTION': {'stateDuration': self.LiqDurHook, 'nextState': 'GAS PRODUCTION'},
            'GAS PRODUCTION': {'stateDuration': self.GasDurHook, 'nextState': 'SHUTIN'}
        }

    def getStateMachine(self):
        """ Major equipment states are:
            SHUTIN - Well shut in prior to producing;
            LIQUID PRODUCTION - Well producing liquids only (slug production);
            GAS PRODUCTION - Well producing gas only (after slug production);
            """
        return self.stateMachine, self.initialState, self.initialStateTime

    def instantiateFromTemplate(self, simdm, **kwargs):
        newInst = super().instantiateFromTemplate(simdm, **kwargs)
        if newInst is None:
            return newInst
        # Determine initial state
        tSI = self.ShutInDurDist.pick()
        tLiq = self.LiqProdDurDist.pick()
        tGas = self.GasProdDurDist.pick()
        tCycle = tSI + tLiq + tGas
        tIntoCycle = int(random.random()*tCycle)
        if tIntoCycle < tSI:
            newInst.initialState = "SHUTIN"
            newInst.initialStateTime = int(tSI-tIntoCycle)
        elif tIntoCycle < tSI+tLiq:
            newInst.initialState = "LIQUID PRODUCTION"
            newInst.initialStateTime = int(tSI + tLiq - tIntoCycle)
        else:
            newInst.initialState = "GAS PRODUCTION"
            newInst.initialStateTime = int(tCycle - tIntoCycle)
        #newInst.stateTimes[newInst.initialState] = newInst.initialStateTime
        newInst.nextTS = newInst.initialStateTime
        return newInst

    def ShutInDurHook(self, currentTime):
        self.delay = int(self.ShutInDurDist.pick())
        self.resetNextTS(currentTime, self.delay)
        return self.delay

    def LiqDurHook(self, currentTime):
        self.delay = int(self.LiqProdDurDist.pick())
        self.resetNextTS(currentTime, self.delay)
        return self.delay

    def GasDurHook(self, currentTime):
        self.delay = int(self.GasProdDurDist.pick())
        self.resetNextTS(currentTime, self.delay)
        return self.delay

    def resetNextTS(self, currentTime, delay):
        self.nextTS = currentTime + delay

    def getNextTS(self):
        return self.nextTS

    def getGasMcfPerDay(self, t):
        """Return the gas production rate (Mcf/d) at simulation time t"""
        # def args include t in signature to match other well models
        cs = self.getCurrentState()
        if cs in ['GAS PRODUCTION']:  # if current state is a producing state
            # adjust production rate by fraction of time its actually in producing state
            tProdAvg = (self.GasProdTimeMinSec + self.GasProdTimeMaxSec) / 2
            tAdj = u.SECONDS_PER_DAY/self.CyclesPerDay/tProdAvg
            Q = self.QiGasMcfPerDay * tAdj
            return Q
        else:  # if current state is not a producing state
            return 0.0

    def getOilBblPerDay(self, t):
        """Return the oil production rate (bbl/d) at simulation time t"""
        # def args include t in signature to match other well models
        cs = self.getCurrentState()
        if cs in ['LIQUID PRODUCTION']:  # if current state is a producing state
            # adjust production rate by fraction of time its actually in producing state
            tProdAvg = (self.LiqProdTimeMinSec + self.LiqProdTimeMaxSec) / 2
            tAdj = u.SECONDS_PER_DAY / self.CyclesPerDay / tProdAvg
            Q = self.QiOilBblPerDay * tAdj
            return Q
        else:  # if current state is not a producing state
            return 0.0

    def getWaterBblPerDay(self, t):
        """Return the water production rate (bbl/d) at simulation time t"""
        # def args include t in signature to match other well models
        cs = self.getCurrentState()
        if cs in ['LIQUID PRODUCTION']:  # if current state is a producing state
            # adjust production rate by fraction of time its actually in producing state
            tProdAvg = (self.LiqProdTimeMinSec + self.LiqProdTimeMaxSec) / 2
            tAdj = u.SECONDS_PER_DAY / self.CyclesPerDay / tProdAvg
            Q = self.QiWaterBblPerDay * tAdj
        else:  # if current state is not a producing state
            return 0.0