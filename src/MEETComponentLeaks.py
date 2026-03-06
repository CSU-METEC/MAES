from EquipmentTable import Facility, MajorEquipment, Emitter, MEETTemplate, ActivityFactor, EquipmentTableEntry
import MEETGlobals as mg
from pathlib import Path
import AppUtils as au
import Units as u
import numpy as np
import math
import logging
from EmitterProfile import EmitterProfile
from MEETClasses import TimedStateBasedEmitter, DESStateEnabled, StateBasedEmitter, StateChangeInitiator, \
    ActivityDistributionEnabled, EmissionDistributionEnabled, \
    EmissionManager
from MEETClasses import StateChangeNotificationDestination
import MEETClasses as mc
import MEETFluidFlow as ff
import GasComposition3 as gc
import MEETExceptions as me

class ComponentLeaks(ActivityDistributionEnabled, EmissionManager):
    """Model steady leaks from components"""
    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = []

    def __init__(self,
                 pLeak=None,
                 MTBF=None,
                 MTTR=None,
                 componentLeakInstance=None,
                 gasComposition=None,
                 emissionDriverUnits=None,
                 secondaryID=None,
                 numLeaks=None,
                 leakNum=None,
                 eqNum=None,
                 afName=None,
                 afValue=None,
                 **kwargs):
        """ :param pLeak - Fraction of components leaking, on average across MC iterations, at start of simulation, t=0;
            :param MTBF - Mean Time Between Failure (unfailed component hours per failure)
            :param MTTR - Mean Time To Repair (failed component hours per repair)
            :param componentLeakInstance - Failures instance of a single component during the simulation """
        super().__init__(**kwargs)
        self.componentLeakInstance = componentLeakInstance
        # make sure all values were provided.  model will not calculate "defaults"
        if not pLeak:
            msg = "pLeak must be set"
            logging.warning(msg)
            raise ValueError(msg)
        # check parameter values are OK
        if pLeak < 0 or pLeak >= 1:
            msg = "pLeak must be in range [0 1)"
            logging.warning(msg)
            raise ValueError(msg)
        self.pLeak = pLeak  # probability of any single component leaking at start of simulation. Actual number is selected probabilistically so may vary
        self.MTBF = MTBF                # mean time between failures (unfailed component hours)
        self.MTTR = MTTR                # mean time to repair (failed component hours)
        self.gasComposition = gasComposition
        self.emissionDriverUnits = emissionDriverUnits or 'scf'
        self.secondaryID = secondaryID
        self.numLeaks = numLeaks
        self.leakNum = leakNum
        self.eqNum = eqNum
        self.afName = afName
        self.afValue = afValue

    def overrideLeaks(self, leakList, simdm,
                      mcRunNum):
        return leakList

    def instantiateMultiple(self,
                            simdm,
                            mcRunNum=-1,
                            **kwargs):
        if self.parsedActivityDistribution is not None:
            activityCount = int(self.parsedActivityDistribution.pick())
        else:
            msg = f"Parameter activityDistribution must be specified for ActivityDistributionEnabled instance"
            logging.error(msg)
            raise me.IllegalArgumentError(msg)

        activityInst = self.createActivityFactorInstance(activityCount, mcRunNum)
        totalCount = 0
        for singleEq in range(int(activityCount)):
            # pick leaking components and timing
            tmax = simdm.config['simDurationSeconds']  # get max sim time
            leakList = self.calcLeakList(tMax=tmax, pLeak=self.pLeak, MTBF_hours=self.MTBF, MTTR_hours=self.MTTR, EFTag=self.emitterModelFactorTag)  # determine if and when this component will leak
            leakList = self.overrideLeaks(leakList, simdm, mcRunNum) #MM edit
            # If you return with no leaks, make sure you return *before* you cal super.instantiateFromTemplate()
            if not leakList:  # if leakList is empty return empty list
                continue

            numLeaks = len(leakList)
            for singleLeakNum, singleLeak in enumerate(leakList):
                newKwargs = {**kwargs,
                             **singleLeak,
                             'emitterID': None,
                             'mcRunNum': mcRunNum,
                             'instanceSerial': singleEq,
                             'activityInstanceKey': activityInst.key,
                             'numLeaks': numLeaks,
                             'leakNum': singleLeakNum,

                             'eqNum': singleEq,
                             'afName': activityInst.emitterID,
                             'afValue': activityInst.equipmentCount
                             }
                newInst = self.instantiateFromTemplate(simdm, **newKwargs)
            totalCount += len(leakList)
        return totalCount

    def initializeDES(self, simdm, env, eh):
        super().initializeDES(simdm, env, eh)
        eh.logLeakCreation(self.startTime, self.endTime-self.startTime, self.key,
                           eqNum=self.eqNum,
                           leakNum=self.leakNum, numLeaks=self.numLeaks,
                           afName=self.afName, afValue=self.afValue)

    # todo: this was cut & pasted from StateBasedEmitterProduction.  We should refactor -- perhaps into a mixin??
    def initializeFluidFlow(self, simdm):
        facility = simdm.getEquipmentTable().elementLookup(self.facilityID, None, None, self.mcRunNum)
        tmpGC = gc.FluidFlowGC(fluidFlowGCFilename=facility.productionGCFilename,
                               flow='Vapor',
                               fluidFlowID=self.gasComposition,
                               gcUnits=self.emissionDriverUnits
                               )
        emissionDriverPath = Path(au.expandFilename(simdm.config['emitterProfileDir'], simdm.config)) / self.emissionDriver
        self.fluidFlow = ff.EmpiricalFluidFlow('Vapor', emissionDriverPath, tmpGC)

    def pickFromMTTR(self, num):
        ret = int(-num * math.log(1 - np.random.random(1)[0]))
        return ret


    def calcLeakList(self, tMax=None, pLeak=None, MTBF_hours=None, MTTR_hours=None, EFTag=None):
        """Returns leak list on a single component
        :param pLeak = probability of leak at initial time, nLeaks/nComponents, (float 0-1)
        :param tMax = max sim time, seconds (integer)
        :param MTBF = mean time between failures, non-failed component hours per failure
        :param MTTR = mean time to repair, failed component hours per repair
        """
        listOfLeaks = []  # variable to hold list of leaks.

        # Convert timing parameters: Model uses secs, but MTBF and MTTR should be specified in hours by user
        MTBF_secs = u.hoursToSecs(MTBF_hours)
        MTTR_secs = u.hoursToSecs(MTTR_hours)

        # determine timing of first failure
        if np.random.random(1)[0] <= pLeak:  # component is failed (i.e. leaking) at start of simulation if rand is less than or equal to pLeak
            tstart = 0
        else:  # calculate time at which component will fail
            # tstart = int(-MTBF_secs * math.log(1 - np.random.random(1)[0]))
            tstart = self.pickFromMTTR(MTBF_secs)

        if tstart > tMax:  # if component does not fail before end of sim return empty list
            return listOfLeaks

        leakcounter = 0  # counter to increment leak instances
        leaking = True  # set leak state to True to enter while loop
        t = tstart  # t = time into simulation for current component (sec)
        while t <= tMax:  # until simulation time tMax is reached
            if leaking:
                # calculate time of repair
                # deltat = int(-MTTR_secs * math.log(1 - np.random.random()))  # time until repair (sec)
                deltat = self.pickFromMTTR(MTTR_secs)
                tstop = t + deltat  # time at which leak is repaired
                # increment leak counter and save leak as dict to listOfLeaks
                leakcounter += 1
                # set timing values for one shot emitter
                thisLeak = {'componentLeakInstance': leakcounter,
                            'startTime': t,
                            'endTime': tstop}
                listOfLeaks.append(thisLeak)
                t = tstop  # time into sim for current component on next while loop
                leaking = False  # turn leak OFF in while loop logic on next while loop
            else:
                # calculate time at which it will fail again
                # deltat = int(-MTBF_secs * math.log(1 - np.random.random()))  # time until failure (sec)
                deltat = self.pickFromMTTR(MTBF_secs)
                tstart = t + deltat  # time at which component fails
                t = tstart  # time into sim for current component on next while loop
                leaking = True  # turn leak ON in while loop logic on next while loop
        # logging.debug(f"Number of leaks: {len(listOfLeaks)}")
        return listOfLeaks


class LeakProduction(mc.FactorManager, ComponentLeaks):
    def __init__(self,
                 totalLeaks=None,
                 pLeak=None,
                 surveyFrequency=None,
                 instanceFormat='{unitID}_leak_{serialNo}',
                 **kwargs):
        MTBF = (1 * surveyFrequency * u.HOURS_PER_DAY) / pLeak
        MTTR = (MTBF * pLeak) / (1 - pLeak)
        newArgs = {**kwargs, 'pLeak': pLeak, 'MTBF': MTBF, 'MTTR': MTTR, 'instanceFormat': instanceFormat}
        super().__init__(**newArgs)
        self.totalLeaks = totalLeaks
        self.surveyFrequency = surveyFrequency

