import EquipmentTable as et
import GasComposition3 as gc
import MEETFluidFlow as ff
# import OGCIClasses as ogci
import Units as u
import MEETClasses as mc
from SimDataManager import SimDataManager
from pathlib import Path
import csv
import Distribution as d
import math
import logging
import MEETExceptions as me
import TimeseriesTable as ts

class MEETFFLink(et.MEETService):

    def __init__(self,
                 outletFacilityID=None,
                 outletUnitID=None,
                 inletFacilityID=None,
                 inletUnitID=None,
                 flowName=None,
                 flowSecondaryID=None,
                 **kwargs):
        super().__init__(**kwargs)  # call this to get the
        self.outletFacilityID = outletFacilityID
        self.outletUnitID = outletUnitID
        self.inletFacilityID = inletFacilityID
        self.inletUnitID = inletUnitID
        self.flowName = flowName
        self.flowSecondaryID = flowSecondaryID
        if isinstance(self.flowSecondaryID, float) and math.isnan(self.flowSecondaryID):
            self.flowSecondaryID = None
            
    def _getFluidFlowSecondaryIDs(self, me):
        allFF = me.getOutletFluidFlows(self.flowName)
        ret = map(lambda x: x.secondaryID, allFF)
        return ret

    def _addFFOptions(self, err, me):
        allSecondaryStr = "\n".join(map(lambda x: f"    {x}", self._getFluidFlowSecondaryIDs(me)))
        if allSecondaryStr:
            err = f"{err}\n  Options are:\n{allSecondaryStr}"
        return err

    def instantiateFromTemplate(self, simdm, **kwargs):
        newInst = super().instantiateFromTemplate(simdm, **kwargs)
        if not newInst:
            return None

        et = simdm.getEquipmentTable()
        mcRunNum = newInst.mcRunNum

        outletME = et.elementLookup(facilityID=self.outletFacilityID, unitID=self.outletUnitID, mcRunNum=mcRunNum)
        if outletME is None:
            err = f"Unknown link1 equipment: {self.outletFacilityID}, {self.outletUnitID}"
            logging.error(err)
            raise me.IllegalArgumentError(err)

        inletME = et.elementLookup(facilityID=self.inletFacilityID, unitID=self.inletUnitID, mcRunNum=mcRunNum)
        if inletME is None:
            err = f"Unknown link2 equipment: {self.inletFacilityID}, {self.inletUnitID}"
            logging.error(err)
            raise me.IllegalArgumentError(err)

        flows = outletME.getOutletFluidFlows(self.flowName, secondaryID=self.flowSecondaryID)
        if not flows:
            outletName = f"{outletME.facilityID}.{outletME.unitID}"
            inletName = f"{inletME.facilityID}.{inletME.unitID}"
            err = f"Outlet flow from {outletName} to {inletName}, name: {self.flowName}, secondaryID: {self.flowSecondaryID} not found"
            err = self._addFFOptions(err, outletME)
            logging.error(err)
            raise me.IllegalArgumentError(err)

        for singleFlow in flows:
            inletME.linkInletFlow(outletME, singleFlow)

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

    def flowFun(self):
        currentState = self.majorEquipment.stateManager.currentState
        ret = (currentState == self.activeState)
        return ret

#         self.stateMachine = {
#             'OPERATING':      {'origNextState': 'MALFUNCTIONING', 'stateDuration': self.pickOpDuration,   'nextState': 'MALFUNCTIONING'},
#             'MALFUNCTIONING': {'origNextState': 'OPERATING',      'stateDuration': self.pickMalfDuration, 'nextState': 'OPERATING'}
#         }

def flowToEmitter(flow, simdm):
    gcKey = flow.gc.serialNum
    tSeries = ts.ConstantTimeseriesTableEntry.factory(flow.driverRate, flow.driverUnits)
    return gcKey, tSeries.serialNum

class EmitterFF(mc.Emitter, mc.DESEnabled, mc.StateChangeNotificationDestination):

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['statesActiveList']

    def __init__(self,
                 fluid=None,
                 secondaryID=None,
                 statesActive=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.fluid = fluid
        self.secondaryID = secondaryID
        self.statesActive = statesActive
        self.statesActiveList = statesActive.split(',')

    def initializeDES(self, simdm, env, eh):
        super().initializeDES(simdm, env, eh)
        self.majorEquipment = simdm.getEquipmentTable().elementLookup(self.facilityID, self.unitID, None, self.mcRunNum)
        self.majorEquipment.registerForStateChangeNotification(self, self.stateChange)
        self.simdm = simdm
        pass


    def stateChange(self, currentTime, state, op, delay=0, relatedEvent=0, initiator=None):
        if op != 'START':
            return
        if state not in self.statesActiveList:
            return

        flowsToEmit = self.majorEquipment.getOutletFluidFlows(self.fluid, secondaryID=self.secondaryID)
        for singleFlow in flowsToEmit:
            dr = singleFlow.driverRate
            if dr != 0.0:
                gcKey, tsKey = flowToEmitter(singleFlow, self.simdm)
                self.eventLogger.logEmission(currentTime, delay, self.key, driverTSKey=tsKey, GCKey=gcKey, flowID=singleFlow.serialNumber)

class CombustedEmitterFF(EmitterFF):
    def __init__(self,
                 combustionEfficiencies=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.combustionEfficiencies = combustionEfficiencies

    def initializeDES(self, simdm, env, eh):
        super().initializeDES(simdm, env, eh)
        self.majorEquipment = simdm.getEquipmentTable().elementLookup(self.facilityID, self.unitID, None, self.mcRunNum)
        self.majorEquipment.registerForStateChangeNotification(self, self.stateChange)
        self.simdm = simdm
        self.flowsToEmit = self.majorEquipment.getOutletFluidFlows(self.fluid, secondaryID=self.secondaryID)
        pass

    def stateChange(self, currentTime, stateInfo, op, delay=0, relatedEvent=0, initiator=None):
        if op != 'START':
            return
        if stateInfo.stateName not in self.statesActiveList:
            return

        for singleFlow in self.flowsToEmit:
            if singleFlow.driverRate != 0.0:
                gcKey, tsKey = flowToEmitter(singleFlow, self.simdm)
                self.eventLogger.logEmission(currentTime, stateInfo.deltaTimeInState, self.key, driverTSKey=tsKey, GCKey=gcKey, flowID=singleFlow.serialNumber)

class DestructionEmitterFF(CombustedEmitterFF):
    def __init__(self,
                 destructionEfficiency=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.destructionEfficiency = destructionEfficiency

    def toDestructionFlow(self, origFlow):
        flowGC = origFlow.gc
        newGC = gc.DestructionGC(origGC=flowGC,
                                 destructionEfficiency=self.destructionEfficiency)
        newFlow = ff.DependentFlow(origFlow, gc=newGC)
        return newFlow

    def initializeDES(self, simdm, env, eh):
        super().initializeDES(simdm, env, eh)
        self.flowsToEmit = list(map(lambda x: self.toDestructionFlow(x),
                                    self.majorEquipment.getOutletFluidFlows(self.fluid, secondaryID=self.secondaryID)))

    def stateChange(self, currentTime, state, op, delay=0, relatedEvent=0, initiator=None):
        super().stateChange(currentTime, state, op, delay, relatedEvent, initiator)

class OutletFFSplitter(EmitterFF):

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['majorEquipment']

    def __init__(self,
                 fluidSecondaryID=None,
                 fluidSecondaryIDStateNames=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.fluidSecondaryID = fluidSecondaryID
        # todo: fluidSecondaryIDStateNames should be the same length as statesActive.  Figure out what we should do if they are not
        self.fluidSecondaryIDStateNames = fluidSecondaryIDStateNames

    def instantiateFromTemplate(self, simdm, **kwargs):
        inst = super().instantiateFromTemplate(simdm, **kwargs)
        if not inst:
            return None
        return inst

    def initializeDES(self, simdm, env, eh):
        super().initializeDES(simdm, env, eh)
        self.majorEquipment.registerForStateChangeNotification(self, self.flowStateChange)

        self.simdm = simdm
        self.majorEquipment = self.simdm.getEquipmentTable().elementLookup(self.facilityID, self.unitID, None,
                                                                           self.mcRunNum)
        #
        # manage states & fluid flows
        #
        self.stateFlowNames = dict(zip(self.statesActiveList, self.fluidSecondaryIDStateNames.split(',')))
        targetOutletFlow = self.majorEquipment.getOutletFluidFlows(fluidName=self.fluid, secondaryID=self.secondaryID)
        if not isinstance(targetOutletFlow, list):
            targetOutletFlow = [targetOutletFlow]
        else:
            targetOutletFlow = targetOutletFlow.copy()  # so we don't have an endless loop of adding dependent flows to dependent flows...
        for singleState, singleFlowName in self.stateFlowNames.items():
            for singleOutletFlow in targetOutletFlow:
                flowForState = StateDependentFluidFlow(singleOutletFlow,
                                                       secondaryID=singleFlowName,
                                                       majorEquipment=self.majorEquipment,
                                                       activeState=singleState)
                self.majorEquipment.addOutletFluidFlow(flowForState)

    def flowSwitch(self, activeState):
        currentState = self.majorEquipment.stateManager.currentState
        ret = (currentState == activeState)
        return ret

    def flowStateChange(self, *args, **kwargs):
        pass
#
# class OGCICommonHeaderFF(mc.MajorEquipment, ff.Volume, mc.LinkedEquipmentMixin, mc.StateChangeInitiator):
#
#     MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['stateMachine',
#                                          'linkedUpstreamEquipment',
#                                          'stateChangeNotificationRecipients',
#                                          'consolidatedFlowTable']
#
#     def __init__(self,
#                  componentCount=None,
#                  fluid=None,
#                  flashGC=None,
#                  flowGCTag=None,
#                  gcTag=None,
#                  **kwargs):
#         super().__init__(**kwargs)
#         self.componentCount = componentCount
#         self.fluid = fluid
#         self.linkedUpstreamEquipment = []
#         self.stateMachine = {
#             'OPERATING': {'stateDuration': self.calculateOperatingDuration, 'nextState': 'OPERATING'},
#         }
#         self.consolidatedFlowTable = {}
#
#     def getStateMachine(self):
#         return self.stateMachine, 'OPERATING', 0
#
#     def link2(self, linkedME1):
#         self.linkedUpstreamEquipment.append(linkedME1)
#
#     def linkInletFlow(self, outletME, flow):
#         if self.fluid != flow.name:
#             return
#
#         self.addInletFluidFlow(flow)
#
#         inboundGC = flow.gc
#         # Common Headers don't produce flash
#
#         if inboundGC not in self.consolidatedFlowTable:
#             self.consolidatedFlowTable[inboundGC] = ff.AggregatedFlow(flow.name, inboundGC, newUnits=flow.driverUnits)
#             self.addOutletFluidFlow(self.consolidatedFlowTable[inboundGC])
#             # simdm.getGasCompositionFF(outflowFlashGC, self.flashGC)
#         self.consolidatedFlowTable[inboundGC].addFlow(flow)
#         pass
#
#
#     # def calculateOperatingDuration(self, currentTime):
#     #     return u.getSimDuration()
#
#     def calculateOperatingDuration(self, currentTime=None, **kwargs):
#         # todo: Hardcoded to just one upstream device.  Fix this!
#         if len(self.linkedUpstreamEquipment) == 0:
#             return u.getSimDuration()
#         nextUpstreamTS = min(map(lambda x: x.nextStateTransitionTS, self.linkedUpstreamEquipment))
#         delay = nextUpstreamTS - currentTime
#         self.nextStateTransitionTS = nextUpstreamTS
#         return delay
#
#     def instantiateFromTemplate(self, simdm, **kwargs):
#         inst = super().instantiateFromTemplate(simdm, **kwargs)
#         if inst is None:
#             return None
#
#         return inst
#
#
# class OGCICyclingWellFF(mc.MajorEquipment, mc.DESStateEnabled, ff.Volume):  # Note that this does *not* inherit from OGCIWellFF, so we can define our own fluid flows
#
#     MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['stateMachine',
#                                          'upstreamEquipment',
#                                          'shutInDistribution',
#                                          'dumpDistribution',
#                                          'oilDumpFlow',
#                                          'waterDumpFlow',
#                                          'condensateGCTag',
#                                          'waterGCTag'
#                                          ]
#
#     def __init__(self,
#                  flowTag=None,
#                  oilBblPerDump=None,
#                  gasMcfPerDay=None,
#                  waterBblPerDump=None,
#                  shutInTimeMin=None,
#                  shutInTimeMax=None,
#                  dumpTimeMin=None,
#                  dumpTimeMax=None,
#                  **kwargs):
#
#         super().__init__(**kwargs)
#         self.flowTag = flowTag
#         self.oilBblPerDump = oilBblPerDump
#         self.gasMcfPerDay = gasMcfPerDay
#         self.waterBblPerDump = waterBblPerDump
#         self.shutInTimeMin = shutInTimeMin
#         self.shutInTimeMax = shutInTimeMax
#         self.dumpTimeMin = dumpTimeMin
#         self.dumpTimeMax = dumpTimeMax
#
#         self.condensateGCTag = f"{self.flowTag}-Condensate"
#         self.waterGCTag = f"{self.flowTag}-Water"
#         # self.gasGCTag = f"{self.flowTag}-Gas"
#         #
#         #
#         # self.addOutletFluidFlow(FluidFlow('Gas', u.scfPerDayToScfPerSec(self.QiGasMcfPerDay*1000), 'scf', self.gasGCTag)) # is scf_wholegas correct?
#
#         self.shutInDistribution = d.Uniform({'min': shutInTimeMin, 'max': shutInTimeMax})
#         self.dumpDistribution = d.Uniform({'min': dumpTimeMin, 'max': dumpTimeMax})
#
#         self.stateMachine = {
#             'PRODUCING': {'stateDuration': lambda x: self.calcStateDuration(x, 'PRODUCING'), 'nextState': 'SHUT-IN'},
#             'SHUT-IN':   {'stateDuration': lambda x: self.calcStateDuration(x, 'SHUT-IN'),   'nextState': 'PRODUCING'}
#             # 'PRODUCING': {'stateDuration': self.dumpDistribution,   'nextState': 'SHUT-IN'},
#             # 'SHUT-IN':   {'stateDuration': self.shutInDistribution, 'nextState': 'PRODUCING'}
#         }
#
#     def getStateMachine(self):
#         return self.stateMachine, 'PRODUCING', 0
#
#     def instantiateFromTemplate(self, simdm, **kwargs):
#         inst = super().instantiateFromTemplate(simdm, **kwargs)
#         if inst == None:
#             return inst
#
#         inst.oilDumpFlow = FluidFlow('Condensate', 0, 'bbl', inst.condensateGCTag)  # flow rate will be updated in calcStateDuration
#         inst.addOutletFluidFlow(inst.oilDumpFlow)
#         # self.addOutletFluidFlow(StateDependentFluidFlow(self.oilDumpFlow, activeState='PRODUCING', majorEquipment=self))
#         inst.waterDumpFlow = FluidFlow('Water', 0, 'bbl', inst.waterGCTag)  # flow rate will be updated in calcStateDuration
#         inst.addOutletFluidFlow(inst.waterDumpFlow)
#         # self.addOutletFluidFlow(StateDependentFluidFlow(self.oilDumpFlow, activeState='PRODUCING', majorEquipment=self))
#
#         return inst
#
#
#     def calcStateDuration(self, currentTime, state):
#         if state == 'PRODUCING':
#             duration = int(self.dumpDistribution.pick())
#             oilDumpRate = self.oilBblPerDump / duration
#             self.oilDumpFlow.driverRate = oilDumpRate
#             waterDumpRate = self.waterBblPerDump / duration
#             self.waterDumpFlow.driverRate = waterDumpRate
#         else:
#             duration = int(self.shutInDistribution.pick())
#             self.oilDumpFlow.driverRate = 0
#             self.waterDumpFlow.driverRate = 0
#         self.nextStateTransitionTS = currentTime + duration
#         return duration
#
#     def registerForStateChangeNotification(self, emitter, stateChange):
#         pass
#
# class OGCISynchedSeparatorFF(OGCIContinuousSeparatorFF):
#
#     MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['stateMachine',
#                                          'linkedUpstreamEquipment',
#                                          'nextStateTransitionTS'
#                                          ]
#
#     def __init__(self,
#                  **kwargs):
#         super().__init__(**kwargs)
#
#         self.nextStateTransitionTS = 0
#
#         self.stateMachine = {
#             'OPERATING': {'stateDuration': self.calculateOperatingDuration, 'nextState': 'OPERATING'},
#             'DUMPING': {'stateDuration': self.dumpDuration, 'nextState': 'OPERATING'},
#         }
#
#     def stateFromUpstreamME(self, upstreamME, currentTime):
#         if upstreamME.stateManager.currentState in ['PRODUCING', 'DUMPING']:
#             retState = 'DUMPING'
#         else:
#             retState = 'OPERATING'
#         if hasattr(upstreamME, 'nextStateTransitionTS'):
#             delay = upstreamME.nextStateTransitionTS - currentTime
#         else:
#             delay = 0
#         return retState, delay
#
#     def getStateMachine(self):
#         retState = 'OPERATING'
#         delay = 0
#         return self.stateMachine, retState, delay
#
#     def calculateOperatingDuration(self, currentTime=None, **kwargs):
#         self.nextStateTransitionTS = u.getSimDuration()
#         delay = self.nextStateTransitionTS - currentTime
#         return delay
#
#     def dumpDuration(self, currentTime):
#         self.nextStateTransitionTS = u.getSimDuration()
#         delay = self.nextStateTransitionTS - currentTime
#         return delay
#
#     def instantiateFromTemplate(self, simdm, **kwargs):
#         inst = super().instantiateFromTemplate(simdm, **kwargs)
#         if inst is None:
#             return inst
#
#         inst.linkedUpstreamEquipment = []
#         return inst
#
#     def link2(self, linkedME1):
#         self.linkedUpstreamEquipment.append(linkedME1)
#
# class OGCILeakFF(ogci.OGCILeak):
#     def __init__(self,
#                  **kwargs):
#         if 'gasComposition' in kwargs:
#             newKWargs = kwargs.copy()
#             newKWargs.pop('gasComposition')
#         else:
#             newKWargs = kwargs
#         super().__init__(**newKWargs)
#
# class OGCIPneumaticEmitterFF(ogci.OGCIPneumaticEmitter):
#     def __init__(self,
#                  **kwargs):
#         if 'gasComposition' in kwargs:
#             newKWargs = kwargs.copy()
#             newKWargs.pop('gasComposition')
#         else:
#             newKWargs = kwargs
#         super().__init__(**newKWargs)
#
# class OGCISpecificComponentLeakMTTRFF(ogci.SpecificLeaks):
#     def __init__(self,
#                  **kwargs):
#         if 'gasComposition' in kwargs:
#             newKWargs = kwargs.copy()
#             newKWargs.pop('gasComposition')
#         else:
#             newKWargs = kwargs
#         super().__init__(**newKWargs)
#
# class OGCIBatteryLeakFF(ogci.OGCIBatteryLeak):
#     def __init__(self,
#                  **kwargs):
#         if 'gasComposition' in kwargs:
#             newKWargs = kwargs.copy()
#             newKWargs.pop('gasComposition')
#         else:
#             newKWargs = kwargs
#         super().__init__(**newKWargs)
