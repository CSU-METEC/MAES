from EquipmentTable import MajorEquipment
from MEETClasses import StateChangeInitiator, StateBasedEmitter
from Distribution import Normal
from EmissionDriver import ManualEmissionDriver
import Units as u

class IntermittentPneumatic(MajorEquipment, StateChangeInitiator):
    def __init__(self, cycleTimeMean=None, cycleTimeSD=None, cycleTimeMeanSecs=None, **kwargs):
        super().__init__(**kwargs)
        self.cycleTimeMean = cycleTimeMean
        self.cycleTimeMeanSecs = u.hoursToSecs(self.cycleTimeMean)
        self.cycleTimeSD = cycleTimeSD

    def initializeDES(self, simdm, env, eh):
        super().initializeDES(simdm, env, eh)
        self.timeDistribution = Normal({'mu': self.cycleTimeMeanSecs, 'sigma': self.cycleTimeSD})
        self.calcStateTimes(0)

    def calcStateTimes(self, ts):
        self.stateTimes = {'EMITTING':    int(max(10, self.timeDistribution.pick())),
                           'NOTEMITTING': int(max(10, self.timeDistribution.pick()))}
        pass

    def getTimeForState(self, ts=0):
        cs = self.getCurrentState()
        if cs == 'NOTEMITTING':
            self.calcStateTimes(ts)
        return self.stateTimes[cs]

    def getStateMachine(self):
        sm = {'EMITTING':    {'stateDuration': self.getTimeForState,  'nextState': 'NOTEMITTING'},
              'NOTEMITTING': {'stateDuration': self.getTimeForState,  'nextState': {'EMITTING': .75, 'FAULT': .25}},
              'FAULT':       {'stateDuration': 300,                   'nextState': 'NOTEMITTING'}
        }

        return sm, 'EMITTING'



class IntermittentPneumaticEmitter(StateBasedEmitter):
    def __init__(self, emissionMean=None, emissionSD=None, emissionDriverKey=None, **kwargs):
        super().__init__(**kwargs)
        self.emissionMean = emissionMean
        self.emissionSD = emissionSD
        self.emissionDriverKey = None

    def instantiateFromTemplate(self, simdm, **kwargs):
        inst = IntermittentPneumaticEmitter(**kwargs)
        emissionDistribution = Normal({'mu': self.emissionMean, 'sigma': self.emissionSD})
        md = {'Time Basis': 's', 'Units': 'scf_wholegas', 'Gas Type': 'whole_gas'}
        driver = ManualEmissionDriver(self.unitID, emissionDistribution, md)
        inst.emissionDriverKey = simdm.addDriver4EC(self.unitID, driver)
        inst.tsKey, _ = simdm.getTimeseries(inst.emissionDriverKey)
        inst.faultTSKey, _ = simdm.scaleTimeseries(inst.tsKey, 2)
        inst.gcKey, _ = simdm.getGasComposition(inst.gasComposition)

    def stateChange(self, currentTime, state, op, delay=0, relatedEvent=0):
        if state == 'EMITTING' and op == 'START':
            self.eventLogger.logEmission(currentTime, delay, self.key, self.tsKey, self.gcKey)
        elif state == 'FAULT' and op == 'START':
            self.eventLogger.logEmission(currentTime, delay, self.key, self.faultTSKey, self.gcKey)
        pass

    def initializeDES(self, simdm, env, eh):
        self.eventLogger = eh
        emissionDistribution = Normal({'mu': self.emissionMean, 'sigma': self.emissionSD})
        md = {'Time Basis': 's', 'Units': 'scf', 'Gas Type': 'whole_gas'}
        driver = ManualEmissionDriver(self.unitID, emissionDistribution, md)
        self.emissionDriverKey = simdm.addDriver4EC(self.unitID, driver)
        self.tsKey, _ = simdm.getTimeseries(self.emissionDriverKey)
        self.gcKey, _ = simdm.getGasComposition(self.gasComposition)


        self.majorEquipment = simdm.getEquipmentTable().elementLookup(self.facilityID, self.unitID, None, self.mcRunNum)
        self.majorEquipment.registerForStateChangeNotification(self, self.stateChange)
        pass