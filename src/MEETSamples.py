import MEETClasses as mc
import Units as u
import EmissionDriver as ed
import SimDataManager as sdm
import MEETProductionWells as mp
import StateManager as sm

class LinkedWell(mc.MajorEquipment, mc.LinkedEquipmentMixin):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def productionAtTime(self, time):
        ret = round(u.secsToDays(time))
        return ret

class LinkedSeparator(mc.MajorEquipment, mc.LinkedEquipmentMixin, mc.StateChangeInitiator):

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['stateMachine', 'connectedWells']

    def __init__(self, volumeBbl=None, fillRateBblPerSec=0, fillTimeSecs=0, **kwargs):
        super().__init__(**kwargs)
        self.volumeBbl = volumeBbl
        self.fillRateBblPerSec = fillRateBblPerSec
        self.fillTimeSecs = fillTimeSecs
        self.connectedWells = []
        self.stateMachine = {
            'FILLING': {'stateDuration': self.calcFillDuration, 'nextState': 'DUMPING'},
            'DUMPING': {'stateDuration': 300, 'nextState': 'FILLING'}
        }

    def link2(self, linkedWell):
        # todo: register self (the separator) with the linkedWell to receieve state change notifications
        self.connectedWells.append(linkedWell)

    def link1(self, linkedTank):
        pass

    def getStateMachine(self):
        return self.stateMachine, 'FILLING', 0

    def calcFillDuration(self, currentTime):
        self.fillRateBblPerSec = 0
        for singleWell in self.connectedWells:
            wellProdRateBblPerDay = singleWell.getOilBblPerDay(currentTime)
            wellProdRateBblPerSec = wellProdRateBblPerDay / u.daysToSecs(1)
            self.fillRateBblPerSec += wellProdRateBblPerSec
        # todo: add some jitter to self.volumeBbl, which will translate to jitter to dump time
        # todo: keep track of current fraction full
        self.fillTimeSecs = int(self.volumeBbl / self.fillRateBblPerSec)
        self.nextDumpTime = currentTime + self.fillTimeSecs
        return self.fillTimeSecs

    # todo: on well state changes, recalulate fill duration

class LinkedSeparatorStateBasedEmitter(mc.Emitter, mc.DESEnabled, mc.StateChangeNotificationDestination):
    def __init__(self, statesActive=None, gasComposition=None, **kwargs):
        super().__init__(**kwargs)
        self.statesActive = statesActive
        self.gasComposition = gasComposition

    def initializeDES(self, simdm, env, eh):
        super().initializeDES(simdm, env, eh)
        self.majorEquipment = simdm.getEquipmentTable().elementLookup(self.facilityID, self.unitID, None, self.mcRunNum)
        self.majorEquipment.registerForStateChangeNotification(self, self.stateChange)
        # self.emissionDriverKey = simdm.getDriver4EC(self.emissionDriver)
        # self.tsKey, _ = simdm.getTimeseries(self.emissionDriverKey)
        self.gcKey, _ = simdm.getGasComposition(self.gasComposition)

    def stateChange(self, currentTime, state, op, delay=0, **kwargs):
        if op != 'START':
            return
        if state != self.statesActive:
            return
        meThroughput = self.majorEquipment.fillRateBblPerSec
        emissionDriverRate = meThroughput / delay
        ts = ed.ConstantEmissionTimeseries(emissionDriverRate, units='scf_wholegas')
        simdm = sdm.SimDataManager.getSimDataManager()
        tsKey, _ = simdm.addTimeseries(ts=ts)
        self.eventLogger.logEmission(currentTime, delay, self.key, tsKey, self.gcKey)
        pass

class LinkedTank(mc.MajorEquipment, mc.LinkedEquipmentMixin, mc.StateChangeNotificationDestination):

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['separators', 'stateMachine', 'stateManager', 'stateChangeNotificationRecipients']

    def __init__(self, fillRateBblPerSec=0, **kwargs):
        super().__init__(**kwargs)
        self.separators = {}
        self.fillRateBblPerSec = 0
        self.stateMachine = {
            'IDLE': {'nextState': 'FLASHING'},
            'FLASHING': {'nextState': 'IDLE', 'entryHook': self.flashStartHook, 'exitHook': self.flashEndHook}
        }
        self.stateChangeNotificationRecipients = {}
        simdm = sdm.SimDataManager.getSimDataManager()
        self.stateManager = sm.PostFactoStateManager(self.key, self.stateMachine, simdm.eventLogger, 'IDLE')

    def link2(self, linkedSeparator):
        self.separators[linkedSeparator.key] = {'separator': linkedSeparator, 'dumpTime': 0}
        linkedSeparator.registerForStateChangeNotification(self, self.separatorStateChange)

    def logRateChange(self, ts, oldRate, newRate, relatedEvent):
        simdm = sdm.SimDataManager.getSimDataManager()
        simdm.eventLogger.logRawEvent(ts, self.key, 'RATE-CHANGE', oldRate=oldRate, newRate=newRate, relatedEvent=relatedEvent)
        pass

    def upstreamSeparatorDumpingCount(self):
        numDumping = sum(map(lambda x: x['separator'].stateManager.currentState == 'DUMPING', self.separators.values()))
        fillRate = sum(map(lambda x: x['separator'].fillRateBblPerSec, self.separators.values()))
        return numDumping, fillRate

    def separatorStateChange(self, currentTime, sepState, op, delay=None, relatedEvent=None, initiator=None):
        if sepState != 'DUMPING':
            return
        oldFillRate = self.fillRateBblPerSec
        numDumping, newFillRate = self.upstreamSeparatorDumpingCount()
        if op == 'START':
            nextState = None
            if numDumping > 1:
                nextState = 'FLASHING'
            self.stateManager.closePrevState(currentTime, nextState)
            self.fillRateBblPerSec += initiator.fillRateBblPerSec
        else:
            nextState = None
            if numDumping > 1:
                nextState = 'FLASHING'
            self.stateManager.closePrevState(currentTime, nextState)
            self.fillRateBblPerSec -= initiator.fillRateBblPerSec
        self.logRateChange(currentTime, oldFillRate, self.fillRateBblPerSec, relatedEvent)

    def flashStartHook(self, currentTime, state, stateData, delay=None, relatedEvent=None):
        for recipKey, recipCallback in self.stateChangeNotificationRecipients.items():
            recipCallback(currentTime, state, 'START', delay=delay, relatedEvent=relatedEvent, initiator=self)


    def flashEndHook(self, currentTime, sepState, op, delay=None, relatedEvent=None):
        pass

    def registerForStateChangeNotification(self, recipient, callback):
        self.stateChangeNotificationRecipients[recipient.key] = callback

    def calcIdleDuration(self, currentTime):
        fillTimes = set()
        for sepKey, sepVal in self.separators.items():
            singleSeparator = sepVal['separator']
            ndt = singleSeparator.nextDumpTime
            sepVal['dumpTime'] = ndt
            fillTimes.add(ndt)
        ret = min(fillTimes)
        return ret

    def instantiateFromTemplate(self, simdm, **kwargs):
        newInst = super().instantiateFromTemplate(simdm, **kwargs)
        if newInst is None:
            return newInst
        return newInst


# Note this class is commented because it shadows the same class definition in MEETLinkedProductionEq.py
# class LinkedWellNoLiquidUnloading(mp.WellNoLiquidUnloading, mc.LinkedEquipmentMixin):
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)

class LinkedTankFlareEmitter(mc.Emitter, mc.DESEnabled, mc.StateChangeNotificationDestination):
    def __init__(self, gasComposition=None, flareLimit=None, **kwargs):
        super().__init__(**kwargs)
        self.gasComposition = gasComposition
        self.flareLimit = flareLimit

    def initializeDES(self, simdm, env, eh):
        super().initializeDES(simdm, env, eh)
        self.majorEquipment = simdm.getEquipmentTable().elementLookup(self.facilityID, self.unitID, None, self.mcRunNum)
        self.majorEquipment.registerForStateChangeNotification(self, self.stateChange)
        self.gcKey, _ = simdm.getGasComposition(self.gasComposition)
        self.simdm = simdm

    def stateChange(self, currentTime, state, op, delay=0, relatedEvent=0, initiator=None):
        if op != 'START':
            return
        if state != 'FLASHING':
            return
        emissionRate = max(self.majorEquipment.fillRateBblPerSec, self.flareLimit)
        ts = ed.ConstantEmissionTimeseries(emissionRate, 'bbl')
        # todo: can we check if emissionrate, units is already in (or close enough) ts then use the same key?
        tsKey, _ = self.simdm.addTimeseries(ts)
        self.eventLogger.logEmission(currentTime, delay, self.key, tsKey, self.gcKey)
        pass

class LinkedTankExcessFlareEmitter(mc.Emitter, mc.DESEnabled, mc.StateChangeNotificationDestination):
    def __init__(self, gasComposition=None, flareLimit=None, **kwargs):
        super().__init__(**kwargs)
        self.gasComposition = gasComposition
        self.flareLimit = flareLimit

    def initializeDES(self, simdm, env, eh):
        super().initializeDES(simdm, env, eh)
        self.majorEquipment = simdm.getEquipmentTable().elementLookup(self.facilityID, self.unitID, None, self.mcRunNum)
        self.majorEquipment.registerForStateChangeNotification(self, self.stateChange)
        self.gcKey, _ = simdm.getGasComposition(self.gasComposition)
        self.simdm = simdm

    def stateChange(self, currentTime, state, op, delay=0, relatedEvent=0, initiator=None):
        if op != 'START':
            return
        if state != 'FLASHING':
            return
        emissionRate = max(self.majorEquipment.fillRateBblPerSec - self.flareLimit, 0)
        ts = ed.ConstantEmissionTimeseries(emissionRate, 'bbl')
        tsKey, _ = self.simdm.addTimeseries(ts)
        self.eventLogger.logEmission(currentTime, delay, self.key, tsKey, self.gcKey)
        pass


