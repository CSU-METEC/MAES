import unittest
import MEETClasses as mc
import EquipmentTable as et
import DESMain2 as dm
import SimDataManager as sdm
from datetime import datetime

class InterruptingStateMachine(et.MajorEquipment, mc.StateChangeInitiator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        pass

    def getStateMachine(self):
        self.sm = {
            'STATE1': {'stateDuration': 1000, 'nextState': 'STATE2'},
            'STATE2': {'stateDuration': 1000, 'nextState': 'STATE1'}
        }

        return self.sm, 'STATE1', 0

class InterruptedStateMachine(et.MajorEquipment, mc.DESStateEnabled, mc.StateChangeNotificationDestination):
    def __init__(self, eventLogger=None, **kwargs):
        super().__init__(**kwargs)
        self.eventLogger = eventLogger
        self.stateDuration = 2000
        self.stateChangeForced = False

    def getStateDuration(self, currentTime):
        return self.stateDuration

    def getStateMachine(self):
        self.sm = {
            'STATE3': {'stateDuration': self.getStateDuration, 'nextState': 'STATE4'},
            'STATE4': {'stateDuration': self.getStateDuration, 'nextState': 'STATE3'}
        }

        return self.sm, 'STATE3', 0

    def stateChangeNotification(self, currentTime, stateName, command, initiator=None, **kwargs):
        self.eventLogger.logRawEvent(currentTime, self.key, 'STATE_CHANGE_NOTIFICATION', event='START', initiator=initiator.key)
        pass

    def stateChangeNotificationWithInterrupt(self, currentTime, stateName, command, initiator=None, **kwargs):
        self.eventLogger.logRawEvent(currentTime, self.key, 'STATE_CHANGE_NOTIFICATION', event='START', initiator=initiator.key)
        if not self.stateChangeForced:
            self.stateChangeForced = True
            self.stateDuration = 3000
            self.forceStateChange()
        pass

SCENARIO_TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"

FAKE_CONFIG = {
    "scenarioTimestampFormat": SCENARIO_TIMESTAMP_FORMAT,
    "outputDir": "testOutput",
    "MCTemplateDir": "{outputDir}/MC_{scenarioTimestamp}/template",
    "MCScenarioDir": "{outputDir}/MC_{scenarioTimestamp}/{MCScenario}/",
    "scenarioTimestamp": datetime.now().strftime(SCENARIO_TIMESTAMP_FORMAT),
    "simDurationSeconds": 10050
}

class BasicStateTest(unittest.TestCase):

    def eventTSTest(self, simdm, key, tsSet, commandToQuery='STATE_TRANSITION'):
        events = simdm.getEventLog().queryIntervals(command=commandToQuery)
        startEventsForInt1 = filter(lambda x: x['name'] == key and x['event'] == 'START', events)
        tsForEvents = set(map(lambda x: x['timestamp'], startEventsForInt1))
        self.assertEqual(tsSet, tsForEvents)

    def test_setup(self):
        with sdm.SimDataManager(FAKE_CONFIG) as simdm:
            fakeFac = et.Facility(facilityID="fakeFac")
            int1 = InterruptingStateMachine(facilityID="fakeFac", unitID="unit1", mcRunNum=0)
            intd1 = InterruptedStateMachine(facilityID="fakeFac", unitID="unit2", mcRunNum=0)
            dm.main(simdm, mcRunNum=0)
            self.eventTSTest(simdm, int1.key, {0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000})
            self.eventTSTest(simdm, intd1.key, {0, 2000, 4000, 6000,  8000, 10000})


    def test_stateComm(self):
        with sdm.SimDataManager(FAKE_CONFIG) as simdm:
            fakeFac = et.Facility(facilityID="fakeFac")
            int1 = InterruptingStateMachine(facilityID="fakeFac", unitID="unit1", mcRunNum=0)
            intd1 = InterruptedStateMachine(facilityID="fakeFac", unitID="unit2", mcRunNum=0)
            int1.registerForStateChangeNotification(intd1, intd1.stateChangeNotification)
            intd1.eventLogger = simdm.eventLogger
            dm.main(simdm, mcRunNum=0)
            self.eventTSTest(simdm, int1.key, {0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000})
            self.eventTSTest(simdm, intd1.key, {0, 2000, 4000, 6000, 8000, 10000})
            self.eventTSTest(simdm,
                             intd1.key,
                             {0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000},
                             commandToQuery='STATE_CHANGE_NOTIFICATION')

    def test_stateInterrupt(self):
        with sdm.SimDataManager(FAKE_CONFIG) as simdm:
            fakeFac = et.Facility(facilityID="fakeFac")
            int1 = InterruptingStateMachine(facilityID="fakeFac", unitID="unit1", mcRunNum=0)
            intd1 = InterruptedStateMachine(facilityID="fakeFac", unitID="unit2", mcRunNum=0)
            int1.registerForStateChangeNotification(intd1, intd1.stateChangeNotificationWithInterrupt)
            intd1.eventLogger = simdm.eventLogger
            dm.main(simdm, mcRunNum=0)
            self.eventTSTest(simdm,
                             intd1.key,
                             {0, 3000, 6000, 9000})
            self.eventTSTest(simdm,
                             intd1.key,
                             {0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000},
                             commandToQuery='STATE_CHANGE_NOTIFICATION')



