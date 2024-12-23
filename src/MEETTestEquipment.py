import EquipmentTable as et
import MEETClasses as mc

class TestEquipment1(et.MajorEquipment):
    def __init__(self,
                 **kwargs):
        super().__init__(**kwargs)


class TestStateEquipment(et.MajorEquipment, mc.StateChangeInitiator):

    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['stateMachine']

    def __init__(self,
                 state1Time=None,
                 state2Time=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.state1Time = state1Time
        self.state2Time = state2Time

        self.stateMachine = {'STATE1': {'nextState': 'STATE2',
                                        'stateDuration': self.state1Time,
                                        },
                             'STATE2': {'nextState': 'STATE1',
                                        'stateDuration': self.state2Time
                                        }
                             }

    def getStateMachine(self):
        return self.stateMachine, 'STATE1', self.state1Time

