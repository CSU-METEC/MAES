# import unittest
# from MEETClasses import StateChangeInitiator, StateChangeNotificationDestination
# from EventLogger import DummyEventLogger
# import simpy

# class MockDESEnabled(StateChangeInitiator):
#     def __init__(self, initialState='INITIAL', name='', **kwargs):
#         super().__init__(**kwargs)
#         self.name = name

#     def getStateMachine(self):
#         sm = {
#             'STATE1': {
#                 'stateDuration': 1000,
#                 'nextState': 'STATE2'
#             },
#             'STATE2': {
#                 'stateDuration': 500,
#                 'nextState': 'STATE1'
#             }
#         }

#         return sm, 'STATE1', 0

# class StateChangeReceiver(StateChangeNotificationDestination):
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.notificationList = []

#     def bind(self, initiator):
#         if not isinstance(initiator, StateChangeInitiator):
#             raise NotImplementedError

#         initiator.registerForStateChangeNotification(self, self.receiveNotification)

#     def receiveNotification(self, *args):
#         self.notificationList.append(args)


# class StateEnabledDESTest(unittest.TestCase):

#     def test_SimpleState(self):
#         desInst1 = MockDESEnabled(name='DES1')
#         dest = StateChangeReceiver()
#         dest.bind(desInst1)
#         with DummyEventLogger() as eh:
#             env = simpy.Environment()
#             desInst1.initializeDES(env, eh)
#             env.run(until=10000)
#         pass
