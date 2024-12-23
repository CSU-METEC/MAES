import json
import math
import pandas as pd
import numpy as np
from Timer import Timer
import csv
from pathlib import Path

CORE_EVENT_FIELDS = [
    'eventID',
    'facilityID',
    'unitID',
    'emitterID',
    'mcRun',
    'timestamp',
    'command',
    'state',
    'event',
    'duration',
    'nextTS',
    'gcKey',
    'flowID',
    'tsKey',
]

SECONDARY_EVENT_FIELDS = [
    'eventID',
    'fieldName',
    'fieldValue'
]

def transformEventFields(event):
    inf = float("inf")
    ret = dict(map(lambda kv: (kv[0], kv[1] if kv[1] != inf else "inf"), event.items()))
    return ret

def flattenEventFields(event):
    nameField = event.get('name', None)
    if not nameField:
        return event
    newEvent = {**event}
    newEvent.pop('name')
    newEvent['facilityID'] = nameField[0]
    newEvent['unitID'] = nameField[1]
    newEvent['emitterID'] = nameField[2]
    newEvent['mcRun'] = nameField[3]
    return newEvent

class EventLogger:

    def __init__(self, logFile, mode, flushOnEvent=False):
        self.logFile = logFile
        self.mode = mode
        self.file = None
        self.flushOnEvent = flushOnEvent
        self.eventSerialNumber = 0

    def __enter__(self):
        if self.logFile:
            self.file = open(self.logFile, self.mode)
        return self

    def __exit__(self, type, value, traceback):
        if self.file:
            self.file.close()
            self.file = None

    def logEvent(self, timestamp, deviceID, state, event, **kwargs):
        newkwargs = {'state': state, 'event': event, **kwargs}
        return self.logRawEvent(timestamp, deviceID, 'STATE_TRANSITION', **newkwargs)

    def logCreation(self, timestamp, deviceID, **kwargs):
        return self.logRawEvent(timestamp, deviceID, "CREATE-DEVICE", **kwargs)

    def logSubStates(self, timestamp, deviceID, **kwargs):
        return self.logRawEvent(timestamp, deviceID, "SUB_STATE_CHANGE", **kwargs)

    def logEmission(self, timestamp, duration, deviceID, driverTSKey=None, GCKey=None, **kwargs):
        newkwargs = {'tsKey': driverTSKey, 'duration': duration, 'nextTS': timestamp + duration, 'gcKey': GCKey, **kwargs}
        return self.logRawEvent(timestamp, deviceID, "EMISSION", **newkwargs)

    def logFluidFlow(self, timestamp, duration, deviceID, tsKey=None, gcKey=None, **kwargs):
        newkwargs = {'tsKey': tsKey, 'duration': duration, 'nextTS': timestamp + duration, 'gcKey': gcKey, **kwargs}
        return self.logRawEvent(timestamp, deviceID, "FLUID-FLOW", **newkwargs)

    def logLeakCreation(self, timestamp, duration, deviceID, tsKey=None, gcKey=None, **kwargs):
        newkwargs = {'tsKey': tsKey, 'duration': duration, 'nextTS': timestamp + duration, 'gcKey': gcKey, **kwargs}
        return self.logRawEvent(timestamp, deviceID, "LEAK-CREATION", **newkwargs)

    def logRawEvent(self, timestamp, name, command, **kwargs):
        event = transformEventFields({'eventID': self.eventSerialNumber, 'timestamp': timestamp, 'name': name, 'command': command, **kwargs})
        self.eventSerialNumber += 1
        str = json.dumps(event)
        self.file.write(str)
        self.file.write("\n")
        if self.flushOnEvent:
            self.file.flush()
        return event['eventID']

def convertEvent(inEvent):
    ts = inEvent['timestamp']
    dur = float(inEvent.get('duration', math.inf))
    outEvent = {
                'name':      inEvent['name'],
                'command':   inEvent['command'],
                'eventStartTS': ts,
                'eventEndTS':  dur+ts,
                'rawEvent':  inEvent
                }
    return outEvent

def eventListToDF(eventList):
    convertedEvents = list(map(convertEvent, eventList))
    outDF = pd.DataFrame.from_records(convertedEvents)
    return outDF

def inInterval(df, interval):
    eventsInIntervalMask = (interval[1] >= df['eventStartTS']) & (interval[0] < df['eventEndTS'])
    return eventsInIntervalMask

class StreamingEventLogger(EventLogger):
    def __init__(self,
                 config=None
                 ):
        self.eventSerialNumber = 0
        self.config = config

    def __enter__(self):
        self.oFile = open(self.config['eventFilename'], "w", newline='')
        self.dw = csv.DictWriter(self.oFile, fieldnames=CORE_EVENT_FIELDS, extrasaction='ignore')
        self.dw.writeheader()

        secondaryEventInfoPath = self.config['secondaryInfoFilename']

        self.seiFile = open(secondaryEventInfoPath, "w", newline='')
        self.seiDW = csv.DictWriter(self.seiFile, fieldnames=SECONDARY_EVENT_FIELDS)
        self.seiDW.writeheader()

        return self

    def __exit__(self, type, value, traceback):
        self.flushWriteCache()
        self.oFile.close()
        self.seiFile.close()

    def dump(self, file):
        pass

    @classmethod
    def restore(cls, file):
        inDF = pd.read_csv(file)
        secInfoDF = pd.read_csv(cls._secondaryInfoPath(file))
        if not secInfoDF.empty:
            wideSecInfoDF = pd.pivot_table(secInfoDF, index='eventID', columns='fieldName', values='fieldValue')
            retDF = inDF.merge(wideSecInfoDF, left_on='eventID', right_index=True, how='left')
        else:
            retDF = inDF
        retDF.replace({np.nan: None}, inplace=True)
        retDF.sort_values('timestamp', inplace=True, kind='mergesort')  # use mergesort for stable sorting
        return retDF

    def logRawEvent(self, timestamp, name, command, **kwargs):
        eventID = self.eventSerialNumber
        event = transformEventFields(
            {'eventID': eventID, 'timestamp': timestamp, 'name': name, 'command': command, **kwargs})
        flatEvent = flattenEventFields(event)
        self.eventSerialNumber += 1
        self.dw.writerow(flatEvent)

        secondaryInfo = filter(lambda x: x[0] not in CORE_EVENT_FIELDS, flatEvent.items())
        for singleSecondaryField in secondaryInfo:
            secondaryDict = {'eventID': eventID, 'fieldName': singleSecondaryField[0], 'fieldValue': singleSecondaryField[1]}
            self.seiDW.writerow(secondaryDict)

        return event['eventID']

    def streamEvents(self):
        for singleEvent in self.eventList:
            yield singleEvent

    def flushWriteCache(self):  # create an entry point for testing
        pass

    def queryIntervals(self, intervals=[(0, math.inf)], emitters=None, command=None, event=None, ordered=False):
        if self.df is None:
            self.flushWriteCache()
        if intervals is None:
            intervals = [(0, math.inf)]

        eventsToReturn = pd.Series([False] * len(self.df))
        for singleInterval in intervals:
            eventsInSingleInterval = inInterval(self.df, singleInterval)
            eventsToReturn = np.logical_or(eventsToReturn, eventsInSingleInterval)

        if emitters is not None:
            eventsForEmitter = self.df['name'].isin(emitters)
            eventsToReturn = np.logical_and(eventsToReturn, eventsForEmitter)

        if command is not None:
            eventsForCommand = self.df['command'] == command
            eventsToReturn = np.logical_and(eventsToReturn, eventsForCommand)

        if event is not None:
            eventsForEvent = self.df.apply(lambda x: x['rawEvent'].get('event', None) == event, axis='columns')
            eventsToReturn = np.logical_and(eventsToReturn, eventsForEvent)

        returnedDF = self.df[eventsToReturn]
        if ordered:
            returnedDF = returnedDF.sort_values('eventStartTS', axis='index', kind='mergesort')
        returnedList = list(returnedDF['rawEvent'])

        return returnedList

class SqlEventLogger(EventLogger):
    def __init__(self):
        self.eventSerialNumber = 0
        self.df = None

    def __enter__(self):
        self.eventList = []
        self.stateTransistionList=[]
        self.emissionList=[]
        self.generalEvents=[]
        return self

    def __exit__(self, type, value, traceback):
        self.flushWriteCache()

    def dump(self,engine):
        self.writeToTable(engine, self.emissionList, 'emissionEventTable')
        self.writeToTable(engine, self.stateTransistionList, 'stateTransistionTable')
        self.writeToTable(engine, self.generalEvents, 'generalEventsTable')

    def writeToTable(self,engine,eventList,tableName):
        eventDataFrame = pd.DataFrame(eventList)
        eventDataFrame[['facility', 'unitid', 'emitter', 'mcrun']] = pd.DataFrame(eventDataFrame.name.tolist())
        emissionDataFrame = eventDataFrame.drop(columns=['name'])
        emissionDataFrame.to_sql(tableName, con=engine.connect(), if_exists='append', index=False)

    def logRawEvent(self, timestamp, name, command, **kwargs):
        event = transformEventFields(
            {'eventID': self.eventSerialNumber, 'timestamp': timestamp, 'name': name, 'command': command, **kwargs})
        self.eventSerialNumber += 1
        self.eventList.append(event)
        if(event['command']=='EMISSION'):
            self.emissionList.append(event)
        elif(event['command']=='STATE_TRANSITION'):
            self.stateTransistionList.append(event)
        else:

          if(event['command']!='SIM-STOP' and event['command']!='SIM-START'):
            self.generalEvents.append(event)

        return event['eventID']


    def flushWriteCache(self):  # create an entry point for testing
        self.df = eventListToDF(self.eventList)

    def queryIntervals(self, intervals=[(0, math.inf)], emitters=None, command=None, event=None):
        if self.df is None:
            self.flushWriteCache()
        if intervals is None:
            intervals = [(0, math.inf)]

        eventsToReturn = pd.Series([False] * len(self.df))
        for singleInterval in intervals:
            eventsInSingleInterval = inInterval(self.df, singleInterval)
            eventsToReturn = np.logical_or(eventsToReturn, eventsInSingleInterval)

        if emitters is not None:
            eventsForEmitter = self.df['name'].isin(emitters)
            eventsToReturn = np.logical_and(eventsToReturn, eventsForEmitter)

        if command is not None:
            eventsForCommand = self.df['command'] == command
            eventsToReturn = np.logical_and(eventsToReturn, eventsForCommand)

        eventsToReturn = self.df[eventsToReturn]
        eventToReturn = list(eventsToReturn['rawEvent'])

        if event is not None:
            eventsForEvent = list(eventToReturn)
            eventsToEvent = pd.DataFrame(eventsForEvent)
            eventsToReturnDf = eventsToEvent.loc[eventsToEvent['event'] == event]
            eventToReturn = eventsToReturnDf.to_dict(orient='records')

        return eventToReturn


