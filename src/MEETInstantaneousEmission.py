import AppUtils as au
from EventLogger import EventLogger
import pandas as pd
import os
import EmissionDriver as ed
import json
import SimDataManager as sdm
import EquipmentTable as et

def guaranteeColumn(df, colName, colSet):
    if colName not in colSet:
        df[colName] = None

class Emission():
    def __init__(self):
        pass

    def dumpInstantaneousFiles(self, instantaneousEmissionPath, instantaneousEventPath, gasCompositionPath, timeSeriesPath, mcRunNum, simdm):
        self.instantaneousEventPath = instantaneousEventPath
        self.instantaneousEmissionPath = instantaneousEmissionPath
        self.gasCompositionPath = gasCompositionPath
        self.timeSeriesPath = timeSeriesPath
        self.mcRunNum = mcRunNum
        self.simdm = simdm
        self.eventHandler = self.simdm.getEventLog()
        self.event()
        self.instantaneousEvent()
        self.gasComposition()
        self.timeSeries()

    def event(self):
        emissionEvents = self.eventHandler.queryIntervals(command='EMISSION')
        emissionRecords = map(lambda x: {**x, **et.JsonEquipmentTable._instanceDict(x['name'])}, emissionEvents)
        emissionDataframe = pd.DataFrame(map(lambda x: {**x,**(self.simdm.evalInstantaneousEmission(x, [x['timestamp']]))}, emissionEvents))
        # emissionDataframe = pd.DataFrame(emissionRecords)
        if emissionDataframe.empty:
            return
        emissionDataframe[['facilityID', 'unitID', 'emitterID', 'mcRun']] = pd.DataFrame(emissionDataframe.name.tolist(),
                                                                                     index=emissionDataframe.index)
        emissionDataframe = emissionDataframe.drop(['driverTSKey', 'gcFingerprint','name'], axis=1)
        self.writingFile(emissionDataframe, self.instantaneousEmissionPath)

    def instantaneousEvent(self):
        events = self.eventHandler.queryIntervals()
        instantaneousEvents = pd.DataFrame(events)
        instantaneousEvents[['facilityID', 'unitID', 'emitterID', 'mcRun']] = pd.DataFrame(instantaneousEvents.name.tolist(),
                                                                                                index=instantaneousEvents.index)
        instantaneousEvents = instantaneousEvents.drop(['name'], axis=1)
        iColumns = set(instantaneousEvents.columns)

        guaranteeColumn(instantaneousEvents, 'gcFingerprint', iColumns)
        guaranteeColumn(instantaneousEvents, 'driverTSKey', iColumns)
        guaranteeColumn(instantaneousEvents, 'proxiedFacilityID', iColumns)
        guaranteeColumn(instantaneousEvents, 'proxiedUnitID', iColumns)
        guaranteeColumn(instantaneousEvents, 'proxiedEmitterID', iColumns)
        fixedColumns = ['eventID', 'facilityID', 'unitID', 'emitterID', 'mcRun',
                        'timestamp', 'command', 'state', 'event', 'duration', 'nextTS',
                        'gcFingerprint', 'driverTSKey',
                        'proxiedFacilityID', 'proxiedUnitID', 'proxiedEmitterID'
                        ]
        list_columns = iColumns - set(fixedColumns)
        instantaneousColumns = fixedColumns + list(list_columns)
        instantaneousEvents = instantaneousEvents[instantaneousColumns]
        self.writingFile(instantaneousEvents, self.instantaneousEventPath)

    def gasComposition(self):
        xformedGC = dict(map(lambda x: (x[0], x[1].toDict()), self.simdm.gasCompositionTable.items()))
        gcList =[]
        for gcFingerPrint in xformedGC.keys():
            for unitType in xformedGC[gcFingerPrint]:
                for gas,composition in xformedGC[gcFingerPrint][unitType].items():
                    row = {}
                    row['mcRun'] = self.mcRunNum
                    row['gcFingerprint'] = gcFingerPrint
                    row['unitType'] = unitType
                    row['gasName'] = gas
                    row['value'] = composition
                    gcList.append(row)
        gcDataFrame=pd.DataFrame(gcList)
        self.writingFile(gcDataFrame, self.gasCompositionPath)

    def timeSeries(self):
        xformedTS = dict(map(lambda x: (x[0], x[1].toDict()), self.simdm.timeseriesTable.items()))
        timeseries=[]
        for driverTs in xformedTS.keys():
            timeseries.append({'mcRunNum': self.mcRunNum, 'key': driverTs, **xformedTS[driverTs]})
        timeSeriesDataFrame = pd.DataFrame(timeseries)
        self.writingFile(timeSeriesDataFrame, self.timeSeriesPath)

    def writingFile(self, dataFrame, filePath):
            dataFrame.to_csv(filePath, header='column_names', index=False)





