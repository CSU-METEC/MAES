import AppUtils as au
from EventLogger import EventLogger
import pandas as pd
import os
import SimDataManager as sdm
import numpy as np


class Summary():
    def __init__(self, summaryPath):
        self.summaryPath = summaryPath
        self.grandSummary = {}
        self.writer = pd.ExcelWriter(self.summaryPath)

    def summarizeSingleMCRun(self, mcRunNum, simdm):
        self.event(simdm, mcRunNum)
        self.leaks(simdm, mcRunNum)

    def dumpFullSummary(self):
        pass
        # with pd.ExcelWriter(self.summaryPath) as writer:
        #     for sheetName in self.grandSummary.keys():
        #         self.grandSummary[sheetName].to_excel(writer, sheet_name=sheetName)

    def event(self, simdm, mcRunNum):
        stateTransitionEvents = simdm.getEventLog().queryIntervals(command='STATE_TRANSITION', event='START')
        emissionEvents = simdm.getEventLog().queryIntervals(command='EMISSION')
        emissionEvents = simdm.getEventLog().queryIntervals(command='EMISSION')
        statesDataframe = pd.DataFrame(stateTransitionEvents)
        emissionDataframe = pd.DataFrame(map(simdm.evalEmissionEventForSummary, emissionEvents))
        self.stateSummary(statesDataframe)
        self.emissionSummary(emissionDataframe, simdm)

    def leaks(self, simdm, mcRunNum):
        mdToDump, eqToDump = simdm.equipmentTable.tablesForMCRun(mcRunNum)
        leaks = mdToDump[(mdToDump['equipmentType'].str.contains('Leak')) | (mdToDump['equipmentType'] == 'ActivityFactor')]
        groupingParams = ['facilityID', 'unitID', 'mcRunNum', 'modelSubcategory', 'modelEmissionCategory']
        aggParams = {'equipmentCount': ['sum'], 'modelReadableName': 'count'}
        file = leaks.groupby(groupingParams).agg(aggParams)
        file.columns = ['Activity Factor', 'Equipment Count']
        file['Equipment Count'] = file['Equipment Count']-1
        grouped = file.reset_index()
        #self.toStateDataframe('Component Leaks', grouped)
        self.dumpSheet('Component Leaks', grouped)

    def stateSummary(self, stateDf):
        if stateDf.empty:
            return
        groupingParams = ['facilityID', 'unitID', 'mcRunNum', 'state']
        aggParams = {'timestamp': 'first', 'duration': ['sum', 'mean', 'min', 'max', 'std', 'count']}
        grouped = self.grouping(stateDf, groupingParams, aggParams)
        grouped.columns = ['first_timestamp', 'duration_sum', 'duration_mean', 'duration_min', 'duration_max', 'duration_std', 'count']
        grouped = grouped.reset_index()
        #self.toStateDataframe('state_summary', grouped)
        self.dumpSheet('state_summary', grouped)

    def emissionSummary(self, emissionDataframe, simdm):
        if emissionDataframe.empty:
            return
        groupingParams = ['facilityID', 'unitID', 'emitterID', 'mcRunNum', 'tsUnits']
        fixedColumns = ['eventID', 'timestamp', 'name', 'command',
                        'driverTSKey', 'duration', 'nextTS', 'gcFingerprint', 'tsVal', 'tsUnits',
                        'proxiedFacilityID', 'proxiedUnitID', 'proxiedEmitterID']
        gasColumns = [col for col in emissionDataframe.columns if col not in fixedColumns]
        gas_params = {}
        for gas in gasColumns:
            gas_params[gas] = ['mean', 'min', 'max']
        aggParams = {'tsVal': ['sum', 'mean'], 'timestamp': 'first', 'duration': ['mean', 'min', 'max', 'std', 'count'],
                     'driverTSKey': 'nunique', 'gcFingerprint': 'nunique'}
        aggParams = {**aggParams, **gas_params}
        newcolumns = [gas + function for gas in gasColumns for function in ['_avg', '_min', '_max']]
        grouped = self.grouping(emissionDataframe, groupingParams, aggParams)
        fixedAggColumns = ['tsVal_sum', 'tsVal_mean', 'first_timestamp', 'duration_mean', 'duration_min', 'duration_max', 'duration_std', 'count','driverTS_count', 'gcFingerprint_count'] + newcolumns
        grouped.columns = fixedAggColumns
        for gas in gasColumns:
            grouped['emissionrate_'+ gas] = grouped[gas + '_avg']/ grouped['duration_mean']
        grouped = grouped.reset_index()
        metadata = simdm.equipmentTable.equipmentAttributes
        #modelSubCategory
        result = (metadata[['facilityID', 'unitID', 'emitterID', 'mcRunNum',
                         'latitude', 'longitude',
                         'modelCategory', 'modelSubcategory', 'modelEmissionCategory', 'modelReadableName']]
                  .merge(grouped, on=['facilityID', 'unitID', 'emitterID', 'mcRunNum']))

        self.dumpSheet('emission_summary', result)

    def grouping(self, eventDataframe, groupingParams, aggParams):
        eventDataframe[['facilityID', 'unitID', 'emitterID', 'mcRunNum']] = pd.DataFrame(eventDataframe.name.tolist(), index=eventDataframe.index)
        eventDataframe['duration'] = eventDataframe['nextTS'] - eventDataframe['timestamp']
        grouped = eventDataframe.groupby(groupingParams).agg(aggParams)
        return grouped

    def toStateDataframe(self, sheetName, summaryDF):
        if sheetName not in self.grandSummary:
            self.grandSummary[sheetName] = pd.DataFrame()
        self.grandSummary[sheetName] = self.grandSummary[sheetName].append(summaryDF)
        pass


    def dumpSheet(self, sheetName, summaryDf):
        outDf = pd.DataFrame(summaryDf)
        if sheetName in self.writer.sheets.keys():
            startingRow = self.writer.sheets[sheetName].max_row
            writeHeader = False
        else:
            startingRow = 0
            writeHeader = True
        outDf.to_excel(self.writer, sheetName, index=False, header=writeHeader, startrow=startingRow)
        self.writer.save()


