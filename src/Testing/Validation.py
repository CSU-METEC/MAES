import os
import sys
sys.path.insert(0, "C:\\Users\\wines\\Documents\\CAMS-MEET\\src")
import pandas as pd
from src import AppUtils as au
from src import ModelFormulation as mf
from src import DistributionProfile as dp
import glob
import pathlib
import matplotlib.pyplot as plt
import itertools
from src import SiteMain2 as sm
import argparse
from src import Units as u
import ColumnNames as cn
import json


class Validation:
    def __init__(self, folderPaths=None, resultsFolder=None):
        self.folderPaths = folderPaths
        self.resultsFolder = resultsFolder

    def validate(self):
        msg = f'Cannot validate {self}'
        raise NotImplementedError(msg)

    def dumpValidation(self):
        msg = f'Cannot dump {self}'
        raise NotImplementedError(msg)

    def validateAndDump(self):
        self.validate()
        self.dumpValidation()

    def getStateInfo(self, ie, unitID, stateName):
        states = ie[(ie['unitID'] == unitID) & (ie['state'] == stateName)]
        stateTime = sum(states[(states['event'] == 'START')]['duration'])
        stateCount = states[(states['event'] == 'START')].shape[0]
        stateTimeAvg = stateTime / stateCount
        return stateTime, stateCount, stateTimeAvg

    def readSingleOutput(self, outputPath):
        outputPath = str(outputPath)
        intakePath = glob.glob(outputPath + '/0/*.xlsx')[0]
        outputPath = pathlib.Path(outputPath)
        outputs = {}
        intake = mf.parseIntakeSpreadsheet(intakePath)

        for folder in outputPath.iterdir():
            iePath = folder / 'instantaneousEvents.csv'
            eqPath = folder / 'equipment.json'
            mdPath = folder / 'metadata.csv'
            sePath = folder / 'secondaryEventInfo.csv'
            etPath = folder / 'emissionTimeseries.csv'
            gcPath = folder / 'gasCompositions.csv'

            seTemp = []
            if sePath.exists():
                seTemp = pd.read_csv(sePath)

            ieTemp = []
            if iePath.exists():
                ieTemp = pd.read_csv(iePath)

            eqTemp = {}
            if eqPath.exists():
                f = open(eqPath)
                eqCount = 0
                for singleLine in f:
                    singleEquipment = json.loads(singleLine)
                    eqTemp[str(eqCount)] = singleEquipment
                    # eqTemp[singleEquipment['implClass']] = singleEquipment
                    eqCount += 1

            mdTemp = pd.DataFrame
            if mdPath.exists():
                mdTemp = pd.read_csv(mdPath)

            etTemp = []
            if etPath.exists():
                etTemp = pd.read_csv(etPath)

            gcTemp = []
            if gcPath.exists():
                gcTemp = pd.read_csv(gcPath)

            outputs[folder.name] = {'instantaneousEvents': ieTemp,
                                    'equipment': eqTemp,
                                    'metadata': mdTemp,
                                    'secondaryEventInfo': seTemp,
                                    'emissionTimeseries': etTemp,
                                    'gasCompositions': gcTemp}

        return intake, outputs


class EmitterValidation(Validation):
    def __init__(self,
                 timestamp=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.timestamp = timestamp
        self.emitterPath = self.folderPaths['Emitters']
        self.inputs, self.outputs = self.readSingleOutput(self.emitterPath)
        self.outputs.pop('template')
        self.totalLeaks = 0
        self.leaksDF = 0

    def validate(self):
        self.totalLeaks, self.leaksDF = self.checkProbabilitiesLeaks(self.inputs, self.outputs, self.timestamp)
        i = 10

    def dumpValidation(self):
        self.totalLeaks.to_csv(self.resultsFolder / 'totalLeaks.csv', index=False)
        self.leaksDF.to_csv(self.resultsFolder / 'leaksDF.csv', index=False)
        i = 10

    def checkProbabilitiesLeaks(self, intake, outputs, t):
        # outputs.pop('template')
        simtime = u.daysToSecs(intake['simParameters'][0]['1/1/2021'])
        noOfMC = intake['simParameters'][1]['1/1/2021']
        LeakEmitterClasses = ['LeakProduction', 'SpecificLeaksProduction', 'PneumaticEmitterProduction']
        LeaksCountList = []
        for singleOutputKey, singleOutputValue in outputs.items():
            md = singleOutputValue['metadata']
            eqJsonToDF = pd.DataFrame(singleOutputValue['equipment']).T
            unitIDs = md['unitID'].dropna().unique()
            for unitID in unitIDs:
                for singleEmitter in LeakEmitterClasses:
                    mdSingleEq = md[(md['unitID'] == unitID) & (md['implClass'] == singleEmitter)]
                    eqJsonSingleEq = eqJsonToDF[(eqJsonToDF['unitID'] == unitID) & (eqJsonToDF['implClass'] == singleEmitter)]
                    for uniqueEmitter in mdSingleEq['modelReadableName'].dropna().unique():
                        mdUniqueEq = mdSingleEq[(mdSingleEq['modelReadableName'] == uniqueEmitter)]
                        eqJsonUnique = eqJsonSingleEq[(eqJsonSingleEq['modelReadableName'] == uniqueEmitter)].dropna(subset=['startTime', 'endTime'])
                        eqJsonUniqueTimeFiltered = eqJsonUnique[(eqJsonUnique['startTime'] <= t)]
                        eqJsonUniqueTimeFiltered = eqJsonUniqueTimeFiltered[(eqJsonUniqueTimeFiltered['endTime'] >= t)]
                        eqCount = int(mdUniqueEq[(mdUniqueEq['equipmentType'] == 'ActivityFactor')]['equipmentCount'])
                        totalLeakCount = mdUniqueEq[(mdUniqueEq['equipmentType'] != 'ActivityFactor')].shape[0]
                        leaksCount = eqJsonUniqueTimeFiltered[(eqJsonUniqueTimeFiltered['equipmentType'] != 'ActivityFactor')].shape[0]
                        meanMTTR = eqJsonUniqueTimeFiltered[(eqJsonUniqueTimeFiltered['equipmentType'] != 'ActivityFactor')]['MTTR'].mean()
                        meanMTBF = eqJsonUniqueTimeFiltered[(eqJsonUniqueTimeFiltered['equipmentType'] != 'ActivityFactor')]['MTBF'].mean()
                        if singleEmitter == 'LeakProduction':
                            MTBFReference = eqJsonUniqueTimeFiltered['surveyFrequency'].unique().all() / \
                                            eqJsonUniqueTimeFiltered['pLeak'].unique().all()
                            MTBFMinRef = MTBFReference
                            MTBFMaxRef = MTBFReference
                            MTTRReference = (MTBFReference * eqJsonUniqueTimeFiltered['pLeak'].unique().all()) / (
                                        1 - eqJsonUniqueTimeFiltered['pLeak'].unique().all())
                            MTTRMinRef = MTTRReference
                            MTTRMaxRef = MTTRReference
                            totalLeakTimeOfAllLeaks = sum(eqJsonUnique['endTime']-eqJsonUnique['startTime'])
                            pLeakTotal = totalLeakTimeOfAllLeaks / (simtime * eqCount)
                            pLeakIn = eqJsonUnique['pLeak'].unique().all()

                            i = 10
                        elif singleEmitter == 'SpecificLeaksProduction':
                            MTTRMinRef = eqJsonUnique[(eqJsonUnique['equipmentType'] != 'ActivityFactor')][
                                'MTTRMinDays'].unique().all()
                            MTTRMaxRef = eqJsonUnique[(eqJsonUnique['equipmentType'] != 'ActivityFactor')][
                                'MTTRMaxDays'].unique().all()
                            pLeak = eqJsonUnique['pLeak'].unique().all()
                            # MTTRReference = {'min': MTTRMin, 'max': MTTRMax}
                            MTBFMinRef = (MTTRMinRef * (1 - pLeak) / pLeak)
                            MTBFMaxRef = (MTTRMaxRef * (1 - pLeak) / pLeak)
                            totalLeakTimeOfAllLeaks = sum(eqJsonUnique['endTime'] - eqJsonUnique['startTime'])
                            pLeakTotal = totalLeakTimeOfAllLeaks / (simtime * eqCount)
                            pLeakIn = eqJsonUnique['pLeak'].unique().all()
                        else:
                            MTTRMinRef = 0
                            MTTRMaxRef = 0
                            MTBFMinRef = 0
                            MTBFMaxRef = 0
                            totalLeakTimeOfAllLeaks = simtime
                            pLeakTotal = 1
                            pLeakIn = 1
                        # MTTRDist = {'min': eqJsonUniqueTimeFiltered['MTTRMin'], 'max': eqJsonUniqueTimeFiltered['MTTRMax']}
                        LeaksCountList.append({'unitID': unitID,
                                               'equipmentCount': eqCount,
                                               'pLeakIn': pLeakIn,
                                               'totalLeakCount': totalLeakCount,
                                               'totalLeakTimeAllLeaks': totalLeakTimeOfAllLeaks,
                                               'mcRunNum': int(singleOutputKey),
                                               'meanMTTRDays': meanMTTR / 24,
                                               'MTTRMinRef': MTTRMinRef,
                                               'MTTRMaxRef': MTTRMaxRef,
                                               'meanMTBFDays': meanMTBF / 24,
                                               'MTBFMinRef': MTBFMinRef,
                                               'MTBFMaxRef': MTBFMaxRef,
                                               'modelReadableName': uniqueEmitter,
                                               'implClass': singleEmitter,
                                               })

        leaksDF = pd.DataFrame(LeaksCountList)
        unitIDForLeaks = leaksDF['unitID'].dropna().unique()
        totalLeaks = []
        for uid in unitIDForLeaks:
            uniqueEms = leaksDF[(leaksDF['unitID'] == uid)]['modelReadableName'].dropna().unique()
            for uniqueEm in uniqueEms:
                eqUniqueLeaks = leaksDF[(leaksDF['unitID'] == uid) & (leaksDF['modelReadableName'] == uniqueEm)]
                totalEqCount = eqUniqueLeaks['equipmentCount'].sum()
                # leakCount = eqUniqueLeaks['leaksCount'].sum()
                totalLeakCount = eqUniqueLeaks['totalLeakCount'].sum()
                totalLeakTimeAllLeaks = eqUniqueLeaks['totalLeakTimeAllLeaks'].sum()
                meanMTTRallMC = eqUniqueLeaks['meanMTTRDays'].mean()
                meanMTBFallMC = eqUniqueLeaks['meanMTBFDays'].mean()
                implClass = eqUniqueLeaks['implClass'].unique().all()
                MTTRMinRef = float(eqUniqueLeaks[(eqUniqueLeaks != True)]['MTTRMinRef'].dropna().unique())
                MTTRMaxRef = float(eqUniqueLeaks[(eqUniqueLeaks != True)]['MTTRMaxRef'].dropna().unique().max())
                MTBFMinRef = float(eqUniqueLeaks[(eqUniqueLeaks != True)]['MTBFMinRef'].dropna().unique())
                MTBFMaxRef = float(eqUniqueLeaks[(eqUniqueLeaks != True)]['MTBFMaxRef'].dropna().unique().max())

                if totalEqCount != 0:
                    pLeakOut = totalLeakTimeAllLeaks / ((simtime*noOfMC) * totalEqCount)
                    pLeakIn = eqUniqueLeaks['pLeakIn'].unique()[0]
                    # pLeakOut = leakCount / totalEqCount
                else:
                    pLeakOut = 0
                    pLeakIn = 0
                totalLeaks.append({'unitID': uid,
                                   'modelReadableName': uniqueEm,
                                   'totalEqCount': totalEqCount,
                                   'totalLeakCount': totalLeakCount,
                                   'pLeakOut': pLeakOut,
                                   'pLeakRef': pLeakIn,
                                   'meanMTTRDays': meanMTTRallMC,
                                   'MTTRMinRef': MTTRMinRef,
                                   'MTTRMaxRef': MTTRMaxRef,
                                   'meanMTBFDays': meanMTBFallMC,
                                   'MTBFMinRef': MTBFMinRef,
                                   'MTBFMaxRef': MTBFMaxRef,
                                   'implClass:': implClass})
        totalLeaks = pd.DataFrame(totalLeaks)
        return totalLeaks, leaksDF


class FlareValidation(Validation):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.flarePath = self.folderPaths['Flares']
        self.inputs, self.outputs = self.readSingleOutput(self.flarePath)
        self.outputs.pop('template')
        self.processedDF = 0

    def validate(self):
        self.processedDF = self.checkFlareStates(self.inputs, self.outputs)

    def dumpValidation(self):
        self.processedDF.to_csv(self.resultsFolder / 'FlareResults.csv', index=False)

    def checkFlareStates(self, inputs, outputs):
        ies = [y['instantaneousEvents'] for x, y in outputs.items()]
        processedNumbers = []

        for ie in ies:
            opStateTime = ie[(ie['state'] == 'OPERATING') & (ie['unitID'] == 'flare_1')]
            opStateTime = sum(opStateTime[(opStateTime['event'] == 'START')]['duration'])
            opCount = ie[(ie['state'] == 'OPERATING') & (ie['unitID'] == 'flare_1')]
            opCount = opCount[(opCount['event'] == 'START')].shape[0]

            unlitStateTime = ie[(ie['state'] == 'UNLIT') & (ie['unitID'] == 'flare_1')]
            unlitStateTime = sum(unlitStateTime[(unlitStateTime['event'] == 'START')]['duration'])
            unlitCount = ie[(ie['state'] == 'UNLIT') & (ie['unitID'] == 'flare_1')]
            unlitCount = unlitCount[(unlitCount['event'] == 'START')].shape[0]

            malfStateTime = ie[(ie['state'] == 'MALFUNCTIONING') & (ie['unitID'] == 'flare_1')]
            malfStateTime = sum(malfStateTime[(malfStateTime['event'] == 'START')]['duration'])
            malfCount = ie[(ie['state'] == 'MALFUNCTIONING') & (ie['unitID'] == 'flare_1')]
            malfCount = malfCount[(malfCount['event'] == 'START')].shape[0]

            # totalTime = ie[(ie('command') == 'SIM-STOP')]['timestamp']
            totalTime = ie[(ie['command'] == 'SIM-STOP')]['timestamp'].values[0]
            opRatio = (opStateTime / opCount) * 0.0000115741
            unlitRatio = (unlitStateTime / unlitCount) * 0.0000115741
            malfRatio = (malfStateTime / malfCount) * 0.0000115741
            unlitCountRatio = unlitCount / (unlitCount + malfCount)
            malfCountRatio = malfCount / (unlitCount + malfCount)
            processedNumbers.append({'totalTime': totalTime,
                                     'opRatio': opRatio,
                                     'unlitRatio': unlitRatio,
                                     'malfRatio': malfRatio,
                                     'unlitCountRatio': unlitCountRatio,
                                     'malfCountRatio': malfCountRatio,
                                     'opStateTime': opStateTime,
                                     'unlitStateTime': unlitStateTime,
                                     'malfStateTime': malfStateTime,
                                     'opCount': opCount,
                                     'unlitCount': unlitCount,
                                     'malfCount': malfCount})

        processedDF = pd.DataFrame(processedNumbers)
        # opDur = [10, 20]
        # unlitDur = [3, 5]
        # malfDur = [1, 2]
        return processedDF


class WellsValidation(Validation):
    def __init__(self,
                 **kwargs):
        super().__init__(**kwargs)
        self.wellsPath = self.folderPaths['Wells']
        self.inputs, self.outputs = self.readSingleOutput(self.wellsPath)
        self.outputs.pop('template')
        self.processedData = 0
        i = 10

    def validate(self):
        self.processedData = self.validateWells(self.inputs, self.outputs)
        pass

    def dumpValidation(self):
        self.processedData.to_csv(self.resultsFolder / 'WellsResults.csv', index=False)
        pass

    # def getStateInfo(self, ie, unitID, stateName):
    #     states = ie[(ie['unitID'] == unitID) & (ie['state'] == stateName)]
    #     stateTime = sum(states[(states['event'] == 'START')]['duration'])
    #     stateCount = states[(states['event'] == 'START')].shape[0]
    #     stateTimeAvg = stateTime / stateCount
    #     return stateTime, stateCount, stateTimeAvg

    def validateWells(self, inputs, outputs):
        ies = [y['instantaneousEvents'] for x, y in outputs.items()]
        processedData = []
        wellsUnitIDs = []
        for wells in ['ContinuousWells', 'CyclingWells']:
            wellsInput = inputs[wells]
            for i in wellsInput:
                wellsUnitIDs.append([i['Unit ID'], i['Model ID']])
        for well, modelID in wellsUnitIDs:
            for ie in ies:
                tempData = {'unitID': well, 'mcRun': int(ie['mcRun'].unique())}
                plannedStateTime, plannedStateCount, plannedStateAvg = self.getStateInfo(ie, well, 'PLANNED')
                tempData.update({'plannedStateTime': plannedStateTime,
                                 'plannedStateCount': plannedStateCount,
                                 'plannedStateAvg': plannedStateAvg})
                drillingStateTime, drillingStateCount, drillingStateAvg = self.getStateInfo(ie, well, 'DRILLING')
                tempData.update({'drillingStateTime': drillingStateTime,
                                 'drillingStateCount': drillingStateCount,
                                 'drillingStateAvg': drillingStateAvg})
                completionStateTime, completionStateCount, completionStateAvg = self.getStateInfo(ie, well, 'COMPLETION')
                tempData.update({'completionStateTime': completionStateTime,
                                 'completionStateCount': completionStateCount,
                                 'completionStateAvg': completionStateAvg})
                productionStateTime, productionStateCount, productionStateAvg = self.getStateInfo(ie, well, 'PRODUCTION')
                tempData.update({'productionStateTime': productionStateTime,
                                 'productionStateCount': productionStateCount,
                                 'productionStateAvg': productionStateAvg})
                if modelID == 'CycledWell.json':
                    shutInStateTime, shutInStateCount, shutInStateAvg = self.getStateInfo(ie, well, 'SHUT_IN')
                    tempData.update({'shutInStateTime': shutInStateTime,
                                     'shutInStateCount': shutInStateCount,
                                     'shutInStateAvg': shutInStateAvg})
                if ie[(ie['unitID'] == well) & (ie['state'] == 'MANUAL_UNLOADING')]['state'].unique().size > 0:
                    manualLUStateTime, manualLUStateCount, manualLUStateAvg = self.getStateInfo(ie, well, 'MANUAL_UNLOADING')
                    tempData.update({'manualLUStateTime': manualLUStateTime,
                                     'manualLUStateCount': manualLUStateCount,
                                     'manualLUStateAvg': manualLUStateAvg})
                if ie[(ie['unitID'] == well) & (ie['state'] == 'AUTOMATIC_UNLOADING')]['state'].unique().size > 0:
                    autoLUStateTime, autoLUStateCount, autoLUStateAvg = self.getStateInfo(ie, well, 'AUTOMATIC_UNLOADING')
                    tempData.update({'autoLUStateTime': autoLUStateTime,
                                     'autoLUStateCount': autoLUStateCount,
                                     'autoLUStateAvg': autoLUStateAvg})
                if ie[(ie['unitID'] == well) & (ie['state'] == 'LU_SHUT_IN')]['state'].unique().size > 0:
                    LUShutInStateTime, LUShutInStateCount, LUShutInStateAvg = self.getStateInfo(ie, well, 'LU_SHUT_IN')
                    tempData.update({'LUShutInStateTime': LUShutInStateTime,
                                     'LUShutInStateCount': LUShutInStateCount,
                                     'LUShutInStateAvg': LUShutInStateAvg})
                processedData.append(tempData)
                i = 10
        processedData = pd.DataFrame(processedData)
        return processedData


class SeparatorsValidation(Validation):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.separatorsPath = self.folderPaths['Separators']
        self.inputs, self.outputs = self.readSingleOutput(self.separatorsPath)
        self.outputs.pop('template')
        self.processedData = pd.DataFrame

    def validate(self):
        self.processedData = self.validateSeps(self.inputs, self.outputs)

    def dumpValidation(self):
        self.processedData.to_csv(self.resultsFolder / 'SeparatorsResults.csv', index=False)

    def validateSeps(self, inputs, outputs):
        processedData = []
        ies = [y['instantaneousEvents'] for x, y in outputs.items()]
        sepUnitIDs = list(map(lambda x: [x['Unit ID'], x['Model ID']], inputs['Separators']))
        for unitID, modelID in sepUnitIDs:
            for ie in ies:
                tempData = {'unitID': unitID, 'mcRun': int(ie['mcRun'].unique())}
                sdvStateTime, sdvStateCount, sdvStateAvg = self.getStateInfo(ie, unitID, 'STUCK_DUMP_VALVE')
                tempData.update({'sdvStateTime': sdvStateTime,
                                 'sdvStateCount': sdvStateCount,
                                 'sdvStateAvg': sdvStateAvg})
                if modelID == 'ContinuousSeparator.json':
                    opStateTime, opStateCount, opStateAvg = self.getStateInfo(ie, unitID, 'OPERATING')
                    tempData.update({'opStateTime': opStateTime,
                                     'opStateCount': opStateCount,
                                     'opStateAvg': opStateAvg})
                if modelID == 'DumpingSeparator.json':
                    fillingStateTime, fillingStateCount, fillingStateAvg = self.getStateInfo(ie, unitID, 'FILLING')
                    tempData.update({'fillingStateTime': fillingStateTime,
                                     'fillingStateCount': fillingStateCount,
                                     'fillingStateAvg': fillingStateAvg})
                    dumpingStateTime, dumpingStateCount, dumpingStateAvg = self.getStateInfo(ie, unitID, 'DUMPING')
                    tempData.update({'dumpingStateTime': dumpingStateTime,
                                     'dumpingStateCount': dumpingStateCount,
                                     'dumpingStateAvg': dumpingStateAvg})

                processedData.append(tempData)
        ret = pd.DataFrame(processedData)
        return ret


class MassFlowValidation(Validation):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.massFlowPath = self.folderPaths['MassFlow']
        self.inputs, self.outputs = self.readSingleOutput(self.massFlowPath)
        self.outputs.pop('template')
        self.processedData = pd.DataFrame
        i = 10

    def validate(self):
        self.processedData = self.validateMassFlow(self.inputs, self.outputs)
        pass

    def dumpValidation(self):
        self.processedData.to_csv(self.resultsFolder / 'MassFlowResults.csv', index=False)
        pass

    def validateMassFlow(self, inputs, outputs):
        processedData = []
        # vaporSum
        ies = [y['instantaneousEvents'] for x, y in outputs.items()]
        secondaryData = [y['secondaryEventInfo'] for x, y in outputs.items()]
        for ie in ies:
            mcRun = ie['mcRun'][0]
            condensateProbeEvents = ie[(ie['unitID'] == 'probe_c')]
            waterProbeEvents = ie[(ie['unitID'] == 'probe_w')]
            vaporProbeEvents = ie[(ie['unitID'] == 'probe_g')]
            se = secondaryData[mcRun]
            vaporChangeTimes = vaporProbeEvents['timestamp'].unique()
            waterChangeTimes = waterProbeEvents['timestamp'].unique()
            condensateChangeTimes = condensateProbeEvents['timestamp'].unique()
            vaporSum = self.getTotalDriverRate(vaporChangeTimes, vaporProbeEvents, se)
            waterSum = self.getTotalDriverRate(waterChangeTimes, waterProbeEvents, se)
            condensateSum = self.getTotalDriverRate(condensateChangeTimes, condensateProbeEvents, se)
            processedData.append({'mcRun': mcRun,
                                  'Oil Production [bbl/simtime]': condensateSum,
                                  'Water Production [bbl/simtime]': waterSum,
                                  'Gas Production [scf/simtime]': vaporSum})
        processedData = pd.DataFrame(processedData)
        return processedData

    def getTotalDriverRate(self, changeTimes, singleFluidProbeEvents, secondaryInfo):
        sumOfMass = 0
        for changeTime in changeTimes:
            ffVals = singleFluidProbeEvents[(singleFluidProbeEvents['timestamp'] == changeTime)]
            duration = min(ffVals['duration'])
            duration = min(ffVals[(ffVals['command'] == 'FLUID-FLOW')]['duration'])
            eventIDs = ffVals[(ffVals['command'] == 'FLUID-FLOW')]['eventID']
            seForMassFlowCalc = secondaryInfo[(secondaryInfo['eventID'].isin(eventIDs))]
            driverRates = seForMassFlowCalc[(seForMassFlowCalc['fieldName'] == 'driverRate')]
            sumOfDriverRates = sum(map(lambda x: float(x), driverRates['fieldValue']))
            sumOfMass += sumOfDriverRates*duration
        return sumOfMass


class TankBatteryValidation(Validation):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.massFlowPath = self.folderPaths['TankBattery']
        self.inputs, self.outputs = self.readSingleOutput(self.massFlowPath)
        self.outputs.pop('template')
        self.processedData = pd.DataFrame
        i = 10

    def validate(self):
        self.processedData = self.validateTankBattery(self.inputs, self.outputs)
        pass

    def dumpValidation(self):
        self.processedData.to_csv(self.resultsFolder / 'TankBatteryResults.csv', index=False)
        pass

    def validateTankBattery(self, inputs, outputs):
        i = 10
        processedData = []
        ies = [y['instantaneousEvents'] for x, y in outputs.items()]
        secondaryData = [y['secondaryEventInfo'] for x, y in outputs.items()]

        pass


class EngineValidation(Validation):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.efficienciesPath = self.folderPaths['EngineEfficiency']
        self.inputs, self.outputs = self.readSingleOutput(self.efficienciesPath)
        self.outputs.pop('template')
        self.processedData = 0
        self.processedEngineGC = pd.DataFrame
        # self.processedDestructionGC = pd.DataFrame
        i = 10

    def validate(self):
        self.processedEngineGC = self.validateEngineGC(self.inputs, self.outputs)
        # self.processedDestructionGC = self.validateDestructionGC(self.inputs, self.outputs)

    def dumpValidation(self):
        self.processedEngineGC.to_csv(self.resultsFolder / 'EngineGCResults.csv', index=False)
        # self.processedDestructionGC.to_csv(self.resultsFolder / 'DestructionGCResults.csv', index=False)

    def validateEngineGC(self, inputs, outputs):
        processedData = []
        study = pd.DataFrame(inputs['Compressors'])
        for mcRun, singleOutput in outputs.items():
            ie = singleOutput['instantaneousEvents']
            md = singleOutput['metadata']
            gc = singleOutput['gasCompositions']
            emts = singleOutput['emissionTimeseries']
            emts = emts.set_index('tsKey')
            for idx, row in study.iterrows():
                engineEfficiency = row['Engine Efficiency']
                compressorPower = row['Compressor Power [kW]']
                unitID = row['Unit ID']
                combustionMd = md[(md['unitID'] == unitID) & (md['implClass'] == 'MEETCombustionEmitter')]
                emitterIDs = combustionMd['emitterID'].to_list()
                ieRows = []
                for emitterID in emitterIDs:
                    ieRowSingleEmitter = ie[(ie['emitterID'] == emitterID) & (ie['unitID'] == unitID)]
                    ieRowSingleEmitter = ieRowSingleEmitter[(ieRowSingleEmitter['command'] == 'EMISSION')]
                    for eventIdx, event in ieRowSingleEmitter.iterrows():
                        tsKey = int(event['tsKey'])
                        gcKey = int(event['gcKey'])
                        singleEmission = emts.loc[tsKey]
                        singleGc = gc[(gc['gcKey'] == gcKey)]  # dest gc
                        origGcKey = int(singleGc['origGC'].unique())
                        engineC = gc[(gc['gcKey'] == origGcKey)]  # engine gc
                        engineGcVal = float(engineC[(engineC['species'] == 'METHANE')]['gcValue'])
                        actualEmissions = float(singleEmission['tsValue']) * engineGcVal
                        methaneLHV = 50048
                        powerOut = float(singleEmission['tsValue'])
                        powerIn = powerOut / engineEfficiency
                        fuelInMassFlow = powerIn / methaneLHV
                        processedData.append({'mcRun': mcRun,
                                              'unitID': unitID,
                                              'emitterID': emitterID,
                                              'compressorPower [KW]': compressorPower,
                                              'actualPower [KW]': float(singleEmission['tsValue']),
                                              'fuelEstimate [kg/s]': actualEmissions,
                                              'fuelValidate [kg/s]': fuelInMassFlow})
        processedData = pd.DataFrame(processedData)
        return processedData


class DestructionValidation(Validation):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.efficienciesPath = self.folderPaths['DestructionEfficiency']
        self.inputs, self.outputs = self.readSingleOutput(self.efficienciesPath)
        self.outputs.pop('template')
        self.destName = pathlib.Path('input/CuratedData/CompressorDestructionEfficiencies')
        self.processedDestGC = pd.DataFrame
        self.species = pd.Series(['METHANE', 'ETHANE', 'PROPANE', 'ISOBUTANE',
                                  'BUTANE', 'ISOPENTANE', 'PENTANE', 'HEXANE', 'HEPTANE', 'OCTANE'])

    def validate(self):
        self.processedDestGC = self.validateDestGC(self.inputs, self.outputs)

    def dumpValidation(self):
        self.processedDestGC.to_csv(self.resultsFolder / 'DestructionGCResults.csv', index=False)

    def validateDestGC(self, inputs, outputs):
        processedData = []
        study = pd.DataFrame(inputs['Compressors'])
        # destEff = study
        for mcRun, singleOutput in outputs.items():
            ie = singleOutput['instantaneousEvents']
            md = singleOutput['metadata']
            gc = singleOutput['gasCompositions']
            emts = singleOutput['emissionTimeseries']
            emts = emts.set_index('tsKey')
            for idx, row in study.iterrows():
                # engineEfficiency = row['Engine Efficiency']
                destEff = self.getDestEff(row, self.destName, self.species)
                # compressorPower = row['Compressor Power [kW]']
                unitID = row['Unit ID']
                combustionMd = md[(md['unitID'] == unitID) & (md['implClass'] == 'MEETCombustionEmitter')]
                emitterIDs = combustionMd['emitterID'].to_list()
                ieRows = []
                for emitterID in emitterIDs:
                    ieRowSingleEmitter = ie[(ie['emitterID'] == emitterID) & (ie['unitID'] == unitID)]
                    ieRowSingleEmitter = ieRowSingleEmitter[(ieRowSingleEmitter['command'] == 'EMISSION')]
                    for eventIdx, event in ieRowSingleEmitter.iterrows():
                        tsKey = int(event['tsKey'])
                        gcKey = int(event['gcKey'])
                        singleEmission = emts.loc[tsKey]
                        singleGc = gc[(gc['gcKey'] == gcKey)]  # dest gc
                        destGcVal = singleGc[(singleGc['species'] == 'METHANE')]['gcValue']
                        methaneDestEff = destEff[(destEff['destSpecies'] == 'METHANE')]['destEfficiency']
                        combustionEm = float(singleEmission['tsValue']) * float(destGcVal)  # em*destGc = (1-ds)gamma*em
                        origGcKey = int(singleGc['origGC'].unique())
                        origGc = gc[(gc['gcKey'] == origGcKey)]   # gamma
                        methaneGamma = float(origGc[(origGc['species'] == 'METHANE')]['gcValue'])
                        combustionEmValidate = float(1 - methaneDestEff) * float(singleEmission['tsValue']) * methaneGamma
                        processedData.append({'mcRun': mcRun,
                                              'unitID': unitID,
                                              'emitterID': emitterID,
                                              'combustionExpectedMethane': combustionEm,
                                              'combustionFromOutputsMethane': combustionEmValidate})
        processedData = pd.DataFrame(processedData)
        return processedData

    def getDestEff(self, row, destName, species):
        destEff = f"{row['Exhaust Factors']}.csv"
        destmeta, destEff = dp.readRawDistributionFile(f'{destName}/{destEff}')
        if destEff.iloc[0]['Species'] == 'ALL':
            retDF = pd.DataFrame({'destSpecies': species,
                                  'destEfficiency': [destEff.iloc[0]['Destruction Efficiency']] * len(species)})
            return retDF
        sp = pd.DataFrame(species, columns=['Species'])
        ret = destEff.merge(sp)
        ret = ret.rename(columns={'Species': 'destSpecies', 'Destruction Efficiency': 'destEfficiency'})
        i = 10
        return ret


def argumentParser():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-vf", "--ValidationFolder", default="output/Validation", help="Output folder containing validation outputs")
    # parser.add_argument("-tp", "--ValidationTimeStamp", default=2, help="Output folder containing validation outputs")
    args = parser.parse_args()
    return args


def getLastFolder(args, testName):
    testPath = pathlib.Path(args.ValidationFolder) / f'{testName}'
    if testPath.exists():
        print(f'{testName} Validation Folder Exists')
        lastTestPath = sorted(list(testPath.rglob('MC_*')), key=lambda x: pathlib.Path.stat(x).st_mtime, reverse=True)[0]
    else:
        print(f'{testName} Validation Folder Does Not Exist')
        lastTestPath = None
    return lastTestPath


def openFolders(args):
    folderPaths = {}
    testNames = ['Flares',
                 'Emitters',
                 'Wells',
                 'Separators',
                 'MassFlow',
                 'TankBattery',
                 'EngineEfficiency',
                 'DestructionEfficiency']
    for test in testNames:
        folderPaths.update({f'{test}': getLastFolder(args, test)})
    return folderPaths


def removePreviousValidation(validationFolder):
    # todo: how to delete previous results files?
    # if pathlib.Path(validationFolder / 'Results').exists():
    #     p = str(pathlib.Path(validationFolder / 'Results').absolute())
    #     os.remove(p)
    renewPath = os.path.join(validationFolder.absolute(), 'Results')
    os.makedirs(renewPath, exist_ok=True)
    return renewPath


def resetValidationFolder(args):
    folderPaths = {i: j for i, j in openFolders(args).items() if j is not None}
    validationFolder = pathlib.Path(args.ValidationFolder)
    newPath = pathlib.Path(removePreviousValidation(validationFolder))
    return folderPaths, newPath


def validateTests(folderPaths, resultsFolder):
    # EmitterValidation(timestamp=timestamp, folderPaths=folderPaths, resultsFolder=newPath).validateAndDump()
    # FlareValidation(folderPaths=folderPaths, resultsFolder=newPath).validateAndDump()
    # WellsValidation(folderPaths=folderPaths, resultsFolder=newPath).validateAndDump()
    # SeparatorsValidation(folderPaths=folderPaths, resultsFolder=newPath).validateAndDump()
    # MassFlowValidation(folderPaths=folderPaths, resultsFolder=resultsFolder).validateAndDump()
    # TankBatteryValidation(folderPaths=folderPaths, resultsFolder=newPath).validateAndDump()
    # EngineEfficiency(folderPaths=folderPaths, resultsFolder=resultsFolder).validateAndDump()
    DestructionValidation(folderPaths=folderPaths, resultsFolder=resultsFolder).validateAndDump()
    i = 10


def main(args):
    folderPaths, newPath = resetValidationFolder(args)
    validateTests(folderPaths=folderPaths, resultsFolder=newPath)


if __name__ == "__main__":
    main(argumentParser())
