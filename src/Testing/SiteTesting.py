import sys
sys.path.insert(0, "C:\\death\\METEC_MEET\\MEET_2.0_0007\\src")
import pandas as pd
from src import AppUtils as au
from src import ModelFormulation as mf
import glob
import pathlib
import matplotlib.pyplot as plt
import itertools
from src import SiteMain2 as sm
import argparse
from src import Units as u
import ColumnNames as cn
import json


# config Parameters: -c config/PtEMEETConfig.json -st input\Studies\OGCI\TestSuite\TestPaths.xlsx
# Working directory: MEET2Models


def readTests():  # input from an excel sheet to conserve the order of the tests
    p = pathlib.Path().absolute()
    p = p / 'input' / 'Studies' / 'OGCI' / 'TestSuite' / 'TestPaths.xlsx'
    pathsDF = pd.read_excel(p)
    sitePaths = pathsDF['testPaths']
    return pathsDF


def readIntake(testPath):
    intake = mf.parseIntakeSpreadsheet(testPath)
    return intake


def genReference(intakeSheet, testName):
    print('Testing ---- ' + testName)
    ref = {}
    for tabs in intakeSheet['masterEquipment']:
        eqDF = pd.DataFrame(intakeSheet[tabs['Tab']]).dropna(how='all')
        try:
            if tabs['Tab'] in ('Wells', 'Continuous Wells', 'Cycling Wells'):
                ref.update(genReferenceWells(eqDF, ref))  # append to original ref dict
            if tabs['Tab'] in 'Common Headers':
                ref.update(genReferenceCH(eqDF, ref))
            if tabs['Tab'] in ('Separators', 'Two Phase Separators', 'Three Phase Separators'):
                ref.update(genReferenceSeparators(eqDF, ref))
            if tabs['Tab'] in 'Tank Battery':
                ref.update(genReferenceTankBattery(eqDF, ref))
            if tabs['Tab'] in ('Compressors', 'Compressor'):
                ref.update(genReferenceCompressors(eqDF, ref))
            if tabs['Tab'] in 'VRUs':
                ref.update(genReferenceVRUs(eqDF, ref))
            if tabs['Tabs'] in 'Flares':
                ref.update(genReferenceFlares(eqDF, ref))
            if tabs['Tabs'] in 'Yard Piping':
                ref.update(genReferenceYP(eqDF, ref))
            if tabs['Tabs'] in 'Dehydrators':
                ref.update(genReferenceDehydrators(eqDF, ref))
            if tabs['Tabs'] in 'FFLinks':
                ref.update(genReferenceFFLinks(eqDF, ref))
        except:
            message = 'Couldn\'t genetate reference for ' + testName + ' ' + str(tabs['Readable Name'])
            print(message)
            continue

    return ref


def validateFluidFlows():
    pass


def genReferenceWells(wellsDF, reference):
    # wells = pd.DataFrame(rawIntake['Wells']).dropna()
    # wellsWithStates = wells[(wells['Model ID'] == 'CyclingWell2.json')]
    # wellsUnitIDs = wellsWithStates['Unit ID']
    # # print(wellsWithStates[['Unit ID', 'Model ID', 'Shut In Time Min [s]', 'Dump Time Min [s]']])
    # for well in wellsUnitIDs:
    #     ieShutInTimes = ie[(ie['unitID'] == well) & (ie['state'] == 'SHUT-IN') & (ie['event'] == 'START')]
    #     check1 = ieShutInTimes['duration'] == float(wellsWithStates[(wellsWithStates['Unit ID'] == well)]['Shut In Time Min [s]'])
    #     print(well+' '+str(check1.all())+' SHUT-IN')
    #     ieProducing = ie[(ie['unitID'] == well) & (ie['state'] == 'PRODUCING') & (ie['event'] == 'START')].iloc[1:]
    #     check2 = ieProducing['duration'] == float(wellsWithStates[(wellsWithStates['Unit ID'] == well)]['Mean Production Time [s]'])
    #     print(well+' '+str(check2.all())+' PRODUCING')

    # write code to gen ref in form of a dict
    for idx, row in wellsDF.iterrows():

        if row[cn.modelID] == cn.cyclingWellModel:  # write code to gen ref in form of a dict
            cyclesPerDay = (row[cn.waterBblPerDay] + row[cn.oilBblPerDay]) / row[
                cn.totalLiquidsPerDump]  # calc cycles/day
            totalCycleTime = u.SECONDS_PER_DAY / cyclesPerDay  # calc cycle time (1/freq) in seconds
            shutInTime = totalCycleTime - row[cn.meanProductionTime]  # calc shut in time
            reference.update({str(row[cn.unitID]): {'productionTime': row[cn.meanProductionTime],
                                                    'shutInTime': shutInTime,
                                                    'actuatorType': row[cn.actuatorType],
                                                    'oilProductionPerDay': row[cn.oilBblPerDay],
                                                    'waterProductionPerDay': row[cn.waterBblPerDay]}})

        elif row[cn.modelID] == cn.wellModel:  # write code to gen ref in form of a dict
            reference.update({str(row[cn.unitID]): {'actuatorType': row[cn.actuatorType],
                                                    'oilProductionPerDay': row[cn.oilBblPerDay],
                                                    'waterProductionPerDay': row[cn.waterBblPerDay]}})

    return reference  # return ref dict and append to genReference


def genReferenceCH(chDF, reference):
    for idx, row in chDF.iterrows():
        reference.update({str(row[cn.unitID]): {'fluid': row[cn.fluid]}})
    return reference


def genReferenceSeparators(sepDF, reference):
    for idx, row in sepDF.iterrows():

        if row[cn.modelID] == cn.continuousSeparatorModel:
            reference.update({str(row[cn.unitID]): {'actuatorType': row[cn.actuatorType],
                                                    'primaryWaterTakeOffRatio': row[cn.primaryWaterTakeOffRatio]}})

        elif row[cn.modelID] == cn.synchedSeparatorModel:
            reference.update({str(row[cn.unitID]): {'actuatorType': row[cn.actuatorType],
                                                    'primaryWaterTakeOffRatio': row[cn.primaryWaterTakeOffRatio]}})

    return reference


def genReferenceTankBattery(tbDF, reference):
    for idx, row in tbDF.iterrows():
        reference.update({str(row[cn.unitID]): {'actuatorType': row[cn.actuatorType],
                                                'batteryType': row[cn.batteryType]}})
    return reference


def genReferenceCompressors(compressorDF, reference):
    for idx, row in compressorDF.iterrows():
        reference.update({str(row[cn.unitID]): {'actuatorType': row[cn.actuatorType],
                                                'sealType': row[cn.sealType],
                                                'compressorPower': row[cn.compressorPower],
                                                'driverType': row[cn.driverType],
                                                'averageLoading': row[cn.averageLoading],
                                                'stdLoading': row[cn.stdLoading],
                                                'operatingFraction': row[cn.operatingFraction],
                                                'nopByNod': row[cn.nopByNod],
                                                'upperLimit': row[cn.upperLimit],
                                                'lowerLimit': row[cn.lowerLimit],
                                                'starterType': row[cn.starterType],
                                                'starterEventDuration': row[cn.starterEventDuration],
                                                'starterEventVolume': row[cn.starterEventVolume],
                                                'blowdownDuration': row[cn.blowdownDuration],
                                                'blowdownEventVolume': row[cn.blowdownEventVolume]}})
    return reference


def genReferenceVRUs(vruDF, reference):
    for idx, row in vruDF.iterrows():
        reference.update({str(row[cn.unitID]): {'opDurMin': row[cn.opDurMin],
                                                'opDurMax': row[cn.opDurMax],
                                                'malfDurMin': row[cn.malfDurMin],
                                                'malfDurMax': row[cn.malfDurMax],
                                                'actuatorType': row[cn.actuatorType],
                                                'compressorKW': row[cn.compressorKW],
                                                'sealType': row[cn.sealType],
                                                'driverType': row[cn.driverType],
                                                'averageLoading': row[cn.averageLoading],
                                                'stdLoading': row[cn.stdLoading]}})
    return reference


def genReferenceFlares(flareDF, reference):
    for idx, row in flareDF.iterrows():
        reference.update({str(row[cn.unitID]): {'malfDurMin': row[cn.malfDurMin],
                                                'malfFurMax': row[cn.malfDurMax],
                                                'malfDestructEfficiency': row[cn.malfDestructEfficiency],
                                                'pMalf': row[cn.pMalf],
                                                'pUnlit': row[cn.pUnlit],
                                                'unlitDurMin': row[cn.unlitDurMin],
                                                'unlitDurMax': row[cn.unlitDurMax],
                                                'unlitDestructEfficiency': row[cn.unlitDestructEfficiency],
                                                'opDurMin': row[cn.opDurMin],
                                                'opDurMax': row[cn.opDurMax],
                                                'opDestructEfficiency': row[cn.opDestructEfficiency]}})
    return reference


def genReferenceYP(ypDF, reference):
    for idx, row in ypDF.iterrows():
        reference.update({str(row[cn.unitID]): {'actuatorType': row[cn.actuatorType]}})
    return reference


def genReferenceDehydrators(dehydratorDF, reference):
    for idx, row in dehydratorDF.iterrows():
        reference.update({str(row[cn.unitID]): {'actuatorType': row[cn.actuatorType]}})
    return reference


def genReferenceFFLinks(ffLinkDF, reference):
    for idx, row in ffLinkDF.iterrows():
        reference.update({str(row[cn.unitID]): {'outletFacilityID': row[cn.outletFacilityID],
                                                'outletUnitID': row[cn.outletUnitID],
                                                'inletFacilityID': row[cn.inletFacilityID],
                                                'inletUnitID': row[cn.inletUnitID],
                                                'flowName': row[cn.flowName]}})
    return reference


# def runTestSite(config, testPath):
#
#     pass


def readDump():
    listOfFFDumps = glob.glob('output/TestSiteFF/MC_TestSiteFF4/0/FFDump*.csv')
    FFDump = {}
    for i in listOfFFDumps:
        idxStart = i.find('FFDump')
        idxEnd = i.find('.csv')
        dictKeys = i[idxStart: idxEnd]
        FFDump[dictKeys] = pd.read_csv(i)
    return FFDump


def readSingleOutput(outputPath):
    intakePath = glob.glob(outputPath + '/0/*.xlsx')[0]
    outputPath = pathlib.Path(outputPath)
    outputs = {}
    intake = mf.parseIntakeSpreadsheet(intakePath)

    for folder in outputPath.iterdir():
        iePath = folder / 'instantaneousEvents.csv'
        eqPath = folder / 'equipment.json'
        mdPath = folder / 'metadata.csv'
        ieTemp = []

        if iePath.exists():
            ieTemp = pd.read_csv(iePath)

        eqTemp = {}
        f = open(eqPath)
        eqCount = 0
        for singleLine in f:
            singleEquipment = json.loads(singleLine)
            eqTemp[str(eqCount)] = singleEquipment
            # eqTemp[singleEquipment['implClass']] = singleEquipment
            eqCount += 1

        mdTemp = pd.read_csv(mdPath)

        outputs[folder.name] = {'instantaneousEvents': ieTemp, 'equipment': eqTemp, 'metadata': mdTemp}

    return intake, outputs


def checkStatesInCompressors(instantaneousEvents, rawIntake):
    compDF = pd.DataFrame(rawIntake['Compressors'])
    ie = instantaneousEvents
    opModeHours = u.HOURS_PER_YEAR * compDF['Operating Fraction'].iloc[0]
    remainingHours = u.HOURS_PER_YEAR - opModeHours
    nopModeHours = remainingHours * compDF['NOP Fraction of NOP/NOD'].iloc[0]
    nodModeHours = remainingHours * (1 - compDF['NOP Fraction of NOP/NOD'].iloc[0])
    opSec = sum(ie[(ie['state'] == 'OPERATING') & (ie['event'] == 'START')]['duration'])
    opHours = opSec / 3600
    pass


def checkEmissions(instantaneousEvents, rawIntake):
    masterEquipment = pd.DataFrame(rawIntake['masterEquipment']).set_index('Readable Name')
    masterEquipment = masterEquipment.drop(['Facilities', 'FFLinks', 'Debug'])
    for equipment in masterEquipment['Tab']:
        eq = pd.DataFrame(rawIntake[equipment]).dropna()
        for unit in eq['Unit ID']:
            pass
    pass


def checkProbabilitiesLeaks(intake, outputs, t):
    # outputs.pop('template')
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
                        MTBFReference = eqJsonUniqueTimeFiltered['surveyFrequency'].unique().all() / eqJsonUniqueTimeFiltered['pLeak'].unique().all()
                        MTBFMinRef = MTBFReference
                        MTBFMaxRef = MTBFReference
                        MTTRReference = (MTBFReference * eqJsonUniqueTimeFiltered['pLeak'].unique().all()) / (1 - eqJsonUniqueTimeFiltered['pLeak'].unique().all())
                        MTTRMinRef = MTTRReference
                        MTTRMaxRef = MTTRReference
                    elif singleEmitter == 'SpecificLeaksProduction':
                        MTTRMinRef = eqJsonUnique[(eqJsonUnique['equipmentType'] != 'ActivityFactor')]['MTTRMinDays'].unique().all()
                        MTTRMaxRef = eqJsonUnique[(eqJsonUnique['equipmentType'] != 'ActivityFactor')]['MTTRMaxDays'].unique().all()
                        pLeak = eqJsonUnique['pLeak'].unique().all()
                        # MTTRReference = {'min': MTTRMin, 'max': MTTRMax}
                        MTBFMinRef = (MTTRMinRef * (1 - pLeak) / pLeak)
                        MTBFMaxRef = (MTTRMaxRef * (1 - pLeak) / pLeak)
                    else:
                        MTTRMinRef = 0
                        MTTRMaxRef = 0
                        MTBFMinRef = 0
                        MTBFMaxRef = 0
                    # MTTRDist = {'min': eqJsonUniqueTimeFiltered['MTTRMin'], 'max': eqJsonUniqueTimeFiltered['MTTRMax']}
                    LeaksCountList.append({'unitID': unitID,
                                           'equipmentCount': eqCount,
                                           'totalLeakCount': totalLeakCount,
                                           'leaksCount': leaksCount,
                                           'mcRunNum': int(singleOutputKey),
                                           'meanMTTRDays': meanMTTR/24,
                                           'MTTRMinRef': MTTRMinRef,
                                           'MTTRMaxRef': MTTRMaxRef,
                                           'meanMTBFDays': meanMTBF/24,
                                           'MTBFMinRef': MTBFMinRef,
                                           'MTBFMaxRef': MTBFMaxRef,
                                           'modelReadableName': uniqueEmitter,
                                           'implClass': singleEmitter,
                                           'timestamp': t})

    leaksDF = pd.DataFrame(LeaksCountList)
    unitIDForLeaks = leaksDF['unitID'].dropna().unique()
    totalLeaks = []
    for uid in unitIDForLeaks:
        uniqueEms = leaksDF[(leaksDF['unitID'] == uid)]['modelReadableName'].dropna().unique()
        for uniqueEm in uniqueEms:
            eqUniqueLeaks = leaksDF[(leaksDF['unitID'] == uid) & (leaksDF['modelReadableName'] == uniqueEm)]
            totalEqCount = eqUniqueLeaks['equipmentCount'].sum()
            leakCount = eqUniqueLeaks['leaksCount'].sum()
            totalLeakCount = eqUniqueLeaks['totalLeakCount'].sum()
            meanMTTRallMC = eqUniqueLeaks['meanMTTRDays'].mean()
            meanMTBFallMC = eqUniqueLeaks['meanMTBFDays'].mean()
            implClass = eqUniqueLeaks['implClass'].unique().all()
            MTTRMinRef = float(eqUniqueLeaks[(eqUniqueLeaks != True)]['MTTRMinRef'].dropna().unique())
            MTTRMaxRef = float(eqUniqueLeaks[(eqUniqueLeaks != True)]['MTTRMaxRef'].dropna().unique().max())
            MTBFMinRef = float(eqUniqueLeaks[(eqUniqueLeaks != True)]['MTBFMinRef'].dropna().unique())
            MTBFMaxRef = float(eqUniqueLeaks[(eqUniqueLeaks != True)]['MTBFMaxRef'].dropna().unique().max())

            if totalEqCount != 0:
                pLeakOut = leakCount / totalEqCount
            else:
                pLeakOut = 0
            totalLeaks.append({'unitID': uid,
                               'modelReadableName': uniqueEm,
                               'totalEqCount': totalEqCount,
                               'totalLeakCount': totalLeakCount,
                               'leakCount': leakCount,
                               'pLeakOut': pLeakOut,
                               'meanMTTRDays': meanMTTRallMC,
                               'MTTRMinRef': MTTRMinRef,
                               'MTTRMaxRef': MTTRMaxRef,
                               'meanMTBFDays': meanMTBFallMC,
                               'MTBFMinRef': MTBFMinRef,
                               'MTBFMaxRef': MTBFMaxRef,
                               'timestamp': t,
                               'implClass:': implClass})
    totalLeaks = pd.DataFrame(totalLeaks)
    return totalLeaks, leaksDF


def checkProbabilitiesInitialStates(intake, outputs):
    # outputs.pop('template')
    # ie = list(map(lambda x: x['instantaneousEvents'], outputs.items()))
    ie = [y['instantaneousEvents'] for x, y in outputs.items()]
    uid = ie[0]['unitID'].dropna().unique()
    stateCount = []
    for unitID in uid:
        states = list(map(lambda x: x[(x['unitID'] == unitID) & (x['command'] == 'STATE_TRANSITION')]['state'], ie))
        initialStates = list(map(lambda x: x.to_list()[0], states))
        stateNames = pd.Series(initialStates).unique()
        for singleState in stateNames:
            count = initialStates.count(singleState)
            stateCount.append({'unitID': unitID,
                               'state': singleState,
                               'count': count})
            i = 10
    return stateCount


def checkProbabilities(intake, outputs, t):
    # 1 - filter out leaks and sp leaks equipment.
    # 2 - filter emitter ids, pleaks from equipment, and emitter ids from instantaneousEvents all mc runs
    # 3.1 - pleak from output = (no of failures)/(no of components) at time 0 (simulation time 0.1 day). (from Clay Bell emails)
    # 3.2 - pleak from output = fraction of total components leaking at time 0 on average. (from Clay Bell emails)
    outputs.pop('template')
    totalLeaks, leaksDF = checkProbabilitiesLeaks(intake, outputs, t)
    outs = checkProbabilitiesInitialStates(intake, outputs)
    pass


def checkStateDurations(input, outputs):
    # for continuous sep with stuck dump valve
    o = outputs['0']

    pass


def getConfigForTesting():
    config, _ = au.getConfigList()
    return config


def checkFlareStates(inputs, outputs):
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
        opRatio = (opStateTime/opCount) * 0.0000115741
        unlitRatio = (unlitStateTime/unlitCount) * 0.0000115741
        malfRatio = (malfStateTime/malfCount) * 0.0000115741
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
    opDur = [10, 20]
    unlitDur = [3, 5]
    malfDur = [1, 2]

    pass

def main(configList):
    for configName in configList.keys():
        # update site sheets wrt model formulations? to make it more robust?
        config = configList[configName]
        rawIntake = mf.parseIntakeSpreadsheet(config['studyFullName'])
        reference = genReference(rawIntake, config['description'])
        sm.main(config)
        # validateFluidFlows()
        # genReferenceFromOutputs
        # compareReference
    pass


# if __name__ == "__main__":
#     configList = getConfigForTesting()
#     main(configList)

def plotSimpleEmitter(ie, dumpDriver, sdvDriver):
    x = ie[(ie['command'] == 'STATE_TRANSITION') & (ie['event'] == 'START')]
    sep = x[(x['unitID'] == 'S1_1')]
    tank = x[(x['unitID'] == 'TBC')]
    tank2 = x[(x['unitID'] == 'TBC')]
    tank3 = x[(x['unitID'] == 'TBC')]
    dumpDriverRate = dumpDriver[(dumpDriver['unitID'] == 'TBC') & (dumpDriver['dir'] == 'outletFluidFlows')]
    dumpDriverRate = sum(dumpDriverRate[(dumpDriverRate['driverUnits'] == 'scf')]['driverRate'])
    fillDriverRate = 0

    sdvDriverRate = sdvDriver[(sdvDriver['unitID'] == 'TBC') & (sdvDriver['dir'] == 'outletFluidFlows')]
    sdvDriverRate = sum(sdvDriverRate[(sdvDriverRate['driverUnits'] == 'scf')]['driverRate'])

    tFlare = sdvDriver[(sdvDriver['unitID'] == 'TBC') & (sdvDriver['dir'] == 'outletFluidFlows')]
    tFlare = tFlare[(tFlare['secondaryID'] == 'tank_gas_outlet')]
    flareVal = sum(tFlare[(tFlare['driverUnits'] == 'scf')]['driverRate']) * 60 * 60 * 0.021

    tPRV = sdvDriver[(sdvDriver['unitID'] == 'TBC') & (sdvDriver['dir'] == 'outletFluidFlows')]
    tPRV = tPRV[(tPRV['secondaryID'] == 'emitted_gas')]
    prvVal = sum(tPRV[(tPRV['driverUnits'] == 'scf')]['driverRate']) * 60 * 60 * 0.021

    sdvSepDriverRate = sdvDriver[(sdvDriver['unitID'] == 'TBC') & (sdvDriver['dir'] == 'inletFluidFlows')]
    sdvSepDriverRate = sum(sdvSepDriverRate[(sdvSepDriverRate['driverUnits'] == 'scf')]['driverRate'])
    sep.loc[sep['state'] == 'STUCK_DUMP_VALVE', 'dr'] = sdvSepDriverRate * 60 * 60 * 0.021
    tank.loc[tank['state'] == 'OVERPRESSURE_VENT', 'dr'] = sdvDriverRate * 60 * 60 * 0.021
    tank.loc[tank['state'] != 'OVERPRESSURE_VENT', 'dr'] = fillDriverRate * 60 * 60 * 0.021
    sep.loc[sep['state'] == 'FILLING', 'dr'] = fillDriverRate * 60 * 60 * 0.021
    sep.loc[sep['state'] == 'DUMPING', 'dr'] = dumpDriverRate * 60 * 60 * 0.021

    tank2.loc[tank2['state'] == 'OVERPRESSURE_VENT', 'dr'] = flareVal
    tank3.loc[tank3['state'] == 'OVERPRESSURE_VENT', 'dr'] = prvVal

    # t = tank.plot(x='timestamp', y='dr', kind='bar', title='TankVentsCombined')
    # t.set_ylabel('Gas flow in kg/s')
    # # plt.show()
    #
    # s = sep.plot(x='timestamp', y='dr', kind='bar', title='Separator')
    # s.set_ylabel('Gas flow in kg/s')
    # # plt.show()
    #
    # f = tank2.plot(x='timestamp', y='dr', kind='bar', title='Flare-Flow')
    # f.set_ylabel('Gas flow in kg/s')
    # # plt.show()
    #
    # p = tank3.plot(x='timestamp', y='dr', kind='bar', title='PRV-Flow')
    # p.set_ylabel('Gas flow in kg/s')
    # # plt.show()

    # fig = plt.figure()
    # plt.bar(x=tank['timestamp'], height=tank['dr'], color='red', width=0.4)
    # plt.show()

    # for frame in [tank, sep, tank2, tank3]:
    #     frame.plot(x='timestamp', y='dr', kind='bar')
    # plt.show()

    # ax = tank.plot(x='timestamp', y='dr', kind='bar', color='r', label='TankVentsCombined')
    # plt.legend('TankVentsCombined')
    bx = sep.plot(x='timestamp', y='dr', kind='bar', color='g', label='Separator', ylabel='gas flow [kg/s]')
    # plt.legend('Separator')
    ax2 = tank2.plot(x='timestamp', y='dr', kind='bar', color='b', label='Flare-Flow', ylabel='gas flow [kg/s]')
    # plt.legend('Flare-Flow')
    ax3 = tank3.plot(x='timestamp', y='dr', kind='bar', ax=ax2, color='r', label='PRV-Flow', ylabel='gas flow [kg/s]')
    # plt.legend('PRV-Flow')
    # plt.legend([ax, bx, ax2, ax3], ['TankVentsCombined', 'Separator', 'Flare-Flow', 'PRV-Flow'])

    # x['Unit']
    i = 10
    pass


# rawIntake = readIntake('input\Studies\OGCI\Test_Site_FFCheck\TestSiteFF3.xlsx')
# # FFDump = readDump()
# intake, outputs = readSingleOutput('output/DumpStuckValve')
# op = checkStateDurations(intake, outputs)
# a = glob.glob('C:/death/METEC_MEET/PtE/trunk/MEET2Models/output/SimpleLargeEmitter/0/instantaneousEvents.csv')
# b = glob.glob('C:/death/METEC_MEET/PtE/trunk/MEET2Models/output/SimpleLargeEmitter/0/FFDump9010.csv')
# c = glob.glob('C:/death/METEC_MEET/PtE/trunk/MEET2Models/output/SimpleLargeEmitter/0/FFDump9030.csv')
# ie = pd.read_csv(a[0])
# driverDump = pd.read_csv(b[0])
# sdvDriver = pd.read_csv(c[0])
# op = plotSimpleEmitter(ie, driverDump, sdvDriver)
# a = glob.glob('C:/death/METEC_MEET/PtE/trunk/MEET2Models/output/FlareTest/MC_20220616_134556/0/instantaneousEvents.csv')
# ie = pd.read_csv('C:/death/METEC_MEET/PtE/trunk/MEET2Models/output/FlareTest/MC_20220616_162550/0/instantaneousEvents.csv')

inputs, outputs = readSingleOutput('C:/death/METEC_MEET/PtE/trunk/MEET2Models/output/Validation/Emitters/MC_20220626_073857')
# outputs.pop('template')
# checkFlareStates(inputs, outputs)
i = 20
t = 63115980  # time for emitter testing in seconds
# intake, outputs = readSingleOutput('output/TestSiteFF/MC_TestSiteFF4/pLeakTest')
checkProbabilities(inputs, outputs, t)
# checkStatesInCompressors(instantaneousEvents0, rawIntake)
# checkStatesInWells(rawIntake, instantaneousEvents0)
# checkEmissions(instantaneousEvents0, rawIntake)
