# This module reads raw data distribution files in Excel workbook or .csv format and creates parsed .json files
# of key parameters for use by the picker modules
import os
import json
import pandas as pd
import AppUtils as au
import PickerUtils as pu
from PickerUtils import GEF_CCsheetMap, GEF_NCsheetMap
import numpy as np
from pathlib import Path
from GHGRPLeakProbabilities import GHGRPLeakProbability

# Global parameters for supported GHGRP reporting years and industry sectors
REPORTING_YEARS = [2015, 2016, 2017, 2018]
SECTORS = ['Production', 'Gathering and Boosting', 'Processing', 'Transmission and Storage']
DEFAULT_CONFIG = "config/Config.json"

# This function ensures that all generated cached distribution files are created at startup
def ensureDistributions(config):
    dehydratorDist = DehydratorDistributions(config)
    dehydratorDist.createDataDistributions()

    compressorDist = CompressorDistributions(config)
    compressorDist.createDataDistributions()

    leakDist = LeakDistributions(config)
    leakDist.createDataDistributions()

# Creates distributions for dehydrator parameters based on GHGRP data tables
class DehydratorDistributions:
    # Two types of dehydrator based on throughput. Third type, desiccant, not currently supported
    types = ['large', 'small']

    def __init__(self, config):
        self.dehydratorPath = Path(au.expandFilename(config['dehydratorDataDirectory'], config))  # Directory for dehydrator data
        self.ghgrpPath = Path(au.expandFilename(config['ghgrpDirectory'], config))  # Directory for GHGRP data tables

    # Creates parsed data file for each type, year, sector combination
    def createDataDistributions(self):
        for type in DehydratorDistributions.types:
            for year in REPORTING_YEARS:
                for sector in SECTORS:
                    distributionFile = f"{type}_{year}_{sector}.json"
                    distributionDirectory = Path(self.dehydratorPath/"CachedFiles")
                    distributionFilePath = distributionDirectory/distributionFile

                    # Checks to see if distribution file has already been created and creates if not
                    if not os.path.isfile(distributionFilePath):
                        au.ensureDirectory(distributionFilePath)
                        GHGRPdata = self.getGHGRP_Distribution(self.ghgrpPath, type, year, sector)
                        with open(distributionFilePath, 'w', newline="") as GHGRP_distribution:
                            json.dump(GHGRPdata, GHGRP_distribution)

    # This method parses the raw GHGRP data tables. Writing out some extra info that is not currently being used,
    # particularly for large dehydrator. Could be used to calculate our own emission factor later, if desired
    def getGHGRP_Distribution(self, ghgrpPath, dehydratorType, reportingYear, sector):
        dataDist = {}

        tableMap = {'small': 'EF_W_DEHYDRATORS_SMALL',
                    'large': 'EF_W_DEHYDRATORS_LARGE',
                    'desiccant': 'EF_W_DEHYDRATORS_DESICCANT'}
        table = tableMap[dehydratorType]
        industrySegment = pu.mapIndustrySegment(sector)
        ventDF = pd.read_csv(ghgrpPath / f"{table}.CSV")
        if dehydratorType == 'small':
            filteredVentDF = ventDF[(ventDF[f"{table}.REPORTING_YEAR"] == reportingYear) &
                                    (ventDF[f"{table}.INDUSTRY_SEGMENT"] == industrySegment) &
                                    (ventDF[f"{table}.TABLE_DESC"] == 'Small Glycol Dehydrators')].copy()
            filteredVentDF = filteredVentDF.fillna(0)   # Fills missing values with zeros
            filteredVentDF['ventedDehydrators'] = filteredVentDF[f"{table}.GLYCOL_DEHYDRATOR_COUNT"] - filteredVentDF[f"{table}.SMALL_DEHYD_VENT_TO_FLARES_CNT"]
            filteredVentDF['percentFlared'] = filteredVentDF[f"{table}.SMALL_DEHYD_VENT_TO_FLARES_CNT"].div(filteredVentDF[f"{table}.GLYCOL_DEHYDRATOR_COUNT"])
            filteredVentDF['flaredCO2perDehy'] = filteredVentDF[f"{table}.SMALL_FLARE_CO2_EMISSIONS"].div(filteredVentDF[f"{table}.SMALL_DEHYD_VENT_TO_FLARES_CNT"])
            filteredVentDF['flaredCH4perDehy'] = filteredVentDF[f"{table}.SMALL_FLARE_CH4_EMISSIONS"].div(filteredVentDF[f"{table}.SMALL_DEHYD_VENT_TO_FLARES_CNT"])
            filteredVentDF['ventedCO2perDehy'] = filteredVentDF[f"{table}.SMALL_NOT_FLARED_CO2_EMISSIONS"].div(filteredVentDF['ventedDehydrators'])
            filteredVentDF['ventedCH4perDehy'] = filteredVentDF[f"{table}.SMALL_NOT_FLARED_CH4_EMISSIONS"].div(filteredVentDF['ventedDehydrators'])
            filteredVentDF = filteredVentDF.replace([np.inf, -np.inf], np.nan)
            dataDist.update({'smallDehydratorsPerBasin': list(filteredVentDF[f"{table}.GLYCOL_DEHYDRATOR_COUNT"]),
                             'percentFlared': list(filteredVentDF['percentFlared'].dropna(how='all')),
                             'flaredCO2perDehy': list(filteredVentDF['flaredCO2perDehy'].dropna(how='all')),
                             'flaredCH4perDehy': list(filteredVentDF['flaredCH4perDehy'].dropna(how='all')),
                             'ventedCO2perDehy': list(filteredVentDF['ventedCO2perDehy'].dropna(how='all')),
                             'ventedCH4perDehy': list(filteredVentDF['ventedCH4perDehy'].dropna(how='all'))
                             })
        elif dehydratorType == 'large':
            filteredVentDF = ventDF[(ventDF[f"{table}.REPORTING_YEAR"] == reportingYear) &
                                    (ventDF[f"{table}.INDUSTRY_SEGMENT"] == industrySegment)].copy()
            ventToFlare = list(filteredVentDF[f"{table}.EMISSIONS_VENT_FLARE_OR_TUBES"].dropna(how='all'))
            for n in range(len(ventToFlare)):
                ventToFlare[n] = True if ventToFlare[n] == 'Yes' else False
            ventToAtmosphere = list(filteredVentDF[f"{table}.EMISSIONS_VENT_NO_FLARE_TUBES"].dropna(how='all'))
            for m in range(len(ventToAtmosphere)):
                ventToAtmosphere[m] = True if ventToAtmosphere[m] == 'Yes' else False
            dataDist.update({'natGasFlowRate': list(filteredVentDF[f"{table}.NATURAL_GAS_FLOW_RATE"].dropna(how='all')),
                             'feedNatGasWaterContent': list(filteredVentDF[f"{table}.FEED_NATURAL_GAS_WATER_CONTENT"].dropna(how='all')),
                             'outletNatGasWaterContent': list(filteredVentDF[f"{table}.OUTLET_NAT_GAS_WATER_CONTENT"].dropna(how='all')),
                             'circPumpType': list(filteredVentDF[f"{table}.ABSORBENT_CIRC_PUMP_TYPE"].dropna(how='all')),
                             'circRate': list(filteredVentDF[f"{table}.ABSORBENT_CIRCULATION_RATE"].dropna(how='all')),
                             'absorbentType': list(filteredVentDF[f"{table}.ABSORBENT_TYPE"].dropna(how='all')),
                             'strippingGas': list(filteredVentDF[f"{table}.IS_STRIPPER_GAS_USED"].dropna(how='all')),
                             'flashTankSeparator': list(filteredVentDF[f"{table}.IS_FLASH_TANK_SEPARATOR_USED"].dropna(how='all')),
                             'opHours': list(filteredVentDF[f"{table}.GLYCOL_DEHYD_OPERATING_HOURS"].dropna(how='all')),
                             'wetNatGasTemp': list(filteredVentDF[f"{table}.WET_NATURAL_GAS_TEMPERATURE"].dropna(how='all')),
                             'wetNatGasPress': list(filteredVentDF[f"{table}.WET_NATURAL_GAS_PRESSURE"].dropna(how='all')),
                             'ventToFlare': ventToFlare,
                             'ventToAtmosphere': ventToAtmosphere,
                             'flaredCO2': list(filteredVentDF[f"{table}.GLY_DEH_MRTHN_FLARING_CO2_EMM"].dropna(how='all')),
                             'flaredCH4': list(filteredVentDF[f"{table}.FLARING_CH4_EMISSIONS"].dropna(how='all')),
                             'flaredN2O': list(filteredVentDF[f"{table}.FLARING_N2O_EMISSIONS"].dropna(how='all')),
                             'ventedCO2': list(filteredVentDF[f"{table}.GLY_DEH_MRTHN_VENTING_CO2_EMM"].dropna(how='all')),
                             'ventedCH4': list(filteredVentDF[f"{table}.VENTING_CH4_EMISSIONS"].dropna(how='all'))
                             })
        return dataDist

# Creates distributions for compressor parameters based on GHGRP data tables
class CompressorDistributions:
    types = ['Reciprocating', 'Centrifugal']

    def __init__(self, config):
        self.compressorPath = Path(au.expandFilename(config['compressorDataDirectory'], config))  # Directory for compressor data
        self.ghgrpPath = Path(au.expandFilename(config['ghgrpDirectory'], config))  # Directory for GHGRP data tables
        self.ghgrpFuelFlowFile = au.expandFilename(config['ghgrpFuelFlowFile'], config)

    def createDataDistributions(self):
        for type in CompressorDistributions.types:
            for year in REPORTING_YEARS:
                for sector in SECTORS:
                    equipmentType = f"{type}Compressor"
                    distributionFile = f"{equipmentType}_{year}_{sector}.json"
                    distributionDirectory = Path(self.compressorPath/"CachedFiles")
                    distributionFilePath = distributionDirectory / distributionFile

                    # Checks to see if distribution file has already been created and creates if not
                    if not os.path.isfile(distributionFilePath):
                        GHGRPdata = self.createGHGRPDataDistribution(equipmentType, sector, self.ghgrpPath, year,
                                                                     self.ghgrpFuelFlowFile)
                        au.ensureDirectory(distributionFilePath)
                        with open(distributionFilePath, 'w', newline="") as GHGRP_distribution:
                            json.dump(GHGRPdata, GHGRP_distribution)

    # Creates a GHGRP data distribution for compressors
    def createGHGRPDataDistribution(self, equipmentType, sector, ghgrpPath, reportingYear, ghgrpFuelFlowFile):
        GHGRPdata = {}
        dataTables = pu.mapTables(equipmentType, sector)
        industrySegment = pu.mapIndustrySegment(sector)

        # Some sectors may rely on multiple data tables to make the distribution
        if isinstance(dataTables['name'], list):
            activityFactorFile = dataTables['name'][0]
            emissionSourceFile = dataTables['name'][1]
            emissionFactorFile = dataTables['name'][2]
            activityFactorDist = self.getActivityFactorDist(ghgrpPath, activityFactorFile, ghgrpFuelFlowFile,
                                                       reportingYear, industrySegment)
            GHGRPdata.update(activityFactorDist)
            emissionFactorDist, emissionUnits = self.getEmissionFactorDist(ghgrpPath, emissionSourceFile, emissionFactorFile,
                                                                      reportingYear, industrySegment)
            GHGRPdata.update(emissionFactorDist)
            GHGRPdata.update(emissionUnits)
        # Some sectors may have all required data reported in a single table
        elif isinstance(dataTables['name'], str):
            data, emissionUnits = self.getCombinedDist(ghgrpPath, dataTables['name'], reportingYear, industrySegment)
            GHGRPdata.update(data)
            GHGRPdata.update(emissionUnits)

        return GHGRPdata


    # Used for industry segments where activity factors and emission factors are reported in separate GHGRP tables
    def getActivityFactorDist(self, ghgrpPath, activityFactorFile, fuelFlowFile, reportingYear, industrySegment):
        afData = pd.read_csv(f"{ghgrpPath}/{activityFactorFile}.CSV")
        compressorType, afColumnHeads = pu.getColumnHeads(afData, activityFactorFile)
        filteredAF = afData[(afData[afColumnHeads['reportingYear']] == reportingYear) &
                            (afData[afColumnHeads['industrySegment']] == industrySegment)].copy()
        # Makes unique compressor ID based on facility ID and unit ID
        filteredAF['compID'] = filteredAF[afColumnHeads['facilityID']].map(str) + filteredAF[
            afColumnHeads['compressorID']]
        facID = filteredAF[afColumnHeads['facilityID']].drop_duplicates()
        facList = list(facID)
        facDict = {}
        for singleFac in facList:
            compressorCount = filteredAF.loc[filteredAF[afColumnHeads['facilityID']] == singleFac].GLOBAL_EVENT_COUNT()
            count = compressorCount[afColumnHeads['facilityID']]
            facDict.update({int(singleFac): int(count)})
        activityFactorDist = {'compressorsPerSite': facDict}
        if 'nopHours' not in afColumnHeads:
            # Centrifugal compressors don't report NOP to GHGRP, assumed to be zero. Should we drop probabilistic state transitions for fixed?
            afColumnHeads.update({'nopHours': 'nopHours'})
            filteredAF['nopHours'] = 0
        filteredAF['totalTime'] = (filteredAF[afColumnHeads['opHours']] +
                                   filteredAF[afColumnHeads['nopHours']] +
                                   filteredAF[afColumnHeads['nodHours']])
        filteredAF['uptime'] = filteredAF[afColumnHeads['opHours']] / filteredAF['totalTime']
        uptime = list(filteredAF['uptime'].dropna(how='all'))
        filteredAF['percNOP'] = filteredAF[afColumnHeads['nopHours']] / filteredAF['totalTime']
        percNOP = list(filteredAF['percNOP'].dropna(how='all'))
        filteredAF['percNOD'] = filteredAF[afColumnHeads['nodHours']] / filteredAF['totalTime']
        percNOD = list(filteredAF['percNOD'].dropna(how='all'))
        activityFactorDist.update({'uptime': uptime, 'percentNOP': percNOP, 'percentNOD': percNOD})
        if 'sealType' in afColumnHeads:
            sealType = list(filteredAF[afColumnHeads['sealType']].dropna(how='all'))
            activityFactorDist['sealType'] = sealType

        # Assumes all reciprocating compressors have reciprocating drivers (mostly but not completely true)
        if compressorType == 'reciprocating':
            matchCriteria = 'RICE (Reciprocating internal combustion engine)'
        # Assumes all centrifugal compressors have simple cycle combustion turbine drivers (mostly but not completely true)
        elif compressorType == 'centrifugal':
            matchCriteria = 'SCCT (CT (Turbine, simple cycle combustion))'

        fuelFlowData = pd.read_csv(f"{ghgrpPath}/{fuelFlowFile}.CSV", index_col=None, na_values=['NaN'], sep=',',
                                   low_memory=False)  # This prevents warnings for sparse data tables
        filteredFF = fuelFlowData[(fuelFlowData[f"{fuelFlowFile}.REPORTING_YEAR"] == reportingYear) &
                                  (fuelFlowData[f"{fuelFlowFile}.UNIT_TYPE"] == matchCriteria) &
                                  (fuelFlowData[f"{fuelFlowFile}.IND_UNIT_HEATINPUTCAPACITY_UOM"] == 'mmBtu/hr')].copy()
        filteredFF['compID'] = filteredFF[f"{fuelFlowFile}.FACILITY_ID"].map(str) + filteredFF[
            f"{fuelFlowFile}.UNIT_NAME"]

        combinedDF = filteredAF.merge(filteredFF, how='left', on='compID')
        combinedDF = combinedDF.set_index('compID')
        hpHeatCapDF = combinedDF[[afColumnHeads['driverSize'], f"{fuelFlowFile}.IND_UNIT_HEAT_INPUT_CAPACITY"]].copy()
        hpHeatCapDF = hpHeatCapDF.dropna(axis=0, how='any')
        driverSizeDist = list(hpHeatCapDF[afColumnHeads['driverSize']])
        heatCapDist = list(hpHeatCapDF[f"{fuelFlowFile}.IND_UNIT_HEAT_INPUT_CAPACITY"])
        activityFactorDist['driverSize'] = driverSizeDist
        activityFactorDist['heatCapacity'] = heatCapDist
        return activityFactorDist

    # Used for industry segments where activity factors and emission factors are reported in separate GHGRP tables
    def getEmissionFactorDist(self, ghgrpPath, emissionSourceFile, emissionFactorFile, reportingYear, industrySegment):
        emissionFactorDist = {}
        efSourceData = pd.read_csv(f"{ghgrpPath}/{emissionSourceFile}.CSV")
        compressorType, efSourceColumnHeads = pu.getColumnHeads(efSourceData, emissionSourceFile)
        filteredEFSource = efSourceData[(efSourceData[efSourceColumnHeads['reportingYear']] == reportingYear) &
                                        (efSourceData[
                                             efSourceColumnHeads['industrySegment']] == industrySegment)].copy()
        leakVentIDs = list(filteredEFSource[efSourceColumnHeads['leakOrVentID']])
        leakVentSource = list(filteredEFSource[efSourceColumnHeads['compressorSource']])
        leakVentTypes = list(filteredEFSource[efSourceColumnHeads['compressorSource']].drop_duplicates())
        leakVentDict = dict(zip(leakVentIDs, leakVentSource))
        efData = pd.read_csv(f"{ghgrpPath}/{emissionFactorFile}.CSV", index_col=None, na_values=['NaN'], sep=',',
                             low_memory=False)  # This prevents warnings for sparse data tables

        compressorType, efColumnHeads = pu.getColumnHeads(efData, emissionFactorFile)
        filteredEF = efData[(efData[efColumnHeads['reportingYear']] == reportingYear) &
                            (efData[efColumnHeads['industrySegment']] == industrySegment) &
                            (efData[efColumnHeads['measurementLocation']] != "After commingling")].copy()
        for singleLeakVent in leakVentTypes:
            singleLeakVentIDs = [key for key, value in leakVentDict.items() if value == singleLeakVent]
            efName = f"emissionFactor_{singleLeakVent}"
            singleLeakRows = filteredEF.loc[filteredEF[efColumnHeads['leakOrVentID']].isin(singleLeakVentIDs)]
            singleLeakEF = list(singleLeakRows[efColumnHeads['measurementFlowRate']].dropna(how='all'))
            emissionFactorDist[efName] = singleLeakEF
            emissionUnits = {'emissionUnits': 'SCFH'}
        return emissionFactorDist, emissionUnits

    # Used for industry segments where all file is on a single GHGRP table
    def getCombinedDist(self, ghgrpPath, dataFile, reportingYear, industrySegment):
        dataDist = {}
        sourceData = pd.read_csv(f"{ghgrpPath}/{dataFile}.CSV")
        compressorType, columnHeads = pu.getColumnHeads(sourceData, dataFile)
        filteredData = sourceData[(sourceData[columnHeads['reportingYear']] == reportingYear) &
                                  (sourceData[columnHeads['industrySegment']] == industrySegment)].copy()
        filteredData[columnHeads['numCompressors']].fillna(0, inplace=True)
        facilityID = list(map(int, filteredData[columnHeads['facilityID']]))
        compressorsPerBasin = list(map(int, filteredData[columnHeads['numCompressors']]))
        filteredData = filteredData[(filteredData[columnHeads['numCompressors']] != 0)].copy()
        filteredData['CO2perCompressor'] = filteredData[columnHeads['totalCO2']] / filteredData[columnHeads['numCompressors']]
        filteredData['CH4perCompressor'] = filteredData[columnHeads['totalCH4']] / filteredData[columnHeads['numCompressors']]
        dataDist.update({'compressorsPerSite': dict(zip(facilityID, compressorsPerBasin)),
                         'CO2perCompressor': list(filteredData['CO2perCompressor']),
                         'CH4perCompressor': list(filteredData['CH4perCompressor'])})
        emissionUnits = {'emissionUnits': 'metric tons per year'}
        return dataDist, emissionUnits

class LeakDistributions:
    # GHGRP data only had component info for 2017, 2018 reporting year.
    GHGRP_LEAK_YEARS = [2017, 2018]
    # GHGRP data only has component count info for Production and Gathering and Boosting sectors.
    # All others use counts for gathering
    GHGRP_LEAK_SECTORS = ["Production", "Gathering and Boosting"]
    # GEF classifies leaks as either compressor or non-compressor
    GEF_equipmentTypes = ['compressor', 'non-compressor']

    def __init__(self, config):
        self.leakPath = Path(au.expandFilename(config['leakDataDirectory'], config))
        self.distributionDirectory = Path(self.leakPath / "CachedFiles")
        self.ghgrpPath = Path(au.expandFilename(config['ghgrpDirectory'], config))
        self.GEFAverageFactorFile = au.expandFilename(config['GEF_AvgFactorInput'], config)
        self.TSEmissionFactorFile = au.expandFilename(config['TS_EFInput'], config)

    # This function is also used by the LeakPicker to find and load the correct cached probability file
    def returnGHGRP_ProbabilityFilename(self, reportingYear, sector):
        # Only have component counts for production and gathering sectors. All other sectors default to gathering
        sector = "Production" if sector == "Production" else "Gathering and Boosting"
        leakProbabilityFile = f"GHGRP_Leak_Probabilities_{reportingYear}_{sector}.json"
        return leakProbabilityFile

    def returnGEF_ProbabilityFilename(self, equipmentType):
        leakProbabilityFile = f"GEF_{equipmentType}_Leak_Probabilities.json"
        return leakProbabilityFile

    # Two different sets of files generated: a parsed set of leak probabilities from GHGRP data, and a parsed set of
    # leak probabilities from the GEF data
    def createDataDistributions(self):
        # Get GHGRP leak probabilities
        ghgrpLeakProbabilities = GHGRPLeakProbability(self.leakPath, self.ghgrpPath)
        for year in LeakDistributions.GHGRP_LEAK_YEARS:
            for sector in LeakDistributions.GHGRP_LEAK_SECTORS:
                leakProbabilityFile = self.returnGHGRP_ProbabilityFilename(year, sector)
                leakProbabilityFilepath = self.distributionDirectory / leakProbabilityFile

                # Checks to see if probability file has already been created and creates if not
                if not os.path.isfile(leakProbabilityFilepath):
                    leakProbabilityData = ghgrpLeakProbabilities.calculateLeakProbabilities(year, sector)
                    au.ensureDirectory(leakProbabilityFilepath)
                    with open(leakProbabilityFilepath, 'w', newline="") as lpFile:
                        json.dump(leakProbabilityData, lpFile)

        for type in LeakDistributions.GEF_equipmentTypes:
            leakProbabilityFile = self.returnGEF_ProbabilityFilename(type)
            leakProbabilityFilepath = self.distributionDirectory / leakProbabilityFile

            # Checks to see if probability file has already been created and creates if not
            if not os.path.isfile(leakProbabilityFilepath):
                leakProbabilities = self.createGEF_LeakProbabilities(type)
                with open(leakProbabilityFilepath, mode='w', newline="") as lpFile:
                    json.dump(leakProbabilities, lpFile)

    def createGEF_LeakProbabilities(self, equipmentClass):
        sheetMap = GEF_CCsheetMap if equipmentClass == 'compressor' else GEF_NCsheetMap
        inverseSheetMap = pu.invertSheetMap(sheetMap)

        sheetList = sheetMap.values()

        # Leakers may be based on per component or per unit. The two match criteria will find and return the right one
        totalMatchCrit1 = "Number Screened Components (Mean)"
        totalMatchCrit2 = "Number Screened Units (Mean)"
        leakMatchCrit1 = "Number Detected Emissions"
        leakMatchCrit2 = "Units with Detected Emissions"

        pLeakDict = {}
        for singleSheet in sheetList:
            sheetDF = pd.read_excel(self.GEFAverageFactorFile, singleSheet)

            totalTest1 = sheetDF.loc[sheetDF["Emission Factor"] == totalMatchCrit1]
            totalTest2 = sheetDF.loc[sheetDF["Emission Factor"] == totalMatchCrit2]

            if len(totalTest1) != 0:
                totalIndex = sheetDF.loc[sheetDF["Emission Factor"] == totalMatchCrit1].index[0]
            elif len(totalTest2) != 0:
                totalIndex = sheetDF.loc[sheetDF["Emission Factor"] == totalMatchCrit2].index[0]

            totalComponents = sheetDF.iat[totalIndex, 1]

            leakTest1 = sheetDF.loc[sheetDF["Emission Factor"] == leakMatchCrit1]
            leakTest2 = sheetDF.loc[sheetDF["Emission Factor"] == leakMatchCrit2]

            if len(leakTest1) != 0:
                leakersIndex = sheetDF.loc[sheetDF["Emission Factor"] == leakMatchCrit1].index[0]
            elif len(leakTest2) != 0:
                leakersIndex = sheetDF.loc[sheetDF["Emission Factor"] == leakMatchCrit2].index[0]

            # Subtracts OGI non-detects from leakers, if present
            if sheetDF["Emission Factor"].str.contains('OGI Non-detect').any():
                nonDetectIndex = sheetDF.loc[sheetDF["Emission Factor"] == "OGI Non-detect"].index[0]
                leakers = sheetDF.iat[leakersIndex, 1] - sheetDF.iat[nonDetectIndex, 1]
            else:
                leakers = sheetDF.iat[leakersIndex, 1]
            keys = inverseSheetMap[singleSheet]
            pLeak = leakers / totalComponents
            for key in keys:
                pLeakDict[key] = pLeak
        return pLeakDict

if __name__ == "__main__":
    config, _ = au.getConfig(DEFAULT_CONFIG)
    ensureDistributions(config)
