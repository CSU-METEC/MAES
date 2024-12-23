import EquipmentTable as em
import pandas as pd
import logging
import numpy as np
import math
import json
import MEETExceptions as me
import AppUtils as au
import re

import MEETClasses    # Don't delete this -- classmap creation in instantiateIntake depends on it
import MEETProductionWells
import MEETLinkedProductionEq
# import MEETVentedLiquidUnloading
import MEETComponentLeaks
import MEETIntermittentPneumatic
# import MEET_1_Compatability
import MEETSamples
# import OGCIClasses
# import MEETFluidFlow
import MEETTestEquipment
import MEETFFClasses
import ModelClasses

MODEL_PARAMETER_FORMAT = "Model Parameter {idx}"
PYTHON_PARAMETER_FORMAT = "Python Parameter {idx}"
PARAMETER_TYPE_FORMAT = "Parameter Type {idx}"
DISTRIBUTION_FORMAT = "Distribution {idx}"

MODEL_FORMULATION_COPY_KEYS = [
    'Model ID',
    'Python Category',
    'Python Class',
    'Readable Name',
    'Category',
    'Emission Category',
    # 'One Line Concept'
]

UNITS_RE = '(?:(?P<val>[^\[\]]+(?![^\[]*\]))(?:.*\[(?P<units>[^\[\]]*)\].*)?)$'

def numberToDict(inDict, idx):
    outDict = {}
    outDict['modelParameter'] = inDict[MODEL_PARAMETER_FORMAT.format(idx=idx)]
    outDict['pythonParameter'] = inDict[PYTHON_PARAMETER_FORMAT.format(idx=idx)]
    outDict['parameterType'] = inDict[PARAMETER_TYPE_FORMAT.format(idx=idx)]
    outDict['distribution'] = inDict[DISTRIBUTION_FORMAT.format(idx=idx)]
    return outDict

def isNan(elt):
    return elt != elt  # tricky check for nan -- by definition, nans are not equal

def parseModelFormulation(formulationList):
    processedFormulationList = []
    for singleForm in formulationList:
        thisForm = {}
        for singleParm in MODEL_FORMULATION_COPY_KEYS:
            thisForm[singleParm] = singleForm[singleParm]
        numberedParmList = []
        for i in range(15):
            idx = i + 1
            mpStr = PYTHON_PARAMETER_FORMAT.format(idx=idx)
            if not isNan(singleForm.get(mpStr, math.nan)):
                numberedParmList.append(numberToDict(singleForm, idx))
        thisForm['parameters'] = numberedParmList
        processedFormulationList.append(thisForm)
    return processedFormulationList

def parseIntakeSpreadsheet(filename):
    with pd.ExcelFile(filename) as xlsFile:
        masterEqDF         = pd.read_excel(xlsFile, sheet_name="Master Equipment")
        simParamsDF        = pd.read_excel(xlsFile, sheet_name="Global Simulation Parameters", header=None)

        ret = {}
        ret['masterEquipment'] = masterEqDF.to_dict('records')
        ret['simParameters'] = simParamsDF.to_dict('records')

        for singleSheet in ret['masterEquipment']:
            tabName = singleSheet['Tab']
            # sheetDF = pd.read_excel(xlsFile, sheet_name=tabName, na_filter=False)
            sheetDF = pd.read_excel(xlsFile, sheet_name=tabName)
            ret[tabName] = sheetDF.to_dict('records')
    return ret

def lookupMajorEquipment(elt):
    et = em.EquipmentTableObsolete.getEquipmentTable()
    eAttrsDF = et.equipmentAttributes
    eqMask = np.logical_and(eAttrsDF['unitID'] == elt, eAttrsDF['implCategory'] == 'MajorEquipment')
    meForModelDF = eAttrsDF[eqMask]
    # todo: make these specific Exception instances
    if len(meForModelDF) == 0:
        logging.warning(f"No Major Equipment instances found for Unit ID {elt}")
        raise NotImplementedError
    if len(meForModelDF) > 1:
        meNames = set(meForModelDF['unitID'])
        logging.warning(f"Multiple Major Equipment instances found for Unit ID {elt}:{meNames}")
        raise NotImplementedError
    meName = meForModelDF.iloc[0]['name']
    mEquipment = et.getEquipmentByName(meName)
    return mEquipment

def toParamKey(param):
    try:
        m = re.match(UNITS_RE, param)
        val = m.group('val')
        valLower = val.lower()
        valElts = valLower.split(' ')
        valKey = '_'.join(filter(lambda x: x != '', valElts))
        units = m.group('units')
        if units:
            units = units.strip('[]')
        val = val.strip()
        return {'valKey': valKey, 'val': val, 'units': units}
    except AttributeError:
        raise Exception("Unable to parameter key {param}; check syntax")

def colNameToPythonVar(colName):
    fields = colName.split(' ')
    ucFields = list(map(lambda x: x.capitalize(), fields))
    ucFields[0] = ucFields[0].lower()
    ret = ''.join(ucFields)
    return ret

def siteSheetRowToParamKeys(ssr):
    ret = {}
    for singleCol in ssr.keys():
        pKey = toParamKey(singleCol)
        ret[pKey['valKey']] = singleCol
    return ret

def instantiateElementFromIntake(simdm, modelFormulation, siteSheetRow, classMap, instParms={}):
    parmList = {}
    usedSheetParms = set()
    siteSheetRowParamKeys = siteSheetRowToParamKeys(siteSheetRow)
    for singleParm in modelFormulation['Model Parameters']:
        parmType = singleParm['Parameter Type']
        if parmType == 'Sheet':
            pythonParm = singleParm['Python Parameter']
            modelParm = singleParm['Model Parameter']
            pk = toParamKey(modelParm)
            siteSheetKey = siteSheetRowParamKeys.get(pk['valKey'], None)
            pParam = siteSheetRow.get(siteSheetKey, None)
            if pd.isna(pParam):
                # Model parameter is not specified.  Decide what to do:
                #   if "Optional" is True in Model Formulation, go on processing parameters
                #   if "Optional" is False in Model Formulation, throw an error
                isOptional = singleParm.get('Optional', 'False')
                if isOptional == 'True':
                    pParam = None
                    usedSheetParms.add(singleParm['Model Parameter'])
                else:
                    msg = f"Model Parameter {modelParm} not specified for {modelFormulation['Python Class']}"
                    logging.error(msg)
                    me.UnknownElementError(msg)
            else:
                usedSheetParms.add(singleParm['Model Parameter'])
        elif parmType == 'Profile':
            pythonParm = singleParm['Python Parameter']
            pParam = singleParm['Distribution']
        elif parmType == 'Constant':
            pythonParm = singleParm['Python Parameter']
            pParam = singleParm['Value']
        elif parmType == 'ProfileDir':
            pythonParm = singleParm['Python Parameter']
            pParam = singleParm['Distribution']
        else:
            logging.warning(f"Unknown parameter type {parmType}")
            raise KeyError
        parmList[pythonParm] = pParam

    # check if we want to instantiate this instance using the instantiationVal / valsForInstantiation protocol
    if ('instantiationVal' in parmList) and ('valsForInstantiation' in parmList):
        iv = parmList['instantiationVal']
        if isinstance(iv, bool):
            if iv:
                iv = 'True'
            else:
                iv = 'False'
        if (isinstance(iv, float) and math.isnan(iv)) or (iv is None):
            iv = ''
        instVal = iv.replace(' ', '').lower()
        vals = parmList['valsForInstantiation'].replace(' ', '').lower().split(',')
        if instVal not in vals:
            logging.debug(f"Instantiation value {parmList['instantiationVal']} not in instantiation values {parmList['valsForInstantiation']}, skipping")
            usedSheetParms.add('Model ID')
            return None, usedSheetParms
        # parmList.pop('instantiationVal')
        # parmList.pop('valsForInstantiation')
    # todo: make this be table-driven, not hard coded.  Put table in intake spreadsheet???
    parmList['modelID'] = siteSheetRow['Model ID']
    usedSheetParms.add('Model ID')
    parmList['implCategory'] = modelFormulation['Python Category']
    parmList['implClass'] = modelFormulation['Python Class']
    parmList['modelReadableName'] = modelFormulation['Readable Name']
    parmList['modelEmissionCategory'] = modelFormulation.get('Emission Category', None)
    parmList['modelCategory'] = parmList.get('modelCategory', modelFormulation.get('Category', None))
    parmList['modelSubcategory'] = parmList.get('modelSubcategory', modelFormulation.get('Subcategory', None))
    cls = classMap[modelFormulation['Python Category']][modelFormulation['Python Class']]['class']
    inst = cls(**{**instParms, **parmList})
    return inst, usedSheetParms

def createClassmap():
    # todo: migrate this into core EquipmentTable code
    classMap = {}
    classMap['Facility'] = em.buildSubclassMap(em.Facility)
    classMap['MajorEquipment'] = em.buildSubclassMap(em.MajorEquipment)
    classMap['Emitter'] = em.buildSubclassMap(em.Emitter)
    classMap['MEETService'] = em.buildSubclassMap(em.MEETService)
    return classMap

def readModelFormulationByConfig(config, modelID):
    modelFilename = au.expandFilename(config['modelTemplate'], {**config, 'modelID': modelID}, readonly=True)
    logging.debug(f"Loading model formulation {modelID}, filename: {modelFilename}")
    try:
        with open(modelFilename, "r") as iFile:
            modelFormulation = json.load(iFile)
        return modelFormulation
    except Exception as e:
        logging.exception(f"JSON error in file: {modelFilename}")
        raise e

def readModelFormulation(simdm, modelID):

    return readModelFormulationByConfig(simdm.config, modelID)

def getEqComponentCount(modelFormulation, singleIntakeRow):
    ret = range(1)
    for param in modelFormulation['Model Parameters']:
        if param['Python Parameter'] == 'eqComponentCount':
            retPar = param['Model Parameter']
            ret = singleIntakeRow.get(retPar)
            if isinstance(ret, str):
                ret = ret.split(',')
            else:
                ret = range(ret)
    return ret

def updateParams(modelFormulation, singleIntakeRow, singleEqCount):
    # newSingleIntakeRow = singleIntakeRow
    # cc = list(param['Value'] for param in modelFormulation['Model Parameters'] if param['Python Parameter'] == 'multipleInstances')
    cc = False
    for param in modelFormulation['Model Parameters']:
        if param['Python Parameter'] == 'multipleInstances':
            cc = param['Value']
    if cc:
        unitID = singleIntakeRow['Unit ID']
        instFormat = next(param['Value'] for param in modelFormulation['Model Parameters'] if param['Python Parameter'] == 'instanceFormat')
        instFormat = instFormat.format(unitID=unitID, eqCount=singleEqCount)
        # singleIntakeRow['Unit ID'] = instFormat
        returnRow = {**singleIntakeRow, 'Unit ID': instFormat}
        return returnRow
    else:
        return singleIntakeRow

def instantiateIntake(simdm, intakeDict):
    classMap = createClassmap()
    # Instantiate elements that are pointed to by the master equipment tab
    for singleTab in intakeDict['masterEquipment']:
        tabName = singleTab['Tab']
        logging.info(f"Instantiating tab {tabName}")
        thisTab = intakeDict[tabName]
        parmsForTab = None
        for singleIntakeRow in thisTab:
            if parmsForTab is None:
                parmsForTab = set(singleIntakeRow.keys())
            modelName = singleIntakeRow['Model ID']
            if (not modelName or
                    (isinstance(modelName, float) and math.isnan(modelName))):
                continue
            modelFormulation = readModelFormulation(simdm, modelName)

            eqCount = getEqComponentCount(modelFormulation, singleIntakeRow)
            # oldSingleIntakeRow = singleIntakeRow
            for singleEqCount in eqCount:
                newSingleIntakeRow = updateParams(modelFormulation, singleIntakeRow, singleEqCount)
                inst, usedParms = instantiateElementFromIntake(simdm, modelFormulation, newSingleIntakeRow, classMap)
                if not inst:
                    continue
                parmsForWarnings = set(map(lambda x: x.get('val'), map(toParamKey, parmsForTab)))
                parmsForTab = parmsForWarnings.difference(usedParms)
                if 'Emitters' in modelFormulation:
                    if modelFormulation.get('Python Category', None) != 'MajorEquipment':
                        msg = f"'Emitters' parameter only valid for MajorEquipment -- {modelFormulation}"
                        logging.warning(msg)
                        me.IllegalElementError(msg)
                        continue
                    emitterParms = {'facilityID': inst.facilityID, 'unitID': inst.unitID}
                    for singleEmitter in modelFormulation['Emitters']:
                        emitterInst, usedParms = instantiateElementFromIntake(simdm, singleEmitter, newSingleIntakeRow, classMap, instParms=emitterParms)
                        if not usedParms:
                            pass
                        else:
                            parmsForTab = parmsForTab.difference(usedParms)
        if parmsForTab:
            logging.warning(f"Unused parameters in tab {tabName}: {parmsForTab}")

