import AppUtils as au
from pathlib import Path
import ModelFormulation as mf
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

LATEX_HEADER = ''

LATEX_TMPL = '''
\\begin{{itemize}}
    \\item Model name: {modelName}
	\\item Column name: {Model Parameter}
	\\item Description: {ddDescription}
	\\item Valid values: {ddUnits}
\\end{{itemize}}
'''

LATEX_FOOTER = ''

# LATEX_HEADER = '''
# \\begin{tabular}{|c|c|c|c|}
# 	\\hline
# 	Model Parameter & Description & Units & Valid Values \\\\
# 	\\hline
# '''
#
# LATEX_TMPL = '''
# {modelParameter} & {ddDescription} & bbl & {ddUnits} \\\\
# \\hline
# '''
#
# LATEX_FOOTER = '''
# \\end{tabular}
# '''

MD_HEADER_TMPL = '''
# Python Class: {pythonClass}
# Model Filename: {filename}

'''

MD_MEMBER_TMPL = '''
## {Model Parameter} {defined}
  - Dynamic Class Name: {pythonClass}
  - Dynamic Class Parameter Name: {dynParamName}
  - Description: {ddDescription}
  - Units: {ddUnits}
  - Used in: {usedIn}
  - Model Definition Parameters
    - Model Parameter: {Model Parameter}
    - Parameter Type: {Parameter Type}
    - Name: {name}
    - Optional: {Optional}
    - Source: {source}
  - Python Implementation Parameters
    - Python Class: {pythonClass}
    - Python Parameter: {Python Parameter}
    - Major Equipment Python Class: {majorEquipmentPythonClass}

'''

SPHINX_HEADER_TMPL = '''
{Readable Name}
{eqLines}

Model Filename: {filename}

{description}
'''

SPHINX_STATE_HDR_TMPL = '''
States
------
'''

SPHINX_STATE_DESC_TMPL = '''
{stateName}
  {description}
'''

SPHINX_STATE_MACHINE_IMAGE_TMPL = '''
.. image:: {StateMachineDiagram}
'''

SPHINX_FLUID_FLOW_HDR_TMPL = '''
Fluid Flows
-----------
'''

SPHINX_FLUID_FLOW_DESC_TMPL = '''
{Fluid}
  {description}
'''

SPHINX_FLUID_FLOW_SECONDARY_ID_TMPL = '''

  *Secondary ID: {secondary_id}*
'''

SPHINX_SITE_DEF_TMPL = '''
Site Definition Columns
-----------------------
'''

SPHINX_MEMBER_TMPL = '''
{indent}**{modelParameter}**
{indent}  {ddDescription}
'''

SPHINX_UNITS_TMPL = '''
{indent}  *Units:* {ddUnits}
'''

SPHINX_EMITTER_HEADER_TMPL = '''
Emitters
--------
'''

SPHINX_EMITTER_TMPL = '''
**{Readable Name}**
  Emitter Category: {Category}
  
  Emission Category: {Emission Category}
  
  Model Parameters:
  
'''

SPHINX_IDX_HEADER_TMPL = '''
.. _model_reference_label:

MEET Model Reference
====================

.. toctree::

'''

SPHINX_IDX_CLASS_TMPL = '''  {Readable Name} <{className}>
'''

SPHINX_REFERENCE_TMPL = '''
.. include:: {filename}
'''

def getModelFiles(config):
    mfFilename = au.expandFilename(config['modelTemplate'], {**config, 'modelID': '.'})
    modelPath = Path(mfFilename)
    return map(lambda x: x.name, modelPath.glob("*.json"))

def paramsFromModelParams(modelParams, source, name, fileName, pythonClass, mePythonClass):
    return map(lambda x: {**x,
                          'source': source,
                          'name': name,
                          'file': fileName,
                          'Optional': x.get('Optional', False),
                          'pythonClass': pythonClass,
                          'majorEquipmentPythonClass': mePythonClass
                          },
               filter(lambda x: x['Parameter Type'] == 'Sheet', modelParams))

def parametersFromModel(mForm, fileName):
    allParams = []
    allParams.extend(paramsFromModelParams(mForm['Model Parameters'],
                                           'Model',
                                           mForm.get('Readable Name', ''),
                                           fileName,
                                           mForm['Python Class'],
                                           mForm['Python Class']
                                           ))
    # for singleEmitter in mForm.get('Emitters', []):
    #     allParams.extend(paramsFromModelParams(singleEmitter['Model Parameters'],
    #                                            'Emitter',
    #                                            singleEmitter.get('Readable Name', ''),
    #                                            fileName,
    #                                            singleEmitter['Python Class'],
    #                                            mForm['Python Class']))
    paramDF = pd.DataFrame.from_records(allParams)

    paramDF['Optional'] = paramDF['Optional'].replace({np.nan: False})
    return paramDF

def readModelParameters(config):
    modelFiles = getModelFiles(config)
    paramDict = {}
    for singleModelFile in modelFiles:
        mForm = mf.readModelFormulationByConfig(config, singleModelFile)
        paramDict[singleModelFile] = mForm
    return paramDict

def expandMultiDF(inDF):
    retDF = pd.DataFrame()
    for singleGroup, grp in inDF.groupby('modelFile'):
        for singleModelFile in singleGroup.split('/'):
            trimmedModelFile = singleModelFile.strip(' ')
            thisGrp = grp.assign(modelFile=trimmedModelFile)
            retDF = pd.concat([retDF, thisGrp])
    return retDF

def mergeAll(baseDF, allDF):
    retDF = baseDF
    for singleModelFile, modelGrp in baseDF.groupby('modelFile'):
        thisGrp = allDF.assign(modelFile=singleModelFile)
        retDF = pd.concat([retDF, thisGrp])
    return retDF

def readDataDictionary(config):
    ddFilename = au.expandFilename(config['modelTemplate'], {**config, 'modelID': 'DataDictionary.xlsx'})
    ddDF = pd.read_excel(ddFilename)
    ddDF['ddUnits'].replace({np.nan: ''}, inplace=True)
    allMask = ddDF['modelFile'] == 'All'
    filteredDF = ddDF[~ddDF['modelFile'].isnull() & ~allMask]
    allDF = ddDF[allMask]
    multiMask = filteredDF['modelFile'].str.contains('/')
    baseDF = filteredDF[~multiMask]
    multiDF = expandMultiDF(filteredDF[multiMask])
    retDF = mergeAll(pd.concat([baseDF, multiDF]), allDF)
    retDF = retDF.assign(modelParameter=retDF['modelParameter'].str.strip())
    
    return retDF

def mergeDataDictionary(modelDF, ddDF):
    mergedDF = modelDF.merge(ddDF,
                             left_on=['file', 'Model Parameter'],
                             right_on=['modelFile', 'modelParameter'],
                             how='left',
                             indicator=True)
    return mergedDF

EMPTYSERIES = pd.Series()
def lookupDDData(modelFile, singleParam, ddDF):
    pName = singleParam['Model Parameter']
    ddData = ddDF[(ddDF['modelFile'] == modelFile) &
                  (ddDF['modelParameter'] == pName)
                  ]
    if len(ddData) == 0:
        logger.warning(f"  Model Parameter {pName} not defined in data dictionary")
        return EMPTYSERIES
    if len(ddData) > 1:
        logger.warning(f"  Model Parameter {pName} defined multiple times in data dictionary")
        return ddData.iloc[0].squeeze()
    ddSeries = ddData.squeeze()
    return ddSeries

def outputParameterDesc(oFile, modelFile, singleParam, ddDF, indent=''):
    pName = singleParam.get('Model Parameter', None)
    if not pName:
        return
    ddSeries = lookupDDData(modelFile, singleParam, ddDF)
    if ddSeries.empty:
        return

    paramData = {**singleParam, **ddSeries.to_dict(), 'indent': indent}
    memberStr = SPHINX_MEMBER_TMPL.format(**paramData)
    oFile.write(memberStr)
    if paramData['ddUnits']:
        unitsStr = SPHINX_UNITS_TMPL.format(**paramData)
        oFile.write(unitsStr)

def outputStateDescriptions(oFile, modelData):
    if ("States" not in modelData) and ("StateMachineDiagram" not in modelData):
        return

    oFile.write(SPHINX_STATE_HDR_TMPL)

    if "States" in modelData:
        for singleState in modelData['States']:
            stateStr = SPHINX_STATE_DESC_TMPL.format(**singleState)
            oFile.write(stateStr)
    if "StateMachineDiagram" in modelData:
        smStr = SPHINX_STATE_MACHINE_IMAGE_TMPL.format(**modelData)
        oFile.write(smStr)

def outputFluidFlows(oFile, modelData):
    if "Fluid Flows" not in modelData:
        return

    oFile.write(SPHINX_FLUID_FLOW_HDR_TMPL)

    for singleState in modelData['Fluid Flows']:
        stateStr = SPHINX_FLUID_FLOW_DESC_TMPL.format(**singleState)
        oFile.write(stateStr)

        if ('secondary_id' in singleState) & bool(singleState['secondary_id']):
            secondaryStr = SPHINX_FLUID_FLOW_SECONDARY_ID_TMPL.format(**singleState)
            oFile.write(secondaryStr)

def outputSphinxDoc(config, modelParams, ddDF):
    for modelFile, modelData in modelParams.items():
        logger.info(f"Processing {modelFile}")
        className = modelFile.strip(".json")
        outputFilename = au.expandFilename(config['sphinxModelTmpl'], {**config, 'baseFile': className})
        with open(outputFilename, "w") as oFile:
            # Prelude
            eqLines = "=" * len(modelData['Readable Name'])
            tmplDict = {'className': className, 'eqLines': eqLines,
                        'filename': modelFile, 'description': modelData.get('Description', ''),
                        **modelData
                        }
            prelude = SPHINX_HEADER_TMPL.format(**tmplDict)
            oFile.write(prelude)

            # State & Fluid Flow info
            outputStateDescriptions(oFile, modelData)
            outputFluidFlows(oFile, modelData)

            # Model Parameters
            oFile.write(SPHINX_SITE_DEF_TMPL)
            for singleParam in modelData['Model Parameters']:
                outputParameterDesc(oFile, modelFile, singleParam, ddDF, indent='')

            # Emitters

            emitters = modelData.get('Emitters', [])
            if emitters:
                oFile.write(SPHINX_EMITTER_HEADER_TMPL)

            for singleEmitter in emitters:
                emitterStr = SPHINX_EMITTER_TMPL.format(**singleEmitter)
                oFile.write(emitterStr)
                for singleParam in singleEmitter['Model Parameters']:
                    outputParameterDesc(oFile, modelFile, singleParam, ddDF, indent='    ')

            # Reference document

            refDoc = modelData.get('Reference', False)

            if refDoc:
                if not isinstance(refDoc, list):
                    refDoc = [refDoc]
                for singleRefDoc in refDoc:
                    oFile.write(SPHINX_REFERENCE_TMPL.format(filename=singleRefDoc))

    # Index File

    idxFilename = au.expandFilename(config['sphinxModelTmpl'], {**config, 'baseFile': 'index'})
    with open(idxFilename, "w") as oFile:
        oFile.write(SPHINX_IDX_HEADER_TMPL)
        for singleModel, modelData in sorted(modelParams.items(), key=lambda x: x[1].get('Readable Name', '')):
            singleClass = singleModel.strip(".json")
            classLine = SPHINX_IDX_CLASS_TMPL.format(**{**modelData, 'className': singleClass})
            oFile.write(classLine)

def printSummary(outDF):
    noDDEntries = outDF[outDF['_merge'] == 'left_only']
    noDDEntries[['Model Parameter', 'name', 'file', 'majorEquipmentPythonClass', 'pythonClass']].to_csv('output/noDDEntries.csv')

def main(config):
    logging.basicConfig(level=logging.INFO)
    modelParams = readModelParameters(config)
    dataDictionaryDF = readDataDictionary(config)
    summaryDF = outputSphinxDoc(config, modelParams, dataDictionaryDF)

    # printSummary(summaryDF)

if __name__ == '__main__':
    _config, _ = au.getConfig()
    main(_config)
