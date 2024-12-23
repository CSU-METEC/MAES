import argparse
import os
import AppUtils as au
import ModelFormulation as mf
from datetime import datetime
import json
import logging
import MEETExceptions as me
import pandas as pd


def tableCleanup(param):
    newList = []
    for line in param:
        for item in line:
            if item not in newList:
                newList.append(item)
    return newList


def getTable(rawIntake, classMap):
    table = {'ClassesUsed': {}, 'unusedClasses': []}
    for json in rawIntake:
        table["ClassesUsed"][json] = {'InJson': [], 'SuperClasses': []}
        for classes in rawIntake[json]:
            if classes[1] not in table['ClassesUsed'][json]['InJson']:
                if classes[1] in classMap[classes[0]]:
                    table["ClassesUsed"][json]['InJson'].append(classes[1])
                    table["ClassesUsed"][json]['SuperClasses'].append(
                        getAllSuperclasses(classMap[classes[0]][classes[1]]['class']))
        table["ClassesUsed"][json]['SuperClasses'] = tableCleanup(table["ClassesUsed"][json]['SuperClasses'])
    for classtype in classMap:
        for classes in classMap[classtype]:
            if not findValue(classes, table["ClassesUsed"]):
                table["unusedClasses"].append(classes)
    # table["jsonFilesCalled"] = jsonCalled
    return table


def findValue(val, dict):
    exists = False
    for listable in dict:
        for line in dict[listable]:
            if val in dict[listable][line]:
                exists = True
    return exists


def toJson(table, config):
    with open(config.outputDir + datetime.now().strftime("%Y%m%d_%H%M%S") + "_MajorEquipmentMap.json", 'w') as fp:
        json.dump(table, fp)
    pass


def getModelFormulation(input):
    classesCalled = {}
    for filename in os.scandir(input):
        if filename.name[-5:] == '.json' and not os.stat(filename).st_size == 0:
            classesCalled[filename.name] = []
            modelFormulation = readModelFormulation(filename)
            classesCalled[filename.name].append((modelFormulation['Python Category'], modelFormulation['Python Class']))
            if 'Emitters' in modelFormulation:
                if modelFormulation.get('Python Category', None) != 'MajorEquipment':
                    msg = f"'Emitters' parameter only valid for MajorEquipment -- {modelFormulation}"
                    logging.warning(msg)
                    me.IllegalElementError(msg)
                    continue
                for singleEmitter in modelFormulation['Emitters']:
                    classesCalled[filename.name].append(
                        (singleEmitter['Python Category'], singleEmitter['Python Class']))
    return classesCalled

def getModelFormulationMetadata(input):
    metadata = []
    for filename in os.scandir(input):
        if filename.name[-5:] == '.json' and not os.stat(filename).st_size == 0:
            singleElement = {'modelID': filename.name}
            modelFormulation = readModelFormulation(filename)
            singleElement['implCategory'] = modelFormulation['Python Category']
            singleElement['implClass'] = modelFormulation['Python Class']
            singleElement['modelReadableName'] = modelFormulation['Readable Name']
            metadata.append(singleElement)
            if 'Emitters' in modelFormulation:
                if modelFormulation.get('Python Category', None) != 'MajorEquipment':
                    msg = f"'Emitters' parameter only valid for MajorEquipment -- {modelFormulation}"
                    logging.warning(msg)
                    me.IllegalElementError(msg)
                    continue
                for singleEmitter in modelFormulation['Emitters']:
                    singleEmitterMD = {
                        'modelID': filename.name,
                        'implCategory': singleEmitter['Python Category'],
                        'implClass': singleEmitter['Python Class'],
                        'modelReadableName': singleEmitter['Readable Name'],
                        'modelCategory': singleEmitter['Category'],
                        'modelEmissionCategory': singleEmitter['Emission Category']
                    }
                    metadata.append(singleEmitterMD)
    metadataDF = pd.DataFrame.from_records(metadata)
    return metadataDF


def readModelFormulation(modelFilename):
    try:
        with open(modelFilename, "r") as iFile:
            modelFormulation = json.load(iFile)
        return modelFormulation
    except Exception as e:
        logging.exception(f"JSON error in file: {modelFilename}")
        raise e


def getAllSuperclasses(cls):
    ret = []  # cls.__name__
    for singleSuperclass in cls.__mro__:
        if singleSuperclass is not cls:
            if singleSuperclass is not cls.__mro__[-1]:
                ret.append(singleSuperclass.__name__)
    return ret


def main(args):
    # config, _ = au.getConfig()
    # classesCalled = getModelFormulationMetadata(args.inputDir)
    # classMap = mf.createClassmap()
    # table = getTable(classesCalled, classMap)
    # toJson(table, args)

    modelMetadata = getModelFormulationMetadata(args.inputDir)
    pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("-i", "--inputDir", default="input/ModelFormulation/Sample",
                        help="ModelFormulationJson directory.  Input/ModelFormulation/Sample by default")
    parser.add_argument("-o", "--outputDir", default="output/", help="Output directory")
    args = parser.parse_args()
    main(args)
