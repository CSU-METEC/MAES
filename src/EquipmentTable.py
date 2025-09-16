import logging

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning) # todo: don't just turn off the warning -- fix this.

import pandas as pd
import json
from abc import ABC, abstractmethod
import queue as q
import numpy as np
from pathlib import Path
import MEETGlobals as mg
import math
import Distribution as d
import MEETExceptions as me
import SimDataManager as sdm

class MEETTemplate():
    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['instantiationSet', 'fluidFlow']

    def __init__(self,
                 valsForInstantiation=None,
                 instantiationVal=None,
                 **kwargs):
        try:
            super().__init__(**kwargs)
        except TypeError:
            msg = f"Class: {self.__class__}: unknown keyword(s): {kwargs}"
            logging.warning(msg)
            raise TypeError(msg)
        if valsForInstantiation is None:
            self.instantiationSet = None
        else:
            self.instantiationSet = set(map(lambda x: x.strip(), valsForInstantiation.split(',')))
        self.instantiationVal = instantiationVal

    def activityPick(self, _, mcRunNum=0):
        return (1, None, None)

    def initializeFluidFlow(self, simdm):
        pass

    def instantiateFromTemplate(self, simdm, **kwargs):
        if self.instantiationSet and self.instantiationVal not in self.instantiationSet:
            return None
        inst = self.__class__(**kwargs)
        inst.initializeFluidFlow(simdm)
        return inst

def cleanValue(x):
    if not isinstance(x, float):
        return x
    if math.isnan(x):
        return "nan"
    if math.isinf(x):
        return "inf"
    return x

def valuesClean(x):
    if not isinstance(x, str):
        return x
    if x == "nan":
        return float("nan")
    if x == "inf":
        return float("inf")
    return x

class EquipmentTableEntry(ABC):
    EQUIPMENT_TABLE_FIELDS = ['equipmentType', 'facilityID', 'unitID', 'emitterID',
                              'latitude', 'longitude',
                              'implCategory', 'implClass',
                              'modelID', 'modelCategory', 'modelSubcategory', 'modelEmissionCategory', 'modelReadableName',
                              'equipmentCount', 'operator', 'psno',
                              'mcRunNum'
                              ]

    def __init__(self,
                 simdm=None,
                 facilityID=None, unitID=None, emitterID=None,
                 latitude=None, longitude=None,
                 implCategory=None, implClass=None,
                 modelID=None, modelCategory=None, modelSubcategory=None,
                 modelReadableName=None,
                 modelEmissionCategory=None,
                 equipmentCount=None,
                 equipmentType=None,
                 mcRunNum=-1,
                 key=None,
                 operator=None,
                 psno=None,
                 **kwargs
                 ):
        super().__init__(**kwargs)
        if equipmentType is not None:
            self.equipmentType = equipmentType
        else:
            # self.equipmentType = modelCategory
            self.equipmentType = self.__class__.__name__
        self.facilityID = facilityID
        self.unitID = unitID
        self.latitude = latitude
        self.longitude = longitude

        self.emitterID = emitterID
        self.implCategory = implCategory
        self.implClass = implClass
        self.modelID = modelID
        self.modelCategory = modelCategory
        self.modelSubcategory = modelSubcategory
        self.modelReadableName = modelReadableName
        self.modelEmissionCategory = modelEmissionCategory
        self.equipmentCount = equipmentCount
        self.mcRunNum = mcRunNum
        self.operator = operator
        self.psno = psno

        simdm = sdm.SimDataManager.getSimDataManager()
        # simdm should be None only for testing
        if simdm:
            eqTable = simdm.getEquipmentTable()
            self.key = eqTable._instanceKey(self)

            # Make sure to update EQUIPMENT_TABLE_FIELDS if you add any additional members

            eqTable.addEquipment(self)

    def __eq__(self, other):
        if not isinstance(other, EquipmentTableEntry):
            return False
        for singleAttr in EquipmentTableEntry.EQUIPMENT_TABLE_FIELDS:
            if getattr(self, singleAttr) != getattr(other, singleAttr):
                return False

        return True

    def filteredClassDict(self):
        classEntry = getMEETClassTable().get(self.__class__.__name__, {})
        fieldsToExclude = classEntry.get('excludes', [])
        ret = dict(filter(lambda x: x[0] not in fieldsToExclude, self.__dict__.items()))
        return ret

    def toJsonable(self):
        ret = self.filteredClassDict()
        cleanedRet = dict(map(lambda x: (x[0], cleanValue(x[1])), ret.items()))
        return cleanedRet

    @classmethod
    def fromJson(cls, inJson, instClass=None):
        uncleanJson = dict(map(lambda x: (x[0], valuesClean(x[1])), inJson.items()))
        if instClass is None:
            instClass = cls
        ret = instClass(**inJson)
        return ret

def getAllSubclasses(cls):
    ret = [cls]
    for singleSubclass in cls.__subclasses__():
        ret.extend(getAllSubclasses(singleSubclass))
    return ret

def buildSubclassMap(cls=EquipmentTableEntry):
    subclasses = getAllSubclasses(cls)
    map = {}
    for singleSubclass in subclasses:
        excludesForClass = set()
        for singleSuperclass in singleSubclass.__mro__:
            if 'MEET_SERIALIZER_FIELDS_TO_EXCLUDE' in singleSuperclass.__dict__:
                excludesForClass.update(singleSuperclass.__dict__['MEET_SERIALIZER_FIELDS_TO_EXCLUDE'])
        map[singleSubclass.__name__] = {'class': singleSubclass, 'excludes': excludesForClass}
    return map

def getMEETClassTable():
    if mg.GLOBAL_MEET_CLASS_TABLE is not None:
        return mg.GLOBAL_MEET_CLASS_TABLE
    mg.GLOBAL_MEET_CLASS_TABLE = buildSubclassMap()
    return mg.GLOBAL_MEET_CLASS_TABLE

def fromJson(jsonDict):
    jsonClass = jsonDict.get('implClass', None)
    if not jsonClass:
        msg = f"json instance must define 'implClass' property: {jsonDict}"
        logging.error(msg)
        raise me.IllegalArgumentError(msg)
    meetClass = getMEETClassTable().get(jsonClass, None)
    if not meetClass:
        msg = f"no MEETInstanceTable subclass defined for: {jsonClass}"
        logging.error(msg)
        raise me.IllegalArgumentError(msg)
    ret = meetClass['class'](**jsonDict)
    return ret



class Facility(EquipmentTableEntry, MEETTemplate):

    def __init__(self,
                 facilityID=None,
                 mcRunNum=-1,
                 **kwargs):
        simdm = sdm.SimDataManager.getSimDataManager()
        if simdm:
            existingFacility = simdm.getEquipmentTable().elementLookup(facilityID=facilityID, mcRunNum=mcRunNum)
            if existingFacility:
                msg = f"Duplicate facility ID: {facilityID}"
                logging.warning(msg)
                raise me.ExistingElementError(msg)
        super().__init__(**{**kwargs, 'facilityID': facilityID, 'mcRunNum': mcRunNum})

class MajorEquipment(EquipmentTableEntry, MEETTemplate):

    def __init__(self, facilityID=None, unitID=None, latitude=None, longitude=None, **kwargs):
        simdm = sdm.SimDataManager.getSimDataManager()
        facility = simdm.getEquipmentTable().elementLookup(facilityID)
        if not facility:
            msg = f"Unknown facility {facilityID}"
            logging.warning(msg)
            raise me.UnknownElementError(msg)
        lat = latitude if latitude else facility.latitude
        lon = longitude if longitude else facility.longitude

        #
        # override any incoming keyword arguments with stuff that we derive locally
        #
        newKwargs = {**kwargs,
                     'facilityID': facilityID,
                     'unitID': unitID,
                     'latitude': lat,
                     'longitude': lon,
                     }

        super().__init__(**newKwargs)

    @classmethod
    def fromJson(cls, inJson, instClass=None):
        if instClass is None:
            instClass = cls
        facility = EquipmentTableObsolete.getEquipmentTable().getEquipmentByName(inJson['facilityName'])
        ret = instClass(**inJson)
        return ret

class Emitter(EquipmentTableEntry, MEETTemplate):
    MEET_SERIALIZER_FIELDS_TO_EXCLUDE = ['serialNumberGenerator']

    DEFAULT_INSTANCE_FORMAT = "{unitID}_{serialNo}"

    def __init__(self, facilityID=None, unitID=None, latitude=None, longitude=None, emitterID=None, emitterProfile=None,
                 instanceFormat=None,
                 facility=None, mcRunNum=-1, # keep this here, even though it's not used, to gobble it out of kwargs
                 **kwargs):
        simdm = sdm.SimDataManager.getSimDataManager()
        majorEquipment = simdm.getEquipmentTable().elementLookup(facilityID, unitID)
        # self.majorEquipment = majorEquipment
        if majorEquipment is None:
            msg = f"Unknown facility/unit: {facilityID}/{unitID}"
            logging.warning(msg)
            raise me.UnknownElementError(msg)
        lat = latitude if latitude else majorEquipment.latitude
        lon = longitude if longitude else majorEquipment.longitude
        self.instanceFormat = instanceFormat or Emitter.DEFAULT_INSTANCE_FORMAT
        self.serialNumberGenerator = UniqueSerialNumberGenerator(mcRunNum)

        if emitterID is None:
            emitterID = self.instanceFormat.format(
                facilityID=facilityID,
                unitID=unitID,
                latitude=latitude,
                longitude=longitude,
                serialNo=self.serialNumberGenerator.getSerial(),
                mcRunNum=mcRunNum
            )

        #
        # override any incoming keyword arguments with stuff that we drive locally
        #
        newKwargs = {**kwargs,
                     'facilityID': facilityID,
                     'unitID': unitID,
                     'emitterID': emitterID,
                     'latitude': lat,
                     'longitude': lon,
                     'mcRunNum': mcRunNum
                     }

        super().__init__(**newKwargs)

    @classmethod
    def fromJson(cls, inJson, instClass=None):
        if instClass is None:
            instClass = cls
        majorEquipment = EquipmentTableObsolete.getEquipmentTable().getEquipmentByName(inJson['majorEquipmentID'])
        # ret = instClass(**{**inJson, 'majorEquipment': majorEquipment})
        ret = instClass(**inJson)
        return ret

class ActivityFactor(EquipmentTableEntry):
    pass

class MEETService(EquipmentTableEntry, MEETTemplate):

    LINK_SERNO = 1

    def __init__(self, **kwargs):
        if 'unitID' not in kwargs:
            uniqueUnitID = f"{self.__class__.__name__}_link_{MEETService.LINK_SERNO}"
            MEETService.LINK_SERNO += 1
            kwargsEx = {**kwargs, 'unitID': uniqueUnitID}
        else:
            kwargsEx = kwargs
        super().__init__(**kwargsEx)

def filterDict(d, fields):
    return dict(map(lambda x: (x, d.get(x, None)), fields))

def sanitizerStream(filename):
    with open(filename, "r") as iFile:
        for singleLine in iFile:
            eqJson = json.loads(singleLine, cls=d.JSONDecode)
            yield eqJson

class EquipmentTable(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def addEquipment(self, instance):
        raise NotImplementedError

class JsonEquipmentTable(EquipmentTable):

    def __init__(self, eqAttributes=None, eqMap=None):
        if eqAttributes is not None:
            self.equipmentAttributes = eqAttributes
        else:
            self.equipmentAttributes = pd.DataFrame(columns=EquipmentTableEntry.EQUIPMENT_TABLE_FIELDS)

        if eqMap is not None:
            self.equipmentMap = eqMap
        else:
            self.equipmentMap = {}  # we want this to be name -> EquipmentTableEntry

    class FakeInstance:
        def __init__(self, facilityID, unitID, emitterID, mcRunNum):
            self.facilityID = facilityID
            self.unitID = unitID
            self.emitterID = emitterID
            self.mcRunNum = mcRunNum

    def getMetadata(self):
        return self.equipmentAttributes

    def getTemplates(self):
        return self.getEquipment(mcRunNum=-1)

    def getEquipment(self, mcRunNum=None):
        tmpls = self.equipmentAttributes[self.equipmentAttributes['mcRunNum'] == mcRunNum]
        tmplKeys = tmpls.apply(self._instanceKey, axis='columns')
        ret = list(map(lambda x: self.equipmentMap[x], tmplKeys))
        return ret

    @classmethod
    def _instanceKey(cls, instance):
        ret = (instance.facilityID, instance.unitID, instance.emitterID, instance.mcRunNum)
        return ret

    @classmethod
    def _instanceDict(cls, key):
        ret = {
            'facilityID': key[0],
            'unitID': key[1],
            'emitterID': key[2],
            'mcRunNum': key[3]
        }
        return ret

    def addEquipment(self, instance):
        instDict = filterDict(instance.__dict__, EquipmentTableEntry.EQUIPMENT_TABLE_FIELDS)
        self.equipmentAttributes = pd.concat([self.equipmentAttributes, pd.DataFrame(instDict, index=[0])])
        key = self._instanceKey(instance)
        if key in self.equipmentMap:
            prevInstance = self.equipmentMap[key]
            msg = f"Duplicate equipment key: {key}, previous instance: {prevInstance}"
            logging.error(msg)
            raise me.IllegalElementError(msg)
        self.equipmentMap[key] = instance

    def elementLookup(self, facilityID, unitID=None, emitterID=None, mcRunNum=-1):
        key = self._instanceKey(JsonEquipmentTable.FakeInstance(facilityID, unitID, emitterID, mcRunNum))
        return self.equipmentMap.get(key, None)

    def tablesForMCRun(self, mcRunNum=None):
        if mcRunNum is None:
            mdToDump = self.equipmentAttributes
            equipmentToDump = self.equipmentMap
        else:
            mdToDump = self.equipmentAttributes[self.equipmentAttributes['mcRunNum'] == mcRunNum]
            equipmentToDump = self.getEquipment(mcRunNum)

        return mdToDump, equipmentToDump

class UniqueSerialNumberGenerator:
    SERIAL_NUMBER = 999999
    def __init__(self, mcRun):
        UniqueSerialNumberGenerator.SERIAL_NUMBER += 1
        self.num = f"{UniqueSerialNumberGenerator.SERIAL_NUMBER}_{mcRun}"

    def getSerial(self):
        return self.num
