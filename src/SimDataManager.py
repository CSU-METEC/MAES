from abc import ABC, abstractmethod
from datetime import datetime
import AppUtils as au
import EquipmentTable as eqt
from EventLogger import StreamingEventLogger
from pathlib import Path
import EmissionDriver as ed
import MEETExceptions as me
import logging
import GasComposition3 as gc
import json
import pandas as pd
import MEETInstantaneousEmission
from Timer import Timer
import shutil
import TimeseriesTable as ts
import numpy as np
import MEETFluidFlow as ff
import os

def guaranteeColumn(df, colName, colSet):
    if colName not in colSet:
        df[colName] = None

class SimDataManager():
    SIM_DATA_MANAGER_SINGLETON = None

    @classmethod
    def getSimDataManager(cls):
        return cls.SIM_DATA_MANAGER_SINGLETON

    def __enter__(self):
        SimDataManager.SIM_DATA_MANAGER_SINGLETON = self
        return self

    def __exit__(self, *args):
        for singleCache in self.caches:
            singleCache.resetCache()
        SimDataManager.SIM_DATA_MANAGER_SINGLETON = None

    def registerCache(self, cache):
        self.caches.add(cache)

    def __init__(self, config):
        self.config = config
        self.equipmentTable = eqt.JsonEquipmentTable()
        if not self.config.get('postAction', None):
            #au.clearDirectory(self.simDir)
            au.ensureDirectory(self.config['templateDir'], takeParent=False)

        self.emissionDriverTable = {}
        self.emissionDriversByName = {}
        self.activityProfiles = {}
        self.activityProfilesByName = {}
        self.gasCompositionsByName = {}
        self.stateDataframe = {}

        self.timeseriesTable = ts.TSTable()
        self.gasCompositionTable = gc.GCTable(self)
        self.ffTable = ff.FFTable()

        self.eventLogger = None
        self.serialNumber = 0
        self.serializer = JsonSDMSerializer(config=self.config)

        self.caches = set()

    @classmethod
    def initStubSimDataManager(cls):
        FAKE_CONFIG = {
            "scenarioTimestampFormat": "%Y%m%d_%H%M%S",
            "scenarioTimestamp": datetime.now(),
            "outputDir": "output",
            "MCSimDir": "{outputDir}",
            "MCTemplateDir": "{outputDir}/template",
            # there wasn't templateDir or simDurationSeconds
            "templateDir": "{simulationRoot}/template",
            "simDurationSeconds": 4,
        }
        fakeSDM = SimDataManager(FAKE_CONFIG)
        SimDataManager.SIM_DATA_MANAGER_SINGLETON = fakeSDM
        return fakeSDM

    def gcInternAction(self, gasComp):
        pass

    def _newSerialNumber(self):
        serNo = self.serialNumber
        self.serialNumber += 1
        return serNo

    def getEquipmentTable(self):
        return self.equipmentTable

    def _readModelFormulation(self, filename):
        try:
            with open(filename, "r") as iFile:
                modelFormulation = json.load(iFile)
            return modelFormulation
        except Exception as e:
            logging.exception(f"JSON error in file: {filename}")
            raise e

    def _getModelFormulationMetadata(self):
        config = self.config
        inputDir = au.expandFilename(config['modelTemplate'], {**config, 'modelID': ''}, readonly=True)
        inputPath = Path(inputDir)
        metadata = []
        for filename in inputPath.glob("*.json"):
            singleElement = {'modelID': filename.name}
            modelFormulation = self._readModelFormulation(filename)
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

    def initMCRun(self, mcRunNum=None):
        self.instantaneousEventsPath = self.config['eventFilename']
        self.eventLogger = StreamingEventLogger(self.config)


        self.mcRunNum = mcRunNum
        # self.gasCompositionsByName = {}

        # copy input spreadsheets to MC dir for reference
        logging.info(f"Copying study files to {self.config['resultsRoot']}")
        MCDirPath = Path(self.config['resultsRoot'])
        studyPath = Path(self.config['studyFilename'])
        outPath = MCDirPath / studyPath.name
        au.ensureDirectory(outPath)
        shutil.copy(studyPath, outPath)

        # dump the config variables to the MC dir for reference
        configDF = pd.DataFrame.from_dict(self.config, orient='index', columns=['value'])
        configDF.to_csv(self.config['configFilename'], index_label='key')
        pass

    def delMCRun(self,mcRunNum):
        self.gasCompositionTable.exitGCTable()
        self.timeseriesTable. exitTSTable()

    def addGasComposition(self, name=None, speciesConversionDict=None):
        pass

    # todo: what is this doing?  How is that different than the implementation in GasComposition3?
    def getGasComposition(self, name=None):
        if name in self.gasCompositionsByName:
            key = self.gasCompositionsByName[name]
            gComp = self.gasCompositionTable[key]
            return key, gComp
        databasePath = Path(au.expandFilename(self.config['emitterProfileDir'], self.config, readonly=True))
        if name is None:
            gComp = None
        else:
            gcPath = databasePath / name
            gComp = gc.GasComposition(gcPath)
        key = self._newSerialNumber()
        self.gasCompositionTable[key] = gComp
        self.gasCompositionsByName[name] = key
        return key, gComp

    def getGasCompositionFF(self, key, gcName=None):
        if key in self.gasCompositionsByName:
            key = self.gasCompositionsByName[key]
            gComp = self.gasCompositionTable[key]
            return key, gComp
        gComp = None
        return key, gComp

    def getGasCompositionByKey(self, key):
        if key not in self.gasCompositionTable:
            msg = f"Unknown Gas Composition {key}"
            logging.warning(msg)
            raise me.UnknownElementError(msg)
        return self.gasCompositionTable[key]

    def addDriver4EC(self, name=None, driver=None):
        if name in self.emissionDriversByName:
            return self.emissionDriversByName[name]
        key = self._newSerialNumber()
        self.emissionDriverTable[key] = driver
        self.emissionDriversByName[name] = key
        return key

    def getActivityProfile(self, name=None):
        if name in self.activityProfilesByName:
            return self.activityProfilesByName[name]
        dataBasePath = Path(au.expandFilename(self.config['emitterProfileDir'], self.config, readonly=True))
        dataPath = dataBasePath / name
        driver = ed.ActivityProfile(name, dataPath)
        key = self._newSerialNumber()
        self.activityProfiles[key] = driver
        self.activityProfilesByName[name] = key
        return key

    def activityProfilePick(self, key):
        activityProfile = self.activityProfiles[key]
        return int(activityProfile.pick())

    def getDriver4EC(self, name=None):
        if name in self.emissionDriversByName:
            return self.emissionDriversByName[name]
        dataBasePath = Path(au.expandFilename(self.config['emitterProfileDir'], self.config, readonly=True))
        dataPath = dataBasePath / name
        driver = ed.EmissionDriver(name, dataPath)
        key = self._newSerialNumber()
        self.emissionDriverTable[key] = driver
        self.emissionDriversByName[name] = key
        return key

    def getTimeseries(self, key):
        if key not in self.emissionDriverTable:
            msg = f"Unknown EmissionDriver key {key}"
            logging.warning(msg)
            raise me.UnknownElementError(msg)
        ed = self.emissionDriverTable[key]
        ts = ed.pick()
        return self.addTimeseries(ts=ts)

    def scaleTimeseries(self, tsKey, scaleFactor):
        if tsKey not in self.timeseriesTable:
            msg = f"Unknown Timeseries key {tsKey}"
            logging.warning(msg)
            raise me.UnknownElementError(msg)
        oldTS = self.timeseriesTable[tsKey]
        newTS = oldTS.scale(scaleFactor, 's')
        newKey = self._newSerialNumber()
        self.timeseriesTable[newKey] = newTS
        return newKey, newTS

    def getTimeseriesByKey(self, tsKey):
        if tsKey not in self.timeseriesTable:
            msg = f"Unknown timeseries key {tsKey}"
            logging.warning(msg)
            raise me.UnknownElementError(msg)

        return self.timeseriesTable[tsKey]

    def eventLogWriteCache(self):
        return self.eventLogger

    def getEventLog(self):
        return self.eventLogger

    def dumpEventLog(self, mcRunNum):
        eventLogFilename = self.config['eventFilename']
        self.eventLogger.dump(eventLogFilename)

    def restoreEventLog(self, mcRunNum):
        eventLogFilename = self.config['eventFilename']
        self.eventLogger.restore(eventLogFilename)
        pass

    def dumpSummary(self, mcRunNum):
        self.summaryManager.summarizeSingleMCRun(mcRunNum, self)

    def dumpEvents(self, mcRunNum):
        self._dumpEvents(self.instantaneousEventsPath)


    FIXED_EVENT_COLUMNS = [
        'eventID', 'facilityID', 'unitID', 'emitterID', 'mcRun',
        'timestamp', 'command', 'state', 'event', 'duration', 'nextTS',
        'gcKey', 'flowID', 'tsKey',
    ]
    EMISSION_COLS_TO_DUMP = ['value', 'gcUnits', 'tsUnits']

    def _dumpEvents(self, iEventPath):
        with Timer("  Dump event log") as t1:
            self.eventLogger.dump('')

    def _dumpEmissions(self, iEmissionPath, gcPath, eventDF):
        emissionDF = eventDF[eventDF['command'] == 'EMISSION']
        if emissionDF.empty:
            return
        emissionDF = emissionDF.reset_index()
        with Timer("  Calculate emissions by species") as t1:
            # r1 = emissionDF.apply(lambda x: self.evalInstantaneousEmission(x, [x['timestamp']]), axis='columns')
            # r1 = pd.DataFrame(list(emissionDF.apply(lambda x: self.calcIntegratedTS(x), axis='columns')),
            #                   index=emissionDF.index)
            r1 = pd.DataFrame(list(emissionDF.apply(self.calcIntegratedTS, axis='columns')), index=emissionDF.index)
            t1.setCount(len(emissionDF))

        gcInds = set(r1[['units', 'gcFingerprint']].itertuples(index=False))
        gcConversions = pd.DataFrame(map(self.toGCConversion, gcInds))

        emissionSpeciesDF = r1.merge(gcConversions, left_on=['units', 'gcFingerprint'],
                                     right_on=['units', 'gcFingerprint'])
        emissionSpecies = self.flashComp.gasSpeciesNames()
        for singleSpecies in emissionSpecies:
            emissionSpeciesDF[singleSpecies] = emissionSpeciesDF['value'] * emissionSpeciesDF[singleSpecies]
        emissionSpeciesDF.drop(['timestamp', 'duration', 'gcFingerprint'], axis='columns', inplace=True)

        emissionDF = emissionDF.merge(emissionSpeciesDF, left_index=True, right_index=True)
        eColsToDump = self.FIXED_EVENT_COLUMNS + self.EMISSION_COLS_TO_DUMP + emissionSpecies
        GC_DUMP_ORDER = [
            'gcFingerprint', 'units', 'MajorEquipment', 'FluidFlow', 'GCUnits',
            'CARBON_DIOXIDE', 'NITROGEN', 'HYDROGEN_SULFIDE', 'METHANE', 'ETHANE', 'PROPANE', 'ISOBUTANE',
            'BUTANE', 'ISOPENTANE', 'PENTANE', 'HEXANE', 'Pseudocomponent 1', 'Pseudocomponent 2'
        ]
        with Timer("  Write gas composition") as t1:
            gcConversions[GC_DUMP_ORDER].to_csv(gcPath, index=False)
        with Timer("  Write emission csv") as t1:
            emissionDF[eColsToDump].to_csv(iEmissionPath, index=False)
            t1.setCount(len(emissionDF))

    def dumpInstantaneousValues(self, iEventPath, iEmissionPath, gcPath):
        with Timer("Dump instantaneous values") as t0:
            eventDF = self._dumpEvents(iEventPath)
            # todo: remove dead code
            # self._dumpEmissions(iEmissionPath, gcPath, eventDF)
        pass

    def toGCConversion(self, gcConv):
        conv = self.rawGasCompositionTable[self.rawGasCompositionTable['gcKey'] == gcConv.gcKey]
        cUnits = conv["gcUnits"].iloc[0]
        conv["gcValue"] = conv["gcValue"]*gcConv.tsValue
        conv = conv.set_index('species').T
        conv = conv.loc['gcValue']
        # conv = self.flashComp._findConversion(gcConv.gcFingerprint, gcConv.units, 'kg', 'GCUnits').iloc[0].to_dict()
        ret = {**conv, 'gcKey': gcConv.gcKey, 'gcUnits': cUnits}
        return ret

    def calcIntegratedTS(self, eventRecord):
        tsKey = eventRecord['tsKey']
        ts = eventRecord['timestamp']
        duration = eventRecord['duration']
        timeseries = self.rawEmissionTimeseries[self.rawEmissionTimeseries['tsKey'] == eventRecord['tsKey']].to_dict('records')[0]
        tsVal = timeseries["tsValue"]
        tsUnits = timeseries["tsUnits"]
        if tsUnits == 'scf_wholegas':
            tsUnits = 'scf'
        ret = {'timestamp': ts, 'duration': duration, 'tsValue': tsVal, 'tsUnits': tsUnits,
               'gcKey': eventRecord['gcKey']}
        return ret

    def dumpTemplates(self):
        self.serializer.dumpEquipmentTable(self, mcRunNum=-1)
        # get the Metadata index
        mdIndexDF = self._getModelFormulationMetadata()
        mdIndexFile = self.config['mdIndexFile']
        mdIndexDF.to_csv(mdIndexFile, index=False)

    def restoreTemplates(self):
        self.serializer.restoreEquipmentTable(self, mcRunNum=-1)

    def dumpInstantiatedScenario(self, mcRunNum=None):
        self.serializer.dumpEquipmentTable(self, mcRunNum=mcRunNum)

    def restoreInstantiatedScenario(self, mcRunNum=None):
        self.serializer.restoreEquipmentTable(self, mcRunNum)

    def dumpDESResults(self, mcRunNum):
        with Timer("all dumps") as t0:
            self.serializer.dumpDriver4EC(self, mcRunNum)
            self.serializer.dumpTimeseries(self, mcRunNum)
            self.serializer.dumpGC(self, mcRunNum)
            self.serializer.dumpFF(self, mcRunNum)
            # self.summaryManager.summarizeSingleMCRun(mcRunNum, self)
            self.dumpEvents(mcRunNum)

    def restore(self, mcRunNum=None):
        # keyed off self.mcRunNum, so self.initMCRun must be called before this function
        self.serializer.restoreEquipmentTable(self, mcRunNum)
        self.serializer.restoreDriver4EC(self, mcRunNum)
        self.serializer.restoreTimeseries(self, mcRunNum)
        self.serializer.restoreGasCompositions(self, mcRunNum)
        self.serializer.restoreEventLog(self, mcRunNum)

    def createSummaries(self):
        self.summaryManager.dumpFullSummary()
        pass

class SDMSerializer(ABC):

    def __init__(self, config=None, **kwargs):
        super().__init__(**kwargs)
        self.config = config

    @classmethod
    def serializerFactory(cls, config):
        # note import here -- I'm doing this to minimize the effect of the circular import dependencey
        # between this module & SQLSimDataManager
        import SQLSimDataManager as SQLsdm
        serializerType = config.get('serializer', 'JSON')
        if serializerType == 'JSON':
            return JsonSDMSerializer(config=config)
        elif serializerType == 'SQL':
            return SQLsdm.SQLSDMSerializer(config=config)
        else:
            raise NotImplementedError

    @abstractmethod
    def dumpEquipmentTable(self, sdm, mcRunNum=-1):
        raise NotImplementedError

    @abstractmethod
    def restoreEquipmentTable(self, sdm, mcRunNum=-1):
        raise NotImplementedError

    @abstractmethod
    def dumpDriver4EC(self, sdm, mcRunNum=-1):
        raise NotImplementedError

    @abstractmethod
    def restoreDriver4EC(self, sdm, mcRunNum=-1):
        raise NotImplementedError

    @abstractmethod
    def dumpTimeseries(self, sdm, mcRunNum=-1):
        raise NotImplementedError

    @abstractmethod
    def restoreTimeseries(self, sdm, mcRunNum=-1):
        raise NotImplementedError

    @abstractmethod
    def restoreGasCompositions(self, sdm, mcRunNum=-1):
        raise NotImplementedError

    @abstractmethod
    def dumpEventLog(self, sdm, mcRunNum=-1):
        raise NotImplementedError

    def restoreEventLog(self, sdm, mcRunNum=-1):
        raise NotImplementedError

class JsonSDMSerializer(SDMSerializer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.MCTemplateDir = self.config['templateDir']
        pass

    def dumpEquipmentTable(self, sdm, mcRunNum=-1):
        if mcRunNum == -1:
            outDir = self.MCTemplateDir
        else:
            outDir = self.config['resultsRoot']

        mdToDump, eqToDump = sdm.equipmentTable.tablesForMCRun(mcRunNum)
        baseDirPath = Path(outDir)
        mdPath = baseDirPath / "metadata.csv"
        mdToDump.to_csv(mdPath, index=False)
        eqPath = baseDirPath / "equipment.json"
        with open(eqPath, "w") as oFile:
            for singleEq in eqToDump:
                eqJson = singleEq.toJsonable()
                try:
                    json.dump(eqJson, oFile)
                except Exception as e:
                    logging.warning(f"Got exception dumping equipment table entry {eqJson}, e: {e}")
                oFile.write("\n")

    def restoreEquipmentTable(self, sdm, mcRunNum=-1):
        mdPath = self.config['metadataFile']
        mdToRestore = pd.read_csv(mdPath).replace({np.nan: None})
        eqPath = self.config['equipmentFile']
        eqTableToRestore = {}
        with open(eqPath, "r") as iFile:
            for singleEq in iFile:
                eqDict = json.loads(singleEq)
                eqDict['key'] = tuple(eqDict['key'])
                eq = eqt.fromJson(eqDict)
                eqTableToRestore[eq.key] = eq
        sdm.equipmentTable = eqt.JsonEquipmentTable(mdToRestore, eqTableToRestore)
        pass

    def dumpTemplates(self):
        self.equipmentTable.dump(self.scenarioTemplateDir, mcRunNum=-1)

    def restoreTemplates(self, mcRunNum=None):
        self.equipmentTable.restore(self.scenarioTemplateDir, mcRunNum=mcRunNum)

    def dumpInstantiatedScenario(self, mcRunNum=None):
        self.equipmentTable.dump(self.mcScenarioDir, mcRunNum=mcRunNum)

    def restoreInstantiatedScenario(self, mcRunNum=None):
        self.equipmentTable.restore(self.mcScenarioDir)

    def dumpDriver4EC(self, sdm, mcRunNum=None):
        emDriverFile = self.config['emissionDriverFilename']
        with open(emDriverFile, "w") as oFile:
            for key, singleEmissionDriver in sdm.emissionDriverTable.items():
                mdToDump = {'key': key, **singleEmissionDriver.md}
                json.dump(mdToDump, oFile)
                oFile.write("\n")

    def restoreDriver4EC(self, sdm, mcRunNum=None):
        emTSFile = self.config['tsFilename']
        sdm.rawEmissionTimeseries = pd.read_csv(emTSFile)
        pass

    def restoreGasCompositions(self, sdm, mcRunNum=None):
        gcFilename = au.expandFilename(self.config['gcTemplate'], {**self.config, 'MCScenario': mcRunNum})
        sdm.rawGasCompositionTable = pd.read_csv(gcFilename)

    def dumpTimeseries(self, sdm, mcRunNum=None):
        tsFilename = self.config['tsFilename']
        with open(tsFilename, "w", newline='') as oFile:
            sdm.timeseriesTable.serialize(oFile, mcRunNum=mcRunNum)

    def restoreTimeseries(self, sdm, mcRunNum=None):
        tsFilename = self.config['tsFilename']
        # todo: this is not the right thing to do -- this is not the correct format for the timeseriesTable
        sdm.rawTimeseriesTable = pd.read_csv(tsFilename)

    def dumpGC(self, sdm, mcRunNum=None):
        gcFilename = self.config['gasCompositionLogFilename']
        with open(gcFilename, "w", newline='') as oFile:
            sdm.gasCompositionTable.serialize(oFile, mcRunNum=mcRunNum)

    def dumpFF(self, sdm, mcRunNum=None):
        ffFilename = self.config['ffFilename']
        with open(ffFilename, "w", newline='') as oFile:
            sdm.ffTable.serialize(oFile)

    def dumpEventLog(self, sdm, mcRunNum):
        eventLogFilename = self.config['eventFilename']
        sdm.eventLogger.dump(eventLogFilename)

    def restoreEventLog(self, sdm, mcRunNum):
        eventLogFilename = self.config['eventFilename']
        sdm.rawEventLog = pd.read_csv(eventLogFilename)

    def dumpSummary(self, mcRunNum):
        self.summaryManager.summary(self.summaryPath,mcRunNum, self)


