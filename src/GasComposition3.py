from abc import ABC, abstractmethod
import json

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning) # todo: don't just turn off the warning -- fix this.

import pandas as pd
import DistributionProfile as dp
import logging
import MEETExceptions as me
import numpy as np
import SimDataManager as sdm
import SDMCache as sdmc
import AppUtils as au
from pathlib import Path
import Distribution as d

logger = logging.getLogger(__name__)

# Serialized GC tables will be stored as "long format" tables of the form:
#  gcKey, flowID, units, species, value, gcType, gcID
# where
#  gcKey    = internal key representation, used in EMISSION events, etc.
#  flow     = FluidFlow (Flash, Condensate, Water) -- corresponds to FluidFlow column in composition file
#  flowName = fluid flow name                      -- corresponds to Name column in composition file
#  species  = gas species
#  gcUnits  = units of the flow.  Will be stored as 'kg/<gcUnits>'
#  gcValue  = ratio of the gas species, in kg/<gcUnits>
#  gcType   = class name of the GC implementation (internal)
#  gcID     = aux. identifier of GC for implementation (internal)

GCTABLE_COLS = [
    'gcKey',
    'flow',
    'flowName',
    'species',
    'gcUnits',
    'gcValue',
    'gcType',
    'gcID',
    'origGC',
    'mcRun'
]

class GCTable():
    GC_TABLE_SINGLETON = None

    @classmethod
    def getGCTable(cls):
        if (sdm.SimDataManager.getSimDataManager() is None
                or sdm.SimDataManager.getSimDataManager().gasCompositionTable is None):
            raise me.IllegalElementError("No sim data manager gc table available")
        return sdm.SimDataManager.getSimDataManager().gasCompositionTable

    def __init__(self,
                 simdm=None
                 ):
        self.gcCache = {}
        self.GCbyKey = {}
        self.simdm = simdm

    def __getitem__(self, item):
        return self.GCbyKey[item]

    def intern(self, gasComp):
        gcKey = gasComp.serialNum
        if gcKey not in self.GCbyKey:
            self.GCbyKey[gcKey] = gasComp
            self.simdm.gcInternAction(gasComp)
        gc = self.GCbyKey[gcKey]
        return gc.serialNum

    def serialize(self, oStream, mcRunNum=None):
        gcDF = pd.DataFrame(columns=GCTABLE_COLS)
        for gcKey, singleGC in self.GCbyKey.items():
            singleGCDF = singleGC.serialForm().copy()
            if singleGCDF.empty:
                continue
            singleGCDF['gcKey'] = gcKey
            singleGCDF['mcRun'] = mcRunNum
            gcDF = pd.concat([gcDF, singleGCDF])
        gcDF[GCTABLE_COLS].to_csv(oStream, index=False)

    def deserialize(self, iStream):
        oldGCT = GCTable.getGCTable()
        GCTable.setGCTable(self)
        for singleLine in iStream:
            desGC = json.loads(singleLine)
            GasComposition.instantiate(desGC)
            pass
        return oldGCT

class GasComposition(ABC):

    GC_SERIAL_NUM = 1

    def __init__(self, _serialNum=None, gcClassName=None, **kwargs):
        gct = GCTable.getGCTable()
        cls = GasComposition
        self._serialNum = cls.GC_SERIAL_NUM
        cls.GC_SERIAL_NUM += 1
        self.gcClassName = self.__class__.__name__
        gct.intern(self)

    @property
    def serialNum(self):
        return self._serialNum

    @serialNum.setter
    def serialNum(self, sNum):
        self._serialNum = sNum

    @classmethod
    def instantiate(cls, serialForm):
        pass

    @abstractmethod
    # return a pandas dataframe of the standardized format for GC tables
    def serialForm(self):
        raise NotImplementedError

    @abstractmethod
    def getSpecies(self, units, **kwargs):
        raise NotImplementedError

class FluidFlowGC(GasComposition, sdmc.SDMCache):

    FLUID_FLOW_GC_CACHE = {}

    @classmethod
    def factory(cls,
                fluidFlowGCFilename=None,
                flow=None,
                fluidFlowID=None,
                gcUnits=None,
                **kwargs
                ):
        cls.registerCache()

        key = (fluidFlowGCFilename, flow, fluidFlowID, gcUnits)
        val = cls.FLUID_FLOW_GC_CACHE.get(key, None)
        if val:
            return val
        ff = FluidFlowGC(fluidFlowGCFilename=fluidFlowGCFilename,
                         flow=flow,
                         fluidFlowID=fluidFlowID,
                         gcUnits=gcUnits,
                         **kwargs)
        cls.FLUID_FLOW_GC_CACHE[key] = ff
        return ff

    @classmethod
    def resetCache(cls):
        cls.FLUID_FLOW_GC_CACHE = {}

    def __init__(self, fluidFlowGCFilename=None, flow=None, fluidFlowID=None, gcUnits=None):
        if gcUnits is None:
            msg = f"FluidFlowGC with empty gcUnits: {fluidFlowGCFilename}, {flow}, {fluidFlowID}"
            logger.error(msg)
            raise me.IllegalArgumentError(msg)
        if fluidFlowID is None:
            msg = f"FluidFlowGC with empty fluidFlowID: {fluidFlowGCFilename}, {flow}, {fluidFlowID}"
            logger.error(msg)
            raise me.IllegalArgumentError(msg)
        self.fluidFlowGCFilename = fluidFlowGCFilename
        self.gcMetadata = dp.FlashComposition.readFile(self.fluidFlowGCFilename).md
        self.flow = flow
        # todo: fluidFlowID of None == TBD
        self.fluidFlowID = fluidFlowID
        self.gcUnits = gcUnits
        self._fCompDF = None
        super().__init__()

    # define __hash__ and __eq__ so we can use GCs as keys in dictionaries -- see, for example, AggregatedFluidFlow

    def __hash__(self):
        ret = hash((self.fluidFlowGCFilename, self.flow, self.fluidFlowID, self.gcUnits))
        return ret

    def __eq__(self, other):
        if not isinstance(other, FluidFlowGC):
            return False
        ret = (
                (self.fluidFlowGCFilename == other.fluidFlowGCFilename)
                & (self.flow == other.flow)
                & (self.fluidFlowID == other.fluidFlowID)
                & (self.gcUnits == other.gcUnits)
        )
        return ret

    def derive(self, newFluidFlowID, flow=None):
        if self.fluidFlowID:
            newFFID = f'{self.fluidFlowID}.{newFluidFlowID}'
        else:
            newFFID = newFluidFlowID

        if flow == None:
            derivedFlow = self.flow
        else:
            derivedFlow = flow

        ret = FluidFlowGC.factory(fluidFlowGCFilename=self.fluidFlowGCFilename,
                                  flow=derivedFlow,
                                  fluidFlowID=newFFID,
                                  gcUnits=self.gcUnits)
        return ret

    def _getGCEntries(self):
        # _getGCEntries will return all the entries for a specific gas composition entry.  In particular,
        # it will return *all* GCUnits.  This is so the returned dataframe can be used for conversion as well
        # as writing the entry to the gas composition file
        if self._fCompDF is not None:
            return self._fCompDF

        fComp = dp.FlashComposition.readFile(self.fluidFlowGCFilename)
        fCompDF = fComp.flashComp
        ffID = self.flow
        fCompMask = (
                (fCompDF['FluidFlow'] == ffID)
                & (fCompDF['Name'] == self.fluidFlowID)
        )
        self._fCompDF = fCompDF[fCompMask]
        return self._fCompDF

    def serialForm(self):
        # todo: revamp the GC format so Flash & ProducedGas fluid flow IDs become 'Vapor'
        if self.fluidFlowID is None:
            return pd.DataFrame()

        fCompDF = self._getGCEntries() # checking for name & fluidFlowID is handled in _getGCEntries
        ffID = self.flow
        fcUnits = f"kg/{self.gcUnits}"
        fCompMask = fCompDF['GCUnits'] == fcUnits

        if fCompMask.sum() == 0:
            msg = f"Unknown GC -- Fluid: {ffID}, GCUnits: {fcUnits} name: {self.fluidFlowID} in {self.fluidFlowGCFilename}, gcSerialNum: {self.serialNum}"
            if ffID == 'Vapor':
                logger.error(msg)
                raise me.IllegalArgumentError(msg)
        dfCols = fCompDF.columns
        co2Idx = None
        for idx, val in enumerate(fCompDF.columns):
            if val == 'CARBON_DIOXIDE':
                co2Idx = idx
                break
        colsToKeep = dfCols[co2Idx:]
        meltedDF = pd.melt(fCompDF[fCompMask],
                           id_vars=['FluidFlow', 'Name', 'GCUnits'],
                           value_vars=colsToKeep,
                           var_name='species', value_name='gcValue')
        meltedDF.rename({'FluidFlow': 'flow', 'Name': 'flowName', 'GCUnits': 'gcUnits'},
                        axis='columns',
                        inplace=True)
        meltedDF['gcType'] = self.__class__.__name__
        meltedDF['gcID'] = self.fluidFlowGCFilename
        meltedDF['origGC'] = ''
        return meltedDF

    def getSpecies(self, units, fluidFlowID=None):
        pass

    def convert(self, newGCUnits):
        fCompDF = self._getGCEntries()
        conversionDFUnits = f"{newGCUnits}/{self.gcUnits}"
        converterMask = fCompDF['DriveFactorUnits'] == conversionDFUnits
        if converterMask.sum() == 0:
            msg = f"No conversion found in {self.flow} {self.fluidFlowID} for {conversionDFUnits} in {self.fluidFlowGCFilename}"
            logger.error(msg)
            raise me.IllegalArgumentError(msg)
        if converterMask.sum() > 1:
            msg = f"Multiple conversions found in {self.flow} {self.fluidFlowID} to {newGCUnits}"
            logger.error(msg)
            raise me.IllegalArgumentError(msg)
        converter = fCompDF[converterMask].squeeze()
        newGC = FluidFlowGC(fluidFlowGCFilename=self.fluidFlowGCFilename,
                            flow=self.flow,
                            fluidFlowID=self.fluidFlowID,
                            gcUnits=newGCUnits)
        newGC._fCompDF = fCompDF
        return newGC, converter

    def getDeltaH(self, stages):
        fCompDF = self._getGCEntries()
        stage = np.array(stages)
        stage = np.unique(stage)
        currentStage = stage[-1]
        # if currentStage in ['Tank', 'Well']:
        #     currentStage = stage[-2]
        #     if currentStage == 'Tank':
        #         currentStage = stage[-3]


        driveFactorUnits = 'scf/bbl'
        if 'Dehy' in self.fluidFlowID:
                driveFactorUnits = 'scf/scf'

        # if stage == 'Well':
        #     deltaH = float(fCompDF[fCompDF['DriveFactorUnits'] == driveFactorUnits][f'DeltaH - Stage 1 Pressure to Pipeline Pressure (kJ/scf)'])
        # else:
        deltaH = float(fCompDF[fCompDF['DriveFactorUnits'] == driveFactorUnits][f'DeltaH - Stage {currentStage[-1]} Pressure to Pipeline Pressure (kJ/scf)'])
        # deltaH = float(fCompDF[fCompDF['DriveFactorUnits'] == driveFactorUnits][f'DeltaH - Stage 1 Pressure to Pipeline Pressure (kJ/scf)'])
        i = 10
        return deltaH

    def getLhvVals(self):
        fCompDF = self._getGCEntries()
        lhv = fCompDF[fCompDF['DriveFactorUnits'] == 'scf/bbl']['LHV (kJ/scf)']
        # conversionDFUnits = f"{newGCUnits}/{self.gcUnits}"
        return float(lhv)

    def convertKgToScf(self, driverRate):
        i = 10
        fCompDF = self._getGCEntries()
        fCompDF = fCompDF[(fCompDF['GCUnits'] == 'kg/scf')]
        sumDensity = float(fCompDF[['CARBON_DIOXIDE', 'NITROGEN', 'HYDROGEN_SULFIDE', 'METHANE', 'ETHANE',
                                    'PROPANE', 'ISOBUTANE', 'BUTANE', 'ISOPENTANE', 'PENTANE', 'HEXANE',
                                    'Pseudocomponent 1', 'Pseudocomponent 2']].sum(axis=1))
        newDR = driverRate / sumDensity
        return newDR


def deriveGCName(gc, newFluidFlowID):
    if gc.fluidFlowID:
        return f'{gc.fluidFlowID}.{newFluidFlowID}'
    else:
        return newFluidFlowID

class CompositeFluidFlowGC(GasComposition, sdmc.SDMCache):

    FLUID_FLOW_GC_CACHE = {}

    @classmethod
    def factory(cls,
                fluidFlowGCFilename=None,
                flow=None,
                fluidFlowID=None,
                gcUnits=None,
                **kwargs
                ):
        cls.registerCache()

        key = (fluidFlowGCFilename, flow, fluidFlowID, gcUnits)
        val = cls.FLUID_FLOW_GC_CACHE.get(key, None)
        if val:
            return val
        ff = CompositeFluidFlowGC(fluidFlowGCFilename=fluidFlowGCFilename,
                                  flow=flow,
                                  fluidFlowID=fluidFlowID,
                                  gcUnits=gcUnits,
                                  **kwargs)
        cls.FLUID_FLOW_GC_CACHE[key] = ff
        return ff

    @classmethod
    def resetCache(cls):
        cls.FLUID_FLOW_GC_CACHE = {}

    def __init__(self, fluidFlowGCFilename=None, flow=None, fluidFlowID=None, gcUnits=None):
        if gcUnits is None:
            msg = f"FluidFlowGC with empty gcUnits: {fluidFlowGCFilename}, {flow}, {fluidFlowID}"
            logger.error(msg)
            raise me.IllegalArgumentError(msg)
        if fluidFlowID is None:
            msg = f"FluidFlowGC with empty fluidFlowID: {fluidFlowGCFilename}, {flow}, {fluidFlowID}"
            logger.error(msg)
            raise me.IllegalArgumentError(msg)
        self.fluidFlowGCFilename = fluidFlowGCFilename
        self.flow = flow
        # todo: fluidFlowID of None == TBD
        self.fluidFlowID = fluidFlowID
        self.gcUnits = gcUnits
        self.subGCs = []
        super().__init__()

    # define __hash__ and __eq__ so we can use GCs as keys in dictionaries -- see, for example, AggregatedFluidFlow

    def __hash__(self):
        ret = hash((self.fluidFlowGCFilename, self.flow, self.fluidFlowID, self.gcUnits))
        return ret

    def __eq__(self, other):
        if not isinstance(other, CompositeFluidFlowGC):
            return False
        ret = (
                (self.fluidFlowGCFilename == other.fluidFlowGCFilename)
                & (self.flow == other.flow)
                & (self.fluidFlowID == other.fluidFlowID)
                & (self.gcUnits == other.gcUnits)
        )
        return ret

    def derive(self, newFluidFlowID, flow=None):
        if self.fluidFlowID:
            newFFID = f'{self.fluidFlowID}.{newFluidFlowID}'
        else:
            newFFID = newFluidFlowID

        if flow == None:
            derivedFlow = self.flow
        else:
            derivedFlow = flow

        ret = CompositeFluidFlowGC.factory(fluidFlowGCFilename=self.fluidFlowGCFilename,
                                           flow=derivedFlow,
                                           fluidFlowID=newFFID,
                                           gcUnits=self.gcUnits)
        return ret

    def serialForm(self):
        # todo: revamp the GC format so Flash & ProducedGas fluid flow IDs become 'Vapor'
        if self.fluidFlowID is None:
            return pd.DataFrame()
        # return pd.DataFrame()
        fComp = dp.FlashComposition.readFile(self.fluidFlowGCFilename)
        fCompDF = fComp.flashComp

        fcUnits = f"kg/{self.gcUnits}"
        ffID = self.flow
        fCompMask = ((fCompDF['FluidFlow'] == ffID) &
                     (fCompDF['Name'] == self.fluidFlowID) &
                     (fCompDF['GCUnits'] == fcUnits)
                     )
        if fCompMask.sum() == 0:
            msg = f"Unknown GC -- Fluid: {ffID}, units: {fcUnits} name: {self.fluidFlowID} in {self.fluidFlowGCFilename}"
            if ffID == 'Vapor':
                logger.error(msg)
                raise me.IllegalArgumentError(msg)
        dfCols = fCompDF.columns
        co2Idx = None
        for idx, val in enumerate(fCompDF.columns):
            if val == 'CARBON_DIOXIDE':
                co2Idx = idx
                break
        colsToKeep = dfCols[co2Idx:]
        meltedDF = pd.melt(fCompDF[fCompMask],
                           id_vars=['FluidFlow', 'Name', 'GCUnits'],
                           value_vars=colsToKeep,
                           var_name='species', value_name='gcValue')
        meltedDF.rename({'FluidFlow': 'flow', 'Name': 'flowName', 'GCUnits': 'gcUnits'},
                        axis='columns',
                        inplace=True)
        meltedDF['gcType'] = self.__class__.__name__
        meltedDF['gcID'] = self.fluidFlowGCFilename
        meltedDF['origGC'] = ''
        return meltedDF

    def addGC(self, subGC):
        self.subGCs.append(subGC)

    def getSpecies(self, units, fluidFlowID=None):
        pass

class ManualGC(GasComposition):

    def __init__(self, units=None, fluidFlowID=None, speciesDict=None, **kwargs):
        self.units = units
        self.fluidFlowID = fluidFlowID
        self.speciesDict = speciesDict
        super().__init__(**kwargs)

    def getSpecies(self, units, **kwargs):
        raise NotImplementedError

    def derive(self, newFluidFlowID):
        newFFID = f'{self.fluidFlowID}.{newFluidFlowID}'
        ret = ManualGC(fluidFlowID=newFFID, speciesDict=self.speciesDict)
        return ret

    def serialForm(self):
        retDF = pd.DataFrame.from_dict(self.speciesDict, orient='index', columns=['value'])
        retDF['species'] = retDF.index
        retDF.reset_index(drop=True, inplace=True)
        retDF['units'] = self.units
        retDF['gcType'] = self.gcClassName
        retDF['flowID'] = self.fluidFlowID
        retDF['gcID'] = ''
        retDF['origGC'] = ''
        return retDF

# These values represent kg of CO2 released per kg of species combusted, assuming 100% combustion efficiency.

COMPLETE_COMBUSTION_MASS_CO2_RATIO = {
    'CARBON_DIOXIDE': 0.0,
    'METHANE': 2.743332215,
    'ETHANE': 2.927235334,
    'PROPANE': 2.994140808,
    'ISOBUTANE': 3.028753708,
    'BUTANE': 3.028753708,
    'ISOPENTANE': 3.049908257,
    'PENTANE': 3.049908257,
    'HEXANE': 3.064176231,
    'HEPTANE': 3.074449648,
    'OCTANE': 3.082200032
}

COMPLETE_COMBUSTION_MASS_CO2_RATIO_DF = pd.DataFrame.from_dict(COMPLETE_COMBUSTION_MASS_CO2_RATIO,
                                                               orient='index',
                                                               columns=['kgCO2perKgAlkane']
                                                               )
class DestructionGC(FluidFlowGC):

    @staticmethod
    def getDestEfficiencies(exhaustFactors):
        # todo: this method of accessing the destruction efficiency file is different than other file access methods.
        # todo: normalize them.
        simdm = sdm.SimDataManager.getSimDataManager()
        dataBasePath = Path(au.expandFilename(simdm.config['emitterProfileDir'], simdm.config, readonly=True)) / 'Common' / 'CompressorDestructionEfficiencies' / 'fake.csv'
        try:
            dataPath = dataBasePath.with_stem(exhaustFactors)
        except NotImplementedError:
            print("Please update to python version 3.9 and above")
        metadata, destEffDF = dp.readRawDistributionFile(dataPath)
        destEffDF = destEffDF.rename(columns={'Species': 'destSpecies', 'Destruction Efficiency': 'destEfficiency'})
        return metadata, destEffDF

    @classmethod
    def destructionEfficiencyFactory(cls, inSpec, origGC, **kwargs):
        if isinstance(inSpec, float) or isinstance(inSpec, int):
            deDF = pd.DataFrame({'destSpecies': ['ALL'], 'destEfficiency': inSpec})
        elif isinstance(inSpec, str):
            metadata, deDF = DestructionGC.getDestEfficiencies(inSpec)
        else:
            err = f"Unknown type for argmument DestructionGC.destructionEfficiencyFactory inSpec value: {inSpec}, type: {type(inSpec)}"
            logger.error(err)
            raise me.IllegalArgumentError(err)

        ret = DestructionGC(destructionEfficiency=deDF, origGC=origGC, **kwargs)
        return ret

    def __init__(self,
                 destructionEfficiency=None,
                 origGC=None,
                 **kwargs):

        flowToUse = origGC.flow
        ffIDToUse = origGC.fluidFlowID
        gcUnitsToUse = origGC.gcUnits
        # initialize these before super().__init__ so the hash function can use them
        if not isinstance(destructionEfficiency, pd.DataFrame):
            msg = f"DestructionGC destructionEfficiency must be a dataframe, type is {type(destructionEfficiency)}"
            logger.error(msg)
            raise me.IllegalArgumentError(msg)
        self.destructionEfficiency = destructionEfficiency
        self.origGC = origGC
        super().__init__(**{'flow': flowToUse, 'fluidFlowID': ffIDToUse, 'gcUnits': gcUnitsToUse,
                         'fluidFlowGCFilename': self.origGC.fluidFlowGCFilename, **kwargs})

    def __hash__(self):
        ret = hash((self.origGC.__hash__(), self.destructionEfficiency))
        return ret

    def __eq__(self, other):
        if not isinstance(other, DestructionGC):
            return False
        ret = (
                (self.origGC == other.origGC)
                & (self.destructionEfficiency == other.destructionEfficiency)
        )
        return ret

    def _expandDE(self, deDF, species):
        if len(deDF) == 0:
            return deDF

        if 'destSpecies' in deDF:
            if deDF.iloc[0]['destSpecies'] == 'ALL':
                retDF = pd.DataFrame({'destSpecies': species,
                                      'destEfficiency': [deDF.iloc[0]['destEfficiency']]*len(species)})
                return retDF
        else:
            r = {}
            for specie in species:
                if specie in deDF.columns:
                    specieProb = deDF[[specie, f'{specie} Probability']]
                    specieHist = d.Histogram.fromPandas(specieProb, probCol=f'{specie} Probability')
                    randValue = specieHist.pick()
                    r.update({specie: randValue})
            re = pd.DataFrame(r.items(), columns=['destSpecies', 'destEfficiency'])
            return re

        return deDF

    def serialForm(self):
        origSerialDF = self.origGC.serialForm()

        tmpDF = origSerialDF.merge(COMPLETE_COMBUSTION_MASS_CO2_RATIO_DF, left_on='species', right_index=True)
        tmpDF = tmpDF.rename(columns={'gcValue': 'gamma'})

        deDF = self._expandDE(self.destructionEfficiency, tmpDF['species'])

        tmpDF = tmpDF.merge(deDF, left_on='species', right_on='destSpecies', how='outer', indicator=True).set_index('species')
        if 'CARBON_DIOXIDE' not in tmpDF.index:  # Force CO2 to be in the DF
            CO2Row = tmpDF.iloc[0].copy()
            CO2Row.name = 'CARBON_DIOXIDE'
            CO2Row['species'] = 'CARBON_DIOXIDE'
            CO2Row['NHV'] = 0
            CO2Row['gamma'] = 0
            CO2Row['gcValue'] = 0
            CO2Row['massFraction'] = 0
            CO2Row['destEfficiency'] = 0
            CO2Row['kgCO2perKgAlkane'] = 0
        else:
            CO2Row = tmpDF.loc['CARBON_DIOXIDE'].copy()
            CO2Row['species'] = 'CARBON_DIOXIDE'
            tmpDF = tmpDF.drop('CARBON_DIOXIDE', axis='index')

        # calculate gamma for uncombusted portions of combusted species
        co2SpeciesDF = tmpDF[(tmpDF['_merge'] == 'both')]
        co2SpeciesDF = co2SpeciesDF.assign(uncombustedRatio=(1-co2SpeciesDF['destEfficiency']),
                                           gcValue=lambda x: x['uncombustedRatio'] * co2SpeciesDF['gamma']
                                           )
        # calculate gamma for CO2 -- the original amount of CO2, plus that which results from combustion of species
        totalCombustionCO2 = (co2SpeciesDF['gamma'] * co2SpeciesDF['kgCO2perKgAlkane'] * co2SpeciesDF['destEfficiency']).sum()
        CO2Row['gcValue'] = CO2Row['gamma'] + totalCombustionCO2

        # carry through non-combusted species
        nonCO2SpeciesDF = tmpDF[(tmpDF['_merge'] == 'right_only')]
        nonCO2SpeciesDF.set_index('destSpecies')
        firstOrigRow = origSerialDF.iloc[0]
        nonCO2SpeciesDF = nonCO2SpeciesDF.assign(
            # from the original GC
            flow=firstOrigRow['flow'],
            flowName=firstOrigRow['flowName'],
            gcUnits=firstOrigRow['gcUnits'],
            gcID=firstOrigRow['gcID'],
            NHV=0,
            massFraction=0,

            # gcType -- set below
            # origGC -- set below

            # from the exhaust efficiency file
            species=nonCO2SpeciesDF['destSpecies'],
            gcValue=nonCO2SpeciesDF['destEfficiency']
        )

        destDF = (pd.concat([co2SpeciesDF.reset_index(),
                             pd.DataFrame(CO2Row).transpose(),
                             nonCO2SpeciesDF])
                  .reset_index(drop=True)
                  .assign(gcType='DestructionGC',
                          origGC=self.origGC.serialNum
                          )
        )

        ret = destDF[origSerialDF.columns]
        return ret


NET_HEATING_VALUES = {
    'METHANE':    50048,
    'ETHANE':     47800,
    'PROPANE':    46400,
    'BUTANE':     44862,
    'ISOBUTANE':  45300,
    'PENTANE':    45241,
    'ISOPENTANE': 45400,
    'HEXANE':     44752,
    'HEPTANE':    44925,
    'OCTANE':     44786,
}

NET_HEATING_VALUES_DF = pd.DataFrame.from_dict(NET_HEATING_VALUES, orient='index', columns=['NHV'])

class EngineGC(FluidFlowGC):
    def __init__(self,
                 driverType=None,
                 engineEfficiency=None,
                 origGC=None,
                 **kwargs):
        flow = origGC.flow
        fluidFlowID = origGC.fluidFlowID # todo: come up with a better FFID
        gcUnits = 'kW'
        # initialize these before super().__init__ so the hash function can use them
        self.driverType = driverType
        self.engineEfficiency = engineEfficiency
        self.origGC = origGC
        super().__init__(**{'flow': flow, 'fluidFlowID': fluidFlowID, 'gcUnits': gcUnits,
                            'fluidFlowGCFilename': self.origGC.fluidFlowGCFilename, **kwargs})

    def __hash__(self):
        ret = hash((self.origGC.__hash__(), self.driverType))
        return ret

    def __eq__(self, other):
        if not isinstance(other, EngineGC):
            return False
        ret = (
                (self.origGC == other.origGC)
                & (self.driverType == other.driverType)
        )
        return ret

    def serialForm(self):
        sf = self.origGC.serialForm()
        heatRateDF = sf.merge(NET_HEATING_VALUES_DF, left_on='species', right_index=True)
        totalSpeciesMass = heatRateDF['gcValue'].sum()                          # total mass of one scf: kg/scf
        heatRateDF['massFraction'] = heatRateDF['gcValue'] / totalSpeciesMass   # mass fraction of gas species: kg/kg = kg/scf * scf/kg
        totalHeatRate = (heatRateDF['gcValue'] * heatRateDF['NHV']).sum()  # relative heat rate: kJ/scf --> sum(kg/scf*kJ/kg)
        heatRateDF['gcValue'] = heatRateDF['gcValue'] / (totalHeatRate * self.engineEfficiency)      # species heat rate kg/scf * scf/kJ --> kg/kJ
        heatRateDF['gcUnits'] = 'kg/kW'
        heatRateDF['gcType'] = 'EngineGC'
        heatRateDF['origGC'] = self.origGC.serialNum
        return heatRateDF

def composeEngineExhaustGC(origGC=None,
                           driverType=None,
                           engineEfficiency=None,
                           destructionEfficiency=None,
                           **kwargs):
    engGC = EngineGC(origGC=origGC, driverType=driverType, engineEfficiency=engineEfficiency)
    destGC = DestructionGC.destructionEfficiencyFactory(destructionEfficiency, engGC)
    return destGC





