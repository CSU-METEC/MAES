import csv
import pandas as pd
from Distribution import Distribution
import logging
import MEETExceptions as me

METADATA_END_TOKEN = "%%%ENDOFMETADATA%%%"                  # needs to be at beginning of row

def readRawDistributionFile(filename):
    metadata = {'filename': str(filename)}
    with open(filename, 'r') as distyFile:

        csvInput = csv.reader(distyFile)
        for row in csvInput:
            if row[0].strip() == METADATA_END_TOKEN:
                break
            else:
                key = row[0].strip()
                value = row[1].strip()
                # rest = row[2:]
            metadata[key] = value

        df = pd.read_csv(distyFile, skipinitialspace=True)  # inhale remaining file as a dataframe

    return metadata, df

def getAllSubclasses(cls):
    ret = [cls]
    for singleSubclass in cls.__subclasses__():
        ret.extend(getAllSubclasses(singleSubclass))        # doing recursion to get sub-subclasses
    return ret                                              # return from lower levels first, then top level

def buildSubclassMap(distro):
    subclasses = getAllSubclasses(distro)
    ret = dict(map(lambda x: (x.__name__, x), subclasses))
    return ret                                              # return dictionary of emission distributions

class DistributionProfile():

    def __init__(self, md, eDisty):
        self.md = md
        self.distribution = eDisty

    @classmethod
    def readFile(cls, filename):                     # a factory
        try:
            if filename == 'input\CuratedData\Allen\Rocky Mountain\Allen_RM_ALLPNM_EF.csv':
                i = 10
            md, emissionRate = cls._readDistributionFile(filename, Distribution)
            return DistributionProfile(md, emissionRate)
        except:
            logging.exception(f"Error reading distribution file {filename}", exc_info=True)

    @classmethod
    def _readDistributionFile(cls, filename, rootClass=Distribution, mdTypeName='Distribution Type'):
        try:
            md, df = readRawDistributionFile(filename)
            dictSubclasses = buildSubclassMap(rootClass)

            distType = md[mdTypeName]
            # colHeader = md['Value Column Name']
            # todo: should we normalize this with Distribution.distFactory?
            distClass = dictSubclasses[distType]
            distribution = distClass.fromPandas(df)

            return md, distribution
        except Exception as e:
            logging.exception(f"DistributionProfile._readDistributionFile failed, filename: {filename}", exc_info=True)
            raise e

    def pick(self, category=None):
        return self.distribution.pick(category)

class FlashComposition():

    # GAS_COMP_SPECIES = ['CARBON_DIOXIDE',
    #                     'NITROGEN',
    #                     'HYDROGEN_SULFIDE',
    #                     'METHANE',
    #                     'ETHANE',
    #                     'PROPANE',
    #                     'ISOBUTANE',
    #                     'N_BUTANE',
    #                     'ISOPENTANE',
    #                     'N_PENTANE',
    #                     'N_HEXANE'
    #                     ]

    # todo: reconcile gas species names w/ Clay's code

    GAS_COMP_SPECIES = {
        'Carbon Dioxide':   'CARBON_DIOXIDE',
        'Nitrogen':         'NITROGEN',
        'Hydrogen Sulfide': 'HYDROGEN_SULFIDE',
        'Methane':          'METHANE',
        'Ethane':           'ETHANE',
        'Propane':          'PROPANE',
        'Isobutane':        'ISOBUTANE',
        'Butane':           'BUTANE',
        'Isopentane':       'ISOPENTANE',
        'Pentane':          'PENTANE',
        'Hexane':           'HEXANE'
    }

    def __init__(self, md, flashComp):
        self.md = md
        self.flashComp = flashComp
        # todo: remove this when species names are normalized
        self.flashComp.rename(self.GAS_COMP_SPECIES, axis='columns', inplace=True)
        pass

    @classmethod
    def readFile(cls, filename):                     # a factory
        try:
            md, df = readRawDistributionFile(filename)
            return FlashComposition(md, df)
        except:
            msg = f"Error reading distribution file {filename}"
            logging.warning(msg)
            raise

    def hasFlash(self, flashName):
        flashDF = self.flashComp[self.flashComp['Name'] == flashName]
        return not flashDF.empty

    def _findConversion(self, flashName, inUnits, outUnits, conversionColumn):
        flashDF = self.flashComp[self.flashComp['Name'] == flashName]
        if flashDF.empty:
            msg = f"Unknown flash name {flashName} in Flash Composition file {self.md['filename']}"
            logging.error(msg)
            raise me.IllegalArgumentError(msg)

        unitsKey = f'{outUnits}/{inUnits}'
        conversionDF = flashDF[flashDF[conversionColumn] == unitsKey]
        if conversionDF.empty:
            msg = f"Unknown conversion factor {unitsKey} in Flash Composition file {self.md['filename']}, Name: {flashName}"
            logging.error(msg)
            raise me.IllegalArgumentError(msg)
        if len(conversionDF) > 1:
            msg = f"Multiple conversion factors for {unitsKey} in Flash Composition file {self.md['filename']}, Name: {flashName}"
            logging.error(msg)
            raise me.IllegalArgumentError(msg)
        return conversionDF

    def convertUnits(self, flashName, inVal, inUnits, outUnits):
        conversionDF = self._findConversion(flashName, inUnits, outUnits, 'DriveFactorUnits')
        ret = inVal * conversionDF.iloc[0]['DriveFactor']
        return ret

    def calculateGasComposition(self, flashName, inVal, inUnits, outUnits='kg'):
        # todo: doing this units check as part of legacy GC conversion.  Should probably be handled by emission factor input file conversion
        if inUnits == 'scf_wholegas':
            inUnits = 'scf'
        conversionDF = self._findConversion(flashName, inUnits, outUnits, 'GCUnits')
        gcDF = conversionDF[self.GAS_COMP_SPECIES.values()]
        gasSpeciesSeries = gcDF.iloc[0] * inVal
        return gasSpeciesSeries.to_dict()

    FLASHCOMP_COLS_TO_REMOVE = ['MajorEquipment', 'FluidFlow', 'Name', 'DriveFactor', 'DriveFactorUnits', 'GCUnits']

    def gasSpeciesNames(self):
        if not hasattr(self, 'flashComp') or self.flashComp.empty:
            return []

        ret = list(self.flashComp.columns)
        for singleCol in self.FLASHCOMP_COLS_TO_REMOVE:
            if singleCol in ret:
                ret.remove(singleCol)

        return ret




