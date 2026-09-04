import csv
import pandas as pd
from Distribution import Distribution
import logging
import MEETExceptions as me

METADATA_END_TOKEN = "%%%ENDOFMETADATA%%%"                  # needs to be at beginning of row

# Per-file cache for DistributionProfile.readFile / FlashComposition.readFile (2026-07-16,
# OOM-investigation follow-up -- see PERFORMANCE_MIGRATION_LOG.md). Both factories previously
# re-opened, re-parsed (csv.reader + pd.read_csv), and re-built a fresh object from scratch on
# EVERY call, even when many equipment instances reference the identical file. Measured directly
# on a real study (Raton Basin, 365-day duration, one MC iteration): 50,793 total calls into
# readRawDistributionFile against only 29 distinct files -- one file (Stage1.csv) alone was
# read 27,331 times. tracemalloc showed this pattern (repeated pandas DataFrame/BlockManager
# construction) as the single largest contributor to peak memory, dwarfing every other lead
# investigated this session (TimeseriesTable/FFTable entries combined were ~75MB; this was not).
#
# Verified safe to cache (not just "probably fine"), traced concretely:
# - Distribution.pick() (e.g. Normal.pick() -> SimRNG.normal(mu, sigma)) draws from the shared
#   GLOBAL SimRNG stream using only immutable parameters captured at construction -- the object
#   itself holds no per-instance mutable state, so many callers sharing one cached instance
#   changes nothing about the random draw sequence (same global stream, same call order).
# - Every real caller of DistributionProfile.readFile (EmissionDriver.py, MEETFluidFlow.py,
#   ModelClasses.py) only ever READS the returned object (`.pick()`) or copies its `.md` dict
#   into a new dict (EmissionDriver.scale()) -- never mutates the returned instance in place.
# - FlashComposition.__init__ DOES mutate its own dataframe in place once
#   (`self.flashComp.rename(..., inplace=True)`), so caching happens at the FlashComposition
#   level (post-__init__, already-renamed), not by sharing the raw (metadata, df) pair from
#   readRawDistributionFile across both factories -- avoids any risk of one factory's
#   type-specific mutation leaking into the other's cached copy. Every other FlashComposition
#   method (hasFlash/_findConversion/convertUnits/calculateGasComposition/gasSpeciesNames) only
#   reads self.flashComp/self.md; no caller anywhere mutates a returned instance afterward.
#
# Kill switch: set to False to disable both caches and fall back to the original
# read-every-time behavior with zero other code changes, if this ever needs a fast revert.
DISTRIBUTION_FILE_CACHE_ENABLED = True

def readRawDistributionFile(filename):
    filename = str(filename).replace('\\', '/')
    metadata = {'filename': filename}
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

    # jobId/filename -> already-built DistributionProfile instance. See
    # DISTRIBUTION_FILE_CACHE_ENABLED's docstring above for the safety reasoning. Deliberately a
    # plain class-level dict, not per-instance -- the whole point is sharing ACROSS instances/
    # equipment within one process. Never explicitly cleared: each worker process (maxtasksperchild=1)
    # exits after its one workitem anyway, so this cannot leak across MC iterations or jobs.
    _READ_FILE_CACHE = {}

    def __init__(self, md, eDisty):
        self.md = md
        self.distribution = eDisty

    @classmethod
    def readFile(cls, filename):                     # a factory
        cacheKey = str(filename).replace('\\', '/')  # matches readRawDistributionFile's own normalization
        if DISTRIBUTION_FILE_CACHE_ENABLED:
            cached = cls._READ_FILE_CACHE.get(cacheKey)
            if cached is not None:
                return cached
        try:
            md, emissionRate = cls._readDistributionFile(filename, Distribution)
            result = DistributionProfile(md, emissionRate)
            if DISTRIBUTION_FILE_CACHE_ENABLED:
                cls._READ_FILE_CACHE[cacheKey] = result
            return result
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

    # See DISTRIBUTION_FILE_CACHE_ENABLED's docstring (top of file) -- cached at this level
    # (post-__init__, after the one-time column rename below), not by sharing the raw
    # (metadata, df) pair with DistributionProfile's own cache.
    _READ_FILE_CACHE = {}

    def __init__(self, md, flashComp):
        self.md = md
        self.flashComp = flashComp
        # todo: remove this when species names are normalized
        self.flashComp.rename(self.GAS_COMP_SPECIES, axis='columns', inplace=True)
        pass

    @classmethod
    def readFile(cls, filename):                     # a factory
        cacheKey = str(filename).replace('\\', '/')  # matches readRawDistributionFile's own normalization
        if DISTRIBUTION_FILE_CACHE_ENABLED:
            cached = cls._READ_FILE_CACHE.get(cacheKey)
            if cached is not None:
                return cached
        try:
            md, df = readRawDistributionFile(filename)
            result = FlashComposition(md, df)
            if DISTRIBUTION_FILE_CACHE_ENABLED:
                cls._READ_FILE_CACHE[cacheKey] = result
            return result
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




