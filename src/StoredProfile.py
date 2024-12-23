import csv
import pandas as pd
from Distribution import Distribution
from Driver4EC import Driver4EC
import logging

METADATA_END_TOKEN = "%%%ENDOFMETADATA%%%"                  # needs to be at beginning of row

def getAllSubclasses(cls):
    ret = [cls]
    for singleSubclass in cls.__subclasses__():
        ret.extend(getAllSubclasses(singleSubclass))        # doing recursion to get sub-subclasses
    return ret

def buildSubclassMap(distro):
    subclasses = getAllSubclasses(distro)
    ret = dict(map(lambda x: (x.__name__, x), subclasses))
    return ret

class StoredProfile():

    def __init__(self, md, eDisty):
        self.md = md
        self.distribution = eDisty

    @classmethod
    def readSPFile(cls, filename):
        metadata = {}

        with open(filename, 'r') as distyFile:
            csvInput = csv.reader(distyFile)
            rowNum = 1
            for row in csvInput:
                if row[0] == METADATA_END_TOKEN:
                    break
                if len(row) < 2:
                    logging.warning(f"Malformed metadata in {filename}, row {rowNum}: {row}")
                    continue
                key = row[0]
                value = row[1]
                metadata[key] = value
                rowNum += 1
            df = pd.read_csv(distyFile, skipinitialspace=True)  # inhale remaining file as a dataframe

        return metadata, df

    @classmethod
    def readProfile(cls, filename, rootClass=Distribution, mdTypeName='Distribution Type'):
        md, df = cls.readSPFile(filename)
        dictSubclasses = buildSubclassMap(rootClass)

        if mdTypeName not in md:
            msg = f"Metadata field '{mdTypeName}' not found in profile file {filename}"
            logging.warning(msg)
            raise ValueError(msg)
        distType = md[mdTypeName]
        distClass = dictSubclasses[distType]
        distribution = distClass.fromPandas(df)

        return md, distribution

class ActivityProfile(StoredProfile):
    @classmethod
    def readProfile(cls, filename):
        md, emissionRate = super().readProfile(filename, rootClass=Distribution)
        return ActivityProfile(md, emissionRate)

    def __init__(self, *args):
        super().__init__(*args)

    def pick(self, category=None):
        return int(self.distribution.pick(category))

class Driver4ECProfile(StoredProfile):
    def __init__(self, *args, name=None, units=None, gcFingerprint=None):
        super().__init__(*args)
        self.name = name
        self.units = units
        self.gcFingerprint = gcFingerprint
        self.distribution.setUnits(self.units)

    @classmethod
    def readProfile(cls, filename):
        md, driver4EC = super().readProfile(filename, rootClass=Driver4EC, mdTypeName='Emission Type')
        return Driver4ECProfile(md, driver4EC, name=filename, units=md['Units'], gcFingerprint=md.get('gcFingerprint', 'default'))

    def instantaneousEmission(self, ts, emissionName=None):
        return self.distribution.instantaneousEmission(ts, emissionName)

    def integratedEmission(self, t0, t1, emissionName=None):
        return self.distribution.integratedEmission(t0, t1, emissionName)

    def pick(self):
        return self.distribution.pick()

