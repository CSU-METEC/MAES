import csv
import pandas as pd
from Distribution import Distribution

METADATA_END_TOKEN = "%%%ENDOFMETADATA%%%"                  # needs to be at beginning of row

def readRawDistributionFile(filename):
    metadata = {}

    with open(filename, 'r') as distyFile:

        csvInput = csv.reader(distyFile)
        for row in csvInput:
            if row[0] == METADATA_END_TOKEN:
                break
            else:
                key = row[0]
                value = row[1]
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

class EmitterProfile():

    def __init__(self, md, eDisty):
        self.md = md
        self.distribution = eDisty

    @classmethod
    def readEmitterFile(cls, filename):                     # a factory
        md, emissionRate = cls.readDistributionFile(filename, Distribution)
        return EmitterProfile(md, emissionRate)

    @classmethod
    def readDistributionFile(cls, filename, rootClass=Distribution, mdTypeName='Distribution Type'):
        md, df = readRawDistributionFile(filename)
        dictSubclasses = buildSubclassMap(rootClass)

        distType = md[mdTypeName]
        # colHeader = md['Value Column Name']
        # todo: should we normalize this with Distribution.distFactory?
        distClass = dictSubclasses[distType]
        distribution = distClass.fromPandas(df)

        return md, distribution

    def pick(self, category=None):
        return self.distribution.pick(category)


