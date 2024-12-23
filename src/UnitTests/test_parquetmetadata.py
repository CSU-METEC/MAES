import unittest
import ParquetLib as pl

class ParquetMetadata(unittest.TestCase):

    def test_parquetMD(self):
        parquetMetadataDF, fakeConfig = pl.getParquetMetadata('../output/ConstantSeparator/MC_20241105_085553/parquet')
        eventDF = pl.readParquetEvents(fakeConfig)
        for singleSite, siteGrp in parquetMetadataDF.groupby('site'):
            for singleMCRun in siteGrp['mcRun']:
                eventForSiteMC = pl.readParquetEvents(fakeConfig, site=singleSite, mcRun=singleMCRun)
                pass
        pass
