import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import unittest
import numpy as np
import pandas as pd
import Timeseries as ts
from Summaries2 import _makePDFRows, _buildPDFForGroupFromCache


def _makeSparseRLE(activeSecs: float, rateKgPerH: float) -> ts.TimeseriesRLE:
    """Build a single-interval RLE timeseries starting at t=0."""
    return ts.TimeseriesRLE.fromCollections(
        np.array([0.0]),
        np.array([activeSecs]),
        np.array([rateKgPerH]),
    )


IDENTITY = {'site': 'TestSite', 'species': 'METHANE', 'operator': 'OP', 'psno': 'PS1',
            'METype': 'Equipment', 'unitID': 'U1', 'modelReadableName': 'Blowdown Event',
            'modelEmissionCategory': 'EQUIPMENT'}


class TestMakePDFRowsProbabilityNormalization(unittest.TestCase):

    def test_probSumsToActiveFractionNotOne(self):
        """Probability sum must equal active_secs/totalSimSecs, not 1.0.

        This is the core regression for the blowdown/start-event PDF bug:
        before the fix, totalCount used pdf.data['count'].sum() (active seconds
        only), so probability always summed to 1.0 regardless of duty cycle.
        """
        activeSecs = 10.0
        totalSimSecs = 100.0
        rle = _makeSparseRLE(activeSecs, rateKgPerH=5.0)
        rows = _makePDFRows([rle], IDENTITY, 'modelReadableName', totalSimSecs)
        self.assertIsNotNone(rows)
        probSum = rows['probability'].sum()
        self.assertAlmostEqual(probSum, activeSecs / totalSimSecs, places=10)

    def test_probSumNotOneForSparseEmitter(self):
        """Probability must not sum to 1.0 for an emitter active only 10% of sim time."""
        rle = _makeSparseRLE(activeSecs=10.0, rateKgPerH=5.0)
        rows = _makePDFRows([rle], IDENTITY, 'modelReadableName', totalSimSecs=100.0)
        self.assertIsNotNone(rows)
        self.assertLess(rows['probability'].sum(), 1.0)

    def test_probSumEqualsOneForAlwaysOnEmitter(self):
        """Probability sums to 1.0 when emitter is active 100% of simulation."""
        totalSimSecs = 100.0
        rle = _makeSparseRLE(activeSecs=totalSimSecs, rateKgPerH=5.0)
        rows = _makePDFRows([rle], IDENTITY, 'modelReadableName', totalSimSecs)
        self.assertIsNotNone(rows)
        self.assertAlmostEqual(rows['probability'].sum(), 1.0, places=10)

    def test_probSumAcrossMultipleMCRuns(self):
        """With N MC runs each active for fraction f, prob sum = f (not 1.0).

        Each MC run: 20s active out of 100s. Two MC runs → totalSimSecs = 200s.
        The blowdown emitter fires in both runs at the same rate, so the PDF
        has one bin. Expected probability = 40/200 = 0.2.
        """
        activeSecs = 20.0
        mcRuns = 2
        totalSimSecs = 100.0 * mcRuns
        rles = list(map(lambda _: _makeSparseRLE(activeSecs, rateKgPerH=5.0), range(mcRuns)))
        rows = _makePDFRows(rles, IDENTITY, 'modelReadableName', totalSimSecs)
        self.assertIsNotNone(rows)
        self.assertAlmostEqual(rows['probability'].sum(), (activeSecs * mcRuns) / totalSimSecs, places=10)

    def test_probSumWithMultipleRates(self):
        """Multi-bin PDF: each bin's probability is its active_secs/totalSimSecs."""
        totalSimSecs = 100.0
        rle_low = ts.TimeseriesRLE.fromCollections(
            np.array([0.0, 50.0]),
            np.array([10.0, 60.0]),
            np.array([2.0, 5.0]),
        )
        rows = _makePDFRows([rle_low], IDENTITY, 'modelReadableName', totalSimSecs)
        self.assertIsNotNone(rows)
        self.assertAlmostEqual(rows['probability'].sum(), 20.0 / totalSimSecs, places=10)

    def test_emptyMCRunListReturnsNone(self):
        """Empty MC run list must return None without raising."""
        ret = _makePDFRows([], IDENTITY, 'modelReadableName', totalSimSecs=100.0)
        self.assertIsNone(ret)

    def test_blowdownLikeProbabilityIsSmall(self):
        """Blowdown emitter active 0.1% of sim time → probability max ~0.001, not 1.0.

        This directly reproduces the reported bug: Blowdown Event was returning
        probability=1.0 for a single emission rate because zero idle time was
        excluded from the denominator.
        """
        simDurationSecs = 365 * 24 * 3600.0
        blowdownDurationSecs = simDurationSecs * 0.001
        totalSimSecs = simDurationSecs * 10  # 10 MC runs
        rles = list(map(
            lambda _: _makeSparseRLE(blowdownDurationSecs, rateKgPerH=2815.52),
            range(10),
        ))
        rows = _makePDFRows(rles, IDENTITY, 'modelReadableName', totalSimSecs)
        self.assertIsNotNone(rows)
        self.assertAlmostEqual(rows['probability'].sum(), 0.001, places=6)
        self.assertLess(rows['probability'].max(), 0.01)


class TestBuildPDFForGroupFromCacheProbabilityNormalization(unittest.TestCase):

    def _makeCacheDF(self, mcRun: int, startTime: float, endTime: float,
                     rateKgPerH: float) -> pd.DataFrame:
        """Build a minimal PDFCache-format DataFrame for one MC run interval."""
        return pd.DataFrame({
            'site': ['TestSite'],
            'species': ['METHANE'],
            'operator': ['OP'],
            'psno': ['PS1'],
            'METype': ['Equipment'],
            'unitID': ['U1'],
            'modelReadableName': ['Blowdown Event'],
            'modelEmissionCategory': ['EQUIPMENT'],
            'mcRun': [mcRun],
            'startTime_s': [startTime],
            'endTime_s': [endTime],
            'emission_kgPerH': [rateKgPerH],
            'cacheLevel': ['modelReadableName'],
        })

    def test_cachePathProbSumsToActiveFraction(self):
        """Cache-path PDF has the same correct normalization as the raw path."""
        activeSecs = 10.0
        totalSimSecs = 100.0
        groupDF = self._makeCacheDF(mcRun=0, startTime=0.0, endTime=activeSecs, rateKgPerH=5.0)
        identityCols = {
            'site': 'TestSite', 'species': 'METHANE', 'operator': 'OP', 'psno': 'PS1',
            'METype': 'Equipment', 'unitID': 'U1', 'modelReadableName': 'Blowdown Event',
        }
        fullRows, noFugRows, stats = _buildPDFForGroupFromCache(
            groupDF, identityCols, 'modelReadableName', totalSimSecs)
        self.assertIsNotNone(fullRows)
        self.assertAlmostEqual(fullRows['probability'].sum(), activeSecs / totalSimSecs, places=10)

    def test_fugitiveExclusionStillNormalizedCorrectly(self):
        """noFug path: FUGITIVE rows excluded but denominator is still totalSimSecs."""
        activeSecs = 10.0
        totalSimSecs = 100.0
        fugDF = self._makeCacheDF(mcRun=0, startTime=0.0, endTime=activeSecs, rateKgPerH=5.0)
        fugDF = fugDF.assign(modelEmissionCategory='FUGITIVE')
        identityCols = {
            'site': 'TestSite', 'species': 'METHANE', 'operator': 'OP', 'psno': 'PS1',
            'METype': 'Equipment', 'unitID': 'U1', 'modelReadableName': 'Blowdown Event',
        }
        fullRows, noFugRows, stats = _buildPDFForGroupFromCache(
            fugDF, identityCols, 'modelReadableName', totalSimSecs)
        self.assertIsNotNone(fullRows)
        self.assertAlmostEqual(fullRows['probability'].sum(), activeSecs / totalSimSecs, places=10)
        self.assertIsNone(noFugRows)


if __name__ == '__main__':
    unittest.main()
