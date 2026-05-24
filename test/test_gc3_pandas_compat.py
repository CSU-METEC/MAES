"""
Regression test for issue #46: GasComposition3.FluidFlowGC methods that wrapped
a single-row pandas Series in float() silently coerced in pandas 1.x but raise
TypeError in pandas 2.x.  Covers getDeltaH(), getLhvVals(), and convertKgToScf().
"""
import pathlib
import pytest
import SimDataManager as sdm
import GasComposition3 as gc3

MAES_ROOT = pathlib.Path(__file__).parent.parent
GC_FILE = str(MAES_ROOT / "input" / "Studies" / "MEET2" / "DeltaHGC.csv")

FLUID_FLOW_ID = "Well-Condensate.Stage1-Flash"
STAGES = ["Stage1"]
DRIVER_RATE = 100.0


@pytest.fixture(scope="module", autouse=True)
def stub_sdm():
    sdm.SimDataManager.initStubSimDataManager()


@pytest.fixture
def gc():
    return gc3.FluidFlowGC(
        fluidFlowGCFilename=GC_FILE,
        flow="Vapor",
        fluidFlowID=FLUID_FLOW_ID,
        gcUnits="bbl",
    )


def test_getDeltaH_returns_scalar(gc):
    result = gc.getDeltaH(STAGES)
    assert isinstance(result, float)
    assert result > 0


def test_getLhvVals_returns_scalar(gc):
    result = gc.getLhvVals()
    assert isinstance(result, float)
    assert result > 0


def test_convertKgToScf_returns_scalar(gc):
    result = gc.convertKgToScf(DRIVER_RATE)
    assert isinstance(result, float)
    assert result > 0
