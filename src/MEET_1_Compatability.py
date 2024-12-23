import EquipmentTable as et
import MEETClasses as mc
import AppUtils as au
from pathlib import Path
import EmitterProfile as ep
import Units as u
import SimDataManager as sdm
from Chooser import EmpiricalDistChooser
import random
import Distribution
import EmissionDriver as ed
import GasComposition3 as gc
import MEETFluidFlow as ff
import TimeseriesTable as ts
import pandas as pd

class MEET1Facility(et.Facility):

    def __init__(self,
                 region=None, sector=None,
                 leakEmissionDataSource=None, leakSurveyMethod=None,
                 userSpecifiedLeakFilename=None,
                 pneumaticDataSource=None,
                 reportingYear=None, compositionID=None, **kwargs):
        super().__init__(**kwargs)
        self.region = region
        self.sector = sector
        self.leakEmissionDataSource = leakEmissionDataSource
        self.leakSurveyMethod = leakSurveyMethod
        self.userSpecifiedLeakFilename = userSpecifiedLeakFilename
        self.pneumaticDataSource = pneumaticDataSource
        self.reportingYear = reportingYear
        self.compositionID = compositionID
    pass


class ImmediateStateBasedEmitterProduction(mc.EmissionManager):
    def __init__(self,
                 emissionVolume=None,
                 emissionDuration=None,
                 emissionDriverUnits=None,
                 gasComposition=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.emissionVolume = emissionVolume
        self.emissionDuration = emissionDuration
        self.emissionDriverUnits = emissionDriverUnits or 'scf'
        self.gasComposition = gasComposition

    def initializeFluidFlow(self, simdm):
        super().initializeFluidFlow(simdm)
        facility = simdm.getEquipmentTable().elementLookup(self.facilityID, None, None, self.mcRunNum)
        tmpGC = gc.FluidFlowGC.factory(fluidFlowGCFilename=facility.productionGCFilename,
                                       flow='Vapor',
                                       fluidFlowID=self.gasComposition,
                                       gcUnits=self.emissionDriverUnits
                                       )
        driverRateInSeconds = self.emissionVolume / self.emissionDuration
        self.fluidFlow = ff.FluidFlow('', driverRateInSeconds, self.emissionDriverUnits, tmpGC)
        self.fluidFlow.ts = ts.ConstantTimeseriesTableEntry.factory(driverRateInSeconds, self.emissionDriverUnits)
