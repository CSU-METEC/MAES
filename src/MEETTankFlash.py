
from EquipmentTable import Facility, MajorEquipment, Emitter, MEETTemplate, ActivityFactor, EquipmentTableEntry
import MEETGlobals as mg
from pathlib import Path
import AppUtils as au
import Units as u
from EmitterProfile import EmitterProfile
from MEETClasses import DESEnabled, DESStateEnabled, StateBasedEmitter, StateChangeInitiator
from MEETClasses import StateChangeNotificationDestination

class TankFlash(StateBasedEmitter):

    def __init__(self, fluidType = None, dumpDuration= None, dumpPeakTiming= None,
                 dumpVolumeBbl= None, timingThresholdMinutes= None,
                 **kwargs):
        super().__init__(**kwargs)

        self.fluidType = fluidType            # type of fluid ["WATER", "CONDENSATE"] to select correct production rate from major equipment
        self.dumpVolumeBbl = dumpVolumeBbl    # Volume of liquid transferred to tank in a single dump (bbl/cycle)
        self.dumpDuration = dumpDuration      # duration of separator liquids dump cycle (seconds)
        self.dumpPeakTiming = dumpPeakTiming  # timing of peak flow rate during liquids dump (seconds into cycle)
        self.timingThresholdMinutes = timingThresholdMinutes  # timing threshold


