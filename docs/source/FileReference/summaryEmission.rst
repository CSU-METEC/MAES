MEET Output File -- summaryEmission.csv
=======================================

The summaryEmission.csv file provides a summary of all emissions by emitter by species over the entire duration of the simulation.  The emitters are merged with
select metadata fields to enable further summarization.

Fields
------

facilityID, unitID, emitterID, mcRun
  Identification fields.  Only emitters will be in the summary.
  
species
  Gas species emitted
  
equipmentType
  Equipment Type field from metadata.csv.
  
latitude, longitude
  Location of emitter.
  
modelCategory
  Category of emitter.
  
modelEmissionCategory
  Emission category of emitter.  Will be one of FUGITIVE, VENTED, or COMBUSTED.
  
modelID
  Model definition file for emitter.
  
modelReadableName
  Friendly name for emitter.
  
modelSubcategory
  Subcategory of emitter.
  
massUnits
  Units for totalMass column.  Currenly limited to kg.
  
totalMass
  Total mass of emission in massUnits.
