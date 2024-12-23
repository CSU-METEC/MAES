.. _gas-compositions-label:

MEET Output File -- gasCompositions.csv
=======================================

The gasCompositions.csv file stores gas composition species data for a particular Fluid Flow.  Data is stored in "long form",
that is, each species is stored as a single row in the table.  This is a flexible format that allows for addition of additional
gas species over time.

Fields
------

gcKey
  Gas composition key from the instantaneousEvents.csv log.
  
flow
  Type of flow.  Currently will only be Vapor.
  
flowName
  Name of flow.
  
species
  Gas species.
  
gcUnits
  Units of GC conversion.
  
gcValue
  Conversion value for multiplication.
  
gcType
  Implementation type of GC.  Used for debugging.
  
gcID
  Location of file defining GC.
  
origGC
  Original GC entry for compound GC calculations.
