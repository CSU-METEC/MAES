MEET Output File -- metadata.csv
================================

The metadata.csv file contains standard metadata for all equipment and emitters defined in the simulation.

Fields
------

facilityID, unitID, emitterID
  Defines the standard identification fields for the element.  Can be used as a key to other files such as instantaneousEvents.csv.

mcRunNum
  Monte Carlo run number to identify the element.


modelID
  Name of the model definition file used to define the element.
	
modelCategory
  Model category field from the model definition file.
	
modelSubcategory
  Model subcategory field from the model definition file.
	
modelEmissionCategory
  Category for emission for emitters.  One of VENTED, FUGITIVE, COMBUSTED
	
modelReadableName
  Readable name for the element.  Defined in model definition file.
	
equipmentCount
  Record of the total number of components chosen from Activity Factors.
	

latitude, longitude
  Location of the element.
	
equipmentType, implCategory, implClass
  Implementation details.