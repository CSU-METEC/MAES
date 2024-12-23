MEET Factors.csv File
=====================

All Activity & Emissions Factors are stored in individual distribution files.  To ease management of these files, and to allow for parameterization
of simulations, references to all of these individual factor distribution files are collected into a single file, `input/CuratedData/FactorsFileReference/factors.csv`.
MEET uses the factors.csv file to find the Activity & Emissions factors for any equipment.

Columns in the factors.csv file are:

emitterModelFactorTag
  Defined in the model formulation file to identify the category of major equipment for lookup.
  
factorTag
  Defined in the site definition file for every piece of major equipment (a generel version is in CuratedData folder). This field can be set by the analyst, and can define a set of distributions to be used for classes
  of equipment.  For example, there are three factor tags defined for wellpad compressors -- wellpad_compressor_0to200HP, wellpad_compressor_200to500HP, and wellpad_compressor_above500HP.
  These different tags refer to different Emission Factor files corresponding to the size of the compressor.
  
activityDistribution
  Distribution file for Activity Factors.
  
emissionDriver
  Distribution file for Emission Factors.
  
notes
  Relevant notes for the row.
  
correctFactors
  Indication whether the factors are measured or estimated.
 
 Setting up Factors.csv file:
 
| **Step1**: Chose an equipment (one row) for which the activity and emission factors have to be defined.
| **Step2**: Make up a factor tag (ex: separator200LitreVolume) for use in site specific Factors.csv file and the study sheet.
| **Step3**: Copy the general list of equipment specific emitter rows from the general Factors.csv file to site specific Factors.csv file to make sure we do not miss an emitter. 
             Make sure to put Factor Tag in both the equipment row and specific emitters in site specific factors file
| **Step4**: Put in activity and emitter factor distributions in CuratedData and copy the file paths in Factors.csv (use the general Factors.csv as a reference). Activity factors can also be numbers.
| **Step5**: One set of emitters have now been set up. We can use the same factor tag for same type of equipment (different unit IDs, same factor tag).
| **Step6**: Continue setting up factor tags for any equipment required. If we do not set up a factor tag, default tags will be selected. The default list is in the general Factors.csv file.