MEETMain
========

Usage
-----
::

       MEETMain [-h] [-s STUDY] [-c CONFIGFILE] [-i INPUTDIR] [-o OUTPUTDIR] [-or OUTPUTROOT] [-t TESTINTERVALDAYS]
                [-ts SCENARIOTIMESTAMP] [-mc MONTECARLOITERATIONS] [-r RUNNUMBER] [-w WORKERS] [-m MEMORY] [-si] [-gr]
                [-su] [-dr DIRECTORY]

Options
-------
::

  -h, --help            show this help message and exit
  -s STUDY, --study STUDY
                        Study definition file (default: MEET2/ConstantSeparator.xlsx)
  -c CONFIGFILE, --configFile CONFIGFILE
                        Set the configuration file (default: config/defaultConfig.json)
  -i INPUTDIR, --inputDir INPUTDIR
                        Input directory. Read from config file by default (default: None)
  -o OUTPUTDIR, --outputDir OUTPUTDIR
                        Output directory. Read from config file by default (default: None)
  -or OUTPUTROOT, --outputRoot OUTPUTROOT
                        Output root directory, used to set the base of outputDir. Read from config file by default
                        (default: ./output)
  -t TESTINTERVALDAYS, --testIntervalDays TESTINTERVALDAYS
                        simulation / serialization duration, days (default: 30)
  -ts SCENARIOTIMESTAMP, --scenarioTimestamp SCENARIOTIMESTAMP
                        simulation / serialization identifier (timestamp) (default: None)
  -mc MONTECARLOITERATIONS, --monteCarloIterations MONTECARLOITERATIONS
                        number of MC iterations (default: None)
  -r RUNNUMBER, --runNumber RUNNUMBER
                        scenario number (default: 0)
  -w WORKERS, --workers WORKERS
                        Number of parallel python images (experimental) (default: 0)
  -m MEMORY, --memory MEMORY
                        Memory size maximum for parallel python images (experimental) (default: 8GB)
  -si, --disableSimulation
                        Disable simulation in MEETMain (default: False)
  -gr, --disableGraph   Disable graph creation in MEETMain (default: False)
  -su, --disableSummary
                        Disable summary generation in MEETMain (default: False)
  -dr DIRECTORY, --directory DIRECTORY
                        Study definition folder. Will run every study sheet in directory (default: False)
