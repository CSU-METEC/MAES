.. _emission-timeseries-label:

MEET Output File -- emissionTimeseries.csv
==========================================

The emissionTimeseries.csv file records fluid flow rates.

Fields
------

tsKey
  Timeseries key from instantaneousEvents.csv.  
  
tsClassName
  Descriptor for type of timeseries.  Only constant timeseries are currently supported.
  
tsOffset, tsDurationPct
  Not used.
  
tsUnits
  Unit for rate.
  
tsValue
  rate in tsUnits / second.