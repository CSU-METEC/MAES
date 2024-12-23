MEET Output File -- summaryState.csv
====================================

The summaryState.csv file contains a summary of state timings by equipment for the duration of the simulation.

Fields
------

facilityID, unitID, emitterID, mcIter
  Identification information
  
state
  Name of the state.
  
count
  Number of times equipment entered the state.
  
duration
  Total time equipment was in the state.
  
minDuration, maxDuration, meanDuration
  Minimum time, maximum time, and mean time equipment was in the state.
  
totalCount
  Total count equipment was in all states.
  
totalDuration
  Total time equipent was in all states.
  
durationRatio
  Ratio of the time equipment was in a particular state vs. totalDuration.
  
countRatio
  Ratio of the number of times equipment was in a particular state vs. totalCount.
