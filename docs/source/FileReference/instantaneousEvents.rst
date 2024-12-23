MEET Output File -- instantaneousEvents.csv
===========================================

The instantaneousEvents.csv file is the primary output of the simulation, logging everything that happens during the simulation.
It can grow very large (> 100 MB) during large simulations, and is designed to be easy and fast to read.  It has fixed columns and additional
information for events are stored in ancillary files.


Fields
------

eventID
  A unique number for each event.
  
facilityID, unitID, emitterID, mcRun
  Component key, can be used as keys to other files such as metadata.csv.
  
timestamp
  Simulation time event occurs (seconds).
  
duration
  Amount of time an event takes (seconds).
  
nextTS
  Timestamp of the end of the event, equivalent to timestamp + duration (seconds).
  
command
  Type of event.  Current event types:
  
  SIM-START
    Start of simulation.
	
  SIM-STOP
    End of simulation.
  STATE_TRANSITION
    State transition event for equipment.
	
  EMISSION
    Emission event -- vapor has been released to atmosphere.
	
  FLUID-FLOW
    Fluid flow rate change event.  Used for debugging.  Enable with Probes.

state
  State name for STATE_TRANSITION events.
  
event
  START (state entry) or STOP (state exit) for STATE_TRANSITION events
  
gcKey
  Link into gasComposition.csv file for EMISSION events.
  
flowID
  Fluid flow identifier for EMISSION events.
  
tsKey
  Link into emissionTimeseries.csv file for EMISSION events.
  
Implementation Considerations
-----------------------------

STATE_TRANSITION Events
***********************

* STATE_TRANSITION STOP events are seldom used.

* It is possible to have the same equipment transition out of a state and immediately into the same state.  These "pseudo transitions" are caused by changes in
  upstream fluid flows.
  
* It is possible to have zero-duration state changes.

EMISSION Events
***************

* gcKey, flowID, tsKey
    Links into :ref:`gas-compositions-label`, :ref:`fluid-flows-label`, and :ref:`emission-timeseries-label` files, respectively.
	
* Emission events for leaks are created & logged at the beginning of the simulation.  Thus timestamps are not guaranteed to be ordered.

* Multiple sources feeding into a component, such as multiple wells with different gas compositions feeding into a common separator, will produce
  multiple emission events with the same (facilityID, unitID, emitterID, mcRun, timestamp) values, differing only by flowID.
  
  