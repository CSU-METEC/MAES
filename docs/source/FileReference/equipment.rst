MEET equipment.json File
========================

The equipment.json file is a "`JSON Lines <https://jsonlines.org/>`_" file that contains the instantiated values for all the equipment defined in a simulation.
The site definition file describes equipment, including distributions for random values.  The equipment.json file records the picked values
for all random distributions for each Monte Carlo iteration.

Each JSON Line contains a key field, which is a list of [facilityID, unitID, emitterID, mcRun] elements.  This key can be used to match
to the identifiers in the instantaneousEvents.csv and metadata.csv files.

The best way to use this file is to:

#. Open the file as text.
#. Read a single line from the file, delimited by '\\n'.
#. Parse this line as a JSON string.
#. Extract the 'key' field from the parsed JSON.  This will be a list of strings & numbers.  If using Python, convert this to a tuple and use it as a key in a dict, with the value being the parsed JSON.

