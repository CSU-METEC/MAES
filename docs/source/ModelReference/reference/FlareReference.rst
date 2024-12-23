Flare Reference
---------------

:ref:`combustion-calculations-label` describes the combustion process, where an incoming Vapor Fluid Flow is converted into Carbon Dioxide and some remaining fraction of the combustible gas species for each individual species.  A key parameter to this calculation is the destruction efficiency, :math:`d_{s}`.  The site definition sheet defines separate values of this parameter for each of the operating states of the flare:

  * **OPERATING** -- *Operating Destruction Efficiency*
  
  * **MALFUNCTIONING** -- *Malfunctioning Destruction Efficiency*
  
  * **UNLIT** -- *Unlit Destruction Efficiency*
  
In each case, the parameter can either be a numeric value between 0 - 1, or a file name.  For the first case, the same numeric value will be applied to all the gas species defined in the incoming gas composition.  In the second case, the file name points to a file under *input/CuratedData/CompressorDestructionEfficiencies*.  The contents of this file are in the standard MEET distribution file format (see :ref:`distribution_file_label`).  In this file, the value of the Destruction Efficiency column is applied to the correspoinding Species.  MEET recognizes a special Species tag, ALL, which means the same Destruction Efficiency value will be applied to all species of the incoming gas composition.
