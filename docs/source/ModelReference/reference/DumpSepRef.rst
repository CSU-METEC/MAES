Dumping Separator Reference

The initial state always starts at filling to make sure we have the
correct proportions from wells or separators or fixed source. The fluid
flows follow the equations:

.. math:: liquidFlow=\frac{liquidVolume}{totalVolume}*currentDriverMultiplier

Where, liquid = oil or water, total = oil+water, and

.. math:: flash=liquidFlow*bblToscfConversionFactor*x

.. math:: flashSales=liquidFlow*bblToscfConversionFactor*(1-x)

.. math:: gasFlow=x*inletGasFlow

.. math:: gasSales=(1-x)*inletGasFlow

Where flash = flashes from condensate and water that go the next
equipment when the separator dump valve is stuck, flashSales = flashes
that go to sales lines, gasFlow = gas from previous stage that goes on
to the next equipment when the separator is stuck, gasSales = gas that
goes to the sales pipeline, bbl-scf-conversionFactor = Driver Factor
from gc files, currentDriverMultiplier = dumping rate of the separator,
x = any random fraction that goes to gas sales when the dump valve is
stuck. When we’re in Filling state, currentDriverMultiplier is set to 0
to indicate none of the flows are dumping, all gas+flash foes to gas
sales. When we’re in the Dumping state, currentDriverMultiplier is set
to DumpVolume/DumpTime indicating the liquids dump at the same
proportional rate as they are filled. We set x to 1 in both the states,
indicating both all the gas going to gas sales. When we’re in the Stuck
Dump Valve state, currentDriverMultiplier is set to 1 indicating the
liquids dump as they’re entering the separator. x set to a random gas
fraction indicating some gas is going to the next equipment. This excess
of gas is the basis of a large emission event in the tanks. Large
emitter failures in separators is not yet modeled.

Working of Dumping Separators without the stuck dump valve failure mode
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the fluid flow process, we sum up all the flows that have the same
gas compositions wrt individual fluid. Since a dumping separator depends
on the flows from the downstream equipment, it is necessary to query the
fluid flow from those equipment. This is done by storing the next fluid
flow change time in a variable (changeTimeAbsolute) when we’re running
the simulation. This needs to be stored from the start of the
simulation, starting from wells and are updated at every stage. Once we
know the time for next fluid flow rate change and the current volume (we
update current volume in the initial Filling state, another reason to
start at Filling state), we can have a state change at
changeTimeAbsolute. Suppose we’re in the filling state initially, we
step out of DES to check when the driver rate changes, now we can update
the current volume and check again when the separators will fill up
(remaining Volume divided by incoming driver rates). If this time is
smaller than when the incoming driver rates change (changeTimeAbsolute),
we set the next state change to this time. If this time is larger than
changeTimeAbsolute, the next state update happens at changeTimeAbsolute,
thus updating the volume at every state. This routine behaves as an
integrator since we add all the flow rates to calculate the volume. Now
that the volume has filled up, we change the state to Dumping. The
outlet driver rate would be DumpVolume/DumpTime, multiplied by
respective ratios to get the appropriate fractions. Gas flows are always
going to gas sales when we’re dumping or filling. Now for stage 2
separators, we need this changeTimeAbsolute again to check whether the
dumping rate is faster or slower than stage1. If we have a slower
dumping rate, the code piles up the volume, and dumps continuously till
all the liquid is dumped, thus using the same integrator to update and
conserve all the volume. This assumes that the separator has an infinite
capacity of liquids. Such an extreme behavior is never designed in real,
making it an interesting boundary value kind of a problem. If the stage
2 rate is faster than or equal to stage 1, the stage 2 will dump
normally at the given dumpTime. If any stage sep has incoming fluid
flows when it is dumping, we update the volume again for the amount
filled when the sep was dumping (updated till changeTimeAbsolute). This
volume is added to the filling volume when state changes back to
filling. It is important to note that when we add up the driver rates to
calculate volume, we ignore the effects of the gas composition on the
driver rates. This is not a very accurate way of calculating the volume,
but it is an approximation. To get better accuracy, a mixing routine
needs to be written.

Working of dumping separators with the Stuck Dump Valve failure mode
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The failure mode of dumping separator with a stuck dump valve is modeled
as a state which can get triggered after the separator has been filled
or dumped, i.e., when he dump-valve opens or closes. pLeak determines
when the dump valve gets stuck by calculating the values of MTBF using
the mttr-mtbf formula. The duration of this state is defined by a
uniformly distributed random variable MTTR which are defined as inputs.
FluidFlows are the same as defined in Continuous separators. We track
the MTTR/MTBF time each time we change fluidflow / state.

Volume Integrator setup instructions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A few steps on how the volume integrator works and how to set it up.

#. Create a variable for updating volume in the init method in a class
   of interest and store it in self.

#. Create a function that can be stored as a bound method when we define
   the state machine dictionary. It can either be stored in the
   ‘stateDuration’ key or ‘nextState’ key as desired.

#. We get to this function when the DES decides to change the state
   while initializing state machine or during DES runs. We also update
   the volume variable at the time of initialization.

#. At each iteration of this function, calculate the change in volume,
   :math:`dV`, (in case of dumping separators, remainingVolume =
   dumpVolume - currentVolume). Calculate the time at which inlet flow
   or outlet flow changes (min (changeTimeAbsolute, FillDuration) in
   dumping separators). Calculate change in time between states,
   :math:`dT`, (min (changeTimeAbsolute, FillDuration) – currentTime).
   Calculate change in driver rate, :math:`dR`, by subtracting inlet
   flow rate to outlet flow rate in the time change that we calculated.

#. Now we have :math:`dV`, :math:`dT` and :math:`dR` (change in volume,
   change in time, and change in driver rate). So, the new
   :math:`volume = V = dV + (dR)(dT)`. This value of volume updates at
   every state change that we desire, and the volume updates itself till
   the end of simulation.

#. We may choose to override the volume updates whenever necessary
   depending on the complexity of the model (example: when the separator
   starts dumping, volume must be reset to 0).

#. The previous three steps are usually slightly different for different
   applications. It follows a general equation of the type:
   :math:`V=\int_{t_1}^{t_2}dRdt`, where V = Volume, dR = change in
   driverRate, dt = change in time (t2-t1). This equation is an
   approximate due to the highly dynamic behavior of fluids and the
   surroundings; hence, it is a work in progress.

Validating Model Behavior
~~~~~~~~~~~~~~~~~~~~~~~~~

#. Like in wells, we note down the input data (pLeak, MTTR and MTBF) and
   run the simulation for a long time, multi-mc runs.

#. Calculate the average of each state time one mc at a time and/or all
   mcs together and observe if the states match the input columns.
   Average state time for Stuck Dump Valve state must be in the range of
   the MTTR values. We also calculate Operating state average time, this
   value is the MTBF. Once we have average MTTR and MTBF, we can
   calculate the average pLeak. Compare this value to the pLeak in the
   input sheet. The average pLeak from the outputs must tend towards
   pLeak in the input sheet as simtime and/or mcRuns tend to infinity.
   It must be noted that the states are changing with change in fluid
   flows. So we’ll get repeated state changes one after the other. These
   should be merged together as one state change. Zero duration states
   are an effect of calculations in DES and those can be ignored.

