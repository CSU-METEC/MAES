’OVERPRESSURE VENT’ in the state machine include thief hatches,
vents/prv that pop open when the pressure goes above a certain
threshold. The over-pressure situation is calculated using a fluid flow
threshold for gas inlet flows. The pressure is measured as a function of
change of vapor fluid flows. These emissions are the mechanistically
modeled large emitters. ’OVERPRESSURE LEAK’ in the state machine implies
accidental opening of the vents that are responsible for undesired
emissions. We first initialize the start machine to OPERATING to record
the proportional flow from separators or a fixed source. The criteria
for state change is as follows:

.. math::

   rateTransform=
   \begin{cases}
       x& sum(flash+sdvGas)<threshold \\
       \frac{mx}{S}+c& threshold<sum(flash+sdvGas)\leq primaryThreshold\\
       \frac{x}{S}+c& sum(flash+sdvGas)>primaryThreshold
   \end{cases}

.. math::

   state=
       \begin{cases}
           \text{OPERATING}& sum(flash+sdvGas)<threshold \\
           \text{OVERPRESSURE VENT}& threshold<sum(flash+sdvGas)\leq primaryThreshold\\
           \text{OVERPRESSURE VENT}& sum(flash+sdvGas)>primaryThreshold
       \end{cases}

| Where,
| x = inlet flow,
| m = slope of the primary equipment flow
  (:math:`\frac{\text{outletFlow from tanks}}{\text{sdvInletFlow into tanks}}`),
| S = Sum of the gas from sdv + tank flashes,
| c = y intercept of the flare flow. All the remaining flow goes to the
  tank vents by subtracting the rateTransform equations by x,
| flash = tank flash,
| threshold = PRV threshold (column "Overpressure Vents Large Emitter
  Threshold"),
| primaryThreshold = value in the column ‘Max Primary Outlet Flow’.

The parameter rateTransform refers to the transformation of fluid flows
from inlet flow to outlet flow. The outlet flows from the above
rateTransform calculations go to the primary downstream equipment
(flares), which can be combusted later. Vents have another parameter
called prvSwitch that turns on the prv flow when the prvs start to open
after the gas flows reach the threshold. The amount of gas going to the
prv is given by :math:`rateTransform - x`, which is the source of large
emissions.

Tanks are in the OPERATING state until the prv threshold is reached,
implying prv opens and we get emissions out of the prv. Since
:math:`prvFlow=flareFlow-x`, the total volume of gas is conserved.

From the equations, we can see two ’OVERPRESSURE VENT’ behaviors which
are illustrated in the diagram below. One example of over-pressure in
tanks is due to stuck dump valve in upstream separator/s.

Note that we’re adding gas flows with different gas compositions as a
weighted sum of the gas densities without considering the gas
compositions themselves. This is an approximate and actual mixing
routine is yet to be implemented.

The remaining state from the above equations is the OVERPRESSURE LEAK.
This state occurs randomly depending on the pLeak. It must be noted that
OVERPRESSURE VENT triggers when there is an overpressure situation and
OVERPRESSURE LEAK triggers randomly depending on pLeak.
