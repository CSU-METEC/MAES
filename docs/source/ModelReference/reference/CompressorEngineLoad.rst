Compressor Timing Models
~~~~~~~~~~~~~~~~~~~~~~~~

To simulate a compressor station stochastically, one must develop a time
series of the operating states, with transitions between them. In rare
cases, this may be acquired directly from SCADA records. In the more
common case when these data are not available, the primary activity data
driving emissions is an estimate of the run time of the compressor over
the simulation period. This is often expressed as an *uptime ratio,*
i.e. the fraction of time the compressor is operating.

For transmission, storage and gas processing stations, yearly averages
can be extracted from GHGRP reports because operators report the number
of hours per year in each operating mode, for each compressor unit. For
other sectors, annual data is not available, but often uptime can be
estimated from SCADA records or other sources.

Uptime is defined as:

.. math:: u = \frac{t_{OP}}{t_{OP} + t_{NOP} + t_{NOD}}

where :math:`t_{*}` is the time in each mode. Often, only
:math:`t_{off} = t_{NOP} + t_{NOD}` is known, and some estimate must be
made for the fraction of time a unit spends in NOP versus NOD mode. [1]_
To model runtimes, we use two steps: probability distributions for the
time in a mode (states) and a transition matrix to define the
probability for transition between modes (arrows). When a compressor
enters a mode, the time in that mode is randomly drawn from an
appropriate distribution. (The example below uses triangular
distributions, but others are possible.) When the time in-mode expires,
the model uses the transition matrix to move to the next step.

*Duration probability distributions:* Each :math:`t_{x}` in the prior
equation is assigned a probability distribution,
:math:`\mathcal{P}_{x}`, based upon analysis of other data or
engineering judgment. Mean values of these distributions must follow the
prior equation to produce the correct uptime, see the appendix for more
details on the calculations required to assure the probability
distributions produce the correct mean uptime during simulation.

*Transition matrix* defines the probability of transitioning from one
state to another whenever there is a choice in possible destination
states. An example table is shown below:

.. table:: Table II. Compressor leak state table.

   +-------------+------------------+-----------------+------------------+
   | Current     | **Future State** |                 |                  |
   | State       |                  |                 |                  |
   +=============+==================+=================+==================+
   |             | OP               | NOP             | NOD              |
   +-------------+------------------+-----------------+------------------+
   | OP          | 0                |  :math          |  :mat            |
   |             |                  | :`\Pi_{OP,NOP}` | h:`\Pi_{OP,NOD}` |
   +-------------+------------------+-----------------+------------------+
   | NOP         |  :mat            | 0               |  :math           |
   |             | h:`\Pi_{NOP,OP}` |                 | :`\Pi_{NOP,NOD}` |
   +-------------+------------------+-----------------+------------------+
   | NOD         |  :math:`\        | 0               | 0                |
   |             | Pi_{NOD,OP} = 1` |                 |                  |
   +-------------+------------------+-----------------+------------------+
   | Note:       |                  |                 |                  |
   | Episodic    |                  |                 |                  |
   | states      |                  |                 |                  |
   | (blowdown   |                  |                 |                  |
   | and gas     |                  |                 |                  |
   | start) are  |                  |                 |                  |
   | not shown   |                  |                 |                  |
   | in the      |                  |                 |                  |
   | matrix, as  |                  |                 |                  |
   | they are    |                  |                 |                  |
   | completely  |                  |                 |                  |
   | dependent   |                  |                 |                  |
   | upon the    |                  |                 |                  |
   | mode        |                  |                 |                  |
   | transitions |                  |                 |                  |
   | shown.      |                  |                 |                  |
   +-------------+------------------+-----------------+------------------+

Probabilities in each row sum to 1. Diagonal values are zero, because we
are drawing duration from duration probability distributions to
determine how long we are in each state. We also assume that transitions
from NOD to NOP are very rare (:math:`\Pi \cong 0`), or short – if a
depressurized compressor is repressurized, it will also be started.

Calculation of Uptime Fractions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Computing length of time in off condition to meet the uptime
requirement. We are given the uptime fraction, :math:`u`, and want to
find the scale factor, :math:`f`, that will give us that uptime factor,
where :math:`a` is the uptime in some convenient time unit, a
:math:`t_{off} = ft_{OP}`. :math:`u` is defined as:

.. math:: u = \frac{\mu_{OP}}{\mu_{OP} + f\mu_{OP}} = \frac{1}{1 + f}

.. math:: f = \frac{1}{u} - 1

Since there are multiple downtime modes, we need to scale downtimes to
considering the means and the fraction in each mode. Assuming that NOD
mode is more common the NOP, we start by estimating
:math:`\mu_{NOD} = f\mu_{OP}`. We then estimate :math:`\mu_{NOP}` using
other information. With these estimates, we have an estimated up-time
fraction:

.. math:: u_{est} = \frac{\mu_{OP}}{\mu_{OP} + \mu_{off}}

where :math:`\mu_{off} = {\Pi_{NOP}\mu}_{NOP} + \Pi_{NOD}\mu_{NOD}` and
:math:`\Pi_{x}` is the relative probability (i.e.
:math:`\Pi_{NOP} + \Pi_{NOD} = 1` where :math:`0 < \Pi_{*} < 1`) of
transitioning into either mode when leaving NOD mode. We desire
:math:`u_{est} = u` by scaling both :math:`\mu_{NOP}` and
:math:`\mu_{NOD}` by factor, :math:`g`:

.. math:: u = \frac{\mu_{OP}}{\mu_{OP} + g\mu_{off}}

Solving for :math:`g`:

.. math:: g = \frac{\mu_{OP}}{\mu_{off}}\left( \frac{1}{u} - 1 \right)

It may be desirable to also scale other parameters of the probability
distributions when scaling :math:`\mu_{x}`. Probabilities are extracted
directly from the transition matrix to calculate :math:`\Pi_{NOP}` and
:math:`\Pi_{NOD}`.

Compressor Vents and Leaks
~~~~~~~~~~~~~~~~~~~~~~~~~~

Compressors, like all pieces of major equipment, are assigned leaks in
three steps:

1. Assign components

2. Determine which components are leaking at any given time based on the
   leak probability

3. Assign emission factors to the leaking components

Each compressor is instantiated with a list of components. Each
component has a probability of leaking, based on the quantity of that
particular component, and the number of leaking components of that type
identified in a field survey or reported to GHGRP. In some cases, this
probability may be zero, where insufficient data exists.

All compressors are assumed to have the following components:

-  One single unit vent

-  One blowdown valve

-  One starter valve

-  Two isolation valves (one inlet and one outlet)

In addition, all reciprocating compressors are assumed to have one rod
packing vent, and all centrifugal compressors have either wet or dry
seal vents. Rod packing and wet/dry seal emissions are classified as
vented emissions, rather than fugitive, but functionally behave as leaks
and are treated as leaks by the simulation, although emissions are
categorized as *vented* rather than *fugitive*.

Other components vary based on data source.

Compressor leaks differ from other leaks in that they may be state
dependent. Table III lists all the possible leaks a compressor could
have, and which state(s) the leaks will be active.

.. table:: Table Combustion Factors

   +---------------------+----------+-----+-----+----------+-------------+
   |                     | RUNNING  | NOP | NOD | STARTING | BLOWDOWN    |
   +=====================+==========+=====+=====+==========+=============+
   | Blowdown valve      | X        | X   |     | X        |             |
   +---------------------+----------+-----+-----+----------+-------------+
   | Connector (Flanged) | X        | X   |     | X        | X           |
   +---------------------+----------+-----+-----+----------+-------------+
   | Connector           | X        | X   |     | X        | X           |
   | (Threaded)          |          |     |     |          |             |
   +---------------------+----------+-----+-----+----------+-------------+
   | Dry seal            | X        |     |     |          |             |
   +---------------------+----------+-----+-----+----------+-------------+
   | Isolation valve     |          | X   | X   |          | X           |
   +---------------------+----------+-----+-----+----------+-------------+
   | Meter               | X        | X   |     | X        | X           |
   +---------------------+----------+-----+-----+----------+-------------+
   | OEL                 | X        | X   |     | X        | X           |
   +---------------------+----------+-----+-----+----------+-------------+
   | Pocket vent         | X        | X   |     | X        | X           |
   +---------------------+----------+-----+-----+----------+-------------+
   | PRV                 | X        | X   |     | X        | X           |
   +---------------------+----------+-----+-----+----------+-------------+
   | Regulator           | X        | X   |     | X        | X           |
   +---------------------+----------+-----+-----+----------+-------------+
   | Rod Packing (OP)    | X        |     |     |          |             |
   +---------------------+----------+-----+-----+----------+-------------+
   | Rod Packing (NOP)   |          | X   |     |          |             |
   +---------------------+----------+-----+-----+----------+-------------+
   | Rod Packing (NOD)   |          |     | X   |          |             |
   +---------------------+----------+-----+-----+----------+-------------+
   | Single unit vent    | X        | X   |     | X        |             |
   +---------------------+----------+-----+-----+----------+-------------+
   | Starter valve       | X        | X   | X   |          | X           |
   +---------------------+----------+-----+-----+----------+-------------+
   | Wet seal            | X        |     |     |          |             |
   +---------------------+----------+-----+-----+----------+-------------+
   | Valve               | X        | X   |     | X        | X           |
   +---------------------+----------+-----+-----+----------+-------------+

Compressor Power & Exhaust Modelling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Inputs
^^^^^^

:math:`P` - Engine rated power, in kW

:math:`l` - Load on engine as a fraction of rated load, at any given
time

:math:`\eta_{e}` - Engine efficiency as a ratio of shaft output (kW) to
fuel input (kW … kJ/s)

:math:`N` - Number of gas species.

:math:`g_{s}` - Fuel gas mass fraction for species :math:`s`, as a mass
ratio, :math:`\frac{kg}{kg}`

:math:`\gamma_{s}` - Fuel gas mass fraction for species :math:`s`, in
driver units, :math:`\frac{kg}{< driver >}`

N\ :sub:`s` - Lower heating value (LHV) of gas species :math:`s`.
(REFPROP uses the term *net heating value*)

:math:`c_{s}` - Mass of CO\ :sub:`2` emitted for complete combustion of
gas species :math:`s` during the combustion process,
:math:`\frac{kg}{kg}`

:math:`\dot{B}` - Driver value, in driver units of
:math:`\frac{< driver >}{s}`, such as :math:`\frac{bbl}{s}` or
:math:`kW` which is :math:`\frac{kJ}{s}`

Note that

.. math:: \sum_{s = 1}^{N}g_{s} = 1

Compressor Power Model
^^^^^^^^^^^^^^^^^^^^^^

The mass fuel flow rate through the prime mover is set by the power
delivered by the prime mover. Therefore, :math:`{\dot{m}}_{s}` must be
calculated from engine power.

Each time a prime mover starts, the load level may change. Resulting
total load on the engine is :math:`P_{l}` (in kW). Applying the
efficiency of engine, the fuel power (heat rate) required is:

.. math:: \dot{E_{l}} = \frac{P_{l}}{\eta_{e}}\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \left\lbrack \frac{kJ}{s} \right\rbrack

While MEET currently uses a single efficiency value, in general
efficiency is a function of load on the engine, relative to rated load,
:math:`\phi_{e} = \frac{P_{l}}{P_{r}}` where :math:`P_{r}` is the rated
load of the engine/driver. In general, the function:

.. math:: \eta_{e} = \eta_{e}(\phi_{e})

is implemented as a quadratic or cubic equation. :math:`{\dot{E}}_{l}`
is the energy input rate – i.e. input fuel power – required by the
engine/driver to produce :math:`P_{l}` output power. Internally in MEET,
this is translated to :math:`kW`, or more precisely,
:math:`\frac{kJ}{s}`.

The incoming fuel gas composition is defined in terms of
:math:`\frac{kg}{< driver >}`; currently MEET always uses kg/scf. Based
upon the driver, the heat rate of the incoming fuel is:

.. math:: N_{f} = \sum_{s = 1}^{N}{{\dot{m}}_{s}N_{s}}\ \ \ \ \ \ \ \ \ \ \ \ \ \left\lbrack \frac{kg}{scf} \bullet \frac{kJ}{kg} = \frac{kJ}{scf} \right\rbrack

where :math:`N_{s}` is the heat rate – lower or higher heating value –
of each species in :math:`\frac{kJ}{kg}`. Either higher or lower heating
value can be used provided :math:`\eta_{e}` is based on a consistent
definition of the fuel’s heating value. :math:`N_{f}` is the fuel’s
heading value, in the fuel’s driver unit; for typical MEET fuel gas
compositions, this would be in :math:`\frac{kJ}{scf}`.

Dividing :math:`{\dot{E}}_{l}` provides the fuel flow rate to the engine
or turbine:

.. math:: {\dot{Q}}_{l} = \frac{\dot{E_{L}}}{N_{f}}\ \ \ \ \ \ \ \ \ \left\lbrack \frac{kJ}{s} \bullet \frac{scf}{kJ} = \frac{scf}{s} \right\rbrack

For current implementation of MEET, :math:`{\dot{Q}}_{l}` would be in
units of :math:`\frac{scf}{s}`. The incoming gas composition,
:math:`f_{s}`, should be in :math:`\frac{kg}{scf}`, which allows the
mass flow by species to be

.. math:: {\dot{m}}_{s} = {\dot{Q}}_{l}f_{s} = \frac{{\dot{E}}_{l}}{N_{f}}f_{s} = \frac{P_{l}}{\eta_{e}N_{f}}f_{s} = P_{l}\left( \frac{f_{s}}{\eta_{e}N_{f}} \right)\ \ \ \ \ \ \ \ \left\lbrack \frac{kJ}{s} \bullet \frac{kg}{scf} \bullet \frac{scf}{kJ} = \frac{kg}{s} \right\rbrack

Assuming that the driver MEET outputs is the load on the engine, then by
analogy to :math:`{\dot{m}}_{s} = \gamma_{s}\dot{B}`, the incoming,
modified, gas composition for an engine is:

.. math:: \gamma_{s}^{(e)} = \frac{f_{s}}{\eta_{e}N_{f}}\ \ \ \ \ \lbrack\frac{kg}{scf}*\frac{scf}{kJ} = \ \frac{kg}{kJ}\rbrack

+---------+------+-------+--------------+--------------+--------------+
| Name    | Ca   | Hyd   | kgCO         | kgH2         | Lower        |
|         | rbon | rogen | 2perKgAlkane | OperKgAlkane | heating      |
|         |      |       |              |              | Value, Ns    |
|         |      |       |              |              | (kJ/kg)\*    |
+=========+======+=======+==============+==============+==============+
| METHANE | 1    | 4     | 2.743332215  | 2.245931009  | 50048        |
+---------+------+-------+--------------+--------------+--------------+
| ETHANE  | 2    | 6     | 2.927235334  | 1.797367605  | 47800        |
+---------+------+-------+--------------+--------------+--------------+
| PROPANE | 3    | 8     | 2.994140808  | 1.634176531  | 46400        |
+---------+------+-------+--------------+--------------+--------------+
| BUTANE  | 4    | 10    | 3.028753708  | 1.54975121   | 44862        |
+---------+------+-------+--------------+--------------+--------------+
| IS      | 4    | 10    | 3.028753708  | 1.54975121   | 45300        |
| OBUTANE |      |       |              |              |              |
+---------+------+-------+--------------+--------------+--------------+
| PENTANE | 5    | 12    | 3.049908257  | 1.498152537  | 45241        |
+---------+------+-------+--------------+--------------+--------------+
| ISO     | 5    | 12    | 3.049908257  | 1.498152537  | 45400        |
| PENTANE |      |       |              |              |              |
+---------+------+-------+--------------+--------------+--------------+
| HEXANE  | 6    | 14    | 3.064176231  | 1.46335111   | 44752        |
+---------+------+-------+--------------+--------------+--------------+
| HEPTANE | 7    | 16    | 3.074449648  | 1.438292924  | 44925        |
+---------+------+-------+--------------+--------------+--------------+
| OCTANE  | 8    | 18    | 3.082200032  | 1.419388738  | 44786        |
+---------+------+-------+--------------+--------------+--------------+

**\*References**

https://www.engineeringtoolbox.com/heating-values-fuel-gases-d_823.html

https://link.springer.com/content/pdf/bbm%3A978-1-4419-7943-8%2F1.pdf

.. [1]
   Some companies do not leave stopped compressors in a pressurized
   state, while others use that mode extensively. For example, storage
   compressors may be in NOP mode for extended period to provide fast
   response to gas demand changes.
