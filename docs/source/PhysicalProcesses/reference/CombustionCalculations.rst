Inputs
------

:math:`N` - Number of gas species.

:math:`g_{s}` - Fuel gas mass fraction for species :math:`s`, as a mass
ratio, :math:`\frac{kg}{kg}`

:math:`\gamma_{s}` - Fuel gas mass fraction for species :math:`s`, in
driver units, :math:`\frac{kg}{< driver >}`

:math:`d_{s}` - Destruction efficiency for gas species :math:`s` during
the combustion process, :math:`\frac{kg}{kg}`

:math:`c_{s}` - Mass of CO\ :sub:`2` emitted for complete combustion of
gas species :math:`s` during the combustion process,
:math:`\frac{kg}{kg}`

:math:`\dot{B}` - Driver value, in driver units of
:math:`\frac{< driver >}{s}`, such as :math:`\frac{bbl}{s}` or
:math:`kW` which is :math:`\frac{kJ}{s}`

Note that

.. math:: \sum_{s = 1}^{N}g_{s} = 1

Gas Combustion Calculations
---------------------------

Gas sent to combustion is given by a driver value, :math:`\dot{B}`, in
driver units (e.g. :math:`\frac{bbl}{s}`). The gas composition file
translates this driver into the mass flow of the gas stream:

.. math:: {\dot{m}}_{s} = \gamma_{s}\dot{B}

with units of :math:`\frac{kg}{s}`.

In this gas flow the fraction :math:`d_{s}`, the species-specific
destruction efficiency, is converted to *CO\ 2* and :math:`(1 - d_{s})`
is released in the exhaust of the combustion process, commonly known as
*combustion slip*. The emission rate by gas species is:

.. math:: E_{s} = \left( 1 - d_{s} \right){\dot{m}}_{s}\ \ \ \ \ \ \ \ s = 1\ldots N,\ s \neq CO_{2}

.. math:: E_{CO_{2}} = {\dot{m}}_{CO_{2}} + \sum_{s = 1}^{N}{\dot{m}}_{s}c_{s}d_{s}\ \ \ \ \ \ \ \ s = 1\ldots N,\ s \neq CO_{2}

with units of :math:`\frac{kg}{s}`. The above calculation must be
converted back to a new gas composition line for output by MEET,
:math:`\gamma_{s}^{*}`, with a driver of :math:`\dot{B}`. Substituting
the earlier equation for :math:`{\dot{m}}_{s}`:

.. math:: E_{s} = \left( 1 - d_{s} \right)\gamma_{s}\dot{B}\ \ \ \ \ \ \ s = 1\ldots N,\ s \neq CO_{2}

.. math:: E_{CO_{2}} = \left\lbrack \gamma_{CO_{2}} + \sum_{s = 1}^{N}\gamma_{s}c_{s}d_{s} \right\rbrack\dot{B}\ \ \ \ \ \ \ \ s = 1\ldots N,\ s \neq CO_{2}

This produces a new mass emission unit :math:`E_{s}` for all species. To
compute a new gas composition, this needs to be normalized by dividing
through by :math:`B\ \dot{}`:

.. math:: \gamma_{s}^{*} = \frac{E_{s}}{\dot{B}}

The new gas composition is:

.. math:: \gamma_{s}^{*} = \left( 1 - d_{s} \right)\gamma_{s}\ \ \ \ \ \ \ \ s = 1\ldots N,\ s \neq CO_{2}

.. math:: \gamma_{CO_{2}}^{*} = \gamma_{CO_{2}} + \sum_{s = 1}^{N}\gamma_{s}c_{s}d_{s}\ \ \ \ \ \ \ \ s = 1\ldots N,\ s \neq CO_{2}

with units of :math:`\frac{kg}{< driver >}`
