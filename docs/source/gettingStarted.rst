CAMS Methane Emissions Estimation Tool
======================================

Updated for MEET 2.0 2/7/2022
-----------------------------

Quickstart
----------

As of June 2020, users need to (1) register to GitHub.com and (2) let
developers know your GitHub ID, so that we can allow access.

Windows
~~~~~~~

If you don't have python on your system, install the miniconda version
of python from ``https://docs.conda.io/en/latest/miniconda.html``. A
python version >= 3.9 is required.

You will also need a git client on your system. I prefer TortiseGit,
which includes GUI functionality on windows. You can download TortiseGit
from: ``https://tortoisegit.org/``

From the start menu, start Anaconda (64-bit) -> Anaconda Prompt
(Miniconda 3). This will start a cmd window with python in the path.

In this cmd prompt, run

``git clone https://github.com/TACC/CAMS-MEET.git -b MEET_2.0_0007``
(this will prompt you for your github username/password)

``cd CAMS-MEET``

If you installed miniconda, install the python dependencies by running
conda install:

``conda install -c conda-forge --file requirements.txt``

Set up input files, and run

``python src\MEETMain.py`` with the flags required for your analysis.  Running with no arguments will produce a default
model run.
