# Mechanistic Air Emissions Simulator - MAES

## Quickstart

### Requirements
* Python 3.11 or later
* Git
* Python environment manager (e.g., Anaconda, Miniconda, pipenv or virtualenv)

### Installation
To install MAES, clone the repository, create & configure a virtual enviroment.  
This example assumes you are using conda.  If you don't have it, download & install from
[miniconda install](https://www.anaconda.com/download/success) -- 
scroll past the Anaconda installers to the Miniconda installers.
1. Start an anaconda prompt
1. `conda create -n MAES python=3.12`
1. `conda activate MAES`
1. `git clone https://github.com/CSU-METEC/MAES`
1. `cd MAES`
1. `conda install -c conda-forge --file requirements.txt`

### Create a project structure

Note -- this section assumes MAES was installed in c:\MAES.  
If you installed it elsewhere, use that directory in the following instructions

1. Create a directory *outside* of c:\MAES for your project files.  For purposes of these instructions, 
assume it is c:\MAES-Project. 
1. Copy the config and input directories from C:\MAES to c:\MAES-Project
1. Start pycharm in c:\MAES-Project
1. Set the virtual envirnment you created above to be the python interpreter
1. Set the c:\MAES\src directory as a content root & mark it as a source directory
1. Run c:\MAES\src\SiteMain2.py & edit the configuration:
   1. Set the working directory to be c:\MAES-Project
   1. Add `-s MEET2/ConstantSeparator.xlsx` as a script parameter
1. This should produce a MAES run in the pycharm output window.  
If it does so, you have successfully installed & configured MAES.

