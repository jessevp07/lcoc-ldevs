Code for "Levelized Cost of Charging Electric Vehicles in the United States"  
============================================================================

Code to reproduce results from Borlaug et al. study published in [Joule](https://doi.org/10.1016/j.joule.2020.05.013) in July 2020. **This software is provided as-is without dedicated support and has not been developed and/or tested for compatibility with platforms outside of MacOS (10.15.7)**. The update_dcfc.py script is not optimized and was executed for this study on NREL's high performance computing resources. It is not recommended that this be run locally (e.g., on a laptop) as it may lock up the machine for multiple days. This code is provided for complete transparency and so that others may reuse aspects if they so wish.

Requires the conda package manager, see Anaconda [website](https://docs.conda.io/en/latest/miniconda.html) for details. Use the terminal or an Anaconda Prompt to activate the Python programming environment for this study –  
  
`conda env create -f environment.yml`

The `nbs/` folder contains Jupyter notebooks demonstrating applications of the code developed for this study.

Citation  
--------
Please cite as:  
```Borlaug, B., Salisbury, S., Gerdes, M., and Muratori, M., Levelized Cost of Charging Electric Vehicles in the United States, Joule (2020), https://doi.org/10.1016/j.joule.2020.05.013```  
  
License  
-------
This code is licensed for use under the terms of the Berkeley Software Distribution 3-clause (BSD-3) lincense; see LICENSE.
