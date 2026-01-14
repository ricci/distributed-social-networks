# Code and Data Related to Habib et al., SIGCOMM '25

This directory contains code and data regarding "Formalizing Dependence of Web Infrastructure" by Habib, et al., SIGCOMM '25

The actual original dataset is not stored here, as it is 10s of GB. It can be downloaded from: https://zenodo.org/records/15733582

`transform.py` is a simple script that transforms any of the datafiles into a 'worldwide' form (*not* what was done in the original paper) so that they can directly be consumed by `centralization_stats.py` in the parent directory

`original-hosting.csv` contains the data for hosting centralization extracted from Table F of the paper (Table 5)

