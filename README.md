# votoutils

Big bucket of VOTO scripts.

This repo works as a package. From the home directory of this repo:

```bash
pip install -r requirements.txt
pip install -e .
```

Functions can then be imported into your python scripts from votoutils e.g.:

```python
from votoutils.utilities.geocode import filter_territorial_data
```

### Organisation

Scripts within the `votoutils` package are split into directories grouped by what they do:

- **ad2cp** process the Nortek AD2CP data files.
- **alerts** for pilots: checks for emails from ALSEAMAR indicating a glider is in trouble and reads out an alert message
- **ctd** process CTD data
- **fixers** modify specific malformed datasets, insert extra metadata etc. For the admin
- **glider** process Seaexplorer data
- **monitor** monitor the pipeline to check everything is working on the pipeline
- **sailbuoy** process sailbuoy data
- **upload** send data files between servers to e.g. the website and ERDDAP
- **utilities** functions and dictionaries used by several of the other script groups

The seperate directory **pipeline** contains scripts co-ordinate the (re)processing of all data on the server. This calls functions from several other libraries, so requires the packages in `requirements-prod.txt`
