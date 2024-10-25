# votoutils

Big bucket of VOTO scripts.

This repo is currently being reorganised into something more like a package

Scripts are split into directories grouped by what they do:

- **ad2cp** process the Nortek AD2CP data files.
- **alerts** for pilots: checks for emails from ALSEAMAR indicating a glider is in trouble and reads out an alert message
- **ctd** process CTD data
- **fixers** modify specific malformed datasets, insert extra metadata etc. For the admin
- **glider** process Seaexplorer data
- **monitor** monitor the pipeline to check everything is working
- **pipeline** co-ordinate the (re)processing of all data on the server
- **sailbuoy** process sailbuoy data
- **upload** send data files between servers to e.g. the website and ERDDAP
- **utilities** functions and dictionaries used by several of the other script groups
