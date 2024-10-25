#!/bin/bash
# Utility to list state of a mission in processing. Requires glider number and mission number e.g. 61 39
glider=$1
mission=$2
echo SEA$glider mission $mission data
echo ""
echo raw data
ls -d1 /data/data_raw/complete_mission/SEA$glider/M$mission*

echo ""
echo processed data
ls /data/data_l0_pyglider/complete_mission/SEA$glider/M$mission*
echo ""
echo timeseries
ls /data/data_l0_pyglider/complete_mission/SEA$glider/M$mission/timeseries
echo if no timeseries file here, run recombine.py
echo ""
echo grid file
ls /data/data_l0_pyglider/complete_mission/SEA$glider/M$mission/gridfiles
echo if no gridded data file here, run recombine.py
echo ""
echo plots
ls /data/plots/complete_mission/SEA$glider/M$mission
