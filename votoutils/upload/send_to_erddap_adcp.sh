#!/bin/bash
# Utility to rsync completed mission data to ERDDAP server
glider=$1
mission=$2
echo send $glider mission $mission data to ERDDAP
tgtdir=/data/complete_mission/$glider/M$mission/ADCP
echo make directory on target if it does not already exist
ssh usrerddap@136.243.54.252 mkdir -p $tgtdir
echo ""
echo rsync data
rsync -v  /data/data_l0_pyglider/complete_mission/$glider/M$mission/ADCP/adcp.nc  "usrerddap@136.243.54.252:$tgtdir"
echo Finished
