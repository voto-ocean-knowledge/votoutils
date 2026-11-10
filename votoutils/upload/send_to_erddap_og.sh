#!/bin/bash
# Utility to rsync completed mission data to ERDDAP server
glider=$1
mission=$2
kind=$3
echo send $glider mission $mission data to ERDDAP
tgtdir=/data/OG_$kind/$glider/M$mission/timeseries
echo make directory on target if it does not already exist
ssh usrerddap@136.243.54.252 mkdir -p $tgtdir
echo ""
echo rsync data
rsync -v  /data/data_l0_pyglider/OG_$kind/$glider/M$mission/timeseries/s*.nc  "usrerddap@136.243.54.252:$tgtdir"
echo Finished
