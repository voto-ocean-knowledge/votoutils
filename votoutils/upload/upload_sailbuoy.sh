#!/bin/bash
# Utility to rsync raw mission data to pipeline
platform=$1
mission=$2
filesdir=$3
echo send $platform mission $mission data to pipeline
tgtdir=/data/data_raw/complete_mission/$platform/M$mission
echo make directory on target if it does not already exist
ssh pipeline@88.99.244.110 mkdir -p $tgtdir
echo ""
echo rsync data
rsync -ruv --stats $filesdir/* "pipeline@88.99.244.110:$tgtdir"
echo Finished
