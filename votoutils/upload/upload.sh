#!/bin/bash
# Utility to rsync raw mission data to pipeline
glider=$1
mission=$2
filesdir=$3
echo send $glider mission $mission data to pipeline
tgtdir=/data/data_raw/complete_mission/$glider/M$mission
echo make directory on target if it does not already exist
ssh pipeline@88.99.244.110 mkdir -p $tgtdir
echo ""
echo rsync data
rsync -v --stats $filesdir/NAV/*.$mission* "pipeline@88.99.244.110:$tgtdir"
rsync -v --stats $filesdir/PLD_raw/*.$mission* "pipeline@88.99.244.110:$tgtdir"
echo Finished
