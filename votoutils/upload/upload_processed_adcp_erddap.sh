#!/bin/bash
# Utility to rsync raw mission data to pipeline
glider=$1
mission=$2
adcpfile=$3
echo send $glider mission $mission adcp data to erddap
rsync $adcpfile "usrerddap@136.243.54.252:/data/gliderad2cp"
