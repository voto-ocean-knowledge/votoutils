#!/bin/bash
# script to process a single mission in the background and log the progress
nohup /home/pipeline/micromamba/envs/process/bin/python /home/pipeline/utility_scripts/pyglider_single_mission.py $1 $2 > /data/log/complete_mission/error_complete_SEA$1_M$2.log 2>&1 &
