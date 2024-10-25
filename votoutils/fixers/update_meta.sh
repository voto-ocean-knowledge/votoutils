#!/bin/bash
# script to update metadata of a single mission in background
nohup /data/miniconda/envs/pyglider/bin/python /home/pipeline/utility_scripts/update_meta.py $1 $2 raw > /data/log/update_meta/error_SEA$1_$2.log 2>&1 &
