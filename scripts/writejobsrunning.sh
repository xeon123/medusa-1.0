#!/bin/bash

# Write job history into a file

LOGLINE=$1
TEMP_HOME=$MAPREDUCE_MANAGER/temp

echo "$LOGLINE" >> $TEMP_HOME/jobs_data
