#!/bin/bash

######
# get the nuber of jobs running
######
#set -xv

OUTPUT=$($HADOOP_HOME/bin/hadoop job -list | awk '{ print $1;exit }')

if [ -z "$OUTPUT" ]
then
    echo "0"
else
    echo "$OUTPUT"
fi

