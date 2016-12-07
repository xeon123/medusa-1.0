#!/bin/bash

######
# get the nuber of jobs running
######
#set -xv

OUTPUT=$($HADOOP_HOME/bin/mapred job -list | awk '{ print $2;exit }')

if [ -z "$OUTPUT" ]
then
    echo "0"
else
    echo "$OUTPUT" |tr -dc '[:digit:]'
fi

