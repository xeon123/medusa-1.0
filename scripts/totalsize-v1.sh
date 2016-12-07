#!/bin/bash

######
# sum the size of the files in the dir and subdirs
######
#set -xv

OUTPUT=$($HADOOP_MAPREDUCE/bin/hadoop dfs -lsr $1 2> /dev/null > >(awk '{SUM += $5} END { print SUM }'))

if [ -z "$OUTPUT" ]
then
    echo "0"
else
    echo "$OUTPUT"
fi

