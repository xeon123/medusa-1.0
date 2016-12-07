#!/bin/bash

######
# sum the size of the files in the dir and subdirs
# e.g. ./scripts/direxists.sh /user/ubuntu/gutenberg
######
#set -xv

OUTPUT=$($HADOOP_HOME/bin/hdfs dfs -ls -R $1 2> /dev/null > >(awk '{SUM += $5} END { print SUM }'))

if [ -z "$OUTPUT" ]
then
    echo "0"
else
    echo "$OUTPUT"
fi

