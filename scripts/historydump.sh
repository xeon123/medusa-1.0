#!/bin/bash

######
# get history of the jobs
######
#set -xv

OUTPUT=$($HADOOP_HOME/bin/hdfs dfs -ls -R $HADOOP_HOME/logs/history/done/ |grep "jhist$" | awk '{ print $NF }')

i=0
while read -r field1 _;do
    ((i++ < 2)) && continue
    $HADOOP_HOME/bin/mapred job -history "$field1"
done <<< "$OUTPUT"

