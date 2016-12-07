#!/bin/bash

######
# Occepy the job tracker with bogus jobs
###### 
#set -xv

LIMIT=$1
# JARFILE=$HADOOP_HOME/occupyjobtracker.jar
JARFILE=$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.6.0.jar

COUNTER=0
while [  "$COUNTER" -lt "$LIMIT" ]; do
    echo "Launch $COUNTER"
    rand=$( echo $RANDOM )
    $HADOOP_HOME/bin/hadoop jar $JARFILE pi 10 $rand
    let COUNTER=COUNTER+1
done
