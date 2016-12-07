#!/bin/bash

$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh stop historyserver

echo "Deleting history"
$HADOOP_HOME/bin/hdfs dfs -rmr /tmp/hadoop-yarn/staging/history/*
$HADOOP_HOME/bin/hdfs dfs -rmr /tmp/hadoop-yarn/staging/ubuntu/.staging/*
$HADOOP_HOME/bin/hdfs dfs -rmr /tmp/logs/ubuntu/logs/*
$HADOOP_HOME/bin/hdfs dfs -rmr $HADOOP_HOME/logs/*

$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh start historyserver
