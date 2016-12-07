#!/bin/bash

# This file starts Hadoop MapReduce

hadoop-daemon.sh start namenode
hadoop-daemons.sh start datanode
yarn-daemon.sh start resourcemanager
yarn-daemons.sh start nodemanager
mr-jobhistory-daemon.sh start historyserver
