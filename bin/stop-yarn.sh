#!/bin/bash

# This file stops Hadoop MapReduce

hadoop-daemon.sh stop namenode
hadoop-daemons.sh stop datanode
yarn-daemon.sh stop resourcemanager
yarn-daemons.sh stop nodemanager
mr-jobhistory-daemon.sh start historyserver
