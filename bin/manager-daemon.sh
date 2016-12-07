#!/bin/bash

HOSTNAME=`hostname`
MAPREDUCE_MANAGER_HOME=/home/pcosta/Programs/medusa-1.0
SRC_HOME=$MAPREDUCE_MANAGER_HOME/src/manager

echo "Starting network manager"
python $SRC_HOME/networkmanager.py `hostname` &
echo "PID=$!"


# echo "Starting jobs manager"
# python $SRC_HOME/jobsmanager.py &
# echo "PID=$!"
