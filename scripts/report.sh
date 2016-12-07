#!/bin/bash

######
# Get jeb status every 10 min
######
set -xv

FILENAME="./report-`hostname`.txt"
while [ 1 ];
do
  echo "SEPARATOR ############################################" >> ${FILENAME}
  source jobslist.sh >> ${FILENAME}
  echo "SEPARATOR ############################################" >> ${FILENAME}
  sleep 600
done
