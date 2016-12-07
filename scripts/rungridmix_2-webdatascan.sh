#!/bin/bash - 
set -o nounset                              # Treat unset variables as an error
# set -xv


# Hadoop installation
# set var only if it has not already been set externally
export EXAMPLE_JAR="${HADOOP_HOME}/medusa-1-examples.jar"
export EXAMPLE_JOB="mywebdatascan"

## Data sources
INDIR=$1
OUTDIR=$2

Date=$(date +%F-%H-%M-%S-%N)
echo $Date >  $1_start.out

${HADOOP_HOME}/bin/hadoop jar $EXAMPLE_JAR $EXAMPLE_JOB $INDIR $OUTDIR


Date=$(date +%F-%H-%M-%S-%N)
echo $Date >  $1_end.out

