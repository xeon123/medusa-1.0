#!/bin/bash - 
set -o nounset                              # Treat unset variables as an error
set -xv


GRID_DIR=$(dirname "$0")
GRID_DIR=$(cd "$GRID_DIR"; pwd)

# Hadoop installation
# set var only if it has not already been set externally
export EXAMPLE_JAR="${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.6.0.jar"
export APP_JAR="${HADOOP_HOME}/share/hadoop/mapreduce/sources/hadoop-mapreduce-client-jobclient-2.6.0-test-sources.jar"
export STREAM_JAR="${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.6.0-tests.jar"

export CLASSFILE="org.apache.hadoop.mapred.GenericMRLoadGenerator"

## Data sources
INDIR=$1
OUTDIR=$2
#/out-compressed_$Date

NUM_OF_REDUCERS=4
Date=$(date +%F-%H-%M-%S-%N)
echo $Date >  $1_start.out

# OUTDIR=perf-out/webdata-scan-out-dir-small_$Date
# ${HADOOP_HOME}/bin/hadoop dfs -rmr $OUTDIR

${HADOOP_HOME}/bin/hadoop jar $EXAMPLE_JAR wordcount $INDIR $OUTDIR
# ${HADOOP_HOME}/bin/hadoop jar $APP_JAR $CLASSFILE -keepmap 1 -keepred 5 -inFormat org.apache.hadoop.mapred.SequenceFileInputFormat -outFormat org.apache.hadoop.mapred.SequenceFileOutputFormat -outKey org.apache.hadoop.io.BytesWritable -outValue org.apache.hadoop.io.BytesWritable -indir $INDIR -outdir $OUTDIR -r $NUM_OF_REDUCERS


Date=$(date +%F-%H-%M-%S-%N)
echo $Date >  $1_end.out

