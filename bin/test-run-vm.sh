#!/bin/bash
# set -xv

ssh xubuntu@hadoop-medusa-1 "/home/xubuntu/Programs/hadoop/bin/hdfs dfs -rmr /output1*"
ssh xubuntu@hadoop-medusa-2 "/home/xubuntu/Programs/hadoop/bin/hdfs dfs -rmr /output1*"
ssh xubuntu@hadoop-medusa-3 "/home/xubuntu/Programs/hadoop/bin/hdfs dfs -rmr /output1*"

echo "Executing simple run"
python ${MEDUSA_HADOOP_HOME}/tests/simpleRun.py
echo "End of execution"
