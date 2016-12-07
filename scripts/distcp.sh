#!/bin/bash

# launches distcp command
FROM=$1 # e.g., $HOST:3888/gutenberg-output2
TO=$2 # e.g., $HOST1:3888/

$HADOOP_HOME/bin/mapred distcp -m 64 webhdfs://"$FROM" webhdfs://"$TO"
