#!/bin/bash

######
#generate the digests from the input path
######
#set -xv

output="$($HADOOP_HOME/bin/hdfs dfs -ls -R $1  | awk '{print $8}')"

count=0
declare -a digests
for path in $output
do
    # sha256sum
    filename[$count] = $path
    digests[$count]=$( $HADOOP_HOME/bin/hdfs dfs -cat "$path" | sha256sum | awk '{ print $1 }')
    (( count ++ ))
done

for i in "${digests[@]}"
do  
    echo $i
done
