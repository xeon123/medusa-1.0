#!/bin/bash

######
#generate the digests from the input path
######
#set -xv

output="$($HADOOP_MAPREDUCE/bin/hadoop dfs -lsr $1 2> /dev/null > >(awk '{print $8}'))"

count=0
declare -A digests
for path in $output
do
    digests[$count]=$( $HADOOP_HOME/bin/hadoop dfs -cat "$path" | sha256sum | awk '{ print $1 }')
    (( count ++ ))
done

for i in "${digests[@]}"
do  
    echo $i
done
