#!/bin/bash
#set -xv


xxxdiff() {
    ((diff_sec=$2-$1))
    echo - | awk '{printf "%d:%d:%d","'"$diff_sec"'"/(60*60),"'"$diff_sec"'"%(60*60)/60,"'"$diff_sec"'"%60}'
    echo ""
    echo "$3: $diff_sec"
}


for i in `seq 1 5`; 
do
    date1=$(date +"%s")
    $HOME/Programs/hadoop/bin/hadoop jar $HOME/Programs/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.0.5-alpha.jar wordcount /wiki /wiki-output
    date2=$(date +"%s")
    xxxdiff $date1 $date2 "Global execution time" 

    /users/xeon/Programs/hadoop/bin/hadoop dfs -rmr /wiki-output
done
