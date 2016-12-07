#!/bin/bash


hosts=(host1 host2 host3)

for i in "${hosts[@]}";
do
    echo "Copy $1 to $i:$2"
    scp -r $1 user@$i:$2
done
