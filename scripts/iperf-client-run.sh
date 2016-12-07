#!/bin/bash

data_size=(400 800 1200)
client_host=$1
server_host=(n2 n3 n4)

for server in ${server_host[*]}
do
    for size in ${data_size[*]}
    do
        echo $client $server $size
        $MAPREDUCE_MANAGER/scripts/iperf-client.sh $client $server $size
    done
done
