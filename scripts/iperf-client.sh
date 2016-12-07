#!/bin/bash

client=$1
server=$2
packetsize=$3

N=$client-$server-$packetsize.json;
echo "" > $MEDUSA_HOME/temp/$N

iperf3 -c $server -f k -n $packetsize -J >> $MEDUSA_HOME/temp/$N
