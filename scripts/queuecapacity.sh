#!/bin/bash

######
# Get capacity of the queue
######
#set -xv

OUTPUT=$($HADOOP_HOME/bin/mapred queue -list 2>/dev/null|grep Scheduling )

PARSED=$( awk -F' : |, ' '{printf "%s\n%s\n%s\n",$2,$3,$4}' <<< $OUTPUT )
PARSED2=$( awk '{printf "%s\n%s\n%s\n",$2,$4,$6}' <<< $PARSED )

#Scheduling Info : Capacity: 100.0, MaximumCapacity: 1.0, CurrentCapacity: 0.0
printf "<data>
\t<global>\n"
{ read line; printf '\t\t<capacity>%s</capacity>\n' "$line";
    read line; printf '\t\t<maximumcapacity>%s</maximumcapacity>\n' "$line";
    read line; printf '\t\t<currentcapacity>%s</currentcapacity>\n' "$line";
} <<< "$PARSED2"

printf "\t</global>
</data>\n"
