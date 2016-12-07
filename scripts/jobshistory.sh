#!/bin/bash

######
# get history of the jobs
######
#set -xv

OUTPUT=$($HADOOP_HOME/bin/mapred job -list all | grep "job_"| awk '{ print $1 }')

printf "<data>"
i=0
while read -r field1 _;do 
    ((i++ < 2)) && continue
    STATUS=$($HADOOP_HOME/bin/mapred job -status "$field1" 2> /dev/null)
    PARSED=$(echo "$STATUS" | awk '/^Number/,/^reduce/{print $0}' )
    STATE=$(echo "$STATUS" | awk '/Job state/{ print $3 }' )
    HDFS=$(echo "$STATUS" | gawk -F: '/HDFS: Number of bytes (read|written)/{ print $2 }' )
    SPAN=$( curl http://`hostname`:19888/jobhistory/app 2> /dev/null|grep "$field1"| awk -f $MAPREDUCE_MANAGER/scripts/subtract.awk )

    printf "<job>
\t<name>$field1</name>
\t<status>$STATE</status>
\t<span>$SPAN</span>"
    
    { read line; printf '\t<nrmaps>%s</nrmaps>\n' "$line";
        read line;printf '\t<nrreduces>%s</nrreduces>\n' "$line"; 
        read line;printf '\t<mapcompletion>%s</mapcompletion>\n' "$line"; 
        read line;printf '\t<redcompletion>%s</redcompletion>\n' "$line"; 
    } <<< "$PARSED"
    
    { read line; printf '\t<hdfsread>%s</hdfsread>\n' "$line";
        read line;printf '\t<hdfswritten>%s</hdfswritten>\n' "$line";
    } <<< "$HDFS"
    
    printf "</job>"
done <<< "$OUTPUT"
printf "</data>"
