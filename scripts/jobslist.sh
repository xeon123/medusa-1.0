#!/bin/bash

######
# gives information about the jobs running
######
#set -xv

#QUEUE=$1
#OUTPUT=$(mapred job -list | grep $QUEUE)

OUTPUT=$($HADOOP_HOME/bin/mapred job -list)

printf "<data>
\t<global>\n"
GPARSED=$(echo "$OUTPUT" | cut -d ":" -f2 )
{ read line; printf '\t\t<total>%s</total>\n' "$line"; } <<< "$GPARSED"

printf "\t\t<time>$( date +%s%N | cut -b1-13)</time>
\t</global>
\t<jobs>\n"

i=0
while read -r field1 field2 field3 _;do 
    ((i++ < 2)) && continue
    STATUS=$($HADOOP_HOME/bin/mapred job -status "$field1" 2> /dev/null)
    PARSED=$(echo "$STATUS" | awk '/^Number/,/^reduce/{print $0}' )
    HDFS=$(echo "$STATUS" | gawk -F: '/HDFS: Number of bytes (read|written)/{ print $2 }' )

    printf "\t\t<job>
\t\t\t<name>$field1</name>
\t\t\t<status>$field2</status>
\t\t\t<starttime>$field3</starttime>\n"
    
    { read line; printf '\t\t\t<nrmaps>%s</nrmaps>\n' "$line";
	read line;printf '\t\t\t<nrreduces>%s</nrreduces>\n' "$line"; 
	read line;printf '\t\t\t<mapcompletion>%s</mapcompletion>\n' "$line"; 
	read line;printf '\t\t\t<redcompletion>%s</redcompletion>\n' "$line"; 
    } <<< "$PARSED"

    { read line; printf '\t\t\t<hdfsread>%s</hdfsread>\n' "$line";
	read line;printf '\t\t\t<hdfswritten>%s</hdfswritten>\n' "$line";
    } <<< "$HDFS"

    printf "\t\t</job>"
done <<< "$OUTPUT"
printf "\t</jobs>
</data>\n"
