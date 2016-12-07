#!/bin/bash
#set -xv


now=$(date "+%s")

COMMAND=$1
DIGESTCOMMAND=$2

dir=$MAPREDUCE_MANAGER/tests_logs/jobs/

#if [ [ ! -d $dir ] ]; then
#    mkdir -p $MAPREDUCE_MANAGER/tests_logs/jobs/
#fi


JOBSRUNNING=$( $MAPREDUCE_MANAGER/scripts/jobsrunning.sh )

OUTPUT="$MAPREDUCE_MANAGER/tests_logs/jobs/hadoop_mapreduce_$now.log"
jobstart=$(date "+%s")
$COMMAND > $OUTPUT 2>&1
jobend=$(date "+%s")
JOBDIFF=$(( $jobend - $jobstart ))

# redirect stderr (2) to stdout (1) 2>&1 
PARSED=$( grep "Submitted application" $OUTPUT | awk '{ print $NF }' )
CLUSTER="${PARSED%%/*}"
JOBID=$( grep "Submitting tokens for job" $OUTPUT | awk '{ print $NF }' )
FILEBYTESREAD=$( grep "FILE: Number of bytes read"  $OUTPUT | awk '{ print $NF }' | cut -d '=' -f2- )
FILEBYTESWRITTEN=$( grep "FILE: Number of bytes written" $OUTPUT | awk '{ print $NF }' | cut -d '=' -f2- )
HDFSBYTESREAD=$( grep "HDFS: Number of bytes read" $OUTPUT | awk '{ print $NF }' | cut -d '=' -f2- )
HDFSBYTESWRITTEN=$( grep "HDFS: Number of bytes written" $OUTPUT | awk '{ print $NF }' | cut -d '=' -f2- )
MAPS=$( grep "Launched map tasks" $OUTPUT  | awk '{ print $NF }' | cut -d '=' -f2- )
REDUCES=$( grep "Launched reduce tasks" $OUTPUT  | awk '{ print $NF }' | cut -d '=' -f2- )
LOCALMAPS=$( grep "Data-local map tasks" $OUTPUT  | awk '{ print $NF }' | cut -d '=' -f2- )
RACKMAPS=$( grep "Rack-local map tasks" $OUTPUT  | awk '{ print $NF }' | cut -d '=' -f2- )
TIMESPENTMAPS=$( grep  "Total time spent by all maps in occupied slots" $OUTPUT  | awk '{ print $NF }' | cut -d '=' -f2- )
TIMESPENTREDUCES=$( grep "Total time spent by all reduces in occupied slots" $OUTPUT  | awk '{ print $NF }' | cut -d '=' -f2- )

QUEUENAME=$( $HADOOP_HOME/bin/mapred queue -list | grep "Queue Name" | awk '{ print $NF }' )

SCHEDULINGINFO=$( $HADOOP_HOME/bin/mapred queue -info $QUEUENAME | grep "Scheduling Info" )

IFS=' ,' read _ _ _ _ QUEUECAPACITY _ MAXIMUMQUEUECAPACITY _ CURRENTQUEUECAPACITY _  <<< "$SCHEDULINGINFO"

DIGESTS=$( $DIGESTCOMMAND )

# printf '<jobs>'
# printf '<job>'
# printf '<id>%s</id>
# <filebytesread>%s</filebytesread>
# <filebyteswritten>%s</filebyteswritten>
# <hdfsbytesread>%s</hdfsbytesread>
# <hdfsbyteswritten>%s</hdfsbyteswritten>' "$JOBID" "$FILEBYTESREAD" "$FILEBYTESWRITTEN" \
#     "$HDFSBYTESREAD" "$HDFSBYTESWRITTEN"

# printf '<maps>%s</maps>
# <reduces>%s</reduces>
# <localmaps>%s</localmaps>
# <rackmaps>%s</rackmaps>
# <jobsrunning>%s</jobsrunning>
# <cluster>%s</cluster>' "$MAPS" "$REDUCES" "$LOCALMAPS" "$RACKMAPS"  "$JOBSRUNNING" "$CLUSTER"

# printf '<totaltime>%s</totaltime>
# <timespentmaps>%s</timespentmaps>
# <timespentreduces>%s</timespentreduces>
# <queuecapacity>%s</queuecapacity>
# <maximumqueuecapacity>%s</maximumqueuecapacity>
# <currentqueuecapacity>%s</currentqueuecapacity>
# <digests>%s</digests>' "$JOBDIFF" "$TIMESPENTMAPS" "$TIMESPENTREDUCES" "$QUEUECAPACITY" \
# "$MAXIMUMQUEUECAPACITY" "$CURRENTQUEUECAPACITY" "$DIGESTS"
# printf '</job>'
# printf '</jobs>
# '


printf '{
  "jobs": {
    "job": {'

printf '"id": "%s",
      "filebytesread": "%s",
      "filebyteswritten": "%s",
      "hdfsbytesread": "%s",
      "hdfsbyteswritten": "%s",' "$JOBID" "$FILEBYTESREAD" "$FILEBYTESWRITTEN" \
	  "$HDFSBYTESREAD" "$HDFSBYTESWRITTEN"

printf '"maps": "%s",
      "reduces": "%s",
      "localmaps": "%s",
      "rackmaps": "%s",
      "jobsrunning": "%s",
      "cluster": "%s",' "$MAPS" "$REDUCES" "$LOCALMAPS" "$RACKMAPS"  "$JOBSRUNNING" "$CLUSTER"

printf '"totaltime": "%s",
      "timespentmaps": "%s",
      "timespentreduces": "%s",
      "queuecapacity": "%s",
      "maximumqueuecapacity": "%s",
      "currentqueuecapacity": "%s",
      "digests": "%s"' "$JOBDIFF" "$TIMESPENTMAPS" "$TIMESPENTREDUCES" "$QUEUECAPACITY" \
	  "$MAXIMUMQUEUECAPACITY" "$CURRENTQUEUECAPACITY" "$DIGESTS"

printf '  }
  }
}'
