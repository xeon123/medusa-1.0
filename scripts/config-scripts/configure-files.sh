#!/bin/bash

# This file configure automatically the Hadop MapReduce to work in the cluster
# set -xv

DATE=`date "+%s"`
MEDUSA_HOME=$HOME/Programs/medusa-1.0
TMP_DIR="$HOME/hadoop-medusa-installation-dir"
PATH_DEST="$HOME/Programs"
MASTER=$1
HADOOP_DEST="$PATH_DEST/hadoop"

HADOOP_SETUP_DIR="$HADOOP_DEST/etc/hadoop"
read -d '' CONFIG_HEADER <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->
EOF

echo "create installation dir"
mkdir $TMP_DIR

cd $TMP_DIR

CORE_FILE="$HADOOP_SETUP_DIR/core-site.xml"
printf ">> core-site.xml\n"
printf "$CONFIG_HEADER
<configuration>
  <property> <name>fs.default.name</name> <value>hdfs://$MASTER:9000</value> </property>
  <property> <name>hadoop.tmp.dir</name> <value>/tmp/hadoop-temp</value> </property>
  <!-- property><name>hadoop.proxyuser.xeon.hosts</name><value>*</value></property>
  <property><name>hadoop.proxyuser.xeon.groups</name><value>*</value></property -->
</configuration>\n" > ${CORE_FILE}

HDFS_FILE="$HADOOP_SETUP_DIR/hdfs-site.xml"
printf ">> hdfs-site\n"
printf "$CONFIG_HEADER
<configuration>
  <property> <name>dfs.replication</name> <value>1</value> </property> 
  <property> <name>dfs.permissions</name> <value>false</value> </property> 
  <property> <name>dfs.name.dir</name> <value>/tmp/data/dfs/name/</value> </property>
  <property> <name>dfs.data.dir</name> <value>/tmp/data/dfs/data/</value> </property>
  <property> <name>dfs.webhdfs.enabled</name> <value>true</value> </property>
  <property> <name>dfs.support.append</name> <value>true</value> </property>
</configuration>\n" > ${HDFS_FILE}

MAPRED_FILE="$HADOOP_SETUP_DIR/mapred-site.xml"
printf ">> mapred-site.xml\n"
printf "$CONFIG_HEADER
<configuration>
 <property> <name>mapreduce.framework.name</name> <value>yarn</value> </property>
 <property> <name>mapreduce.jobhistory.done-dir</name> <value>/root/Programs/hadoop/logs/history/done</value> </property>
 <property> <name>mapreduce.jobhistory.intermediate-done-dir</name> <value>/root/Programs/hadoop/logs/history/intermediate-done-dir</value> </property>
 <!-- property> <name>mapreduce.map.output.compress</name> <value>false</value> </property>
 <property> <name>mapred.map.output.compress.codec</name> <value>org.apache.hadoop.io.compress.BZip2Codec</value> </property -->
 <property> <name>mapreduce.job.reduces</name> <value>8</value> </property>

 <property> <name>yarn.nodemanager.resource.memory-mb</name> <value>2048</value> </property>
 <property> <name>yarn.scheduler.minimum-allocation-mb</name> <value>1024</value> </property>

</configuration>\n" > ${MAPRED_FILE}

YARN_FILE="$HADOOP_SETUP_DIR/yarn-site.xml"
printf ">> yarn-site.xml\n"
printf "$CONFIG_HEADER
<configuration>
 <property> <name>yarn.log-aggregation-enable</name> <value>true</value> </property>
 <property> <name>yarn.nodemanager.aux-services</name> <value>mapreduce_shuffle</value> </property>  
 <property> <name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name> <value>org.apache.hadoop.mapred.ShuffleHandler</value> </property>
 <property> <name>yarn.resourcemanager.resource-tracker.address</name> <value>$MASTER:8025</value> </property>
 <property> <name>yarn.resourcemanager.scheduler.address</name> <value>$MASTER:8030</value> </property>
 <property> <name>yarn.resourcemanager.address</name> <value>$MASTER:8032</value> </property>
 <property> <name>yarn.log.server.url</name> <value>http://$MASTER:19888/jobhistory/logs/</value> </property> 

 <!-- job history -->
 <property> <name>yarn.log-aggregation-enable</name> <value>true</value> </property>
 <property> <name>yarn.nodemanager.log.retain-seconds</name> <value>900000</value> </property>
 <property> <name>yarn.nodemanager.remote-app-log-dir</name> <value>/app-logs</value> </property>

</configuration>\n" > ${YARN_FILE}

cd -

# create setup files
echo "create setup files"
mkdir ${MEDUSA_HOME}/logs/
mkdir -p ${MEDUSA_HOME}/tests_logs/jobs/
mkdir ${MEDUSA_HOME}/temp
echo "" > ${MEDUSA_HOME}/temp/jobs_data
echo "" > ${MEDUSA_HOME}/temp/jobs_history_data
echo "" > ${MEDUSA_HOME}/temp/network_data

echo "remove installation dir"
rm -rf ${TMP_DIR}
