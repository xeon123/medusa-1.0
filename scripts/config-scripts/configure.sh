#!/bin/bash
#set -xv

DATE=`date "+%s"`
HADOOP_SRC="http://mirrors.fe.up.pt/pub/apache/hadoop/common/hadoop-2.0.5-alpha/hadoop-2.0.5-alpha.tar.gz"
TMP_DIR="$HOME/hadoop-manager-installation-dir"
OUTPUT_FILE="hadoop-2.0.5-alpha.tar.gz"
PATH_DEST="$HOME/Programs"
HOSTNAME=$1
PROGRAM_DEST="$PATH_DEST/${OUTPUT_FILE%.tar.gz}"
PROGRAM_SETUP_DIR="$PROGRAM_DEST/etc/hadoop"
read -d '' CONFIG_HEADER <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

<!-- Put site-specific property overrides in this file. -->
EOF

echo "create installation dir"
mkdir $TMP_DIR

cd $TMP_DIR
wget $HADOOP_SRC -O $OUTPUT_FILE
tar zxvf $OUTPUT_FILE

echo "Copy ${OUTPUT_FILE%.tar.gz} to $PATH_DEST"
cp -R "${OUTPUT_FILE%.tar.gz}" "$PATH_DEST"

echo "Add this in .bashrc"
printf "export JAVA_HOME=/usr/lib/jvm/java-7-oracle/jre
HADOOP_HOME=$PROGRAM_DEST
HADOOP_MANAGER=$( dirname `pwd` )
ACTIVEPYTHON=~/Programs/python-2.7
export PYTHONPATH=\$HADOOP_MANAGER/src:\$ACTIVEPYTHON/lib/python2.7/site-packages
export EC2_HOME=/home/xeon/.ec2
export EC2_PRIVATE_KEY=\$EC2_HOME/xeon123-amazon.pem
export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$HADOOP_MANAGER/src:\$JAVA_HOME/bin:\$ACTIVEPYTHON/bin:\$EC2_HOME/bin"


CORE_FILE="$PROGRAM_SETUP_DIR/core-site.xml"
printf ">> core-site.xml\n"
printf "$CONFIG_HEADER
<configuration>
  <property> <name>fs.default.name</name> <value>hdfs://$HOSTNAME:9000</value> </property>
  <property> <name>hadoop.tmp.dir</name> <value>/tmp/hadoop-temp</value> </property>
</configuration>\n" > $CORE_FILE

HDFS_FILE="$PROGRAM_SETUP_DIR/hdfs-site.xml"
printf ">> hdfs-site\n"
printf "$CONFIG_HEADER
<configuration>
        <property> <name>dfs.replication</name> <value>3</value> </property> 
        <property> <name>dfs.permissions</name> <value>false</value> </property> 
        <property> <name>dfs.name.dir</name> <value>/tmp/data/dfs/name/</value> </property>
        <property> <name>dfs.data.dir</name> <value>/tmp/data/dfs/data/</value> </property>
</configuration>\n" > $HDFS_FILE

MAPRED_FILE="$PROGRAM_SETUP_DIR/mapred-site.xml"
printf ">> mapred-site.xml\n"
printf "$CONFIG_HEADER
<configuration>
 <property>
   <name>mapreduce.framework.name</name>
   <value>yarn</value>
 </property>

 <property>
   <name>mapreduce.jobhistory.done-dir</name>
   <value>/home/ubuntu/Programs/hadoop/logs/history/done</value>
 </property>

 <property>
   <name>mapreduce.jobhistory.intermediate-done-dir</name>
   <value>/home/ubuntu/Programs/hadoop/logs/history/intermediate-done-dir</value>
 </property>
</configuration>\n" > $MAPRED_FILE

YARN_FILE="$PROGRAM_SETUP_DIR/yarn-site.xml"
printf ">> yarn-site.xml\n"
printf "$CONFIG_HEADER
<configuration>
  <property>
    <name>yarn.log-aggregation-enable</name>
    <value>true</value>
  </property>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce.shuffle</value>
  </property>
  <property>
    <name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>
    <value>org.apache.hadoop.mapred.ShuffleHandler</value>
  </property>
  <property>
    <name>yarn.resourcemanager.resource-tracker.address</name>
    <value>$HOSTNAME:8025</value>
  </property>
  <property>
    <name>yarn.resourcemanager.scheduler.address</name>
    <value>$HOSTNAME:8030</value>
  </property>
  <property>
    <name>yarn.resourcemanager.address</name>
    <value>$HOSTNAME:8040</value>
  </property>
 </configuration>\n" > $YARN_FILE

cd -

echo "remove installation dir"
rm -rf $TMP_DIR
