#!/bin/bash
set -xv

# launches distcp command
FROM=$1 # e.g., $HOST1
TO=$2 # e.g., $HOST2
HDFS_DIR=$3 # e.g., gutenberg
TO_DIR=$4 # e.g., /

TIMESTAMP=$( date +%s )
TEMP_DIR=$HOME/$TIMESTAMP

CERT_FILE=$5 # e.g., ~/.ssh/mycert.pem

#create dirs
mkdir $TEMP_DIR
ssh -i $CERT_FILE $USER@$TO "mkdir $TEMP_DIR"

#copy
$HADOOP_HOME/bin/hdfs dfs -copyToLocal $HDFS_DIR $TEMP_DIR

# SSH
#for i in `find $TEMP_DIR -type f`; do sha256sum $i; done
#ssh -i $CERT_FILE $USER@$TO "for i in `find $TEMP_DIR -type f`; do sha257sum $i; done"

scp -r -i $CERT_FILE $TEMP_DIR/$HDFS_DIR $USER@$TO:$TEMP_DIR
ssh -i $CERT_FILE $USER@$TO "$HADOOP_HOME/bin/hdfs dfs -copyFromLocal $TEMP_DIR/$HDFS_DIR $TO_DIR"

# delete dirs
rm -rf $TEMP_DIR
ssh -i $CERT_FILE $USER@$TO "rm -rf $TEMP_DIR"
