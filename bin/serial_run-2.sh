#!/bin/bash
#set -xv

##########
# This script executes a job
##########

xxxdiff() {
    ((diff_sec=$2-$1))
    echo - | awk '{printf "%d:%d:%d","'"$diff_sec"'"/(60*60),"'"$diff_sec"'"%(60*60)/60,"'"$diff_sec"'"%60}'
    echo ""
    echo "$3: $diff_sec"
}

SSH_PERM_OREGON="$HOME/.ssh/ec2-medusa2-mpc-oregon.pem"
SSH_PERM_NVIRGINIA="$HOME/.ssh/ec2-medusa2-mpc-nvirginia.pem"
SSH_PERM_CALIFORNIA="$HOME/.ssh/ec2-medusa2-mpc-california.pem"
SSH_PERM_IRELAND="$HOME/.ssh/ec2-medusa2-mpc-ireland.pem"

HOST_OREGON="medusa-ec2-oregon-1"
HOST_NVIRGINIA="medusa-ec2-nvirginia-1"
HOST_CALIFORNIA="medusa-ec2-california-1"
HOST_IRELAND="medusa-ec2-ireland-1"

HADOOP_PATH="$HOME/Programs/hadoop"
SSH_USER="ubuntu"
OUTPUT="/output1"

# Step 1
ssh -i ${SSH_PERM_OREGON} ${SSH_USER}@${HOST_OREGON} "${HADOOP_PATH}/bin/hdfs dfs -rmr ${OUTPUT}"
ssh -i ${SSH_PERM_NVIRGINIA} ${SSH_USER}@${HOST_NVIRGINIA} "${HADOOP_PATH}/bin/hdfs dfs -rmr ${OUTPUT}"
ssh -i ${SSH_PERM_CALIFORNIA} ${SSH_USER}@${HOST_CALIFORNIA} "${HADOOP_PATH}/bin/hdfs dfs -rmr ${OUTPUT}"
ssh -i ${SSH_PERM_IRELAND} ${SSH_USER}@${HOST_IRELAND} "${HADOOP_PATH}/bin/hdfs dfs -rmr ${OUTPUT}"

# Step 2
gdate1=$(date +"%s")
echo "Executing"
date1=$(date +"%s")
python tests/testRun.py
date2=$(date +"%s")

xxxdiff $date1 $date2 "Execution time"
