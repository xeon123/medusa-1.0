#!/bin/bash

# This script starts full execution

MANAGER_HOME="$HOME/repositories/git/medusa-1.0/"

ssh -i $HOME/.ssh/id_geni_ssh_rsa.pub root@NodeGroup0-0 "/root/Programs/hadoop/bin/hdfs dfs -rmr /wiki2 /wiki3 /*output*"
ssh -i $HOME/.ssh/id_geni_ssh_rsa.pub root@NodeGroup1-0 "/root/Programs/hadoop/bin/hdfs dfs -rmr /wiki /wiki3 /*output*"
ssh -i $HOME/.ssh/id_geni_ssh_rsa.pub root@NodeGroup1-4 "/root/Programs/hadoop/bin/hdfs dfs -rmr /wiki /wiki2 /*output*"
ssh -i $HOME/.ssh/id_geni_ssh_rsa.pub root@NodeGroup2-0 "/root/Programs/hadoop/bin/hdfs dfs -rmr /wiki* /*output*"

python $MANAGER_HOME/manager/tests/testRun.py
