#!/bin/bash

# This file is used to install Medusa 2 from scratch.
sudo apt-add-repository ppa:webupd8team/java
sudo add-apt-repository ppa:patrickdk/general-lucid
sudo apt-get update

sudo apt-get install -y tree
sudo apt-get install -y elinks
sudo apt-get install -y htop

echo "Installing java"
sudo apt-get install -y oracle-java8-installer

# install iperf3
echo "Installing iperf3"
sudo apt-get install -y iperf3

cd "$HOME"/Programs/
echo "Installing python"
sudo apt-get -y install python-dev
sudo apt-get -y install python-virtualenv
sudo apt-get -y install python-pip
sudo apt-get install python-psutil

echo "Installing hadoop"
cd "$HOME"/Programs
wget --no-check-certificate http://mirrors.fe.up.pt/pub/apache/hadoop/common/hadoop-2.6.0/hadoop-2.6.0.tar.gz
tar zxvf hadoop-2.6.0.tar.gz
ln -s "$HOME"/Programs/hadoop-2.6.0 "$HOME"/Programs/hadoop

cd $HOME
echo "Configure medusa-1.0"

cat << 'EOF' >> "$HOME"/.bashrc


# Paths {
    export JAVA_HOME=/usr/lib/jvm/java-8-oracle
    export HADOOP_HOME=$HOME/Programs/hadoop
    export HADOOP_COMMON_HOME=$HADOOP_HOME
    export HADOOP_HDFS_HOME=$HADOOP_HOME
    export YARN_HOME=$HADOOP_HOME
    export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
    export MEDUSA_HOME=$HOME/Programs/medusa-1.0

    export PYTHONPATH=${MEDUSA_HOME}/medusa:${PYTHONPATH}

    export PATH=$PATH:${JAVA_HOME}/bin:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin
 #}
EOF

echo "Configure python virtualenv"
cd $HOME/Programs/medusa-1.0
virtualenv medusa-env

mkdir "$HOME"/Programs/hadoop/logs
