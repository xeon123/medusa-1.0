#!/bin/bash
set -xv

echo "Hadoop mapreduce manager setup at Ubuntu $(date "+%s")"
sudo apt-get install tree
sudo apt-get install gcc

# install java 7
tee -a /etc/apt/sources.list <<< "deb http://www.duinsoft.nl/pkg debs all"
#sudo apt-key adv --keyserver keys.gnupg.net --recv-keys 5CB26B26
sudo add-apt-repository ppa:webupd8team/java
sudo apt-get update
#sudo apt-get install update-sun-jre
sudo apt-get install oracle-jdk7-installer


sudo apt-get install python-dev
