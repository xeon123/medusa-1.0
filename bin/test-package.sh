#!/bin/bash
#set -xv

project=${PWD##*/}

cd .. || exit
##############################
# create tar package useful for tests. It does not contain 
# config files (celery and settings )
##############################
echo "Creating tar"
echo "tar -zchf $HOME/hadoop-mapreduce-manager-python.tar.gz '$project'/{config-scripts,src,scripts,README,bin,submit,temp}"
tar -zchf $HOME/hadoop-mapreduce-manager-python.tar.gz --exclude='settings.*' --exclude='celeryconfig.*' --exclude='htmlcov*' "$project"/{config-scripts,src,scripts,README,bin,submit,temp}
echo "DONE"
