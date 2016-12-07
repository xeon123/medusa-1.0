#!/bin/bash
#set -xv

project=${PWD##*/}

cd .. || exit

echo "Creating tar"
echo "tar -zchf $HOME/medusa_hadoop.tar.gz '$project'/{src,*scripts,README,bin,submit}"
tar -zchf $HOME/medusa_hadoop.tar.gz "$project"/{src,*scripts,README,bin,submit}
echo "DONE"
