#!/bin/bash

# This file is used to install Hadoop Mapreduce from scratch.

echo "Installing java"
sudo apt-add-repository ppa:webupd8team/java
sudo add-apt-repository ppa:patrickdk/general-lucid
sudo apt-get update --fix-missing

sudo apt-get install -y oracle-java8-installer

# install iperf3
sudo apt-get instaall -y iperf3


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

cd $HOME/Programs
echo "Configure medusa-1.0"

cat << 'EOF' > "$HOME"/.bashrc
# ~/.bashrc: executed by bash(1) for non-login shells.
# see /usr/share/doc/bash/examples/startup-files (in the package bash-doc)
# for examples

# don't put duplicate lines in the history. See bash(1) for more options
# don't overwrite GNU Midnight Commander's setting of `ignorespace'.
HISTCONTROL=$HISTCONTROL${HISTCONTROL+,}ignoredups
# ... or force ignoredups and ignorespace
HISTCONTROL=ignoreboth

# ignore pressing CTRL+D once to exit the shell. It must be pressed twice.
export IGNOREEOF=1

# append to the history file, don't overwrite it
shopt -s histappend

# You may want to put all your additions into a separate file like
# ~/.bash_aliases, instead of adding them here directly.
# See /usr/share/doc/bash-doc/examples in the bash-doc package.

if [ -f ~/.bash_aliases ]; then
    . ~/.bash_aliases
fi

#if [ -f ~/.profile ]; then
#    . ~/.profile
#fi

# enable programmable completion features (you don't need to enable
# this, if it's already enabled in /etc/bash.bashrc and /etc/profile
# sources /etc/bash.bashrc).
if [ -f /etc/bash_completion ] && ! shopt -oq posix; then
    . /etc/bash_completion
fi

# set a fancy prompt (non-color, unless we know we "want" color)
case "$TERM" in
    xterm-color) color_prompt=yes;;
esac

# uncomment for a colored prompt, if the terminal has the capability; turned
# off by default to not distract the user: the focus in a terminal window
# should be on the output of commands, not on the prompt
#force_color_prompt=yes

if [ -n "$force_color_prompt" ]; then
    if [ -x /usr/bin/tput ] && tput setaf 1 >&/dev/null; then
	# We have color support; assume it's compliant with Ecma-48
	# (ISO/IEC-6429). (Lack of such support is extremely rare, and such
	# a case would tend to support setf rather than setaf.)
	color_prompt=yes
    else
	color_prompt=
    fi
fi

# Terminal line
#blue=$(tput setaf 4)
#purple=$(tput setaf 5)
#reset=$(tput sgr0)
PS1="\[$blue\]\h:\[$purple\]\w\[$reset\]\\$ "

# Paths {
    export JAVA_HOME=/usr/lib/jvm/java-1.6.0-openjdk
    HADOOP_HOME=$HOME/Programs/hadoop
    MEDUSA_HOME=$HOME/Programs/medusa-1.0
    export PYTHONPATH=${MEDUSA_HOME}/medusa:${PYTHONPATH}

    export PATH=$PATH:$JAVA_HOME/bin:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin
    export LANG=en_US.UTF-8
# }

EOF

echo "Configure python virtualenv"
cd $HOME/Programs/medusa-1.0
virtualenv medusa-env

cd $HOME/Programs/medusa-1.0/medusa
ln -s server_settings.py settings.py

echo "Delete installation files"
rm -rf $HOME/Programs/Python-2.7.8*
rm -rf $HOME/Programs/ez_setup.py
rm -rf $HOME/Programs/hadoop-2.5.0.tar.gz
rm -rf $HOME/Programs/python27-on-debian

echo "Configure Hadoop MapReduce"
cd "$HOME"/Programs/hadoop
mkdir logs
