#!/bin/bash

######
# Give load to the CPU
######
#set -xv

# Repeat the command in the curly brackets as many times as the number of threads you want to produce (here 4 threads). Simple enter hit will stop it (just make sure no other dd is running on this user or you kill it too).
fulload() {
    dd if=/dev/zero of=/dev/null &
#    dd if=/dev/zero of=/dev/null | dd if=/dev/zero of=/dev/null &
#    dd if=/dev/zero of=/dev/null | dd if=/dev/zero of=/dev/null | dd if=/dev/zero of=/dev/null &
#    dd if=/dev/zero of=/dev/null | dd if=/dev/zero of=/dev/null | dd if=/dev/zero of=/dev/null | dd if=/dev/zero of=/dev/null &
};

fulload;
read;
killall dd

# Example to stress 2 cores for 60 seconds
# --cpu spawn N workers
# --timeout timout after N seconds
# stress --cpu 2 --timeout 60

