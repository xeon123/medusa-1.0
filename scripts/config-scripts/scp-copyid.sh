#!/bin/bash


hosts=(host1 host2 host3)

for i in "${hosts[@]}";
do
    echo "Copy scp id to $i"
    cat ~/.ssh/id_rsa.pub |ssh user@$i 'sh -c "cat - >>~/.ssh/authorized_keys"'
done
