#!/bin/bash


itf=wlan0

start() {
    # Before starting to configure qdiscs, first we need to remove any existing qdisc from the root.
    tc qdisc del dev $itf root

    # add default qdisc in the interface that uses algorithm htb.
    # if the packets does not satisfy the classes defined, it goes to class 10
    tc qdisc add dev $itf root handle 1:0 htb default 10

    # principal class that limits the interface to 100mbit
    tc class add dev $itf parent 1:0 classid 1:1 htb rate 100mbit

    # default class 10
    # bandwith from 2mbit to 3mbit
    tc class add dev $itf parent 1:1 classid 1:10 htb rate 2mbit ceil 3000kbit prio 0

    # HTTP/s TCP
    tc class add dev $itf parent 1:1 classid 1:21 htb rate 2mbit ceil 3mbit prio 0
    tc class add dev $itf parent 1:1 classid 1:22 htb rate 2mbit ceil 3mbit prio 0

    # HTTP/s UDP
    tc class add dev $itf parent 1:1 classid 1:23 htb rate 800kbit ceil 1mbit prio 0
    tc class add dev $itf parent 1:1 classid 1:24 htb rate 800kbit ceil 1mbit prio 0


    # zero iptables configuration
    iptables -t mangle -F FORWARD

    # Download
    iptables -t mangle -A FORWARD -p tcp -m multiport --sports 80 -j CLASSIFY --set-class 1:21
    iptables -t mangle -A FORWARD -p udp -m multiport --sports 80 -j CLASSIFY --set-class 1:23

    # Upload
    iptables -t mangle -A FORWARD -p tcp -m multiport --dports 80 -j CLASSIFY --set-class 1:22
    iptables -t mangle -A FORWARD -p udp -m multiport --dports 80 -j CLASSIFY --set-class 1:24

}

stop() {
    # remove all the classes
    tc qdisc del dev $itf root
    # removes all the rules that classifies packets
    iptables -t mangle -F  FORWARD
}

monitor() {
    tc -s -d class show dev $itf
}

case $1 in
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        start; stop;
        ;;
    monitor)
        monitor
        ;;
    *)
        echo "Options available: start|stop|restart|monitor"
        ;;
esac
