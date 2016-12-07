import sys

from medusa.networkdaemon import network_latency


"""
 write the latency of the network between clusters
"""


def updateNetworkInfo(hostname):
    while True:
        network_latency(hostname)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "python networkmanagep.py hostname"

    hostname = sys.argv[1]
    updateNetworkInfo(hostname)
