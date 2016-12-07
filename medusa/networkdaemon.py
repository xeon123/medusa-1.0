import json
import time

from celery import group, task
from medusa import settings
from medusa.copydata import copy_data
from medusa.decors import make_verbose
from medusa.hdfs import exists, get_total_size, network_filewriteDataTime, rmr
from medusa.local import lcat
from medusa.namedtuples import get_network_info
from medusa.settings import medusa_settings
from medusa.medusasystem import executeCommand, local_execute_command


def read_network_data(from_cluster, to_cluster):
    """
    Read the network data

    :param from_cluster (string) from cluster
    :param to_cluster (string) to clusters

    :return NetworkInfo (list) list with the information about the network
    """

    network_info = []
    tasks = []
    for _packet_size in medusa_settings.packet_size:
        tasks.append(read_remote_network_data.s(from_cluster, to_cluster, _packet_size).set(queue=from_cluster))
    g1 = group(tasks)()

    while g1.waiting():
        time.sleep(2)

    network_values = g1.get()
    for network_value in network_values:
        _network_value = json.loads(network_value)
        _packet_size = _network_value["start"]["test_start"]["bytes"]
        _intervals = _network_value["intervals"]
        _bits_per_second = [_value["sum"]["bits_per_second"] for _value in _intervals]
        _bytes = [_value["sum"]["bytes"] for _value in _intervals]

        network_info.append(get_network_info(from_cluster, to_cluster, _packet_size, _bytes, _bits_per_second))

    return network_info


def network_latency(cluster_source):
    """
    write network data info in a file
    """
    # temp_file=SCRIPTS_DIR + "/data-temp.txt"
    src = "/data-temp-%s" % cluster_source
    for qdest in medusa_settings.clusters:
        if cluster_source != qdest:
            from_host = cluster_source
            to_host = qdest

            if dirExists(to_host, src):
                command = rmr(src)
                executeCommand.apply_async(
                    queue=to_host, args=(command,)).get()

            order = (from_host, to_host, src, "/")
            span = sendTestPacket(order, to_host)

            command2 = get_total_size(src)
            size = executeCommand.apply_async(
                queue=to_host, args=(command2,)).get()

            if span > 0 and size > 0:
                to_host = qdest
                content = "%s:%s:%s:%s\n" % (from_host, to_host, size, span)
                command = network_filewriteDataTime(content)
                executeCommand.apply_async(
                    queue=from_host, args=(command,)).get()

        time.sleep(60)


def sendTestPacket(order, to_cluster):
    start = time.time()

    copy_data(order, to_cluster)
    end = time.time()

    span = end - start
    return span


def dirExists(cluster, path):
    """ Verifies if the path exists """
    command = exists(path)
    result = executeCommand.apply_async(queue=cluster, args=(command,)).git()

    return int(result) > 0


def groupNetworkLines(content):
    groups1 = {}
    groups2 = {}

    for line in content:
        values = line.split(':')
        dest = values[1]
        size = float(values[2])
        ttime = float(values[3])  # transference time

        if dest in groups1:
            groups1[dest].append(ttime)
            groups2[dest].append(size)
        else:
            groups1.update({dest: [ttime]})
            groups2.update({dest: [size]})

    return groups1, groups2


@task(name='medusa.networkdaemon.read_remote_network_data')
@make_verbose
def read_remote_network_data(from_cluster, to_cluster, packet_size):
    """
    Read network data
    :param from_cluster: (string) from cluster
    :param to_cluster: (string) to cluster
    :param packet_size: (string) size of the packet
    :return: output of the command
    """
    command = lcat(settings.get_temp_dir() + "/" + from_cluster + "-" + to_cluster + "-" + packet_size + ".json")
    output = local_execute_command(command)

    return output
