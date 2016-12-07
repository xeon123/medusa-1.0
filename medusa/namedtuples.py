from collections import namedtuple


def set_execution_params(job_name, currentqueuecapacity, hdfsbytesread, hdfsbyteswritten, maps, reduces, mem_load, cpu_load, total_time):
    """
    Returns a namedtuple with information about the execution

    :param job_name:
    :param currentqueuecapacity:
    :param hdfsbytesread:
    :param hdfsbyteswritten:
    :param maps:
    :param reduces:
    :param mem_load:
    :param cpu_load:
    :param totaltime:
    :return: Information about the execution
    """
    ExecutionParams = namedtuple("ExecutionParams", ["job_name", "currentqueuecapacity", "hdfsbytesread", "hdfsbyteswritten",
                                               "maps", "reduces", "cpu", "mem", "time"])
    return ExecutionParams(job_name, currentqueuecapacity, hdfsbytesread, hdfsbyteswritten, maps, reduces,
                           cpu_load, mem_load, total_time)


def get_network_info(from_cluster, to_cluster, packet_size, bytes, bits_per_seconds):
    """
    Returns a namedtuple with information about the network

    :param from_cluster: (string) name of the cluster
    :param to_cluster: (string) name of the cluster
    :param packet_size: (int) size of the packet in bytes
    :param bytes: (list) list of bytes of all exchanged packets
    :param bits_per_seconds: (list) list of bits per second of all exchanged packets

    :return: Information about the network
    """
    NetworkInfo = namedtuple('NetworkInfo', ['from_cluster', 'to_cluster', 'packet_size', 'bytes', 'bits_per_second',
                                             'transference_time'], verbose=True)

    transference_time = [(a * 8) / b for a, b in zip(bytes, bits_per_seconds)]  # transference time

    return NetworkInfo(from_cluster, to_cluster, packet_size, bytes, bits_per_seconds, transference_time)

def get_order(from_cluster, to_cluster, path):
    """

    :param from_cluster: (string)
    :param to_cluster: (string)
    :param path: (string)
    :return: Order object
    """
    Order = namedtuple("Order", ['from_cluster', 'to_cluster', 'src_path', 'dst_path'], verbose=True)
    return Order(from_cluster, to_cluster, path, path)

def set_job_params(job_name, current_queue_capacity,
                   input_size, output_size, maps,
                   cpu_load, mem_load,
                   total_time=0):
    """
    Set the job params used for prediction

    :return (nametuple)
    """

    JobParams = namedtuple("JobParams", ["job_name", "current_queue_capacity", "input_size", "output_size", "maps", "cpu_load", "mem_load", "total_time"])
    job_params = JobParams(job_name, current_queue_capacity, input_size, output_size, maps, cpu_load, mem_load, total_time)

    return job_params


def set_penalization_params(current_time, predicted_time, error):
    """
    Set penalization values
    :param current_time:
    :param predicted_time:
    :param error:
    :return:
    """

    PenalizationParams = namedtuple("PenalizationParams", ["current_time", "predicted_time", "error"])
    penalization_params = PenalizationParams(current_time, predicted_time, error)

    return penalization_params

def get_reference_digest(host, digests):
    """
    Reference digests
    :param host: (string) host name
    :param digests: (dict) with {filename: digest}

    :return: reference digest obs
    """
    RefDigests = namedtuple('RefDigests', ['host', 'digests'])
    return RefDigests(host, digests)

def get_aggregation_reference_digest(host, digests):
    """
    Reference digests for the aggregation phase
    :param host: (list) list of host names
    :param digests: (dict) with {filename: digest}

    :return: reference digest obs
    """
    RefDigests = namedtuple('RefDigests', ['host', 'digests'])
    return RefDigests(host, digests)
