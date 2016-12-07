import collections
import logging
from collections import Counter
import operator

from medusa.decors import make_verbose
from medusa.mergedirs import _set_rank
from medusa.namedtuples import get_reference_digest
from medusa.orders import executeOrder, needs_more_copies, get_order
from medusa.medusasystem import getRunningClusters
from medusa.ranking import rank_clusters
from medusa.sets import find_include_exclude_clusters

"""
this methods are used for aggregation part
"""


def aggregationMergeDirsWrapper(args):
    """
    print "---"
    print args
    print "---"
    print args.faults
    print args.job
    print args.history_rank
    print args.command
    print args.poutput
    print args.pinput
    print args.ref_digests
    print "---"
    """
    job = (args.command, args.pinput, args.poutput)
    params = (job, args.faults, args.history_rank)

    return aggregationMergeDirs(params, args.ref_digests)


@make_verbose
def aggregationMergeDirs(job, reference_digests):
    """

    :param job: (Job) job structure
    :param reference_digests: (list) reference digests
    :return:
    """
    # contains included_hosts and excluded_hosts sets
    active_clusters = getRunningClusters()
    host_spec = find_include_exclude_clusters(active_clusters, job.input_path, True)

    rank = rank_clusters(job.input_path, active_clusters, host_spec)
    host_spec = _set_rank(host_spec, rank)

    logging.info("Merging dirs (%s)" % job.command)
    included_hosts = naiveAggregationOrders(
        job.input_path, host_spec, job.faults_tolerate, reference_digests)

    return included_hosts


@make_verbose
def naiveAggregationOrders(pinput, host_spec, faults, reference_digests):
    """ aggregate dirs """
    included_hosts, excluded_hosts = host_spec

    # need_merge = needMerge(pinput, included, faults)
    orders = naiveMergeDirs(host_spec, pinput)

    for order in orders:
        if needs_more_copies(included_hosts, pinput, faults):
            src_cluster = order.from_cluster
            _pinput = order.src_path
            for r in reference_digests:
                if src_cluster in r.host:
                    if any(_pinput in _v for _v in r.digests.keys()):
                        to_cluster = executeOrder(order, [get_reference_digest(src_cluster, r.digests)])

            for idx, ehost in enumerate(excluded_hosts):
                if ehost.cluster == to_cluster:
                    host = excluded_hosts[idx]
                    host.paths.append(order.src_path)
                    if len(host.paths) == len(pinput):
                        included_hosts.append(host)
                        excluded_hosts.pop(idx)

    clusters = [ihost.cluster for ihost in included_hosts]

    return clusters


def naiveMergeDirs(host_spec, pinput):
    """
    This method creates orders to merge the dirs
    subset is a defaultdict with the input
    max_files is the number of max dirs for the aggregation
    """

    # get dict entry with max values
    included_hosts, excluded_hosts = host_spec

    """
    build orders based on a reference element.
    The reference element is the one that contain all the paths
    """
    orders = []
    if len(included_hosts) > 0:
        for path_statistics_obj in included_hosts:
            for excluded_host in excluded_hosts:
                diff = set(pinput) - set(excluded_host.paths)
                for path in diff:
                    orders.append(get_order(path_statistics_obj.cluster, excluded_host.cluster, path))
    else:
        """ if there is no reference element, i.e., an host with all input dirs,
        try to find out which dir are missing to which host, and create orders
        for that. """

        for i, p1 in enumerate(excluded_hosts):
            for j, p2 in enumerate(excluded_hosts):
                if i != j:
                    # and len(p1.paths) < len(p2.paths):
                    diff = list(set(p1.paths) - set(p2.paths))
                    for path in diff:
                        orders.append(get_order(p1.cluster, p2.cluster, path))

    return sortOrders(orders)


def sortOrders(orders):
    """ sort orders by the number of copies that must happen to the destination
    host. E.g., if we need to process 1 order for destination host1, and 2 orders
    for destination host2, the order to host1 comes first than the order for
    destination host2 """

    countCopiesToDstHost = dict(Counter(elem[1] for elem in orders))
    sorted_countCopiesToDstHost = sorted(countCopiesToDstHost.items(),
                                         key=operator.itemgetter(1))
    sortedOrders = []
    for host in sorted_countCopiesToDstHost:
        for order in orders:
            if host[0] == order[1]:
                sortedOrders.append(order)

    return sortedOrders


def needMerge(subset, faults):
    """
    checks how many clusters does it have with all the directories there.
    The number of clusters with replicated data must be at least equal to the
    number of replication in case of no faults (n-f).
    """
    # get the majority value
    n = (2 * faults) + 1
    maj = n - faults

    # return a dict with the number of cluster names exists in the subset
    # Counter({'Node10-0': 2, 'Node00-2': 1, 'Node00-0': 1})
    how_many = collections.Counter(my_obj.cluster for my_obj in subset)
    count = sum(1 for v in how_many.values() if v >= maj)

    return count < maj
