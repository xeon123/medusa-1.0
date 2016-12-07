import time
import logging
import collections

from medusa.copydata import copy_data
from medusa.decors import make_verbose
from medusa.namedtuples import get_order
from medusa.utility import majority


@make_verbose
def appendOrders(from_cluster, excluded, paths):
    """
    append orders. The orders are the params to do a distcp.
    Function not used in aggregation

    :param from_cluster (string) src host where the data is going to be copied
    :param excluded (list) list of path statistics of hosts that does not contain the path
    :param paths (list) list of paths to copy
    """
    orders = []

    for inst in excluded:
        to_cluster = inst.cluster
        for path in paths:
            orders.append(get_order(from_cluster, to_cluster, path))

    return orders


def executeOrder(order, reference_digests):
    """
    Execute the order.

    :param order (Order) object with the order of the copy
    :param reference_digests (RefDigests) object with the reference digests

    :return the cluster where data was copied
    """
    cstart = time.time()

    cluster = copy_data(order, reference_digests)

    # if medusa_settings.ranking_scheduler == "prediction":
    #     manager.scheduler.predictionranking.history_choice[cluster] += 1
    #     save_penalization_file(manager.scheduler.predictionranking.history_choice)

    cend = time.time()
    span = str(cend - cstart)

    logging.info("Copy data time (start: %s, end: %s): %s" % (cstart, cend, span))
    return cluster


def needs_more_copies(subset, pinput, faults):
    """
    check how many clusters contains all the inputs
    """

    # get the majority value
    entries = collections.Counter(
        {my_obj.cluster: len(my_obj.paths) for my_obj in subset})
    count = sum(1 for entry in entries.items() if entry[1] >= len(pinput))

    return count < majority(faults)


