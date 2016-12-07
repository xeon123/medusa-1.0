import sys
import logging
import operator
from collections import defaultdict

import hadoopy
from celery import group, task
from celery.exceptions import TimeoutError
from medusa.simplecache import get_reexecute_another_cloud
from medusa.decors import make_verbose
from medusa.hdfs import exists
from medusa.orders import appendOrders, executeOrder
from medusa.scheduler.predictionranking import save_prediction
from medusa.ranking import rank_clusters
from medusa.settings import medusa_settings
from medusa.medusasystem import executeCommand, getRunningClusters
from medusa.sets import find_include_exclude_clusters

"""
This file returns a list of tuples that gives the information about which clusters the files are copied.
E.g. (cluster1, cluster2, src, dest) tells that src will be copied from cluster1 to cluster2 to the dest dir

The entry point to this file is merge_dirs
"""


def mergeDirsWrapper(args):
    """
    Wrapper to the mergeDirs.

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

    return mergeDirs(params, args.ref_digests)


@make_verbose
def mergeDirs(job, reference_digests):
    """
    merge dirs but not for the aggregation

    :param job (Job) job structure
    :param reference_digests

    :return (list) a list of clusters where the input was copied

    """
    # preparation
    active_clusters = getRunningClusters()
    host_spec = find_include_exclude_clusters(active_clusters, job.input_path, False)
    included_hosts, excluded_hosts = rank_clusters(job.input_path, host_spec)

    count = len(included_hosts)

    logging.debug("Merging dirs (%s)" % job.command)
    if count == 0:
        sys.exit("You cannot copy data that do not exist")

    reexecute = get_reexecute_another_cloud()
    if count == 1 or reexecute:
        from_host = included_hosts[0].cluster

        new_included = [] if reexecute else [from_host]

        orders = appendOrders(from_host, excluded_hosts, included_hosts[0].paths)
        logging.debug("orders %s" % (str(orders)))

        if len(orders) > 0:
            if medusa_settings.ranking_scheduler == "prediction":
                for ehost in excluded_hosts:
                    prediction_value = ehost.rank
                    q = ehost.cluster
                    save_prediction.apply_async(
                        queue=q, args=((prediction_value, 0),)).get()

            count = 0
            while count < len(orders):
                try:
                    new_included.append(executeOrder(orders[count], reference_digests))
                except (TimeoutError, IOError):
                    count += 1
                    if count >= len(orders):
                        raise Exception("No more orders available")
                else:
                    break
        elif len(orders) == 0 and len(excluded_hosts) == 0:
            new_included = [host.cluster for host in included_hosts]
    else:
        new_included = [included_host.cluster for included_host in included_hosts]

    return new_included


@make_verbose
def mergeDirs_reexecution(path):
    """
    merge dirs but not for the aggregation

    :param path (list) path

    :return (list) a list of clusters where the input was copied

    """
    # preparation
    active_clusters = getRunningClusters()
    host_spec = find_include_exclude_clusters(active_clusters, path, False)
    included_hosts, excluded_hosts = rank_clusters(path, host_spec)

    new_included = [excluded_host.cluster for excluded_host in excluded_hosts]

    return new_included


@make_verbose
def mergeDirsWithoutRules(params):
    """
    merge dirs but not for the aggregation
    """
    gid, job, faults, history_rank = params
    command, pinput, poutput = job
    included, excluded = rank_clusters(gid, pinput, faults)

    count = len(included)

    logging.debug("Merging dirs (%s)" % command)
    if count == 0:
        sys.exit("You cannot copy data that do not exist")

    new_included = [cluster[0] for cluster in included]

    from_host = included[0][0]
    orders = appendOrders(from_host, excluded, pinput, faults)
    logging.debug("orders %s" % (str(orders)))

    if len(orders) > 0:
        if medusa_settings.ranking_scheduler == "prediction":
            prediction_value = excluded[0][1]
            error_value = excluded[0][2]
            q = excluded[0][0]
            save_prediction.apply_async(
                queue=q, args=((prediction_value, error_value),)).get()

        new_included = [executeOrder(orders[0])]

    return new_included, command, poutput


def findSubset(reference_paths, clusters):
    """
    find a subset of clusters that has at least 1 reference_path.
    The subset is ordered in reverse to the clusters with the bigger number of paths being in the front

    reference_paths paths to look in clusters
    """
    exec_commands = []
    for cluster in clusters:
        s1 = existsbatch.s(reference_paths)
        exec_commands.append(s1.set(queue=cluster))

    g = group(exec_commands)
    g = g()
    data_list = g.get()

    """
    list that contains tuples that tells what path a cluster has. E.g.,
    [('host1', ['/gutenberg-output']),
    ('host2', ['/gutenberg-output2']),
    ('host3', ['/gutenberg-output']),
    ('host4', ['/gutenberg-output2'])]
    """
    listpairs = zip(clusters, data_list)

    """
    rank_dict=defaultdict(<type 'list'>, {1: [('host3', ['/gutenberg-output3']),
                                                ('host4', ['/gutenberg-output2'])],
                                          2: [('host1', ['/gutenberg-output', '/gutenberg-output2']),
                                              ('host2', ['/gutenberg-output', '/gutenberg-output3'])]
                                         })
    """
    rank_dict = defaultdict(list)
    for (cluster, _paths) in listpairs:
        rank_dict[len(_paths)].append((cluster, _paths))

    return rank_dict


@task(name='medusa.mergedirs.existsbatch')
def existsbatch(paths):
    """
    check if exists several reference_paths and tell which reference_path exist
    """

    list_result = []
    for path in paths:
        command = exists(path)
        result = executeCommand(command)
        if int(result) > 0:
            list_result.append(path)

    return list_result


@task(name='medusa.mergedirs.removedir')
def removedir(paths):
    """
    remove paths
    """

    for path in paths:
        hadoopy.rmr(path)
    return True


def _set_rank(clusters, rank):
    """ set rank value to the  path statistics """
    included_hosts, excluded_hosts = clusters
    for name, value in rank:
        [inst.set_rank(value)
         for inst in included_hosts if inst.cluster == name]
        [inst.set_rank(value)
         for inst in excluded_hosts if inst.cluster == name]

    # sort_by_rank
    included_hosts = sorted(included_hosts, key=operator.attrgetter('rank'))
    excluded_hosts = sorted(excluded_hosts, key=operator.attrgetter('rank'))

    return included_hosts, excluded_hosts
