# import time
import operator
import collections
from collections import defaultdict, Counter

from medusa.aggregation import aggregationMergeDirs


# from multiprocessing.dummy import Pool

# from manager.mergedirs import mergeDirs
# from manager.settings import RANKING_SCHEDULER

# from manager.xmlparser import getJobs, getAggregator

# This class runs an example that it is defined in the wordcount
# read wordcount xml
# cluster1: job1 --> aggregation: job3
# cluster2: job2 -----^
# def set_jobs():
# path="/home/xeon/repositories/git/medusa_hadoop/submit/wordcount.xml"

#     gstart= time.time()
#     """
#     e.g. job_list
#     [('hadoop jar hadoop-mapreduce-examples-2.0.4-alpha.jar wordcount /input /output', '/input', '/output'),
#     ('hadoop jar hadoop-mapreduce-examples-2.0.4-alpha.jar wordcount /input2 /output2', '/input2', '/output2')]
#     """
#     job_list=getJobs(path)
#     aggregator=getAggregator(path)

#     sequence = [job_list, aggregator]

#     boolean_result=[False]*3
#     step=0

#     jobs=sequence[step]

#     print "Step %s: running jobs %s" %(step, str(jobs))

#     run_element(jobs, boolean_result, step==1)

#     gend= time.time()
#     span=str(gend-gstart)
#     print "Global time: %s" %(span)


# def run_element(jobs, boolean_result, aggregation):
#     """
#     jobs is a list of jobs to execute
#     aggregation tells  if it is the aggregation phase
#     boolean_result is a list that tells which jobs must repeat. E.g. [True, True]
#     """

#     faults_tolerate= 1

#     mstart= time.time()
#     run_execution(faults_tolerate, jobs, boolean_result, aggregation)
#     mend= time.time()
#     span=str(mend-mstart)
#     print "Execution time: %s" %(span)


# def run_execution(faults, jobs, boolean_result, aggregation):
#     """
#     faults is the number of faults to tolerate
#     jobs is the
#     """
#     group_jobs = []
#     if not jobs:
#         return group_jobs

#     print " Running scheduling: %s" %(RANKING_SCHEDULER)

#     pool=Pool(processes=4)

#     history_rank=defaultdict(lambda: 1)

#     args = []
#     for job in jobs:
#         gid = str(int(time.time()))
#         args.append((gid, job, faults, history_rank))

#     mergeDirs(args[0])
#     pool.map(mergeDirs, args)

def merge_elems():
    """
    returns a set of orders from a set of hosts. In the case where all the clusters needs to copy some dirs, this method create a set of orders suitables for the host. E.g., if host1 needs dir3, and host2 needs dir1 and dir2, a set of orders will be created so that all hosts at the end have dir1, dir2 and dir3
    """

    s = [(
        2, ('pc2.hadoopmrbftpython1.emulab-net.emulab.larc.usp.br', ['/wiki-output', '/wiki-output2'])),
        (2, ('pc20.hadoopmrbftpython0.emulab-net.utahddc.geniracks.net',
             ['/wiki-output', '/wiki-output3'])),
        (2, ('pc48.hadoopmrbftpython3.emulab-net.uky.emulab.net', ['/wiki-output2', '/wiki-output3']))]
    d = defaultdict(list)
    for k, v in s:
        d[k].append(v)

    pinput = ['/wiki-output', '/wiki-output2', '/wiki-output3']
    """
    list of orders to execute [(nr_dirs_to_copy, from cluster, to cluster, [src_dirs], [dest_dirs])].     verbose decors.py:18
    run_element testExample.py:61
    [(1, 'host1', 'host2', ['/gutenberg-output3'], ['/gutenberg-output    set_jobs testExample.py:37
    (2, 'host3', 'host4', ['/gutenberg-output', '/gutenberg-out    <module> testExample.py:81
    """
    orders = []
    for k in d:
        vlist = d[k]
        for v in vlist:
            target_host = v[0]
            paths = v[1]
            leftovers = list(set(pinput) - set(paths))
            for leftover in leftovers:
                src_host = who_has_it(leftover, d)
                if len(src_host) > 0:
                    orders.append(
                        (len([leftover]), src_host, target_host, [leftover], [leftover]))

    print orders
    return orders


def test_aggregation():
    counter = collections.Counter({'Node10-0': 2, 'Node00-2': 2, 'Node00-0': 2})
    params = ((
                  '/root/Programs/hadoop/bin/hadoop jar /root/Programs/hadoop/wordcountaggregator.jar /wiki-output,/wiki-output3 /aggregate-output',
                  ['/wiki-output', '/wiki-output3'], '/aggregate-output'), 1, counter)

    aggregationMergeDirs(params)


def test_sort_orders():
    """Sort order requests """

    orders = [('Host1', 'Host2', '/wiki-output', '/wiki-output'),
              ('Host1', 'Host2', '/wiki-output3', '/wiki-output3'),
              ('Host1', 'Host4', '/wiki-output', '/wiki-output'),
              ('Host2', 'Host1', '/wiki-output2', '/wiki-output2'),
              ('Host2', 'Host4', '/wiki-output2', '/wiki-output2'),
              ('Host3', 'Host1', '/wiki-output2', '/wiki-output2'),
              ('Host3', 'Host2', '/wiki-output', '/wiki-output'),
              ('Host3', 'Host4', '/wiki-output2', '/wiki-output2'),
              ('Host3', 'Host4', '/wiki-output', '/wiki-output'),
              ('Host4', 'Host2', '/wiki-output3', '/wiki-output3'),
              ('Host4', 'Host3', '/wiki-output3', '/wiki-output3')]

    countCopiesToDstHost = dict(Counter(elem[1] for elem in orders))
    sorted_countCopiesToDstHost = sorted(countCopiesToDstHost.items(), key=operator.itemgetter(1))
    print countCopiesToDstHost
    print sorted_countCopiesToDstHost
    sortedOrders = []
    for k in sorted_countCopiesToDstHost:
        for o in orders:
            if k[0] == o[1]:
                sortedOrders.append(o)

    print sortedOrders


def who_has_it(leftover, default_dict):
    """
    which host has the leftover path
    """
    for k in default_dict:
        value = default_dict[k]
        for host, paths in value:
            if leftover in paths:
                return host

    return ""


if __name__ == "__main__":
    # set_jobs()
    # merge_elems()
    # test_aggregation()
    test_sort_orders()
