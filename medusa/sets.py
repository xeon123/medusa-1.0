from hdfs import ls, exists_all
from medusasystem import my_apply_async

"""
 rank clusters according several metrics
"""

def find_include_exclude_clusters(clusters, paths, aggregator):
    """ find clusters that contains and not contains the input

        :param clusters (list) list of clusters to search
        :param paths (list) list of paths to look for
        :param aggregator (booleas) tells it if it is for the aggregation stage or not

        :param tuple with a list of hosts that have the path or not.
    """

    included_hosts = includedSet(clusters, paths, aggregator)
    if not aggregator:
        """ List of Pathstatistics object that contains data of the paths, and clusters """
        if len(included_hosts) == 0:
            raise Exception("Cannot find clusters with %s" % paths)

        # if len(included_hosts) <= majority(faults):
        excluded_hosts = excludedSet(included_hosts, clusters)
    else:
        # list the dirs that are in each cluster
        host_dir_list = includedSet(clusters, paths, False)
        excluded_hosts = []
        # there is a cluster that contains all the input:
        if len(included_hosts) > 0:
            for ihost in included_hosts:
                for ehost in host_dir_list:
                    if ihost.cluster != ehost.cluster:
                        excluded_hosts.append(ehost)
        else:
            excluded_hosts = host_dir_list

    return included_hosts, excluded_hosts


def includedSet(clusters, paths=None, aggregator=False):
    """ creates a list with the clusters that contains the input
        gid unique ID
        paths a list of input directories.

        All the directories must be in the cluster, otherwise
        does not belong to the set
    """

    if aggregator:
        subset = findSubsetThatContainsAllPaths(paths, clusters)
    else:
        subset = findSubsetThatContainsPath(paths, clusters)

    return subset


def excludedSet(included_hosts, clusters):
    """
    creates a list with the clusters that does not contains the input
    we need to have the include set defined

    :param included_hosts (list) list of clusters that contains the input files
    :param clusters (list) list with clusters that are not in the include set -> CLUSTERS/include set

    :return (list) list of clusters that does not contains the input files
    """

    # list that contains the cluster of the PathStatistics elements in the cluster
    clusters_included = [inc.cluster for inc in included_hosts]

    # get the clusters that does't contain the path
    subset = list(set(clusters) - set(clusters_included))
    clusters_excluded = [PathStatistics(entry) for entry in subset]

    return clusters_excluded


def findSubsetThatContainsPath(paths, clusters):
    """
    find a subset of clusters where the path is
        all the paths must be in it, otherwise it does not belong to the subset
    """

    included_hosts = []
    for cluster in clusters:
        # it is used to create a ranking of the clusters that have more files
        for path in paths:
            files = my_apply_async(ls, queue=cluster, args=(path,)).get()
            number_total_files = len(files)

            if number_total_files > 0:
                if any(inst.cluster == cluster for inst in included_hosts):
                    for idx, o in enumerate(included_hosts):
                        if o.cluster == cluster:
                            o.paths.append(path)
                            included_hosts[idx] = o
                else:
                    included_hosts.append(PathStatistics(cluster, 0, [path]))

    return included_hosts


def findSubsetThatContainsAllPaths(paths, clusters):
    """
    find a subset of clusters where the path is
        all the paths must be in it, otherwise it does not belong to the subset
    """

    included_hosts = []
    for q in clusters:
        # it is used to create a ranking of the clusters that have more files
        f = my_apply_async(exists_all, queue=q, args=(paths, ))
        if f.get():
            included_hosts.append(PathStatistics(q, 0, paths))

    return included_hosts


class PathStatistics:

    def __init__(self, cluster, rank=0, paths=None):
        self.cluster = cluster
        self.paths = paths
        self.rank = rank

    def set_rank(self, rank):
        self.rank = rank

    def __str__(self):
        return "cluster: %s, paths: %s, rank: %s" % (self.cluster, self.paths, self.rank)
