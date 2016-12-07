import random
from medusa.sets import PathStatistics


def get_random_metrics(clusters, input):
    """
    Rank clusters randomly

    :param clusters (list) list of clusters
    :param input (string) input path
    :return (list of tuples) with the tuple (cluster, rank value)
    """

    random.shuffle(clusters)
    include = [PathStatistics(cluster, 0, input) for cluster in clusters]

    return include
