import logging

from medusa.decors import make_verbose
from medusa.scheduler.predictionranking import get_prediction_metrics
from medusa.scheduler.randomranking import get_random_metrics
from medusa.settings import medusa_settings

"""
 rank clusters according several metrics
"""


@make_verbose
def rank_clusters(paths, host_spec=None):
    """
        Rank the clusters

        :param paths is the input dir
        :param host_spec (tuple) tuple that contains 2 lists. One with the clusters that contains the input path,
        and another one that doesn't have.

        :return The rank is saved in a tuple with the form (included set, excluded set) and returned
        Included and excluded set are ("cluster":points)
        e.g. ([('adminuser-VirtualBox-073n', 28.6)], [('adminuser-VirtualBox-074n', 24.7)])
    """

    rank_name = medusa_settings.ranking_scheduler

    rank = ()
    if rank_name == "prediction":
        rank = get_prediction_metrics(host_spec, paths)
    elif rank_name == "random":
        rank = host_spec
        # rank = get_random_metrics(host_spec, paths)

    logging.debug("Host ranking list: " + str(rank))

    return rank
