from collections import Counter

from medusa.decors import make_verbose
from medusa.settings import decrease_faults, medusa_settings
from medusa.utility import majority


@make_verbose
def vote(digests, aggregation, faults=0):
    """
    from a matrix get a majority of values

    E.g.str(data).splitlines()
    {gid:[[list digests 1], [list digests 2]}
    """
    if faults == 0:
        return None

    value = Counter(tuple(item) for item in digests).most_common(1)

    if medusa_settings.digests_fault and not aggregation and medusa_settings.faults_left > 0:
        decrease_faults()
        return None, False

    nr_produced_equal_digests = value[0][1]
    if nr_produced_equal_digests < majority(faults):
        return None, False

    digests_selected = value[0][0]
    return digests_selected, True
