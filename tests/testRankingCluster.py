import logging
import time
from collections import defaultdict

from medusa import xmlparser
from medusa.ranking import rank_clusters
from medusa.settings import medusa_settings


# This class runs an example that it is defined in the wordcount
# read wordcount xml
# cluster1: job1 --> aggregation: job3
# cluster2: job2 -----^
def set_jobs():
    path = "/home/pcosta/Programs/medusa_hadoop/submit/wordcount.xml"
    # path = "/home/pcosta/repositories/git/medusa_hadoop/submit/wordcount.xml"

    gstart = time.time()
    """
    e.g. job_list
    [('hadoop jar hadoop-mapreduce-examples-2.0.4-alpha.jar wordcount /input /output', '/input', '/output'),
    ('hadoop jar hadoop-mapreduce-examples-2.0.4-alpha.jar wordcount /input2 /output2', '/input2', '/output2')]
    """
    faults_tolerate = 1
    job_list = xmlparser.parser(path, faults_tolerate, "job")

    sequence = [job_list]

    boolean_result = [False] * 3
    step = 0
    jobs = sequence[step]

    print "Step %s: running jobs %s" % (step, str(jobs))

    run_element(jobs, boolean_result, step == 1)

    gend = time.time()
    span = str(gend - gstart)
    print "Global time: %s" % span


def run_element(jobs, boolean_result, aggregation):
    """
    jobs is a list of jobs to execute
    aggregation tells  if it is the aggregation phase
    boolean_result is a list that tells which jobs must repeat. E.g. [True, True]
    """

    faults_tolerate = 1

    mstart = time.time()
    run_executionxxx(faults_tolerate, jobs, boolean_result, aggregation)
    mend = time.time()
    span = str(mend - mstart)
    print "Ranking time: %s" % span


def run_executionxxx(faults, jobs, boolean_result, aggregation):
    """
    faults is the number of faults to tolerate
    jobs is the
    """
    group_jobs = []
    if not jobs:
        return group_jobs

    print " Running scheduling: %s" % medusa_settings.ranking_scheduler

    nr_job = 0
    history_rank = defaultdict(lambda: 1)
    for job in jobs:
        if not boolean_result[nr_job] or aggregation:
            gid = str(int(time.time()))
            command, pinput, poutput = job

            if not aggregation:
                clusters = rank_clusters(gid, pinput, faults)
                print "- %s ------------" % pinput
                print clusters
                print "-----------------"


if __name__ == "__main__":
    logging.basicConfig(filename='myapp.log', level=logging.INFO)

    # profile.run('set_jobs()')
    set_jobs()
