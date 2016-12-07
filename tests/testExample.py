import logging
import time

from medusa import xmlparser
from medusa.algorithm import run_execution, run_verification
from medusa.faultmode import run_execution_faultmode
from medusa.settings import medusa_settings


# This class runs an example that it is defined in the wordcount
# read wordcount xml
# cluster1: job1 --> aggregation: job3
# cluster2: job2 -----^
def set_jobs():
    path = medusa_settings.xml_wordcount

    gstart = time.time()
    """
    e.g. job_list
    [('hadoop jar hadoop-mapreduce-examples-2.0.4-alpha.jar wordcount /input /output', '/input', '/output'),
    ('hadoop jar hadoop-mapreduce-examples-2.0.4-alpha.jar wordcount /input2 /output2', '/input2', '/output2')]
    """
    faults_tolerate = 1
    job_list = xmlparser.parser(path, faults_tolerate, "job")
    aggregator = xmlparser.parser(path, faults_tolerate, "aggregator")

    sequence = [job_list, aggregator]

    boolean_result = [False] * 3
    step = 0
    while step < len(sequence):
        jobs = sequence[step]

        print "Step %s: running jobs %s" % (step, str(jobs))
        if len(jobs) == 0:
            step += 1
            continue

        digest_selected = run_element(jobs, boolean_result, step == 1)
        boolean_result = [output[1] for output in digest_selected]
        jobs_reexecute = [job for job,
                          result in zip(jobs, boolean_result) if not result]

        if len(jobs_reexecute) == 0:
            print "Step %s completed" % step
            step += 1
        else:
            sequence[step] = jobs

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
    digests_matrix = run_execution(
        faults_tolerate, jobs, boolean_result, aggregation)
    mend = time.time()
    span = str(mend - mstart)
    print "Execution time: %s" % span

    vstart = time.time()
    validation, digest_selected, failed_launch_command = run_verification(
        digests_matrix, faults_tolerate, aggregation)
    vend = time.time()
    span = str(vend - vstart)
    print "Verification time: %s" % span

    while not validation:
        print "Error in jobs. Re-executing again."
        digests_matrix = run_execution_faultmode(
            99, failed_launch_command, boolean_result, digests_matrix, aggregation)

        vstart = time.time()
        validation, digest_selected, failed_launch_command = run_verification(
            digests_matrix, faults_tolerate, aggregation)
        vend = time.time()
        span = str(vend - vstart)
        print "Verification time: %s" % span

    # Return digests: [(('7954a378837f4f24719c28c197e08098a22b5be8',), True)]
    return digest_selected


if __name__ == "__main__":
    logging.basicConfig(filename='myapp.log', level=logging.INFO)

    # profile.run('set_jobs()')
    # remote_write_pinger()
    set_jobs()
