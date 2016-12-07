import time
import logging
from collections import defaultdict

from medusa.aggregation import aggregationMergeDirs
from medusa.execution import run_job
from medusa.decors import make_verbose
from medusa.mergedirs import mergeDirsWithoutRules
from medusa.settings import medusa_settings


@make_verbose
def run_execution_faultmode(
        faults, jobs, digests_matrix, aggregation):
    logging.info("Execution mode: %s" % medusa_settings.execution_mode)

    # if EXECUTION_MODE == "process":
    #     group_data = run_faultmode_processes(faults, jobs, boolean_result, aggregation)
    # else:
    group_data = run_faultmode_serial(faults, jobs, aggregation)

    digests_matrix = concat_matrix(group_data, digests_matrix)

    return digests_matrix


@make_verbose
def run_faultmode_serial(faults, jobs, aggregation):
    """
    faults is the number of faults to tolerate
    jobs is the
    """
    group_jobs = []
    if not jobs:
        return group_jobs

    logging.info(" Running scheduling: %s" % medusa_settings.ranking_scheduler)

    history_rank = defaultdict(lambda: 1)

    args = []
    for job in jobs:
        gid = str(int(time.time()))
        if not aggregation:
            args.append((gid, job, faults, history_rank))
        else:
            args.append((gid, job, faults))

    outputs = []
    if not aggregation:
        for arg in args:
            outputs.append(mergeDirsWithoutRules(arg))
    else:
        for arg in args:
            outputs.append(aggregationMergeDirs(arg))

    job_args = []
    for output in outputs:
        new_included, new_command, new_poutput = output
        logging.debug("Clusters included %s" % new_included)

        params = (new_command, new_poutput + '/part*')
        job_args.append((new_included, params, len(new_included)))

    logging.info("Running jobs")
    seffective_job_runtime = time.time()
    job_outputs = []
    for job_arg in job_args:
        job_outputs.append(run_job(job_arg))

    group_data = parseXmlData(job_outputs)
    eeffective_job_runtime = time.time()

    span = str(eeffective_job_runtime - seffective_job_runtime)

    """ The total time that it took to execute all jobs """
    logging.info("Effective job run-time: %s" % span)

    return group_data


def concat_matrix(group_data, digests_matrix):
    """
    concat the group_data into the digests_matrix
    """
    for idx, row in enumerate(digests_matrix):
        if group_data[0][0] == row[0]:
            concat_matrix = row[1] + group_data[0][1]
            l = list(row)
            l[1] = concat_matrix
            digests_matrix[idx] = tuple(l)

    return digests_matrix
