import json
import logging
import multiprocessing as mp
import time
from collections import Counter
from threading import Thread

import os
from medusa import settings
from medusa.aggregation import aggregationMergeDirs
from medusa.decors import make_verbose
from medusa.execution import run_job
from medusa.hdfs import writeJobRunning, rmr
from medusa.jobsmanager import ExecutionJob
from medusa.jobsmanager import JobOutput
from medusa.medusasystem import my_apply_async_with_waiting
from medusa.medusasystem import update_json_file
from medusa.mergedirs import mergeDirs, mergeDirs_reexecution
from medusa.namedtuples import set_execution_params
from medusa.scheduler.predictionranking import load_prediction
from medusa.settings import medusa_settings
from medusa.simplecache import save_reexecute_another_cloud, get
from medusa.utility import majority


def run_execution(faults, jobs, aggregation, reference_digests=None):
    """
    Executes the job from different ways (serial|processes)

    :param faults: (int) number of faults to tolerate
    :param jobs: (int, tuple) command line to execute
    :param aggregation: (boolean) tells if it is the aggregation phase
    :param reference_digests: (list) list with the reference digests of the input files.

    :return:
    """
    logging.info("Execution mode: %s" % medusa_settings.execution_mode)

    if medusa_settings.execution_mode == "serial":
        group_data = run_execution_serial(
            faults, jobs, aggregation, reference_digests)
    else:
        group_data = run_execution_threads(
            faults, jobs, aggregation, reference_digests)

    return group_data


def run_execution_threads(faults, jobs, aggregation, reference_digests):
    """
     Execute jobs in serial

    :param faults: (int) Number of faults to tolerate
    :param jobs: (list) list of Job structures
    :param aggregation: (boolean) is it the aggregation phase or not
    :param reference_digests:
    :return:
    """

    group_jobs = []
    if not jobs:
        return group_jobs

    logging.info(" Running scheduling: %s" % medusa_settings.ranking_scheduler)

    job_args = []

    # Setup a list of processes that we want to run
    output = mp.Queue()
    processes = [Thread(target=_copy_and_aggregate, args=(job, reference_digests, aggregation, output)) for job in jobs]

    # Run and exit processes
    [p.start() for p in processes]
    [p.join() for p in processes]

    # Get process results from the output queue
    list_clusters = [output.get() for _ in processes]

    for clusters_to_launch_job, job in zip(list_clusters, jobs):
        logging.debug("Clusters included %s" % clusters_to_launch_job)
        job_args.append(
            ExecutionJob(job.id, clusters_to_launch_job, job.command, job.output_path + '/part*', majority(faults)))

    # if medusa_settings.relaunch_job_other_cluster and not aggregation:
    #     logging.warn("Please shut one cluster down... Execution will resume in 10 secs.")
    #     time.sleep(10)

    logging.info("Running %s jobs..." % (len(job_args)))
    seffective_job_runtime = time.time()

    processes = []
    for execution_parameters in job_args:
        # Each thread executes a job in the respective clusters
        processes.append(Thread(target=run_job, args=(execution_parameters, output, )))

    # Run processes
    [p.start() for p in processes]
    [p.join() for p in processes]

    logging.info("Run_job took %s" % str(seffective_job_runtime - time.time()))

    job_output_list = []
    _output_list = [output.get() for _ in processes]
    _job_output = [_output for _output in _output_list[0]]
    for _output in _job_output:
        job_output_list.append(parse_data(_output))

    digests_matrix = []
    while True:
        successful, digests = run_verification(job_output_list, aggregation)
        if not successful:
            if medusa_settings.relaunch_job_same_cluster:
                # relaunch job in the same cloud
                path_to_remove = os.path.dirname(execution_parameters.output_path)
                _relaunch_job_same_cluster(execution_parameters, path_to_remove)
            else:
                logging.debug("Re-launching job %s" % execution_parameters.command)
                save_reexecute_another_cloud(True)
                execution_parameters = _relaunch_job_other_cluster(execution_parameters, jobs,
                                                                   reference_digests, aggregation)

            _job_output = run_job(execution_parameters)
            for _output in _job_output:
                job_output_list.append(parse_data(_output))
        else:
            digests_matrix.append(digests)
            break

    # save progress of the job
    filename = settings.get_temp_dir() + "/job_progress_log.json"
    step = 2 if not aggregation else 5
    update_json_file(filename, step)

    eeffective_job_runtime = time.time()
    span = str(eeffective_job_runtime - seffective_job_runtime)

    """ The total time that it took to execute all jobs """
    logging.info("Effective job run-time: %s" % span)

    return digests_matrix


def run_execution_serial(faults, jobs, aggregation, reference_digests):
    """
     Execute jobs in serial

    :param faults: (int) Number of faults to tolerate
    :param jobs: (list) list of Job structures
    :param aggregation: (boolean) is it the aggregation phase or not
    :param reference_digests: (RefDigests) digests of reference
    :return: list with the result of the selected digest. Ex: (True, {u'/aggregate-output/part-r-00000': u'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'})
    """

    group_jobs = []
    if not jobs:
        return group_jobs

    logging.info(" Running scheduling: %s" % medusa_settings.ranking_scheduler)

    job_args = []
    for job in jobs:
        clusters_to_launch_job = _copy_and_aggregate(job, reference_digests, aggregation)

        logging.debug("Clusters included %s" % clusters_to_launch_job)
        job_args.append(
            ExecutionJob(job.id, clusters_to_launch_job, job.command, job.output_path + '/part*', majority(faults)))

    # if medusa_settings.relaunch_job_other_cluster and not aggregation:
    #     logging.warn("Please shut one cluster down... Execution will resume in 10 secs.")
    #     time.sleep(10)

    logging.info("Running %s jobs..." % (len(job_args)))
    seffective_job_runtime = time.time()

    digests_matrix = []
    for execution_parameters in job_args:
        _job_output_list = []
        while True:
            _job_output = run_job(execution_parameters) # run job in the set of clusters
            for _output in _job_output:
                _job_output_list.append(parse_data(_output))

            successful, digests = run_verification(_job_output_list, aggregation)
            if not successful:
                if medusa_settings.relaunch_job_same_cluster:
                    # relaunch job in the same cloud
                    path_to_remove = os.path.dirname(execution_parameters.output_path)
                    _relaunch_job_same_cluster(execution_parameters, path_to_remove)
                else:
                    # if len(_failed_exec) > 0:
                    logging.debug("Re-launching job %s" % execution_parameters.command)
                    save_reexecute_another_cloud(True)
                    execution_parameters = _relaunch_job_other_cluster(execution_parameters, jobs,
                                                                       reference_digests, aggregation)
            else:
                digests_matrix.append(digests)
                break

    # save progress of the job
    filename = settings.get_temp_dir() + "/job_progress_log.json"
    step = 2 if not aggregation else 5
    update_json_file(filename, step)

    eeffective_job_runtime = time.time()
    span = str(eeffective_job_runtime - seffective_job_runtime)

    """ The total time that it took to execute all jobs """
    logging.info("Effective job run-time: %s" % span)

    return digests_matrix


def parse_data(job_output):
    """
    Read the job output, write the result in the remote host, and return a job object.

    :param (string) result of job in string format
    :return Returns a Job object
    """
    job = JobOutput()
    job.loads(job_output)
    cluster = job.cluster
    logging.debug("Job finished at %s" % cluster)

    job_data = json.loads(job_output)
    job_prediction = load_prediction.apply_async(queue=cluster).get()
    job_prediction = json.loads(job_prediction)

    currentqueuecapacity = job_data["jobs"]["job"]["currentqueuecapacity"]
    hdfsbytesread = job_data["jobs"]["job"]["hdfsbytesread"]
    hdfsbyteswritten = job_data["jobs"]["job"]["hdfsbyteswritten"]
    maps = job_data["jobs"]["job"]["maps"]
    reduces = job_data["jobs"]["job"]["reduces"]
    total_time = job_data["jobs"]["job"]["totaltime"]
    mem_load = job_prediction["mem_load"]
    cpu_load = job_prediction["cpu_load"]

    step = get("step")

    if step == 0:
        job_list = get("job")[0]
        job_name = job_list.name
    else:
        aggregator_list = get("aggregator")[0]
        job_name = aggregator_list.name

    execution_params = set_execution_params(job_name, currentqueuecapacity, hdfsbytesread, hdfsbyteswritten,
                                            maps, reduces, mem_load, cpu_load, total_time)


    my_apply_async_with_waiting(writeJobRunning, queue=cluster, args=(json.dumps(execution_params._asdict()),))

    return job


def parse_digests(job_output):
    """
    return a list of digests
    """

    maj = majority(medusa_settings.faults)
    # list of dicts
    nset_digests = []
    for job in job_output:
        ndigests = {}
        for digest in job.digests:
            ndigests.update(digest) # concat dicts

        nset_digests.append(ndigests) # append it to a list

    keys = []
    for _d in job_output[0].digests:
        keys += _d.keys()

    digests_matrix = []
    for k in keys:
        temp_val = []
        for dset in nset_digests:
            temp_val.append(dset[k])

        v, k = Counter(temp_val).most_common(1)[0]

        if k >= maj:
            digests_matrix.append((True, v))
        else:
            return False, None

    return True, digests_matrix


@make_verbose
def run_verification(job_output, aggregation):
    """
    Check the digests of the set of jobs that have run

    :param job_output (list) list of output of the jobs (json)
    :param aggregation (Boolean) is it for aggregation?

    :return (tuple) with the result of the validation (True|False) or the selected digest
    """
    result, selected_digest = parse_digests(job_output)

    if settings.medusa_settings.faults_left > 0:
        result = False
        settings.medusa_settings.faults_left -= 1

    if result:
        filename = settings.get_temp_dir() + "/job_progress_log.json"
        step = 3 if aggregation == 0 else 6
        update_json_file(filename, step)

        # for purpose when it is necessary to execute in another cloud
        save_reexecute_another_cloud(False)

        return result, selected_digest

    return False, None


def run_verification_global(digests_matrix):
    """ check if the digests_matrix got all results of execution True

    :param digests_matrix (list) list with tuples with result of execution and the selected digests. [(True|False, "digest")]

    """
    for success in digests_matrix:
        if not success:
            return False

    return True


def _copy_and_aggregate(job, reference_digests, aggregation=False, output=None):
    """
    merge dirs in aggregation and in a normal merge
    :param job (Job) job structure
    :param reference_digests
    :param aggregation (boolean) check if it should aggregate data

    :return (list) list of clusters that contain the data
    """

    filename = settings.get_temp_dir() + "/job_progress_log.json"
    if not aggregation:
        new_clusters = mergeDirs(job, reference_digests)

        # save progress of the job
        update_json_file(filename, 1)
    else:
        new_clusters = aggregationMergeDirs(job, reference_digests)

        # save progress of the job
        update_json_file(filename, 4)

    if output is not None:
        output.put(new_clusters)
        return

    return new_clusters  # [new_clusters[-1]]  # shortcut just for this test. Must be removed in the end and put just "return new_clusters"


def _copy_and_aggregate_other_cluster(job, reference_digests, aggregation=False, output=None):
    """
    merge dirs in aggregation and in a normal merge
    :param job (Job) job structure
    :param reference_digests
    :param aggregation (boolean) check if it should aggregate data

    :return (list) list of clusters that contain the data
    """

    filename = settings.get_temp_dir() + "/job_progress_log.json"
    if not aggregation:
        new_clusters = mergeDirs_reexecution([job.output_path])

        # save progress of the job
        update_json_file(filename, 1)
    else:
        new_clusters = aggregationMergeDirs(job, reference_digests)

        # save progress of the job
        update_json_file(filename, 4)

    if output is not None:
        output.put(new_clusters)
        return

    return new_clusters  # [new_clusters[-1]]  # shortcut just for this test. Must be removed in the end and put just "return new_clusters"


def _relaunch_job_same_cluster(execution_parameters, path):
    """ relaunch job in the same cloud.

    :param execution_parameters (ExecutionJob) parameters for job execution
    :param path (string) path to remove
    """
    clusters = execution_parameters.clusters
    for cluster in clusters:
        rmr.apply_async(
            queue=cluster, args=(path,)).get()

    logging.debug(">>>>> relaunching the job in the same cluster <<<<<<")
    logging.debug("Hosts: %s\tPath: %s" % (clusters, path))


def _relaunch_job_other_cluster(execution_parameters, jobs, reference_digests, aggregation):
    """ deals with error that happens when a job did not return result in a specific interval.
    Therefore, it relaunches the job in another cluster.

     :param execution_parameters (ExecutionJob) parameters of execution
     :param jobs (list of Job) list of Job structure
     :param reference_digests (RefDigests) reference digests of a job
     :param aggregation (Boolean) tells if it is for the aggregation phase.
    """

    for job in jobs:
        if job.id == execution_parameters.id:
            new_clusters = _copy_and_aggregate_other_cluster(job, reference_digests, aggregation)
            execution_parameters.clusters = new_clusters

    return execution_parameters