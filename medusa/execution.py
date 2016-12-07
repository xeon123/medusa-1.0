import json
import logging
import time
import traceback
from collections import defaultdict

from celery import group
from medusa.decors import make_verbose
from medusa.hdfs import ls
from medusa.jobsmanager import JobExecution
from medusa.medusasystem import executeCommand, get_queue_info
from medusa.medusasystem import my_apply_async
from medusa.medusasystem import my_apply_async_with_waiting, waiting
from medusa.scheduler.predictionranking import load_prediction, \
    save_penalization, set_penalization_params
from medusa.settings import getJobJSON, medusa_settings
from medusasystem import generate_one_digest


@make_verbose
def run_job(execution_param, queue=None):
    """
    execute a job.

    :param execution_param (ExecutionJob) object used to prepare the execution of the job.
    """

    clusters = execution_param.clusters
    how_many_runs = execution_param.how_many_runs
    command = execution_param.command
    output_path = execution_param.output_path

    executors = []
    execution_time = defaultdict(int)
    try:
        sstart = time.time()
        for idx, cluster in enumerate(clusters):
            if idx < how_many_runs:
                # only launches maximum 2 jobs (2 is the majority)
                execution_time[cluster] = time.time()

                # execute the job
                logging.info("Executing job at %s " % cluster)
                g1 = group(executeCommand.s(command, 1, ).set(queue=cluster), get_queue_info.s().set(queue=cluster))
                executors.append(JobExecution(g1(), cluster, sstart))
    except:
        logging.error(str(traceback.format_exc()))

    json_results = []
    failed_exec = []
    for executor in executors:
        _exec = executor.get_execution()
        cluster = executor.get_cluster()

        try:
            """ waiting for a task to finish """
            waiting(cluster, _exec)
            _output = _exec.get()
        except Exception:
            failed_exec.append(execution_param)
            continue

        job_output, queue_info = _output

        makespan = time.time() - executor.get_start_time()
        logging.info("Job executed in %s seconds", makespan)
        dstart = time.time()

        files = my_apply_async_with_waiting(
            ls, queue=cluster, args=(output_path,))

        tasks = []
        for _file in files:
            tasks.append(generate_one_digest.s(_file, medusa_settings.digest_command).set(queue=cluster))
        g1 = group(tasks)()

        while g1.waiting():
            time.sleep(2)

        digests = g1.get()
            # f.append(my_apply_async_without_waiting(
            #     generate_one_digest, queue=cluster, args=(_file, medusa_settings.digest_command,)))

        # digests = {}
        # for _f in tasks:
        #     digests.update(_f.get())

        logging.info("Digests generated in %s seconds", time.time()-dstart)


        # very slow
        # dstart = time.time()
        # digests2 = my_apply_async_with_waiting(generate_digests, queue=cluster, args=(output_path, medusa_settings.digest_command, ))
        # logging.info("Digests2 generated in %s seconds", time.time() - dstart)

        if not "FileAlreadyExistsException" in job_output:
            json_out = getJobJSON(job_output, cluster, queue_info, makespan, digests)
            json_out = json_out.replace("\n", "").replace("\'", "\"")

            logging.debug("Got result from %s" % cluster)
            json_results.append(json_out)

        if medusa_settings.ranking_scheduler == "prediction":
            f = my_apply_async(load_prediction, queue=cluster)

            value = f.get()
            prediction_value = json.loads(value)["total_time"]
            error = makespan - prediction_value
            penalization_params = set_penalization_params(makespan, prediction_value, error)

            logging.info("Job executed in %s seconds; predicted: %s seconds; error %s seconds", makespan, prediction_value, error)

            my_apply_async(save_penalization, queue=cluster, args=(json.dumps(penalization_params._asdict()),)).get()

    if queue is not None:
        queue.put(json_results)
        return

    return json_results
