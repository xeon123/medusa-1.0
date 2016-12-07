import json
import logging
import time
import traceback
from collections import defaultdict
from multiprocessing.dummy import Pool

from medusa import xmlparser
from medusa.aggregation import aggregationMergeDirs
from medusa.decors import make_verbose
from medusa.faultmode import run_execution_faultmode
from medusa.hdfs import generateDigests, writeJobRunning
from medusa.mergedirs import mergeDirs
from medusa.medusasystem import execute_and_get_digests, executeCommand
from medusa.namedtuples import set_execution_params
from medusa.utility import majority
from medusa.vote.voting import vote
from medusa.xmlparser import parseJobOutputMetrics
from medusa.settings import medusa_settings


# This class runs an example that it is defined in the wordcount
# read wordcount xml
# cluster1: job1 --> aggregation: job3
# cluster2: job2 -----^
def set_jobs():
    path = "/home/xeon/repositories/git/medusa_hadoop/submit/wordcount.xml"

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


@make_verbose
def run_job_test(pparams):
    """
    execute a jobs
    clusters is the cluster used to launch the job
    command command to run
    poutput is the output path of the execution that will be used to create the digests
    how_many_runs tells how many jobs are going to be launched at most
    """
    clusters, params, how_many_runs = pparams
    command, poutput = params
    command2 = generateDigests(poutput)
    chain = execute_and_get_digests(command, command2)

    executors = []
    nr_jobs_launched = 0
    try:
        for cluster in clusters:
            # only launches maximum 2 jobs (2 is the majority)
            if nr_jobs_launched < how_many_runs:
                nr_jobs_launched += 1
                print "Execute the job at %s (%s): %s" % (cluster, str(time.time()), chain)
                # execute
                s1 = executeCommand.apply_async(
                    queue=cluster, args=(chain, cluster,))
                executors.append(s1)
    except:
        print str(traceback.format_exc())

    results = []
    for executor in executors:
        results.append(executor.get())

    return results


def get_digests(cluster, path):
    """ generate digests from the given path and returns these value """

    # list output files
    command = generateDigests(path)
    output = executeCommand.apply_async(queue=cluster, args=(command,))
    digests = output.get()

    return str(digests).splitlines()


def run_execution(faults, jobs, aggregation):
    """
    faults is the number of faults to tolerate
    jobs is the
    """
    group_jobs = []
    if not jobs:
        return group_jobs

    print " Running scheduling: %s" % medusa_settings.ranking_scheduler

    seffective_job_runtime = 0
    pool = Pool(processes=4)

    history_rank = defaultdict(lambda: 1)

    args = []
    for job in jobs:
        gid = str(int(time.time()))
        if not aggregation:
            args.append((gid, job, faults, history_rank))
        else:
            args += (gid, job, faults)

    outputs = []
    if not aggregation:
        outputs = pool.map(mergeDirs, args)
    else:
        outputs = pool.map(aggregationMergeDirs, args)

    for output in outputs:
        new_included, new_command, new_poutput = output
        print "Clusters included %s" % new_included

        params = (new_command, new_poutput + '/part*')
        pparams = (new_included, params, majority(faults))
        output = run_job_test(pparams)
        # job_args.append((new_included, params, majority(faults)))

    # outputs=pool2.map(run_job, job_args)
    # seffective_job_runtime = time.time()
    # print "Running jobs (%s)" %(new_command)
    # gjobs = run_job(new_included, params, majority(faults))
    # group_jobs.append({gid: gjobs})

    group_data = []
    for waiter in group_jobs:
        dlist = []
        key, value = waiter.items()[0]
        for v in value:
            cluster, xmlfile = v.get()
            # print xmlfile
            print "Job finished at %s" % cluster
            data = parseJobOutputMetrics(xmlfile)

            logline = "%s:%s:%s:%s:%s:%s:%s:%s" % (cluster,
                                                   data[
                                                       'currentqueuecapacity'],
                                                   data['hdfsbytesread'],
                                                   data['hdfsbyteswritten'],
                                                   data['jobsrunning'],
                                                   data['maps'],
                                                   data['reduces'],
                                                   data['time'])

            command = writeJobRunning(logline)
            s1 = executeCommand.apply_async(queue=cluster, args=(command,))
            s1.get()

            dlist.append([data['digests']])

        group_data.append({key: dlist})

    eeffective_job_runtime = time.time()

    span = str(eeffective_job_runtime - seffective_job_runtime)

    """ The total time that it took to execute all jobs """
    print "Effective job run-time: %s" % span

    return group_data


@make_verbose
def run_verification(data_list, faults):
    """
    [
     [{'host1': ['a11e2cf']}, {'host2': ['a11e2cf']}],
    [{'host1': ['ajxbkcf']}, {'host4 ['ajxbcf']}]
    ]
    """
    result = []

    """
    data_list = [
    {'gid1': [['c240321469'], ['c240321469']]}
    ]
    """
    for data in data_list:
        print "Validate data"
        output = vote(data, faults)
        logging.debug("voting on this: " + str(data))
        logging.debug("voted: " + str(output))

        """
        output is a tuple (('c240321469',), True),
         where the boolean tells if there is a majority of values, and give the elected value
        """
        result.append(output)

    return result


def run_element(jobs, boolean_result, aggregation):
    """
    jobs is a list of jobs to execute
    aggregation tells  if it is the aggregation phase
    boolean_result is a list that tells which jobs must repeat. E.g. [True, True]
    """
    faults_tolerate = 1

    mstart = time.time()
    digests_matrix = run_execution(
        faults_tolerate, jobs, aggregation)
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
            99, failed_launch_command, digests_matrix, aggregation)

        vstart = time.time()
        validation, digest_selected, failed_launch_command = run_verification(
            digests_matrix, faults_tolerate, aggregation)
        vend = time.time()
        span = str(vend - vstart)
        print "Verification time: %s" % span

    # Return digests: [(('7954a378837f4f24719c28c197e08098a22b5be8',), True)]
    return digest_selected

def test_write_job_running():
    job_data = json.dumps({     "jobs": {        "job": {        "id": " job_1457608311823_0013",       "cluster": "medusa1-blank2",       "filebytesread": "129",       "filebyteswritten": "954031",       "hdfsbytesread": "150",       "hdfsbyteswritten": "57",       "maps": "1",       "reduces": "8",       "localmaps": "1",       "rackmaps": "0",       "totaltime": "45.8013808727",       "timespentmaps": "2653",       "timespentreduces": "101162",       "digests": {"/output1/part-r-00004": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", "/output1/part-r-00005": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", "/output1/part-r-00006": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", "/output1/part-r-00007": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", "/output1/part-r-00000": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", "/output1/part-r-00001": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", "/output1/part-r-00002": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", "/output1/part-r-00003": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}    ,         "queuecapacity": " 100.0",        "maximumqueuecapacity": " 100.0",        "currentqueuecapacity": " 0.0 "            }     } })
    job_data = json.loads(job_data)
    currentqueuecapacity = job_data["jobs"]["job"]["currentqueuecapacity"]
    hdfsbytesread = job_data["jobs"]["job"]["hdfsbytesread"]
    hdfsbyteswritten = job_data["jobs"]["job"]["hdfsbyteswritten"]
    maps = job_data["jobs"]["job"]["maps"]
    reduces = job_data["jobs"]["job"]["reduces"]
    totaltime = job_data["jobs"]["job"]["totaltime"]
    mem = 123
    cpu = 1234

    execution_params = set_execution_params(currentqueuecapacity, hdfsbytesread, hdfsbyteswritten, maps, reduces, mem, cpu, totaltime)

    writeJobRunning(json.dumps(execution_params._asdict()))

if __name__ == "__main__":
    # set_jobs()
    test_write_job_running()