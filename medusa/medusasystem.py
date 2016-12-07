import hashlib
import logging
import shlex
import subprocess
import time

import hadoopy
import simplejson as json
from celery import task
from celery.exceptions import TimeoutError
from celery.result import AsyncResult
from hadoopy._hdfs import _checked_hadoop_fs_command
from medusa import settings
from medusa.simplecache import MemoizeCalls
from medusa.decors import make_verbose
from medusa.local import lcat
from medusa.namedtuples import get_reference_digest, get_aggregation_reference_digest
from medusa.pingdaemon import clusterWorking
from medusa.settings import medusa_settings


@task(name='medusa.medusasystem.executeCommand')
@make_verbose
def executeCommand(command, stdout=0):
    """ Execute locally the command """

    logging.debug("Executing: %s" % command)
    process = subprocess.Popen(
        shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out = process.communicate()
    output = out[stdout]

    logging.debug("Result of command %s: %s" % (command, output))

    # If we were capturing, this will be a string; otherwise it will be None.
    return output


def local_execute_command(command, stdout=0):
    """ Execute locally the command """

    logging.debug("Executing: %s" % command)
    process = subprocess.Popen(
        shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out = process.communicate()
    output = out[stdout]

    logging.debug("Result of command %s: %s" % (command, output))

    # If we were capturing, this will be a string; otherwise it will be None.
    return output


@make_verbose
def execute_and_get_digests(run_command, gen_digests_command):
    # return HDFS_BIN + " -ls -R " + path
    return (
        "%s/execute.sh \"%s\" \"%s\"" % (settings.get_medusa_home() + "/scripts",
                                         run_command, gen_digests_command)
    )


@task(name='medusa.medusasystem.hello')
@make_verbose
def hello(hostname):
    return "Hello %s" % hostname


@task(name='medusa.medusasystem.error_handler')
def error_handler(uuid):
    """
    Function that handle errors
    """

    result = AsyncResult(uuid)
    exc = result.get(propagate=False)
    logging.error(('Task %r raised exception: %r\n' % (
        exc, result.traceback)))


@task(name='medusa.medusasystem.generate_digests')
def generate_digests(pinput, digest_command):
    """
    generate digests from the input
    :param pinput (string) the input path
    :param digest_command (string) tells if should execute the -text or -cat command
    :return a dict with pairs <file, digest>
    """
    digests = {}

    try:
        files = hadoopy.ls(pinput)
    except IOError:
        return digests


    for afile in files:
        if digest_command == "-cat":
            stdout, stderr = subprocess.Popen([get_hadoop_path() + "/bin/hadoop", "fs", "-cat", afile],
                                              stdout=subprocess.PIPE).communicate()
        elif digest_command == "-text":
            stdout, stderr = subprocess.Popen([get_hadoop_path() + "/bin/hadoop", "fs", "-text", afile],
                                              stdout=subprocess.PIPE).communicate()

        m = hashlib.sha256()
        m.update(stdout)

        # for afile in files:
        #     data = hadoopy.readtb(afile)
        #     m = hashlib.sha256()
        #
        #     for key, value in data:
        #         m.update(value)

        digests.update({afile: m.hexdigest()})

    return digests


@task(name='medusa.medusasystem.generate_one_digest')
def generate_one_digest(afile, digest_command):
    """
    generate digests from the input
    :param afile (string) the file name
    :param digest_command (string) tells if should execute the -text or -cat command
    :return a dict with pairs <file, digest>
    """

    if digest_command == "-cat":
        stdout, stderr = subprocess.Popen([get_hadoop_path() + "/bin/hadoop", "fs", "-cat", afile],
                                          stdout=subprocess.PIPE).communicate()
    elif digest_command == "-text":
        stdout, stderr = subprocess.Popen([get_hadoop_path() + "/bin/hadoop", "fs", "-text", afile],
                                          stdout=subprocess.PIPE).communicate()

    m = hashlib.sha256()
    m.update(stdout)

        # for afile in files:
        #     data = hadoopy.readtb(afile)
        #     m = hashlib.sha256()
        #
        #     for key, value in data:
        #         m.update(value)

    return {afile: m.hexdigest()}


def readFileDigests(paths, aggregation):
    """
    get digests from a set of paths
    """
    # digest_values = {} if not aggregation else []
    digest_values = []
    clusters = getRunningClusters()
    func_cache = []
    for q in clusters:
        for path in paths:
            f = my_apply_async(
                generate_digests, queue=q, args=(path, medusa_settings.digest_command,))
            func_cache.append(MemoizeCalls(path, q, f))

    for memcache in func_cache:
        logging.debug("Cache: %s, %s" % (memcache.path, memcache.queue))
        digests = memcache.func.get()
        if len(digests) > 0:
            q = memcache.queue

            if not aggregation:
                digest_values.append(get_reference_digest(q, digests))
            else:
                # if values does not exist in dict
                if any(reference_digests.digests == digests for reference_digests in digest_values):
                    for idx, reference_digest in enumerate(digest_values):
                        if reference_digest.digests == digests:
                            _h = reference_digest.host
                            _h.append(q)
                            digest_values[idx] = get_aggregation_reference_digest(_h, reference_digest.digests)
                            # update dict with new tuple
                            # same code in a single line
                            # map(lambda x: hosts.append(x), filter(lambda x: x.digests == digests, digest_values))
                            # [r.hosts.append(q) for r in digest_values if r.digests == digests]
                            # [r.hosts.append(q) for r in digest_values if r.digests == digests]
                else:
                    digest_values.append(get_aggregation_reference_digest([q], digests))

    return digest_values


@task(name='medusa.medusasystem.read_remote_job_data')
def read_remote_job_data():
    """
    Read job data

    :return: output of the command
    """
    command = lcat(settings.get_temp_dir() + "/" + "job_log.json")
    output = local_execute_command(command)

    return output


@task(name='medusa.medusasystem.get_queue_info')
def get_queue_info():
    """ get info of the queue """
    cmd = "mapred queue -list"
    rcode, stdout, stderr = _checked_hadoop_fs_command(cmd)
    return stdout


# demo functions
def func1():
    for i in range(1000):
        pass


@task(name='medusa.medusasystem.func2')
@make_verbose
def func2():
    for i in range(1000):
        func1()


@task(name='medusa.medusasystem.delay')
def delay():
    logging.info("Going to sleep...")
    time.sleep(10)
    logging.info("Waking up...")
    return "up"


@task(name='medusa.medusasystem.ping')
def ping():
    return "up"


# util functions


@task(name='medusa.medusasystem.getPublicIp')
@make_verbose
def getPublicIp():
    """ run the command and wait that it finish """
    command = "%s/public-ip.sh" % settings.get_medusa_home() + "/scripts",
    output = executeCommand(command)

    return output


@task(name='medusa.medusasystem.getPrivateIp')
@make_verbose
def getPrivateIp():
    """ run the command and wait that it finish """
    command = "%s/private-ip.sh" % settings.get_medusa_home() + "/scripts",
    output = executeCommand(command)

    return output


@task(name='medusa.medusasystem.get_hadoop_path')
def get_hadoop_path():
    return settings.get_hadoop_home()


def update_json_file(path, step):
    """
    update json file data. The key is the json field, and the value
    is the new content.
    """
    content = read_data(path)
    content = json.loads(content[0])
    content["step"] = step

    write_data(path, json.dumps(content))


def write_data(path, line):
    with open(path, 'w') as out_file:
        out_file.write(line)


def append_data(path, line):
    with open(path, 'a') as out_file:
        out_file.write(line)


def read_data(path):
    with open(path, 'r') as in_file:
        content = in_file.readlines()

    return content

def read_data_oneline(path):
    with open(path, 'r') as in_file:
        content = in_file.readlines()

    return "".join(content)


def my_apply_async(f, **kwargs):
    return f.apply_async(**kwargs)


def my_apply_async_with_waiting(f, **kwargs):
    """ wait for a task to finish """
    f = my_apply_async(f, **kwargs)
    waiting(kwargs['queue'], f)
    return f.get()


def my_apply_async_without_waiting(f, **kwargs):
    """ wait for a task to finish """
    f = my_apply_async(f, **kwargs)
    return f


def waiting(cluster, f):
    """ wait for a task to finish """
    error_counter = 0
    while not f.ready():
        try:
            f1 = my_apply_async(ping, queue=cluster)
            f1.wait(timeout=None, interval=5)
        except TimeoutError:
            error_counter += 1
            if error_counter < 3:
                continue
            else:
                logging.warn("WARNING: Cluster %s is down" % cluster)
                setRunningClusters()
                raise Exception("Job execution failed")

    return f


def my_get(f):
    try:
        return f.get()
    except IOError, e:
        raise IOError('Function got an IO error', e)


def setRunningClusters():
    """
    save running clusters in a file
    """
    active = []
    for q in medusa_settings.clusters:
        if _isClusterWorking(q) is not None:
            active.append(q)

    # line = getActiveClustersJSON(active)
    filename = settings.get_medusa_home() + "/temp/clusters.json"
    with open(filename, 'w') as out_file:
        out_file.write(json.dumps(active))


def getRunningClusters():
    """
    get a list of running clusters
    """

    filename = settings.get_medusa_home() + "/temp/clusters.json"
    with open(filename, 'r') as in_file:
        clusters = json.load(in_file)

    return clusters


def _isClusterWorking(q, timeout=5):
    # f = my_apply_async(clusterWorking, queue=q)
    f = clusterWorking.apply_async(queue=q)
    try:
        result = f.get(timeout)
    except TimeoutError:
        logging.error("%s is down" % q)
        return None

    return result
