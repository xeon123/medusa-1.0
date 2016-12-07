import json
import time

from medusa import settings
import hadoopy
from hadoopy._hdfs import _checked_hadoop_fs_command
from celery import task
from medusa.medusasystem import executeCommand, write_data, read_data_oneline


def copyToLocal(src, dest):

    return "%s dfs -copyToLocal %s %s" % (settings.get_hadoop_home() + "/bin/hdfs", src, dest)


def copyFromLocal(src, dest):
    return "%s dfs -copyFromLocal %s %s" % (settings.get_hadoop_home() + "/bin/hdfs", src, dest)


def cat(path):
    return "%s dfs -cat %s" % (settings.get_hadoop_home() + "/bin/hdfs", path)


@task(name='medusa.hdfs.rmr')
def rmr(path):
    """ remove path from HDFS """
    try:
        hadoopy.rmr(path)
    except IOError:
        return False
    return True


@task(name='medusa.hdfs.get_total_size')
def get_total_size(path, _format="%b"):
    """ get the total size of the path """

    size = 0
    if hadoopy.isdir(path):
        files = hadoopy.ls(path)
        for file in files:
            size += int(hadoopy.stat(file, _format))
    else:
        size = hadoopy.stat(path, _format)
    return size


def generateDigests(path):
    """ generate digests from the files of the path """
    return "%s/generatedigests.sh %s" % (settings.get_medusa_home() + "/scripts", path)


@task(name='medusa.hdfs.writeJobRunning')
def writeJobRunning(job_output):
    """
    append job execution results to a file
    :param job_output (string) job output data
    """
    path = settings.get_temp_dir() + "/job_log.json"
    job_remote_dataset = json.loads(read_data_oneline(path))
    job_remote_dataset["data"].append(json.loads(job_output))
    write_data(path, json.dumps(job_remote_dataset, indent=2))


@task(name='medusa.hdfs.network_filewriteDataTime')
def network_filewriteDataTime(logline):
    """ write info about the data transfer
        logline=from_host:to_host:size:timetosent
    """
    cmd = "%s/writedatatime.sh %s" % (settings.get_medusa_home() + "/scripts", logline)
    executeCommand(cmd)


@task(name='medusa.hdfs.distcp')
def distCp(clusters, src, dest):
    """
    create the dist command

    bash$ hadoop distcp hdfs://nn1:8020/foo/a \
    hdfs://nn1:8020/foo/b \
    hdfs://nn2:8020/bar/foo

    Through HDFS proxy (httpfs)
    e.g. hadoop distcp webhdfs://nn1:3888/gutenberg/  webhdfs://nn2:3888/
    """

    start = time.time()
    cmd = "hadoop distcp %s %s" % (src, dest)
    rcode, stdout, stderr = _checked_hadoop_fs_command(cmd)
    end = time.time()
    span = end - start
    from_cluster, to_cluster = clusters

    # update network data
    totalsize = get_total_size(src)
    print "(Total size of data transfered: %s)" % totalsize

    if totalsize > 0 and span > 0:
        logline = "%s:%s:%s:%s" % (from_cluster, to_cluster, totalsize, span)
        network_filewriteDataTime(logline)

    return stdout.rstrip()


@task(name='medusa.hdfs.ls')
def ls(pinput):
    """ list hdfs files """

    try:
        files = hadoopy.ls(pinput)
    except IOError:
        files = []

    return files


@task(name='medusa.hdfs.exists_all')
def exists_all(inputs):
    for input in inputs:
        if not exists(input):
            return False

    return True


@task(name='medusa.hdfs.exists')
def exists(input):
    return len(ls(input)) > 0


def main():
    print("Hello")

if __name__ == "__main__":
    # print copyToLocal("a", "b")
    print distCp("a", "b")
