from collections import defaultdict
import json
import sys
import time

from medusa.mapreduce import getJobsHistory
from medusa.pingdaemon import jobs_historyfile
from medusa.medusasystem import executeCommand, write_data

"""
 write the job history
"""


class JobOutput:
    """
    Class that contains the output of a job execution
    """

    def __init__(self):
        self.cluster = ""
        self.currentqueuecapacity = 0
        self.digests = []
        self.filebytesread = 0
        self.filebyteswritten = 0
        self.hdfsbytesread = 0
        self.hdfsbyteswritten = 0
        self.id = 0
        self.localmaps = 0
        self.maps = 0
        self.maximumqueuecapacity = 0
        self.rackmaps = 0
        self.reduces = 0
        self.timespentmaps = 0
        self.timespentreduces = 0
        self.totaltime = 0

    def dump(self, out_file):
        json.dump(self.__dict__, out_file, sort_keys=True, ensure_ascii=False, indent=4)

    def dumps(self):
        return json.dumps(self.__dict__)

    def load(self, in_file):
        self.__dict__ = json.load(in_file)

    def loads(self, data_str):
        self.__dict__ = json.loads(data_str)["jobs"]["job"]


def updateJobHistoryInfo():
    while True:
        command = getJobsHistory()
        content = executeCommand(command)

        write_data(jobs_historyfile(), content)
        time.sleep(60)


if __name__ == "__main__":
    if len(sys.argv) != 1:
        print "python jobsmanager.py"

    updateJobHistoryInfo()


class Job:
    def __init__(self, id, name, faults_tolerate, command, input_path, output_path):
        """

        :param id: (int) id of the job
        :param name: (string) name of the job
        :param faults_tolerate: (int) number of faults to tolerate
        :param command: (string) command to execute
        :param input_path: (list) list of input paths
        :param output_path: (string) output path
        :return:
        """
        self.id = id
        self.name = name
        self.faults_tolerate = faults_tolerate
        self.command = command
        self.input_path = input_path
        self.output_path = output_path
        self.history_rank = defaultdict(lambda: 1)


class ExecutionJob:
    """
    Auxiliary class to launch a job.

    """

    def __init__(self, id, clusters, command, output_path, how_many_runs):
        """

        :param id: id of the job
        :param clusters: (list) clusters where the job is going to be launched
        :param command: (string) command to execute
        :param output_path: (string) output path
        :param how_many_runs (int) in tell how many executions will be launched.
        """
        self.id = id
        self.clusters = clusters
        self.command = command
        self.output_path = output_path
        self.how_many_runs = how_many_runs


class JobExecution:
    """
    Auxiliary class used to execute the job. Different class from ExecutionJob.
    This class is used during the job execution. The other one is to be used to execute the job
    """

    def __init__(self, execution, cluster, stime):
        """

        :param execution: function that is being used to run the job
        :param cluster: (string) in which cluster the job is running
        :param stime:  (int) starting time of execution
        """
        self.execution = execution
        self.cluster = cluster
        self.stime = stime

    def get_execution(self):
        return self.execution

    def get_start_time(self):
        return self.stime

    def get_cluster(self):
        return self.cluster
