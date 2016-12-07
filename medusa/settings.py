import json
import logging

import os

logging.basicConfig(filename='myapp.log', level=logging.INFO)


class MedusaSettings:
    def __init__(self):
        # cluster configurations
        self.clusters = []

        # namenode address
        self.namenode_address = ""
        self.httpfs_port = 0
        self.hdfs_port = 0

        # Faults that tolerates
        self.faults = 1
        self.blocksize = 0

        self.httpfs_used = False
        self.ranking_scheduler = "random"  # naive|random|prediction

        # display verbose all the calls
        self.verbose = False
        self.local_copy = False
        self.execution_mode = "serial"  # thread|process

        # Failure models
        self.digests_fault = False
        self.faults_left = 0
        self.packet_size = ["4000M", "8000M"]

        self.relaunch_job_same_cluster = False
        self.digest_command = "-cat"


    def dump(self, out_file):
        json.dump(self.__dict__, out_file, sort_keys=True, ensure_ascii=False, indent=4)


    def dumps(self):
        return json.dumps(self.__dict__)


    def load(self, in_file):
        self.__dict__ = json.load(in_file)


    def loads(self, data_str):
        self.__dict__ = json.loads(data_str)


def get_medusa_home():
    return os.environ["MEDUSA_HOME"]


def get_hadoop_home():
    return os.environ["HADOOP_HOME"]


def get_hadoop_bin():
    return os.environ["HADOOP_HOME"] + "/bin"


def get_temp_dir():
    return os.environ["MEDUSA_HOME"] + "/temp"


def save_settings(settings):
    """
    Saves the settings params into a json file.
    :param MedusaSettings
    """
    with open(get_medusa_home() + settings_filename, 'w') as out_file:
        settings.dump(out_file)


def load_settings(filename):
    """
    Read configuration file from a json file and put it in MedusaSettings object
    :param (string) filename of the json file

    :return: MedusaSettings with all configuration
    """

    settings = MedusaSettings()
    with open(get_medusa_home() + filename, 'r') as in_file:
        settings.load(in_file)

    return settings


settings_filename = "/settings.json"
medusa_settings = load_settings(settings_filename)


# medusa_settings = MedusaSettings()
# save_settings(medusa_settings)

def decrease_faults():
    if medusa_settings.faults_left > 0:
        medusa_settings.faults_left -= 1
        logging.debug("decreasefaults: %s" % medusa_settings.faults_left)


def getJobStepJSON(gid, command, step):
    data = """
     {
     "jobs": {
     "job": { "gid": %s, "command": %s, "step": %s
        }
     } }

    """ % (gid, command, step)

    return data


def getJobJSON(job_output, cluster, queue_info, totaltime, digests):
    part1 = _get_job_output_json(job_output, cluster, totaltime, digests)
    part2 = _get_queue_json(queue_info)

    data = """
     {
     "jobs": {
        "job": { %s, %s
        }
     } }
    """ % (part1, part2)

    return data


def getQueueJSON(queue_info):
    output = _get_queue_json(queue_info)
    line = """ {
        %s
    }
    """ % output

    return line


def _get_job_output_json(output, cluster, totaltime, digests):
    """
    get info about job execution
    """
    output = output.splitlines()
    aux = [token for token in output if "Running job" in token]
    jobid = aux[0].split(":")[-1]

    aux = [token for token in output if "Number of bytes read" in token]
    filebytesread = aux[0].split("=")[1]
    hdfsbytesread = aux[1].split("=")[1]

    aux = [token for token in output if "Number of bytes written" in token]
    filebyteswritten = aux[0].split("=")[1]
    hdfsbyteswritten = aux[1].split("=")[1]

    aux = [token for token in output if "Launched map tasks" in token]
    maps = aux[0].split("=")[1]

    aux = [token for token in output if "Launched reduce tasks" in token]
    reduces = aux[0].split("=")[1]

    aux = [token for token in output if "Data-local map tasks" in token]
    localmaps = 0 if len(aux) == 0 else aux[0].split("=")[1]

    aux = [token for token in output if "Rack-local map tasks" in token]
    rackmaps = 0 if len(aux) == 0 else aux[0].split("=")[1]

    aux = [
        token for token in output if "Total time spent by all map tasks" in token]
    timespentmaps = 0 if len(aux) == 0 else aux[0].split("=")[1]

    aux = [
        token for token in output if "Total time spent by all reduce tasks" in token]
    timespentreduces = 0 if len(aux) == 0 else aux[0].split("=")[1]

    line = """
       "id": "%s",
       "cluster": "%s",
       "filebytesread": "%s",
       "filebyteswritten": "%s",
       "hdfsbytesread": "%s",
       "hdfsbyteswritten": "%s",
       "maps": "%s",
       "reduces": "%s",
       "localmaps": "%s",
       "rackmaps": "%s",
       "totaltime": "%s",
       "timespentmaps": "%s",
       "timespentreduces": "%s",
       "digests": %s
    """ % (jobid, cluster, filebytesread, filebyteswritten, hdfsbytesread, hdfsbyteswritten, maps, reduces, localmaps,
           rackmaps, totaltime, timespentmaps, timespentreduces, digests)

    return line


def _get_queue_json(output):
    """
    get info about queue
    """
    output = output.splitlines()

    aux = [token for token in output if "Scheduling Info" in token]
    values = aux[0].split(",")
    capacity = values[0].split(":")[-1]
    maxcapacity = values[1].split(":")[-1]
    currentcapacity = values[2].split(":")[-1]

    line = """
        "queuecapacity": "%s",
        "maximumqueuecapacity": "%s",
        "currentqueuecapacity": "%s"
    """ % (capacity, maxcapacity, currentcapacity)

    return line
