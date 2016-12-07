import time
import xml.etree.ElementTree as ET
from decimal import Decimal

from medusa.jobsmanager import Job


def parser(xmlfile, faults_tolerate, keyword):
    """
    Parses xml file and returns Job structure

    :param xmlfile (string) path of the xml file
    :param faults_tolerate (int) number of faults to tolerate
    :param keyword (string) job|aggregator

    :return Job job structure that is used to execute
    """

    result = []
    xmlfile = xmlfile.strip()

    tree = ET.parse(xmlfile)
    root = tree.getroot()

    for child in root:
        if child.tag == keyword:
            _id = child[0].text
            command = child[1].text
            input_path = child[2].text
            output_path = child[3].text
            name = child[4].text

            result.append(Job(_id, name, faults_tolerate, command, input_path.split(), output_path))

    return result


def parseText(xmltext, keyword):
    result = []
    xmltext = xmltext.strip()

    tree = ET.fromstring(xmltext)
    elem = tree.findall(keyword)

    for child in elem:
        l = []
        for c in child:
            l.append(c.text)
        result.append(l)

    return result


def parseTextByField(xmltext, keyword, field):
    tree = ET.fromstring(xmltext)
    elem = tree.findall(keyword)

    result = []
    for child in elem:
        for c in child:
            if c.tag == field:
                result.append(c.text)

    return result

"""
parse xml functions
"""


def parseOutputMetrics(xmlfile):
    total_jobs = []
    if len(xmlfile) == 0:
        return total_jobs

    xmlfile = xmlfile.strip()
    queue_data = parseText(xmlfile, "job")
    # print queue_data

    # 0 to 100
    for elem in queue_data:
        if elem[1] is not None and elem[1] == "SUCCEEDED":
            in_bytes = 0
            readbytes = elem[7]
            if readbytes is not None:
                in_bytes = readbytes.split("=")[1]

            out_bytes = 0
            written = elem[8]
            if written is not None:
                out_bytes = written.split("=")[1]

            total_jobs.append((int(in_bytes), int(out_bytes)))

    return total_jobs


def parseInputMetrics(xmlfile):
    total_jobs = []
    if len(xmlfile) == 0:
        return total_jobs

    xmlfile = xmlfile.strip()
    queue_data = parseText(xmlfile, "job")
    # print queue_data

    # 0 to 100
    for elem in queue_data:
        if elem[1] is not None and elem[1] == "SUCCEEDED":
            span = 0
            if elem[2] is not None:
                span = int(elem[2])  # time

            in_bytes = 0
            readbytes = elem[7]
            if readbytes is not None:
                in_bytes = readbytes.split("=")[1]

            total_jobs.append((int(in_bytes), int(span)))

    return total_jobs


def parseInputOutputMetrics(xmlfile):
    total_jobs = []
    if len(xmlfile) == 0:
        return total_jobs

    xmlfile = xmlfile.strip()
    queue_data = parseText(xmlfile, "job")
    # print queue_data

    # 0 to 100
    for elem in queue_data:
        if elem[1] is not None and elem[1] == "SUCCEEDED":
            in_bytes = 0
            readbytes = elem[7]
            if readbytes is not None:
                in_bytes = readbytes.split("=")[1]

            out_bytes = 0
            written = elem[8]
            if written is not None:
                out_bytes = written.split("=")[1]

            total_jobs.append((int(in_bytes), int(out_bytes)))

    return total_jobs


def parseQueueMetrics(xmlfile):
    if len(xmlfile) == 0:
        return 0

    xmlfile = xmlfile.strip()
    queue_data = parseText(xmlfile, "global")
    # print queue_data

    # 0 to 100
    capacity = 0.0
    curcapacity = 0.0
    for elem in queue_data:
        capacity = float(elem[0])  # capacity
        curcapacity = float(elem[2])  # current capacity

    return capacity - curcapacity


def parseNaiveMetrics(xmltext):
    xmltext = xmltext.strip()
    a = parseText(xmltext, "global")
    jobs = int(a[0][0])

    return jobs


def parseCompletionMetrics(xmlfile):
    jobs = parseText(xmlfile, "jobs/job")

    now = int(time.time()) * 1000  # for padding purpose
    sum_eat = 0
    for elem in jobs:
        if elem[1] == "RUNNING":
            xtime = int(elem[2])  # start time

            mapcomp = Decimal(float(elem[5].split(":")[1]))  # map completion
            # reduce completion
            redcomp = Decimal(float(elem[6].split(":")[1]))

            execution_progress = (mapcomp + redcomp) / 2
            execution_time = now - xtime

            unit_time = execution_progress / execution_time
            # estimated time to finish
            eat = (1 - execution_progress) * unit_time

            sum_eat += eat

    if len(jobs) > 0:
        return sum_eat / len(jobs)

    return 0


def parseJobOutputMetrics(xmlfile):
    jobvalues = {}
    if len(xmlfile) == 0:
        return jobvalues

    xmlfile = xmlfile.strip()
    # print xmlfile

    value = parseTextByField(xmlfile, "job", "hdfsbytesread")
    jobvalues.update({"hdfsbytesread": value[0]})

    value = parseTextByField(xmlfile, "job", "hdfsbyteswritten")
    jobvalues.update({"hdfsbyteswritten": value[0]})

    value = parseTextByField(xmlfile, "job", "maps")
    jobvalues.update({"maps": value[0]})

    value = parseTextByField(xmlfile, "job", "reduces")
    jobvalues.update({"reduces": value[0]})

    jobtime = parseTextByField(xmlfile, "job", "totaltime")
    jobvalues.update({"time": jobtime[0]})

    value = parseTextByField(xmlfile, "job", "currentqueuecapacity")
    jobvalues.update({"currentqueuecapacity": value[0]})

    value = parseTextByField(xmlfile, "job", "jobsrunning")
    jobvalues.update({"jobsrunning": value[0]})

    value = parseTextByField(xmlfile, "job", "digests")
    jobvalues.update({"digests": value[0]})

    value = parseTextByField(xmlfile, "job", "cluster")
    jobvalues.update({"cluster": value[0]})

    return jobvalues
