from medusa.mapreduce import getJobsHistory
from medusa.settings import medusa_settings
from medusa.medusasystem import executeCommand
from medusa.xmlparser import parseJobOutputMetrics


# This class runs an example that it is defined in the wordcount
# read wordcount xml
# cluster1: job1 --> aggregation: job3
# cluster2: job2 -----^
def get_jobhistory():
    command = getJobsHistory()
    s2 = executeCommand(command)

    print s2

    for q in medusa_settings.clusters:
        s1 = executeCommand.apply_async(queue=q, args=(command, ))
        xmlfile = s1.get()

        points2 = parseJobOutputMetrics(xmlfile)

        print points2


if __name__ == "__main__":
    get_jobhistory()
