from celery import task
from medusa import settings


def ping_file():
    return "%s/ping_data" % settings.get_medusa_home() + "/scripts"


def jobs_historyfile():
    return "%s/jobs_history_data" % settings.get_medusa_home() + "/temp"


@task(name='medusa.pingdaemon.clusterWorking')
def clusterWorking():
    return "up"
