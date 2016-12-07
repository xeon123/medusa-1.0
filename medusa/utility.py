import logging

import psutil
from celery import task
from medusa.settings import medusa_settings

"""
It takes information about the CPU
"""
def cpuCycles():
    logging.debug("CPU cycles")

    logging.debug(psutil.cpu_times())

    for x in range(3):
        logging.debug(psutil.cpu_percent(interval=1))

    for x in range(3):
        logging.debug(psutil.cpu_percent(interval=1, percpu=True))

    for x in range(3):
        psutil.cpu_times_percent(interval=1)

    logging.debug(psutil.NUM_CPUS)


def memInfo():
    logging.debug("Mem info")

    logging.debug(psutil.virtual_memory())
    logging.debug(psutil.swap_memory())
    logging.debug(psutil.phymem_usage())
    logging.debug(psutil.avail_virtmem())
    logging.debug(psutil.avail_phymem())


def diskInfo():
    logging.debug("Disk info")

    logging.debug(psutil.disk_partitions())
    logging.debug(psutil.disk_usage('/'))
    logging.debug(psutil.disk_io_counters())


def networkInfo():
    logging.debug("Network info")

    logging.debug(psutil.network_io_counters())
    logging.debug(psutil.network_io_counters(pernic=True))


@task(name='medusa.system.get_cpu_load')
def get_cpu_load():
    """
    Get CPU load

    :return: (float) percentage of cpu
    """
    return psutil.cpu_percent()


@task(name='medusa.system.get_mem_load')
def get_mem_load():
    """
    Get Memory usage load

    :return: (float) percentage of cpu
    """
    return psutil.virtual_memory().percent

@task(name='medusa.utility.getNamenodeAddress')
def getNamenodeAddress():
    logging.debug(
        "%s - %s - %s" % (medusa_settings.httpfs_used, medusa_settings.namenode_address, medusa_settings.httpfs_port))

    port = medusa_settings.httpfs_port if medusa_settings.httpfs_used else medusa_settings.hdfs_port
    address = "%s:%s" % (medusa_settings.namenode_address, port)

    return address


if __name__ == "__main__":
    cpuCycles()
    memInfo()
    diskInfo()
    networkInfo()


def majority(faults):
    """
    get majority value
    n = 2f + 1
    f = (n-1)/2
    n-f=majority
    """
    n = 2 * faults + 1

    return n - faults
