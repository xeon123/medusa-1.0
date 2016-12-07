import json
import logging
import math
import random

import statsmodels.api as sm

import numpy as np
from celery import task
from medusa import settings
from medusa.hdfs import get_total_size
from medusa.medusasystem import my_apply_async, get_queue_info, read_remote_job_data, read_data, write_data
from medusa.namedtuples import set_job_params, set_penalization_params
from medusa.networkdaemon import read_network_data
from medusa.numpylinearregression import estimate_job_execution
from medusa.sets import PathStatistics
from medusa.settings import getQueueJSON, medusa_settings
from medusa.simplecache import get
from medusa.utility import get_cpu_load, get_mem_load

"""
 Job Queue metrics
"""

def get_prediction_metrics(host_spec, pinput):
    """
    rank jobs using linear regression

    :param host_spec tuple with included_cluster and excluded_clusters
    :param pinput (string) input path

    """
    included_clusters, excluded_clusters = host_spec

    included_lat_list = []
    included_jobs_list = []
    """ get metrics related to job """
    for cluster in included_clusters:
        f_list = [my_apply_async(get_total_size, queue=cluster.cluster, args=(path,)) for path in pinput]
        input_size = sum(f.get() for f in f_list)

        # included_lat_list += get_prediction_on_network(cluster.cluster, excluded_clusters)
        predicted_time = get_prediction_metrics_on_job(cluster.cluster, input_size)
        penalization = json.loads(my_apply_async(load_penalization, queue=cluster.cluster).get())
        path_statistics = PathStatistics(cluster.cluster, predicted_time+penalization["error"], path)
        logging.info("Prediction: %s", path_statistics.__str__())
        included_jobs_list.append(path_statistics)

    included_jobs_list.sort(key=lambda x: x.rank, reverse=False)
    excluded_jobs_list = []
    for cluster in excluded_clusters:
        predicted_time = get_prediction_metrics_on_job(cluster.cluster, input_size)
        excluded_jobs_list.append(PathStatistics(cluster.cluster, predicted_time, path))

    logging.info("Prediction on network: %s" % included_lat_list)

    logging.info("Prediction on job (included): %s" % "; ".join(str(job) for job in included_jobs_list))
    logging.info("Prediction on job (excluded): %s" % "; ".join(str(job) for job in excluded_jobs_list))

    # rank = _get_prediction_metrics_on_all_clusters(included_clusters, excluded_clusters,
    #                                                included_lat_list, included_jobs_list, excluded_jobs_list)
    rank_list = (included_jobs_list, excluded_jobs_list)

    return rank_list


def _get_prediction_metrics_on_all_clusters(included_clusters, excluded_clusters,
                                            included_lat_list, included_jobs_list, excluded_jobs_list):
    """

    :param included_clusters: (list) cluster that contains the data
    :param excluded_clusters: (list) list of clusters that does not contain data
    :param included_lat_list:
    :param included_jobs_list:
    :param excluded_jobs_list:
    :return: list with the ranking for each host
    """

    included_list = []
    included_jobs = 0

    if len(included_jobs_list) > 0:
        included_jobs = [aux_temp(included_clusters, included_jobs_list)]

    error = 0
    points = int(included_jobs[0] + error)
    # if points <= 0:
    #     points = sys.maxsize

    included_list.append((included_clusters, points))

    included_list.sort(key=lambda tup: tup[1])

    excluded_list = []
    random.shuffle(excluded_clusters)
    for idx, _cluster in enumerate(excluded_clusters):
        included_latency = excluded_jobs = 0

        if len(included_lat_list) > 0:
            included_latency = aux_temp(_cluster.cluster, included_lat_list)

        if len(excluded_jobs_list) > 0:
            excluded_jobs = aux_temp(_cluster.cluster, excluded_jobs_list)

        # error = 0
        # points = int(included_latency + (excluded_jobs * penalization) + error)
        # if points <= 0:
        #     points = sys.maxsize
        excluded_list.append((_cluster, points))

    excluded_list.sort(key=lambda tup: tup[1], reverse=True)

    """
    [('host1', 12.0),
     ('host2', 15.0),
     ('host3', 42.0)]
    """
    return included_list, excluded_list


def get_prediction_on_network(included_cluster, excluded_clusters):
    """
    Read network logs made by iperf3 tool.

    :param included_cluster (string) cluster that contains the data
    :param excluded_clusters (list) list of clusters that does not contain the data
    """

    network_prediction = []
    for excluded_cluster in excluded_clusters:
        if included_cluster != excluded_cluster.cluster:
            network_prediction.append(read_network_data(included_cluster, excluded_cluster.cluster))

    return network_prediction


def get_prediction_metrics_on_job(cluster, input_size):
    """

    :param cluster: (string) cluster of the job
    :param input_size:
    :return: (float) time to execute
    """
    task1 = read_remote_job_data.apply_async(queue=cluster)
    task2 = get_queue_info.apply_async(queue=cluster)

    queue_data = task2.get()
    queue_json = json.loads(getQueueJSON(queue_data))

    data_file = task1.get()
    current_capacity = float(queue_json["currentqueuecapacity"])
    data = json.loads(data_file)

    """
      logline = "%s:%s:%s:%s:%s:%s:%s:%s" %(data['cluster'],
                                   data['currentqueuecapacity'],
                                   data['hdfsbytesread'],
                                   data['hdfsbyteswritten'],
                                   data['maps'],
                                   data['reduces'],
                                   data['cpu'],
                                   data['mem']
                                   data['totaltime'])
    """
    # put data in a matrix
    params_matrix = []
    params_matrix2 = []
    for _line in data["data"]:
        params_matrix.append([
            float(_line["currentqueuecapacity"]),
            float(_line["hdfsbytesread"]),
            float(_line["hdfsbyteswritten"]),
            float(_line["maps"]),
            float(_line["reduces"]),
            float(_line["cpu"]),
            float(_line["mem"])])

        params_matrix2.append([
            float(_line["hdfsbytesread"]),
            float(_line["hdfsbyteswritten"]),
            _line["job_name"]])

    time_matrix = [float(line["time"]) for line in data["data"]]

    xx = np.array(params_matrix)
    yy = np.array(time_matrix)

    # coeffs for the params:
    # currentqueuecapacity:hdfsbytesread:maps:CPU:MEM
    model = calculate_linear_regression(yy, xx)
    coeffs = model.params
    cpu_load = get_cpu_load.apply_async(queue=cluster).get()
    mem_load = get_mem_load.apply_async(queue=cluster).get()

    step = get("step")

    if step == 0:
        job_list = get("job")[0]
        job_name = job_list.name
    else:
        aggregator_list = get("aggregator")[0]
        job_name = aggregator_list.name

    output_size = _filter_output_data(input_size, job_name, params_matrix2)

    maps = int(math.ceil((input_size * 1.0) / medusa_settings.blocksize))

    job_params = set_job_params(job_name, current_capacity, input_size, output_size, maps, cpu_load, mem_load)
    time = estimate_job_execution(coeffs, job_params)

    job_params = set_job_params(job_name, current_capacity, input_size, output_size, maps, cpu_load, mem_load, time)
    save_prediction.apply_async(queue=cluster, args=(json.dumps(job_params._asdict()),)).get()

    return time


def _filter_output_data(input_size, job_name, params_matrix):
    """
    Predict the output data of the job by doing an arithmetic mean the the previous results that got simila input data
    :param input_size: size of the input data
    :param job_name: name of the job
    :param params_matrix: predicted output data
    :return:
    """
    limit = input_size * 0.1
    min_input_size = input_size - limit
    max_input_size = input_size + limit

    sum_value = 0
    iter = 0
    predicted_output_data = 0
    for params in params_matrix:
        if params[0] > min_input_size and params[0] < max_input_size\
                and job_name == params[-1]:
            sum_value += params[1]
            iter+=1

    if iter > 0:
        predicted_output_data = sum_value/iter

    return predicted_output_data


def calculate_linear_regression(y, x):
    # adds a constant of ones for y intercept
    X = np.insert(x, 0, np.ones((1,)), axis=1)

    return sm.OLS(y, X).fit()


def aux_temp(needle, haystack):
    for hay in haystack:
        if len(hay) > 1 and hay[0] == needle:
            return hay[1]

    # return sys.maxsize
    return 0


@task(name='medusa.predictionranking.save_prediction')
def save_prediction(job_params):
    """
    Save prediction values of the job into a file
    :param job_params (string) data to be saved
    """

    prediction_file = "%s/prediction.json" % settings.get_temp_dir()
    write_data(prediction_file, job_params)


@task(name='medusa.predictionranking.load_prediction')
def load_prediction():
    """
    Load prediction values of the job into a file
    :return:
    """

    prediction_file = "%s/prediction.json" % settings.get_temp_dir()
    data = read_data(prediction_file)[0]
    print "----"
    print data
    print "----"

    return data

@task(name='medusa.predictionranking.reset_prediction')
def reset_prediction():
    """
    Reset prediction values of the job into a file
    """
    save_prediction(json.dumps(set_job_params(0,0,0,0,0)._asdict()))

@task(name='medusa.predictionranking.save_penalization')
def save_penalization(penalization_values):
    """ Save penalization values """
    prediction_file = "%s/penalization.json" % settings.get_temp_dir()

    with open(prediction_file, 'w') as the_file:
        the_file.write(penalization_values)


@task(name='medusa.predictionranking.load_penalization')
def load_penalization():
    """ Load penalization values """
    prediction_file = "%s/penalization.json" % settings.get_temp_dir()
    data = read_data(prediction_file)[0]

    print "----"
    print data
    print "----"

    return data

@task(name='medusa.predictionranking.reset_penalization')
def reset_penalization():
    """
    Reset the prediction file
    """
    save_penalization(json.dumps(set_penalization_params(0,0,0)._asdict()))
