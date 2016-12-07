import logging


def calculate_linear_regression_numpy(xx, yy):
    """ calculate multiple linear regression """
    import numpy as np
    from numpy import linalg

    A = np.column_stack((xx, np.ones(len(xx))))

    # linearly generated sequence
    coeffs = linalg.lstsq(A, yy)[0]  # obtaining the parameters


    # plotting the line
    # line = w[0]*xi+w[1] # regression line
    # m, c => y = mx + c
    return coeffs


def estimate_job_execution(coeffs, params):
    """ estimate the time to execute the job

    :param coeffs list of coefficients
    :param params (namedtuple) with the job measures

    :return time predicted execution time of the job
    """

    """
    float(_line["currentqueuecapacity"]),
    float(_line["hdfsbytesread"]),
    float(_line["hdfsbyteswritten"]),
    float(_line["maps"]),
    float(_line["reduces"]),
    float(_line["cpu"]),
    float(_line["mem"])])
    """

    # currentqueuecapacity:hdfsbytesread:maps:CPU:MEM
    cst = coeffs[0]
    factor_current_queue_capacity = coeffs[1]
    factor_bytes_read = coeffs[2]
    factor_bytes_written = coeffs[3]
    factor_maps = coeffs[4]
    factor_cpu_load = coeffs[6]
    factor_mem_load = coeffs[7]

    logging.info("Factors: Bytes read %s, maps: %s, cpu load: %s, mem load: %s" % (factor_bytes_read, factor_maps, factor_cpu_load, factor_mem_load))

    time = (factor_current_queue_capacity * params.current_queue_capacity) + \
           (factor_bytes_read * params.input_size) + \
           (factor_bytes_written * params.output_size) + \
           (factor_maps * params.maps) + (factor_cpu_load * params.cpu_load) + \
           (factor_mem_load * params.mem_load) + cst

    return time
