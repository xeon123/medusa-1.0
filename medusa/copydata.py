import time
import logging

from medusa.hdfs import distCp, exists
from medusa.settings import medusa_settings
from medusa.medusasystem import generate_digests, my_apply_async, my_get
from medusa.utility import getNamenodeAddress

"""
this file copy data between namenodes
"""


def copydata_prepareEnvironment(clusters, src, localcopy=False):
    """
    This function checks if the dest cluster contains the data from the src cluster.
    If so, no data is copied. Otherwise, the data is copied and verified by digetst

    clusters is a tuple with (cluster_from_data_is_copied, cluster_to_data_is_copied)
    namenodes is a tuple with (namenode_from_data_is_copied, namenode_to_data_is_copied)
    src is from directory
    dest is to directory
    """

    if localcopy:
        result1 = exists(src)
    else:
        from_cluster = clusters[0]
        f = my_apply_async(exists, queue=from_cluster, args=(src,))
        result1 = my_get(f)

    if not result1:
        raise Exception("Source directory doesn't exist")

    # new version
    to_cluster = clusters[1]
    logging.info("Preparing environment in cluster %s: %s" % (to_cluster, src))
    f = my_apply_async(exists, queue=to_cluster, args=(src,))
    result2 = my_get(f)

    if result2:
        raise Exception("Target directory exists")

    return result1, result2


def copydata_runEnvironment(clusters, namenodes, src, dest, reference_digests):
    """
    Copy data between HDFS and check if the copy was done correctly by comparing the digests

    :param clusters (tuple) with (source, dst) hosts
    :param namenodes (tuple) with (source, dst) namenodes
    :param src (string) source path
    :param dest (string) destination path
    :param reference_digests reference digests

    :return True if the copy was made successfully. False, otherwise
    """

    from_cluster, to_cluster = clusters

    logging.info("Running copy in cluster %s: %s" % (from_cluster, src))
    try:
        digest1 = copydata_getDigests(from_cluster, src)  # generate digests

        concat_digest = {}
        for ref_digest in reference_digests:
            concat_digest.update(ref_digest.digests)

        # digest1 is not contained in concat_digests
        # all(item in concat_digest.items() for item in digest1.items()):
        value = all(
            k in concat_digest and concat_digest[k] == digest1[k] for k in digest1)
        if not value:
            msg = "Digests from %s are different from the reference: %s != %s" % (
                from_cluster, digest1, reference_digests)
            raise Exception(msg)

        path_source = "hdfs://%s%s" % (namenodes[0], src)
        path_dest = "hdfs://%s%s" % (namenodes[1], dest)
        span = _copydata_distcp(clusters, path_source, path_dest)
        logging.info("Copy %s -> %s: %s secs." % (path_source, path_dest, span))

        digest2 = copydata_getDigests(to_cluster, src)  # generate digests
    except TypeError:  # catch when for loop fails
        # not a sequence so just return repr
        return repr(clusters) + " " + repr(namenodes)

    # Compare digests with reference table
    if not digest1 == digest2:
        diff = [k for k in digest1 if k not in digest2]  # lc
        msg = "%s == %s (diff %s)" % (str(digest1), str(digest2), diff)
        logging.warn("WARNING: Couldn't copy correctly the data")
        logging.warn(msg)

        return False

    return True


def copy_data(order, reference_digests):
    """
    Prepares the environment for the copy and actually copies the data between HDFS instances.

    :param order (Order) object with info about the copy. The order object contains:
            from_cluster cluster that contains the data
            to_cluster is the cluster that the data will be copied
            pinput is the source path
            poutput is the dest path where data will be copied
    :param reference_digests (RefDigetsts) object with the reference digests

    :return (string) the cluster that was copied the data
    """

    logging.info("Starting to copy data (%s): %s" % (str(time.time()), str(order)))

    from_cluster = order.from_cluster
    to_cluster = order.to_cluster
    pinput = order.src_path
    poutput = order.dst_path

    if isinstance(pinput, list):
        pinput = ' '.join(pinput)

    if isinstance(poutput, list):
        poutput = ' '.join(poutput)

    f = my_apply_async(getNamenodeAddress, queue=to_cluster)
    toNamenodeAddress = my_get(f)

    if medusa_settings.local_copy:
        port = medusa_settings.httpfs_port if medusa_settings.httpfs_used else medusa_settings.hdfs_port
        fromNamenodeAddress = "%s:%s" % (medusa_settings.namenode_address, port)
    else:
        f = my_apply_async(getNamenodeAddress, queue=from_cluster)
        fromNamenodeAddress = my_get(f)

    # tuple with queues from the cluster of origin and destination
    t = (from_cluster, to_cluster)

    # tuple with namenode address from the cluster of origin and destination
    tn = (fromNamenodeAddress, toNamenodeAddress)

    pzipped = zip(
        pinput,
        poutput) if isinstance(pinput,
                               list) else zip([pinput],
                                              [poutput])

    copy_done = False
    while not copy_done:
        retrylist = []
        for _input, _output in pzipped:
            copydata_prepareEnvironment(t, _input, medusa_settings.local_copy)

            if not copydata_runEnvironment(t, tn, _input, _output, reference_digests):
                # copy failed
                logging.error("Copy failed: %s -> %s" % (from_cluster, to_cluster))
                copydata_rmr(to_cluster, _output)
                retrylist.append((_input, _output))

        if len(retrylist) > 0:
            pzipped = retrylist
        else:
            copy_done = True

    return to_cluster


def _copydata_distcp(clusters, src, dest):
    """
    Copy the data between HDFS instances.

    :param clusters: (tuple) with source and dest hosts
    :param src: (string) source path
    :param dest: (string) dest path
    :return: (int) the time that it took to copy data.
    """
    start = time.time()
    f = my_apply_async(distCp, queue=clusters[0], args=(clusters, src, dest,))

    while not f.ready():
        logging.debug("Waiting for response from %s" % (clusters[0]))
        time.sleep(10)

    f.get()
    end = time.time()

    span = end - start

    return span


def copydata_getDigests(cluster, path):
    """ generate digests from the given path and returns the value

    :param cluster (string)
    :param path (string) source path

    :return dict with (filename: digest)
    """

    # new version
    f = my_apply_async(generate_digests, queue=cluster, args=(path, medusa_settings.digest_command,))
    while not f.ready():
        time.sleep(5)

    digests = f.get()

    return digests


def copydata_rmr(queue_name, paths):
    """
    remove paths remotely.
    paths is a list of path
    """
    from mergedirs import removedir
    logging.info("removing %s at %s" % (paths, queue_name))

    removedir.apply_async(queue=queue_name, args=(paths, )).get()
    return
