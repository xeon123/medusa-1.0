from medusa.copydata import copydata_prepareEnvironment, copydata_runEnvironment, copydata_getDigests


def test_distcp(clusters, namenodes, src_dir, to_dir):
    result1, result2 = copydata_prepareEnvironment(clusters, src_dir)
    print "%s - %s" % (result1, result2)
    reference_digests = copydata_getDigests(clusters[0], src_dir)  # generate digests
    copydata_runEnvironment(clusters, namenodes, src_dir, to_dir, reference_digests)

    print "DONE"


if __name__ == "__main__":
    clusters = ("Node00-0", "Node00-2")
    namenodes = ("hdfs://172.16.100.5:9000", "hdfs://172.16.100.7:9000")
    src_dir = '/wiki'
    to_dir = '/wiki'

    test_distcp(clusters, namenodes, src_dir, to_dir)
