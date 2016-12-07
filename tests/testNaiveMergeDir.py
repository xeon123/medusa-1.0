from medusa.aggregation import naiveMergeDirs
from medusa.sets import PathStatistics


def test_naive_mergedir_with_reference():
    pinput = ['/wiki-output', '/wiki-output2', '/wiki-output3']
    faults = 1

    # NodeGroup0-0: /wiki-output, /wiki-output2
    # NodeGroup1-0: /wiki-output, /wiki-output2, /wiki-output3
    # NodeGroup2-0:
    # NodeGroup2-3: /wiki-output3

    # RefDigests = namedtuple('RefDigests', ['hosts', 'digests'])
    # r1 = RefDigests(hosts=['NodeGroup0-0', 'NodeGroup1-0'], digests={'/wiki-output/_SUCCESS': 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855', '/wiki-output/part-r-00000': 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855', '/wiki-output/part-r-00001': 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'})
    # r2 = RefDigests(hosts=['NodeGroup0-0', 'NodeGroup1-0'], digests={'/wiki-output2/_SUCCESS': 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855', '/wiki-output2/part-r-00001': 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855', '/wiki-output2/part-r-00000': 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'})
    # r3 = RefDigests(hosts=['NodeGroup1-0', 'NodeGroup2-4'], digests={'/wiki-output3/_SUCCESS': 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855', '/wiki-output3/part-r-00000': 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855', '/wiki-output3/part-r-00001': 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'})
    # digests = [r1, r2, r3]

    p1 = PathStatistics('NodeGroup0-0', ['/wiki-output', '/wiki-output2', '/wiki-output3'])
    p2 = PathStatistics("NodeGroup1-0", ['/wiki-output', '/wiki-output2'])
    p3 = PathStatistics("NodeGroup2-4", ['/wiki-output3'])

    included_hosts = [p1]
    excluded_hosts = [p2, p3]

    host_spec = (included_hosts, excluded_hosts)

    orders = naiveMergeDirs(host_spec, pinput)
    print "1: %s" % orders


def test_naive_mergedir_with_reference2():
    pinput = ['/wiki-output', '/wiki-output2', '/wiki-output3']
    faults = 1

    # NodeGroup0-0: /wiki-output, /wiki-output2
    # NodeGroup1-0: /wiki-output, /wiki-output2, /wiki-output3
    # NodeGroup2-0:
    # NodeGroup2-3: /wiki-output3

    p1 = PathStatistics('NodeGroup0-0', ['/wiki-output', '/wiki-output2', '/wiki-output3'])
    p2 = PathStatistics("NodeGroup1-0", ['/wiki-output', '/wiki-output2'])
    p3 = PathStatistics("NodeGroup2-4", ['/wiki-output3'])
    p4 = PathStatistics("NodeGroup2-0", [])

    included_hosts = [p1]
    excluded_hosts = [p2, p3, p4]

    host_spec = (included_hosts, excluded_hosts)

    orders = naiveMergeDirs(host_spec, pinput)
    print "2: %s" % orders


def test_naive_mergedir_with_2references():
    pinput = ['/wiki-output', '/wiki-output2', '/wiki-output3']
    faults = 1

    # NodeGroup0-0: /wiki-output, /wiki-output2
    # NodeGroup1-0: /wiki-output, /wiki-output2, /wiki-output3
    # NodeGroup2-0:
    # NodeGroup2-3: /wiki-output3

    p1 = PathStatistics('NodeGroup0-0', ['/wiki-output', '/wiki-output2', '/wiki-output3'])
    p2 = PathStatistics("NodeGroup1-0", ['/wiki-output', '/wiki-output2', '/wiki-output3'])
    p3 = PathStatistics("NodeGroup2-4", ['/wiki-output3'])
    p4 = PathStatistics("NodeGroup2-0", [])

    included_hosts = [p1]
    excluded_hosts = [p2, p3, p4]

    host_spec = (included_hosts, excluded_hosts)

    orders = naiveMergeDirs(host_spec, pinput)
    print "3: %s" % orders


def test_naive_mergedir_without_reference():
    pinput = ['/wiki-output', '/wiki-output2', '/wiki-output3']
    faults = 1

    # NodeGroup0-0: /wiki-output, /wiki-output2
    # NodeGroup1-0: /wiki-output, /wiki-output2, /wiki-output3
    # NodeGroup2-0:
    # NodeGroup2-3: /wiki-output3

    # {'cluster': 'NodeGroup2-4', 'rank': 0, 'paths': ['/wiki-output3']}
    # {'cluster': 'NodeGroup2-0', 'rank': 0, 'paths': ['/wiki-output3']}
    # {'cluster': 'NodeGroup2-0', 'rank': 0, 'paths': ['/wiki-output3']}
    # {'cluster': 'NodeGroup2-4', 'rank': 0, 'paths': ['/wiki-output3']}
    #
    # {'cluster': 'NodeGroup0-0', 'rank': 0, 'paths': ['/wiki-output3']}
    # {'cluster': 'NodeGroup0-0', 'rank': 0, 'paths': ['/wiki-output3']}
    # {'cluster': 'NodeGroup1-0', 'rank': 0, 'paths': ['/wiki-output3']}
    # {'cluster': 'NodeGroup1-0', 'rank': 0, 'paths': ['/wiki-output3']}
    #

    p1 = PathStatistics('NodeGroup0-0', ['/wiki-output', '/wiki-output2'])
    p2 = PathStatistics("NodeGroup1-0", ['/wiki-output', '/wiki-output2'])
    p3 = PathStatistics("NodeGroup2-4", ['/wiki-output3'])
    p4 = PathStatistics("NodeGroup2-0", ['/wiki-output3'])

    included_hosts = []
    excluded_hosts = [p1, p2, p3, p4]

    host_spec = (included_hosts, excluded_hosts)

    orders = naiveMergeDirs(host_spec, pinput)
    print "4: %s" % orders


if __name__ == "__main__":
    test_naive_mergedir_with_reference()
    test_naive_mergedir_with_reference2()
    test_naive_mergedir_with_2references()
    test_naive_mergedir_without_reference()
