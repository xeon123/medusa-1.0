
import unittest
from pprint import pprint

from simplepartioner import xdistribute


class TestSimplePartitionerFunctions(unittest.TestCase):

    def test_simplePartitioner(self):
        clusters = ["cluster1", "cluster2", "cluster3"]
        files = ["file1", "file2", "file3"]
        output = xdistribute(clusters, files)
        pprint(output)

        clusters = ["cluster1", "cluster2", "cluster3", "cluster4", "cluster5"]
        files = ["file1", "file2", "file3"]
        output = xdistribute(clusters, files)
        pprint(output)

        clusters = ["cluster1", "cluster2", "cluster3"]
        files = ["file1", "file2", "file3", "file4", "file5", "file6", "file7"]
        output = xdistribute(clusters, files)
        pprint(output)

        clusters = ["cluster1", "cluster2"]
        files = ["file1", "file2", "file3"]
        output = xdistribute(clusters, files)
        pprint(output)

        clusters = ["cluster1"]
        files = ["file1", "file2", "file3"]
        output = xdistribute(clusters, files)
        pprint(output)

        clusters = []
        files = ["file1", "file2", "file3"]
        output = xdistribute(clusters, files)
        pprint(output)

        clusters = ["cluster1"]
        files = [""]
        output = xdistribute(clusters, files)
        pprint(output)

if __name__ == '__main__':
    unittest.main()
