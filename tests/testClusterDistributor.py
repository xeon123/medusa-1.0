import pprint
import unittest

from fabric.operations import local
from medusa.distributor import distribute
from medusa.local import lrm, mkdir
from medusa.settings import medusa_settings


class TestClusterDistributorFunctions(unittest.TestCase):
    def test_createMatrix(self):
        pathSize = 17
        clusters = ["cluster1", "cluster2", "cluster3"]
        path = list("input" + str(x)
                    for x in range(0, pathSize * len(medusa_settings.clusters)))
        matrix = distribute(clusters, path)
        pprint.pprint(matrix)

    '''
    distribute files through all clusters
    '''

    def test_distribute(self):
        # VMs conf
        user_home = "/home/adminuser"
        localsource = user_home + "/sample/gutenberg"
        pathsize = 17

        local(mkdir("-p " + localsource), capture=True)
        path = list(localsource + "/input" + str(x)
                    for x in range(0, pathsize * len(medusa_settings.clusters)))

        matrix = distribute(medusa_settings.clusters, path)

        expected_clusters = dict(
            zip(medusa_settings.clusters, [list() for x in range(len(medusa_settings.clusters))]))
        for cluster in medusa_settings.clusters:
            x = medusa_settings.clusters.index(cluster)
            for y in range(0, pathsize):
                expected_clusters[cluster].append(
                    "gutenberg/input" + str(y + (x * pathsize)))

        self.assertDictEqual(expected_clusters, matrix)
        local(lrm(localsource))


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
