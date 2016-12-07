import unittest

# Test the algorithm with a full use case
# HDFS and MAPREDUCE must be running
from medusa.xmlparser import parseNaiveMetrics, parseCompletionMetrics


class TestRanking(unittest.TestCase):
    def test_parseNaiveMetrics(self):
        """ calculate metrics based on the cluster """
        xmlfile = """<?xml version="1.0"?>
        <data>
            <global>
                <total>Total jobs: 4</total>
                <time>Time: 1369838884</time>
            </global>
            <jobs>
                <job>
                    <name>job_1368077063111_162538</name>
                    <status>RUNNING</status>
                    <starttime>1369838884</starttime>
                    <nrmaps>Number of maps: 2</nrmaps>
                    <nrreduces>Number of reduces: 10</nrreduces>
                    <mapcompletion>map() completion: 1.0</mapcompletion>
                    <redcompletion>reduce() completion: 0.123</redcompletion>
                </job>
                <job>
                    <name>job_1368077063111_158976</name>
                    <status>RUNNING</status>
                    <starttime>1369838885</starttime>
                    <nrmaps>Number of maps: 30</nrmaps>
                    <nrreduces>Number of reduces: 35</nrreduces>
                    <mapcompletion>map() completion: 1.0</mapcompletion>
                    <redcompletion>reduce() completion: 0.0234</redcompletion>
                </job>
                <job>
                    <name>job_1368077063111_162546</name>
                    <status>RUNNING</status>
                    <starttime>1369838886</starttime>
                    <nrmaps>Number of maps: 3</nrmaps>
                    <nrreduces>Number of reduces: 11</nrreduces>
                    <mapcompletion>map() completion: 0.2</mapcompletion>
                    <redcompletion>reduce() completion: 0.0</redcompletion>
                </job>
            </jobs>
        </data>
        """

        print parseNaiveMetrics(xmlfile)

    def test_parseCompletionMetrics(self):
        """ calculate metrics based on the cluster """
        xmlfile = """<?xml version="1.0"?>
        <data>
            <global>
                <total>Total jobs: 4</total>
                <time>Time: 1369838884</time>
            </global>
            <jobs>
                <job>
                    <name>job_1368077063111_162538</name>
                    <status>RUNNING</status>
                    <starttime>1369838885</starttime>
                    <nrmaps>Number of maps: 2</nrmaps>
                    <nrreduces>Number of reduces: 10</nrreduces>
                    <mapcompletion>map() completion: 1.0</mapcompletion>
                    <redcompletion>reduce() completion: 0.123</redcompletion>
                </job>
                <job>
                    <name>job_1368077063111_158976</name>
                    <status>RUNNING</status>
                    <starttime>1369838886</starttime>
                    <nrmaps>Number of maps: 30</nrmaps>
                    <nrreduces>Number of reduces: 35</nrreduces>
                    <mapcompletion>map() completion: 1.0</mapcompletion>
                    <redcompletion>reduce() completion: 0.0234</redcompletion>
                </job>
                <job>
                    <name>job_1368077063111_162546</name>
                    <status>RUNNING</status>
                    <starttime>1369838887</starttime>
                    <nrmaps>Number of maps: 3</nrmaps>
                    <nrreduces>Number of reduces: 11</nrreduces>
                    <mapcompletion>map() completion: 0.2</mapcompletion>
                    <redcompletion>reduce() completion: 0.0</redcompletion>
                </job>
            </jobs>
        </data>
        """
        print parseCompletionMetrics(xmlfile)


if __name__ == "__main__":
    unittest.main()
