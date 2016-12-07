import unittest

from medusa import xmlparser

import os


class TestClusterFunctions(unittest.TestCase):
    def test_xml_parse(self):
        path = os.environ['MEDUSA_HOME'] + "/submit/wordcount.xml"
        faults_tolerate = 1
        job_list = xmlparser.parser(path, faults_tolerate, "job")
        aggregator = xmlparser.parser(path, faults_tolerate, "aggregator")

        print job_list
        print aggregator


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
