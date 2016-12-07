import unittest

from medusa.networkdaemon import read_network_data
from medusa.scheduler.predictionranking import get_prediction_on_network


class TestExecution(unittest.TestCase):
    def test_read_network_data(self):
        get_prediction_on_network("hadoop-medusa-1", ["hadoop-medusa-2"])
        read_network_data("hadoop-medusa-1", "hadoop-medusa-2")


if __name__ == "__main__":
    unittest.main()

