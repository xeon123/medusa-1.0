import profile
import unittest
import time
from collections import defaultdict

from medusa.medusasystem import getRunningClusters
import simplejson as json
from medusa.scheduler.predictionranking import get_prediction_metrics


# Tests the scheduler

class TestScheduling(unittest.TestCase):
    def test_run_predicion_ranking(self):
        """
         This unit test tests the prediction ranking alg
        """

        # from pudb import set_trace; set_trace()
        history_rank = defaultdict(lambda: 1)
        clusters = getRunningClusters()
        pinput = ["/wiki", "/wiki3"]

        for path in pinput:
            print get_prediction_metrics(clusters, [path])

    def test_save_and_load_prediction_file(self):
        temp = "/home/xeon/repositories/git/medusa_hadoop/temp"

        # from pudb import set_trace; set_trace()
        path = temp + "/prediction.json"
        with open(path) as afile:  # Use file to refer to the file object
            data = json.load(afile)

        results = data["cluster"]
        # update list
        results["error"] = 2
        results["prediction"] = int(time.time())

        data["cluster"] = results

        with open(path, "w") as afile:  # Use file to refer to the file object
            afile.write(json.dumps(data))

        print results


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    profile.run('unittest.main()')
