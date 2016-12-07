import profile
import time
import unittest
import logging
from multiprocessing.pool import ThreadPool

from medusa import xmlparser
from medusa.algorithm import run_execution, run_verification_global
from medusa.medusasystem import readFileDigests, setRunningClusters


# This unit test runs an example that it is defined in the wordcount
class TestExecution(unittest.TestCase):
    def test_run(self):
        # read wordcount xml
        # cluster1: job1 --> aggregation: job3
        # cluster2: job2 -----^
        path = "/root/Programs/medusa-1.0/submit/job.xml"
        from pudb import set_trace; set_trace()
        format = "%(asctime)s [%(levelname)s] %(message)s"
        logging.basicConfig(format=format, level=logging.NOTSET)

        faults_tolerate = 1
        job_list = xmlparser.parser(path, faults_tolerate, "job")
        aggregator = xmlparser.parser(path, faults_tolerate, "aggregator")

        sequence = [job_list, aggregator]

        gstart = time.time()

        pool = ThreadPool(processes=4)
        step = 0
        while step < len(sequence):
            jobs = sequence[step]

            if len(jobs) == 0:
                step += 1
                continue

            logging.info("Step %s starting" % step)
            if step == 0:
                logging.info("Checking clusters that are running...")
                setRunningClusters()

            # prepare environment for the test
            logging.info("Generating reference digests...")
            ss = time.time()

            reference_digests = []
            plist = []
            for job in jobs:
                plist.append(pool.apply_async(readFileDigests, args=(job.input_path, step == 1)))

            for p in plist:
                while not p.ready():
                    logging.debug("Still waiting for reference digests...")
                    time.sleep(5)

                _output = p.get()

                if len(_output) > 0:
                    if not step == 1:
                        reference_digests += _output
                    else:
                        reference_digests = _output
            ee = time.time()
            logging.info("Reference digests created in %s sec." % (int(ee - ss)))


            # start the test
            mstart = time.time()
            # CPU_CORES
            digests_matrix = run_execution(faults_tolerate, jobs, step == 1, reference_digests)
            mend = time.time()
            span = str(mend - mstart)
            logging.info("Execution time (start: %s, end: %s): %s" % (mstart, mend, span))
            logging.debug("Return digests: %s" % digests_matrix)

            res = run_verification_global(digests_matrix)

            if res is True:
                logging.info("Step %s completed" % step)
                step += 1

        gend = time.time()
        gspan = str(gend - gstart)
        logging.info("Full execution (start: %s, end: %s): %s" % (gstart, gend, gspan))


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    # profile.run('unittest.main()')
    unittest.main()
