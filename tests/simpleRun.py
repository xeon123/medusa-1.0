import time
import logging
from multiprocessing.pool import ThreadPool

from medusa import xmlparser
from medusa.simplecache import save
from medusa.algorithm import run_execution, run_verification_global
from medusa.medusasystem import readFileDigests, setRunningClusters


# Runs an example that it is defined in the wordcount
def test_run():
    # read wordcount xml
    # cluster1: job1 --> aggregation: job3
    # cluster2: job2 -----^

    path = "/root/Programs/medusa-1.0/submit/job.xml"
    from pudb import set_trace; set_trace()
    format = "%(asctime)s [%(levelname)s] %(message)s"
    logging.basicConfig(format=format, level=logging.DEBUG)

    faults_tolerate = 1
    job_list = xmlparser.parser(path, faults_tolerate, "job")
    aggregator = xmlparser.parser(path, faults_tolerate, "aggregator")

    save("job", job_list)
    save("aggregator", aggregator)

    sequence = [job_list, aggregator]

    pool = ThreadPool(processes=4)
    step = 0
    while step < len(sequence):
        jobs = sequence[step]
        save("step", step)

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


        if step == 0:
            gstart = time.time()

        # start the test
        mstart = time.time()
        # CPU_CORES
        digests_matrix = run_execution(faults_tolerate, jobs, step == 1, reference_digests)
        mend = time.time()
        span = mend - mstart
        logging.info("Execution time (start: %s, end: %s): %s" % (mstart, mend, str(span)))

        logging.info("Return digests: %s" % digests_matrix)

        res = run_verification_global(digests_matrix)

        if res is True:
            logging.info("Step %s completed" % step)
            step += 1

    gend = time.time()
    gspan = str(gend - gstart)
    print("Full execution (start: %s, end: %s): %s" % (gstart, gend, gspan))


if __name__ == "__main__":
    test_run()
