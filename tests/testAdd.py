import unittest
from datetime import datetime

from celery import chain, group
import settings


class TestClusterFunctions(unittest.TestCase):
    def test_make_add(self):
        for q in medusa_settings.clusters:
            taskset_result = add.apply_async(queue=q, args=(1, 9))

            print "Dispatching task"
            self.assertEqual(10, taskset_result.get())

    def test_make_add_chain(self):
        for q in medusa_settings.clusters:
            print "Queue: " + q
            mstart = datetime.now()
            # taskset_result=chain(add.s(1,2).set(queue=q), add.s(3,).set(queue=q)).apply_async()
            # taskset_result.get()
            mend = datetime.now()
            print "Execution time:" + str(mend - mstart)

            mstart = datetime.now()
            taskset_result = chain(
                add.si(1, 2).set(queue=q), add.si(3, 4).set(queue=q)).apply_async()
            taskset_result.get()
            mend = datetime.now()
            print "Execution time:" + str(mend - mstart)

            mstart = datetime.now()
            taskset_result = add.apply_async(queue=q, args=(1, 2))
            taskset_result.get()
            taskset_result = add.apply_async(queue=q, args=(3, 4))
            taskset_result.get()
            mend = datetime.now()
            print "Execution time:" + str(mend - mstart)

    def test_make_add_queue(self):
        # for q in CLUSTERS:
        mstart = datetime.now()
        # taskset_result=chain(add.si(1,2).set(queue=q), add.si(3,4).set(queue=q)).apply_async()
        g = group(add.s(1, 2)
                  .set(queue=medusa_settings.clusters[0]), add.s(1, 2).set(queue=medusa_settings.clusters[1]))
        g = g()
        a = g.get()
        mend = datetime.now()
        print a
        # taskset_result=chain(add.s(1,2).set(queue=q), add.s(3,).set(queue=q)).apply_async()
        # taskset_result.get()
        print "Execution time:" + str(mend - mstart)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
