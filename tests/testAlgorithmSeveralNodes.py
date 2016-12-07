import unittest
from datetime import datetime

from fabric.network import disconnect_all
from medusa import settings
import medusa
from medusa.execution import executeCommand
from medusa.settings import medusa_settings


# Test the algorithm with a full use case
# HDFS and MAPREDUCE must be running
class TestClustersFunctions(unittest.TestCase):
    def test_distributeWorkThroughClusters(self):
        """
        This test distributes data through different clusters and execute the same example on them
        """

        # For localhost configuration
        clusters = ["localhost"]  # the clusters used
        hadoop_source = "/user/xeon/gutenberg"
        hadoop_output = "/user/xeon/gutenberg-output"

        hadoop_jar = settings.get_hadoop_home() + "/hadoop-examples-1.0.4.jar"
        command = """%s jar %s wordcount %s %s""" % (
            settings.get_hadoop_bin(), hadoop_jar, hadoop_source, hadoop_output)

        print "Execute the job"
        mstart = datetime.now()
        for q in clusters:
            executeCommand.apply_async(queue=q, args=(command,))
        mend = datetime.now()

        # copy data from HDFS to local
        print "Validate data"
        vstart = datetime.now()
        data = []
        for q in clusters:
            output = medusa.generateDigests.apply_async(
                queue=q, args=(hadoop_output + "/part*",))
            data.append(output.get())

        # the validation of the digests is made by the scheduling algorithm
        vend = datetime.now()
        print data

        disconnect_all()
        print "Execution time: " + str(mend - mstart)
        print "Verification time: " + str(vend - vstart)

    def test_runValidation(self):
        # For localhost conf
        hadoop_output = "/user/xeon/gutenberg-output"

        print "Voting..."
        vstart = datetime.now()
        data = []
        for q in medusa_settings.clusters:
            output = medusa.generateDigests.apply(
                queue=q, args=(hadoop_output + "/part*",))
            data.append(output.get())

        vend = datetime.now()

        print "Verification time: " + str(vend - vstart)
        print data

    """
    def test_voting(self):
        d={"a":["1","2","3"],"b":["1","2","3"],"c":["1","2","3"]}
        self.assertEqual(("1","2","3"), vote(d,1))

        d={"a":(1,2,3),"b":(1,2,3),"c":(1,2,3)}
        self.assertEqual((1,2,3), vote(d,1))

        d={"a":(1,2,3),"b":(1,2,3),"c":(4,5,6)}
        self.assertEqual((1,2,3), vote(d,1))

        d={"a":(1,2,3),"b":(4,5,6),"c":(1,2,3)}
        self.assertEqual((1,2,3), vote(d,1))

        d={"a":(4,5,6),"b":(1,2,3),"c":(1,2,3)}
        self.assertEqual((1,2,3), vote(d,1))

        d={"a":(4,5,6),"b":(4,5,6),"c":(1,2,3)}
        self.assertEqual((4,5,6,), vote(d,1))

        d={"a":(1,2,3),"b":(4,5,6),"c":(4,5,6)}
        self.assertEqual((4,5,6), vote(d,1))

        d={"a":(4,5,6),"b":(1,2,3),"c":(7,8,9)}
        self.assertEqual(None, vote(d,1))
    """


if __name__ == "__main__":
    single = unittest.TestSuite()
    single.addTest(TestClustersFunctions("test_runExecution"))
    unittest.TextTestRunner().run(single)
