import random
import unittest

from os.path import expanduser
from fabric.operations import local
from medusa.hdfs import cat, copyFromLocal, copyToLocal, generateDigests, rmr
from medusa.local import lcat, lrm


class TestHDFSFunctions(unittest.TestCase):

    def setUp(self):
        self.seq = range(10)

    def test_shuffle(self):
        # make sure the shuffled sequence does not lose any elements
        random.shuffle(self.seq)
        self.seq.sort()
        self.assertEqual(self.seq, range(10))

        # should raise an exception for an immutable sequence
        self.assertRaises(TypeError, random.shuffle, (1, 2, 3))

    def test_choice(self):
        element = random.choice(self.seq)
        self.assertTrue(element in self.seq)

    def test_sample(self):
        with self.assertRaises(ValueError):
            random.sample(self.seq, 20)
        for element in random.sample(self.seq, 5):
            self.assertTrue(element in self.seq)

    def test_sha1sum(self):
        path = expanduser("~") + "/test.txt"
        createLocalFile(path, "abc")
        command = generateDigests(path)
        digests = executelocal(command)
        self.assertEqual(
            "a9993e364706816aba3e25717850c26c9cd0d89d", str(digests))
        local(lrm(path))

    def test_copyFromLocal(self):
        path = expanduser("~") + "/test.txt"
        createLocalFile(path, "abc")
        local(copyFromLocal(path, "/"))
        output = local(cat("/test.txt"))
        self.assertEqual("abc", output[0])
        local(lrm(path))

    def test_copyToLocal(self):
        path = expanduser("~") + "/test.txt"
        local(copyToLocal("/test.txt", expanduser("~")))
        output = local(lcat(path))
        self.assertEqual("abc", output[0])
        local(rmr("/test.txt"))

if __name__ == '__main__':
    unittest.main()
