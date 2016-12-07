import hashlib
import unittest
from pprint import pprint


# Test the algorithm with a full use case
# HDFS and MAPREDUCE must be running


class TestClusterDistributorFunctions(unittest.TestCase):

    def test_Cache(self):
        cachex = {}
        paths = ["a", "b", "c"]

        m = hashlib.md5()
        m.update(''.join(paths))
        digest = m.hexdigest()

        # update cache
        cachex.update({digest: (paths, digest)})

        pprint(cachex)

        self.assertDictEqual(cachex, {digest: (paths, digest)})


if __name__ == "__main__":
    unittest.main()
