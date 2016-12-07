import unittest

from medusa.vote.voting import vote


class TestClusterFunctions(unittest.TestCase):
    def test_validatedata(self):
        d = {'cluster1': ['abc', 'def'], 'cluster2': ['abc', 'def']}
        faults = 1

        print "Validate data"
        output = vote(d, faults)

        self.assertEqual(True, output[1])
        self.assertSequenceEqual(['abc', 'def'], output[0])

    def test_validatedata2(self):
        d = {'cluster1': ['abc', 'def', 'ghi', 'jkl', 'mno'], 'cluster2': [
            'abc', 'def', 'ghi', 'jkl', 'mno'], 'cluster3': ['cui', 'oeu', 'gao', 'jko', 'oeoo']}
        faults = 1

        print "Validate data"
        output = vote(d, faults)

        self.assertEqual(True, output[1])
        self.assertSequenceEqual(
            ['abc', 'def', 'ghi', 'jkl', 'mno'], output[0])

    def test_validatedata3(self):
        d = {'cluster1': ['abd', 'def', 'ghi', 'jkl', 'mno'], 'cluster2': [
            'abc', 'def', 'ghi', 'jkl', 'mno'], 'cluster3': ['cui', 'oeu', 'gao', 'jko', 'oeoo']}
        faults = 1

        print "Validate data"
        output = vote(d, faults)

        self.assertSequenceEqual((None, False), output)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
