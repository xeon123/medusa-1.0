import unittest

from medusa.medusasystem import generate_digests
from medusa.settings import medusa_settings

class TestClusterFunctions(unittest.TestCase):

    def test_generate_digest(self):
        for q in medusa_settings.clusters:
            pinput = "/output1"
            output = generate_digests.apply_async(queue=q, args=(pinput,))
            digests = output.get()

            l = str(digests).splitlines()
            print l


if __name__ == "__main__":
    unittest.main()
