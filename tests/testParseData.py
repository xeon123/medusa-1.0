import profile
import unittest
from collections import Counter

from medusa import settings
import simplejson as json
from medusa.medusasystem import read_data, write_data


# This unit test runs an example that it is defined in the wordcount


class TestExecution(unittest.TestCase):
    def test_run(self):
        l = [{"1": "b", "2": ".", }, {"1": "b", "2": ".", }, {"1": "i", "2": "x", }]

        # from pudb import set_trace; set_trace()
        keys = l[0].keys()
        tuples = [tuple(d[k] for k in keys) for d in l]
        values = Counter(tuples).most_common(1)
        v, k = values[0]
        print k, v
        print keys
        print dict(zip(keys, v))

    def test_update_json(self):
        path = settings.get_temp_dir() + "/job_log.json"
        content = read_data(path)
        content = ''.join(content)
        content = content.replace("\n", "")
        content = content.strip()
        content = json.loads(content)

        gid = "3"
        for x in content["jobs"]["job"]:
            if x["gid"] == gid:
                x["step"] = "20"

        write_data(path, json.dumps(content))

    def test_add_entry_json(self):
        # from pudb import set_trace; set_trace()
        path = settings.get_temp_dir() + "/job_log.json"
        content = read_data(path)
        content = ''.join(content)
        content = content.replace("\n", "")
        content = content.strip()
        content = json.loads(content)

        gid = "40"
        for x in content["jobs"]["job"]:
            if x["gid"] == gid:
                x["step"] = "20"

        content["jobs"]["job"].append({"gid": "40", "command": "test", "step": "1"})
        write_data(path, json.dumps(content))


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    profile.run('unittest.main()')
