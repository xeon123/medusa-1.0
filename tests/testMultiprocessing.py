import multiprocessing
from collections import defaultdict
import time

data = (
    ['a', '2'], ['b', '4'], ['c', '6'], ['d', '8'],
    ['e', '1'], ['f', '3'], ['g', '5'], ['h', '7']
)


# <Yhg1s> xeon123: you do understand that ((params, refdigests)) is (params, ref_digests), yes? It's not a tuple containing a two-tuple.
def mp_worker((inputs, the_time)):
    print " Processs %s\tWaiting %s seconds" % (inputs, the_time)
    time.sleep(int(the_time))
    print " Process %s\tDONE" % inputs


def mergeDirs(args):
    print "---"
    print args
    print "---"
    print args.faults
    print args.job
    print args.history_rank
    print args.command
    print args.poutput
    print args.pinput
    print args.ref_digests
    print "---"


def return_one():
    return 1


def mp_handler():
    p = multiprocessing.Pool(2)
    # p.map(mp_worker, data)

    story_rank = defaultdict(return_one)
    # xeon123: replace 'defaultdict(lambda: 1)' with a top-level function 'return_one' ('def return_one(): return 1'),  and a 'defaultdict(return_one)'
    # defaultdict(lambda: 1)
    params = (('jar wordcount /wiki /wiki-output', ['/wiki'], '/wiki-output'), 1, story_rank)
    ref_digests = [{'N1-0': {'/wiki/a.txt': 'e3b0'}}, {'N1-2': {'/wiki3/c.txt': 'e3b0c44298fc1c149'}}]

    job, faults, history_rank = params
    command, pinput, poutput = job

    print "simple call"
    w = Wrapper(faults, job, history_rank, ref_digests, command, pinput, poutput)
    # mergeDirs((params, ref_digests))
    mergeDirs(w)

    print "multiprocessing call"
    p.map(mergeDirs, [w])


class Wrapper:
    def __init__(self, faults, job, history_rank, ref_digests, command, pinput, poutput):
        self.faults = faults
        self.job = job
        self.history_rank = history_rank
        self.ref_digests = ref_digests
        self.command = command
        self.pinput = pinput
        self.poutput = poutput


if __name__ == '__main__':
    mp_handler()
