import logging
from itertools import izip
from multiprocessing import Pipe, Process


def spawn(f):
    # 1 - how the pipe and x attributes end up here?
    def fun(pipe, x):
        pipe.send(f(x))
        pipe.close()

    return fun


def parmap(f, X):
    pipe = [Pipe() for _ in X]
    # 2 - what is happening with the tuples (c,x) and (p, c)?
    proc = [Process(target=spawn(f), args=(c, x))
            for x, (p, c) in izip(X, pipe)]
    # [p.start() for p in proc]
    for p in proc:
        logging.debug("Spawn")
        p.start()
    # [p.join() for p in proc]
    for p in proc:
        logging.debug("Joining")
        p.join()
    return [p.recv() for (p, c) in pipe]


if __name__ == '__main__':
    print parmap(lambda x: x ** x, range(1, 5))
