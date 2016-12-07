import multiprocessing

from medusa.settings import medusa_settings
from medusa.medusasystem import error_handler, hello, _isClusterWorking, setRunningClusters, getRunningClusters


def test_hello():
    print "Serial test"
    for q in medusa_settings.clusters:
        print "%s\n" % q
        output = hello.apply_async(queue=q, args=(q,))
        print output.get()


def test_hello_aux(q):
    output = hello.apply_async(
        queue=q, args=(q,), link_error=error_handler.s())
    print "request sent"
    print output.get()


def test_hello_proc():
    print "Multi process test"
    procs = []
    for q in medusa_settings.clusters:
        proc = multiprocessing.Process(target=test_hello_aux, args=(q,))
        proc.start()
        print "Launching proc %s" % proc.pid
        procs.append(proc)

    for proc in procs:
        print "Joining proc %s" % proc.pid
        proc.join()


active = []
def test_running_cluster():
    for q in medusa_settings.clusters:
        if _isClusterWorking(q) is not None:
            active.append(q)

    print "Active clusters"
    print active

    setRunningClusters()
    print getRunningClusters()


if __name__ == "__main__":
    print "test_hello"
    # test_hello()
    print "test_hello_proc"
    # test_hello_proc()
    print "cluster working"
    test_running_cluster()
