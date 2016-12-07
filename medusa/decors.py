import threading

from medusa.settings import medusa_settings


class ResultCatcher:

    def __init__(self, f):
        self.f = f
        self.val = None

    def __call__(self, *args, **kwargs):
        self.val = self.f(*args, **kwargs)


def make_verbose(func):
    def verbose(*args):
        # will print (e.g.) fib(5)
        if medusa_settings.verbose:
            print '%s(%s)' % (func.__name__, ', '.join(repr(arg) for arg in args))
        return func(*args)  # actually call the decorated function

    return verbose


def synchronized(func):
    func.__lock__ = threading.Lock()

    def synced_func(*args, **kws):
        with func.__lock__:
            return func(*args, **kws)

    return synced_func


def timed(func):
    import time
    from functools import wraps

    @wraps(func)
    def wrapper(*args, **kwds):
        start = time.time()
        result = func(*args, **kwds)
        elapsed = time.time() - start
        print "%s took %d time to finish" % (func.__name__, elapsed)
        return result

    return wrapper


def run_async(func):
    """
        run_async(func)
            function decorator, intended to make "func" run in a separate
            thread (asynchronously).
            Returns the created Thread object

            E.g.:
            @run_async
            def task1():
                do_something

            @run_async
            def task2():
                do_something_too

            t1 = task1()
            t2 = task2()
            ...
            t1.join()
            t2.join()
    """

    """
    import threading

    class MyThread(threading.Thread):

        def __init__(self, group=None, target=None, name=None, *args, **kw):
            self.t = target
            threading.Thread.__init__(self, group, target, name, *args, **kw)

        def run(self):
            # do proper error checking here too!
            self.result = self.t()

        t = MyThread(target=lambda: 1)
        t.start()
        t.join()
        print t.result
    """
    from threading import Thread
    from functools import wraps

    @wraps(func)
    def async_func(*args, **kwargs):
        func_hl = Thread(target=func, args=args, kwargs=kwargs)
        func_hl.start()
        return func_hl

    return async_func
