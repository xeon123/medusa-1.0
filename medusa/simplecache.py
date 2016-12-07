from dogpile.cache import make_region


class Memoize:
    def __init__(self, f):
        self.f = f
        self.memo = {}

    def __call__(self, *args):
        if not args in self.memo:
            self.memo[args] = self.f(*args)
        return self.memo[args]


class MemoizeCalls:
    """ This class saves the remove function calls. """

    def __init__(self, path, queue, func):
        self.path = path
        self.queue = queue
        self.func = func


region = make_region().configure('dogpile.cache.memory')


def get_reexecute_another_cloud():
    return region.get_or_create("reexecute-in-another-cloud", lambda: False)


def save_reexecute_another_cloud(value):
    save("reexecute-in-another-cloud", value)


def save(key, value):
    """
    general purpose method to save data (value) in the cache

    :param key (string) key of the value to be saved in cache
    :param value (any type) the value to be saved
    """
    region.set(key, value)


def get(key):
    """
    general purpose method to get data from the cache

    :param key (string) key of the data to be fetched
    :return value (any type) data to be returned from the cache
    """
    return region.get(key)
