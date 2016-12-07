#!/usr/bin/python

import thread
from datetime import datetime

import medusa


def print_hello():
    cluster = "ip-10-37-197-69"
    output = medusa.hello.apply_async(queue=cluster)
    hello = output.get()

    print hello


# Define a function for the thread
def print_time():
    start = datetime.now()
    print_hello()
    end = datetime.now()
    print "Duration: %s" % (str(end - start))

    # profile.run("print_hello()")

if __name__ == '__main__':
    # Create two threads as follows
    thread.start_new_thread(print_time)
    thread.start_new_thread(print_time)
    thread.start_new_thread(print_time)
    thread.start_new_thread(print_time)

    while True:
        pass
