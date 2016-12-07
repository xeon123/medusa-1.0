#!/bin/bash
#set -xv

# this is a workaround because I am having troubles
# executing locally with python and celery. It simply hangs.
#
# def run_job(clusters, params):
#     """ 
#     execute a jobs 
#     clusters is the cluster used to launch the job
#     command command to run
#     poutput is the output path of the execution that will be used to create the digests 
#     """
#     gid = params[ 0]
#     command = params[ 1]
#     poutput = params[ 2]
#     command2=generateDigests(poutput)
#     chain = command + "; " + command2
#        
#     executors = [ ]
#     for cluster in clusters:
#         print "Execute the job at %s: %s" %(cluster, chain)
#         s1=executeCommand.s(chain).set(queue=cluster[ 0])  # execute
#         executors.append(s1)
#    
#     job = group(executors)
#     waiter = job.apply_async()
#     r = waiter.get()
#     return r

$1/queuecapacity.sh
$1/jobslist.sh
