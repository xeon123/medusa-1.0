"""
rank the clusters according the time of executing a dummy HDFS copy

"""


# def getClusterMetrics(from_host, src, dest):
#     """ get metrics related to the cluster by executing a dummy HDFS copy """
#
#     rank = []
#     for q in CLUSTERS:
#         result = getNamenodeAddress.apply_async(queue=q)
#         namenode = result.get()
#
#         print namenode
#         print from_host
#
#         if from_host == namenode:
#             rank.append((q, 0))
#             continue
#
#     target = "%s/%s" % (namenode, dest)
#     command = distCp(from_host + src, target)
#
#     print command
#
#     start = time.time()
#     output = executeCommand.apply_async(queue=q, args=(command, ))
#     output.get()
#     end = time.time()
#     span = int(end - start)
#
#     logline = "%s:%s:%s" % (from_host, dest, span)
#     command = network_filewriteDataTime(logline)
#     s1 = executeCommand.apply_async(queue=from_host, args=(command, ))
#     s1.get()
#
#     rank.append((q, span))
#
#     rank.sort(key=lambda tup: tup[1])
#
#     return rank
