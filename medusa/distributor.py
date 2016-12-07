from simplepartioner import xdistribute

"""
 rearrange the files in the clusters and copy the output to the clusters
"""


def distribute(files, clusters):
    # defines the distribution of the files
    if isinstance(files, list):
        return xdistribute(clusters, files)
    elif isinstance(files, dict):
        return files

    return None
