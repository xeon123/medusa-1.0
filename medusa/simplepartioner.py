def xdistribute(clusters, files):
    """ get a matrix [cluster:(path1, path2, ..., path n)], which distributes files to the clusters """

    if not clusters:
        return None

    csize = len(clusters)

    matrix = dict(zip(clusters, [list() for _ in range(len(clusters))]))
    for i, path in enumerate(files):
        matrix[clusters[i % csize]].append(path)

    return matrix
