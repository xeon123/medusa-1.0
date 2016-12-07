# -*- coding: utf-8 -*-
"""
file      matlib.py

author    Ernesto P. Adorio
          UPDEPP (UP Clarkfield)
          ernesto.adorio@gmail.com

notes
    1. This library is for educational purposes only
    2. Use numpy or scipy for faster execution!

revisions
    Ver 0.0.1 initial release
               2009.01.16 added matdots,matrandom, isiterable
    Ver 0.0.2  2009.10.12 added vec2colmat, vec2rowmat,
                mat2vec, matSelectCols, matDelCols, matInsertConstCol,
    Ver 0.0.3 2009.12.21 add matDelRows
"""
import random
from argparse import ArgumentError


def tabular(hformat, headers, bformat, M):
    # added nov.29, 2008.
    # prints the table data.
    # nrows = len(M)
    # ncols = len(M[0])

    # print headers.
    for j, heading in enumerate(headers):
        print hformat[j] % heading,
    print

    # print the body.
    for i, row in enumerate(M):
        for j, col in enumerate(M[i]):
            print bformat[j] % M[i][j],
        print
    print


def vecadd(X, Y):
    """
    Performs vector addition X + Y.
    """
    n = len(X)
    if n != len(Y):
        raise ArgumentError("incompatible vector lengths in vecadd().")
    return [x + y for x, y in zip(X, Y)]


def vecsub(X, Y):
    n = len(X)
    if n != len(Y):
        raise ArgumentError("incompatible vector lengths in vecsub().")
    return [x - y for x, y in zip(X, Y)]


def eye(m, n=None):
    """
    Identity matrix of size m,n
    """
    if n is None:
        n = m
    B = [[0] * n for _ in range(m)]
    for i in range(m):
        B[i][i] = 1.0
    return B


matiden = eye


def vec2colmat(X):
    """
    Retuns a 1 column matrix out of array or vector X.
    """
    return [[x] for x in X]


def vec2rowmat(X):
    """
    Retuns a 1 row matrix out of array or vector X.
    """

    return [[x for x in X]]


def mat2vec(M, column=0):
    """
    Returns a vector from M.
    """
    return [m[column] for m in M]


def matSelectCols(M, jindices):
    """
    Extracts a submatrix from M with col indices in jindices.
    """
    N = []
    for i in range(len(M)):
        N.append([M[i][j] for j in jindices])
    return N


def matDelCols(M, colindices):
    """
    Deletes specified columns from matrix M.
    M - input matrix
    colindices = array indices of columns to delete.
    """
    jindices = []
    for j in range(len(M[0])):
        if j not in colindices:
            jindices.append(j)
    return matSelectCols(M, jindices)


def matInsertConstCol(X, column, c, inplace=True):
    """
    Inserts a constant column to vector or matrix X at position column.
    NEVER forget that indexing starts at zero.
    """
    if not inplace:
        Xcopy = [x[:] for x in X]
    else:
        Xcopy = X
    for i in range(len(X)):
        Xcopy[i].insert(column, c)
    return Xcopy


def matDelRows(M, rowindices):
    """
     Deletes specified rows in rowindices from matrix M.
    """
    rindices = []
    for i in range(len(M)):
        if i not in rowindices:
            rindices.append(i)

    return [M[i][:] for i in rindices]


def matzero(m, n=None):
    """
    Returns an m by n zero matrix.
    """
    if n is None:
        n = m
    return [[0] * n for _ in range(m)]


def matdiag(D):
    """
    Returns a diagonal matrix D.
    """
    n = len(D)
    A = [[0] * n for _ in range(n)]
    for i in range(n):
        A[i][i] = D[i]
    return A


def diag(M):
    # Returns the diagonal elements of M as a vector.
    minlen = min(len(M), len(M[0]))
    return [M[i][i] for i in range(minlen)]


def matcol(X, j):
    # Returns the jth column of matrix X as a vector.
    nrows = len(X)
    return [X[i][j] for i in range(nrows)]


def trace(A):
    """
    Returns the trace of a matrix.
    """
    return sum([A[i][i] for i in range(len(A))])


def matadd(A, B):
    """
    Returns matrix sum C = A + B.
    """
    try:
        m, n = matdim(A)
        if m != len(B) or n != len(B[0]):
            raise ArgumentError("Incompatible matrices in matadd")
        C = matzero(m, n)
        for i in range(m):
            for j in range(n):
                C[i][j] = A[i][j] + B[i][j]
        return C

    except:
        return None


def matsub(A, B):
    """
    returns matrix difference C = A - B.
    """
    try:
        m, n = matdim(A)
        if m != len(B) or n != len(B[0]):
            raise ArgumentError("Incompatible matrices for matsub")
        C = matzero(m, n)
        for i in range(m):
            for j in range(n):
                C[i][j] = A[i][j] - B[i][j]
        return C

    except:
        return None


def matcopy(A):
    """
    Returns a copy of matrix A.
    revised dec. 21
    """
    try:
        return [row[:] for row in A]
    except:
        return A[:]


def matkmul(A, k):
    """
    Multiplies each element of A by k.
    """
    B = matcopy(A)
    for i in range(len(A)):
        for j in range(len(A[0])):
            B[i][j] *= k
    return B


def transpose(A):
    """
    Returns the transpose of A.
    """
    m, n = matdim(A)
    At = [[0] * m for _ in range(n)]
    for i in range(m):
        for j in range(n):
            At[j][i] = A[i][j]
    return At


matt = transpose
mattrans = transpose


def matdim(A):
    # Returns the number of rows and columns of A.
    if hasattr(A, "__len__"):
        m = len(A)
        if hasattr(A[0], "__len__"):
            n = len(A[0])
        else:
            n = 1
    else:
        m = 0  # not a matrix!
        n = 0

    return m, n


def matprod(A, B):
    """
    Computes the product of two matrices A*b
    2009.01.16 Revised for matrix or vector B.

    A and B are matrices. If one of them is a vector,
    it must be transformed into a matrix with one row
    or one column. This is not done automatically in this routine.
    You can use mattvec or matvec
    """
    m, n = matdim(A)
    p, q = matdim(B)
    if n != p:
        raise ArgumentError("Incompatible matrices in matprod")
    try:
        if iter(B[0]):
            q = len(B[0])
    except:
        q = 1
    C = matzero(m, q)
    for i in range(m):
        for j in range(q):
            t = sum([A[i][k] * B[k][j] for k in range(p)])
            C[i][j] = t
    return C


matmul = matprod


def matvec(A, y):
    """
    Returns the product of matrix A with vector y.
    """
    m = len(A)
    n = len(A[0])
    out = [0] * m

    for i in range(m):
        for j in range(n):
            out[i] += A[i][j] * y[j]
    return out


def mattvec(A, y):
    """
    Returns the vector A^t y.
    """
    At = transpose(A)
    return matvec(At, y)


def dot(X, Y):
    return sum(x * y for (x, y) in zip(X, Y))


def matdots(X):
    # Added Jan 16, 2009.
    # Returns the matrix of dot products of the column vectors
    # This is the same as X^t X.
    (nrow, ncol) = matdim(X)

    M = [[0.0] * ncol for _ in range(ncol)]
    for i in range(ncol):
        for j in range(i + 1):
            dot = sum([X[p][i] * X[p][j] for p in range(ncol)])
            M[i][j] = dot
            if i != j:
                M[j][i] = M[i][j]
    return M


def mattmat(A):
    """
    Returns the product transpose(A) B
    """
    return matprod(transpose(A), A)


def matrandom(nrow, ncol=None):
    # Added Jan. 16, 2009
    if ncol is None:
        ncol = nrow
    R = []
    for i in range(nrow):
        R.append([random.random() for _ in range(ncol)])
    return R


def matunitize(X, inplace=False):
    # Added jan. 16, 2009
    # Transforms each column vector in X to have unit length.
    if not inplace:
        V = [x[:] for x in X]
    else:
        V = X
    nrow = len(X)
    ncol = len(X[0])
    for j in range(ncol):
        recipnorm = sum([X[j][j] ** 2 for j in range(ncol)])
        for i in range(nrow):
            V[i][j] *= recipnorm
    return V


def matprint(A, format="%8.4f"):
    # prints the matrix A using format
    if hasattr(A, "__len__"):
        for i, row in enumerate(A):
            try:
                if iter(row):
                    for c in row:
                        print format % c,
                        print
            except:
                print row
    else:
        print "Not a matrix!"
    print  # prints a blank line after matrix


def mataugprint(A, Y, format="%8.4f"):
    # prints the augmented matrix A|Y using format
    try:
        ycols = len(Y[0])
    except:
        ycols = 1
    for i, row in enumerate(A):
        for c in row:
            print format % c,
        print "|",
        if ycols == 1:
            print format % Y[i]
        else:
            for _ in Y[i]:
                print format % Y[i],
    print


def gjinv(AA, inplace=False):
    """
    Determines the inverse of a square matrix BB by Gauss-Jordan reduction.
    """
    n = len(AA)
    B = eye(n)
    if not inplace:
        A = [row[:] for row in AA]
    else:
        A = AA

    for i in range(n):
        # Divide the ith row by A[i][i]
        m = 1.0 / A[i][i]
        for j in range(i, n):
            A[i][j] *= m  # # this is the same as dividing by A[i][i]
        for j in range(n):
            B[i][j] *= m

        # lower triangular elements.
        for k in range(i + 1, n):
            m = A[k][i]
            for j in range(i + 1, n):
                A[k][j] -= m * A[i][j]
            for j in range(n):
                B[k][j] -= m * B[i][j]

        # upper triangular elements.
        for k in range(0, i):
            m = A[k][i]
            for j in range(i + 1, n):
                A[k][j] -= m * A[i][j]
            for j in range(n):
                B[k][j] -= m * B[i][j]
    return B


matinverse = gjinv


def Test():
    X = [1, 1, 1]
    print dot(X, X)
    AA = [[1, 2, 3],
          [4, 5, 8],
          [9, 7, 6]]
    BB = eye(3)

    print "inputs:"
    print AA
    print BB
    print "product"
    print matprod(AA, AA)

    print "inverse of AA:"
    BB = gjinv(AA)

    print BB
    print matprod(AA, BB)


if __name__ == "__main__":
    Test()
