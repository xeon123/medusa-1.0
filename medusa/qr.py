"""
File    qr.py
Author  Ernesto P. Adorio, Ph.D.
        U.P. Clarkfield, Pampanga
Version 0.0.1  2009.01.16 first version.
"""
from math import sqrt

from matlib import matprint, matprod, transpose


def qr(A, method="gramm"):
    # Performs a QR decomposition of A
    # default is via gramm-schmidt orthogonalization.
    global Q, R
    if method == "gramm":
        Q = gramm(A)
        R = matprod(transpose(Q), A)

    return Q, R


def gramm(X, inplace=False):
    # Returns the Gramm-Schmidt orthogonalization of matrix X

    if not inplace:
        V = [row[:] for row in X]  # make a copy.
    else:
        V = X

    k = len(X[0])          # number of columns.
    n = len(X)             # number of rows.
    for j in range(k):
        for i in range(j):
            # D = < Vi, Vj>
            D = sum([V[p][i] * V[p][j] for p in range(n)])
            for p in range(n):
                # Note that the Vi's already have length one!
                # Vj = Vj - <Vi,Vj> Vi/< Vi,Vi >
                V[p][j] -= (D * V[p][i])

        # Normalize column V[j]
        value = sqrt(sum([(V[p][j]) ** 2 for p in range(n)]))
        if int(value) == 0:
            value = 1.0

        invnorm = 1.0 / value

        for p in range(n):
            V[p][j] *= invnorm

    return V

if __name__ == "__main__":
    A = [[12, -51, 4],
         [6, 167, -68],
         [-4, 24, -41]]
    print "A:"
    matprint(A)
    Q, R = qr(A)
    print "Q:"
    matprint(Q)
    print "R:"
    matprint(R)
    print "QR:"
    matprint(matprod(Q, R))
