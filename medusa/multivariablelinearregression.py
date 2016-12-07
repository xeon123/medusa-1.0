from matlib import matdim, mattmat, transpose
from qr import qr


def readdat():
    f = open('dat', 'r')
    x, y = [], []
    f.next()
    for line in f:
        val = line.split()
        y.append(float(val[1]))
        x.append([float(p) for p in val[2:]])
    return x, y


def bsub(r, z):
    """ solves "R b = z", where r is triangular"""
    m, n = matdim(r)
    p, q = matdim(z)
    b = [[0] * n]
    pp, qq = matdim(b)
    for j in range(n - 1, -1, -1):
        zz = z[0][j] - sum(r[j][k] * b[0][k] for k in range(j + 1, n))
        if int(r[j][j]) == 0:
            r[j][j] = 1.0

        b[0][j] = zz / r[j][j]
    return b


def linreg(y, x):
    if len(y) == 0 and len(x) == 0:
        return [0] * 5

    # prepend x with 1
    for xx in x:
        xx.insert(0, 1.0)

    # QR decomposition
    q, r = qr(x)

    # z = Q^T y
    z = mattmat(q)

    # back substitute to find b in R b = z
    b = bsub(r, transpose(z))
    b = b[0]

    return b


def tester():
    # read test data
    x, y = readdat()

    # calculate coeff
    b = linreg(y, x)

    for i, coef in enumerate(b):
        print 'coef b%d: %f' % (i, coef)

if __name__ == "__main__":
    tester()
