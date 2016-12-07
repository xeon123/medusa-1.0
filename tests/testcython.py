import ctypes
import time
import numpy as np
import example_cython


def mult_sum(a):
    b = 0
    for i in range(10000):
        b += a * i
        return b


def mult_sum_np(a):
    return np.sum(np.arange(10000) * a)


def evaluate(name, func):
    st = time.time()
    out = [func(x) for x in range(10)]
    print('%s[%f]' % (name, time.time() - st))
    return out


def main():
    example = ctypes.cdll.LoadLibrary('example.so')

    out = evaluate('Ctypes', example.mult_sum_c)
    out2 = evaluate('Python', mult_sum)
    out3 = evaluate('numpy', mult_sum_np)
    out4 = evaluate('Cython', example_cython.mult_sum)
    assert out == out2 == out3 == out4


if __name__ == '__main__':
    main()
