# coding: utf-8

from os import listdir
from os.path import isfile, join

percentile = lambda pt: [1.0 / float(pt) * i for i in xrange(1, pt)]
listfile = lambda d: [join(d, f) for f in listdir(d) if isfile(join(d, f))]

if __name__ == "__main__":

    # print percentile(2)
    print listfile("/Users/penghuan/Tmp/")
