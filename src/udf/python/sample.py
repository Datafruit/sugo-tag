#coding: utf-8

import sys

for line in sys.stdin:
    fields = line.split('\t')
    fields[-1] = fields[-1].strip()
    print '\t'.join(fields)
