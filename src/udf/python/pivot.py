# coding: utf-8

import sys

## usage: python pivot.py all_columns axis_columns pivot_columns
## all_columns: column[|column,]
## axis_columns: column[|column,]
## pivot_columns: column[|column,]

sep = '|'
assert len(sys.argv) >= 4
columns_index = {col:idx for idx, col in enumerate(sys.argv[1].split(sep))}
columns_axis = sys.argv[2].split(sep)
columns_pivot = sys.argv[3].split(sep)

for line in sys.stdin:
    fields = line.split('\t')
    fields[-1] = fields[-1].strip()
    values_axis = [fields[columns_index[col]] for col in columns_axis]
    for column_pivot in columns_pivot:
        value_pivot = fields[columns_index[column_pivot]]
        if value_pivot and value_pivot.lower() != "null" and value_pivot != "\N":
            print '\t'.join(values_axis + [column_pivot, value_pivot])
