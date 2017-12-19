# coding utf-8

import sys
import json

SEP = '`'

for line in sys.stdin:
    fields = line.split('\t')
    fields[-1] = fields[-1].strip()
    uid = fields[0]
    name = fields[1]
    value = fields[2]
    type = fields[3]
    tags = json.loads(fields[4])

    for tag_name, tag_value in tags.iteritems():
        tag_value_interval = tag_value.split(SEP)
        i_found_you = False
        if '1' == type:
            value = float(value)
            if tag_value_interval[0] and tag_value_interval[1]:
                i_found_you = value > float(tag_value_interval[0]) and value <= float(tag_value_interval[1])
            elif tag_value_interval[0]:
                i_found_you = value > float(tag_value_interval[0])
            elif tag_value_interval[1]:
                i_found_you = value <= float(tag_value_interval[1])
        elif '2' == type:
            i_found_you = value in tag_value_interval
        if i_found_you:
            print '\t'.join((uid, name, tag_name))
            break
