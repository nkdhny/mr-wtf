#!/usr/bin/env python

import re
import random
import sys


def parse_line(line):
    pat = '([(\d\.)]+) - - \[(.*?)\] "(.*?)" (\d+) (\d+) "(.*?)" "(.*?)"'

    record = re.match(pat, line).groups()

    return {
        'code': int(record[3]),
        'ip': record[0],
        'epoch': random.randint(0, 10),
        'file': 'some_file'
    }


def mapred_map(line):
    record = parse_line(line)
    print "{}\t{}".format(record['ip'], record['epoch'])

if __name__ == '__main__':
    for line in sys.stdin:
        mapred_map(line)