#!/usr/bin/env python

import re
import datetime
import sys


def parse_line(line):
    pat = '([\d\.:]+) - - \[(\S+) [^"]+\] "(\w+) ([^"]+) (HTTP/[\d\.]+)" (\d+) \d+ "([^"]+)" "([^"]+)"'

    match = re.match(pat, line)

    if match is None:
        return {
            'code': None
        }

    record = match.groups()

    return {
        'code': int(record[5]),
        'ip': record[0],
        'epoch': (
            datetime.datetime.strptime(
                    record[1], "%d/%b/%Y:%H:%M:%S") -
            datetime.datetime(year=1970, month=1, day=1)).total_seconds(),
    }


def mapred_map(line):
    record = parse_line(line)
    if record['code'] == 200:
        print "{}\t{}".format(record['ip'], record['epoch'])

if __name__ == '__main__':
    for line in sys.stdin:
        mapred_map(line)