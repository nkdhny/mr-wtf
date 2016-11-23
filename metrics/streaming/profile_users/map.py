#!/usr/bin/env python

import re
import datetime
import sys
import re

_profile_request_re = re.compile(r'/id(\d+)(\?like=1)?$')

def parse_profile(raw_req):
    m = _profile_request_re.match(raw_req['file'])

    if m is None:
        raw_req['profile'] = None
        raw_req['like'] = None

    else:
        gr = m.groups()

        raw_req['profile'] = int(gr[0])
        raw_req['like'] = (gr[1] is not None)

    return raw_req

def parse_line(line):
    pat = '([(\d\.)]+) - - \[(.*?)\] "(.*?)" (\d+) (\d+) "(.*?)" "(.*?)"'

    match = re.match(pat, line)

    if match is None:
        return {
            'code': None,
            'profile': None
        }

    record = match.groups()

    req_time = datetime.datetime.strptime(record[1][:-6], "%d/%b/%Y:%H:%M:%S")

    raw_req = {
        'code': int(record[3]),
        'ip': record[0],
        'referrer': record[5],
        'req': record[2],
        'file': (record[2]).split(' ')[1],
        #-6 due to dropping tz
        'epoch': (req_time - datetime.datetime(year=1970, month=1, day=1)).total_seconds(),
        'date': req_time.date().strftime('%Y-%m-%d'),
        'hour': req_time.hour
    }

    return parse_profile(raw_req)

def is_completed_profile_request(req):
    return req['code'] == 200 and req['profile'] is not None

def mapred_map(line):
    record = parse_line(line)
    if is_completed_profile_request(record):
        print "{}\t{}\t{}\t{}\t{}".format(record['profile'], record['epoch'], record['ip'], record['date'], record['hour'])

if __name__ == '__main__':
    for line in sys.stdin:
        mapred_map(line)