#!/usr/bin/env python

#/user/sandello/logs/access.log.2016-10-07

import sys
from pyspark import SparkContext
from pyspark import SparkConf
import re
import datetime

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

    #-6 due to dropping tz
    req_time = datetime.datetime.strptime(record[1][:-6], "%d/%b/%Y:%H:%M:%S")

    raw_req = {
        'code': int(record[3]),
        'ip': record[0],
        'referrer': record[5],
        'req': record[2],
        'file': (record[2]).split(' ')[1],
        'epoch': (req_time - datetime.datetime(year=1970, month=1, day=1)).total_seconds(),
        'date': req_time.date().strftime('%Y-%m-%d'),
        'hour': req_time.hour
    }

    return parse_profile(raw_req)

def _count_likes_in_rdd(rdd):
    return rdd.map(parse_line)\
                .filter(lambda r: r['code'] == 200 and r['profile'] is not None and r['like'] is True)\
                .map(lambda r: r['profile'])\
                .distinct()\
                .count()

def count_likes(file, *other_files):
    files = [file] + list(other_files)

    conf = SparkConf() \
        .setAppName("NkdhnyProfileLikes3Days") \
        .set("spark.ui.port", "4070")

    sc = SparkContext(conf=conf)
    files = [sc.textFile(x) for x in files]

    likes = sum([_count_likes_in_rdd(x) for x in files])

    sc.stop()
    return likes

if __name__ == '__main__':
    likes = count_likes(sys.argv[2], sys.argv[3], sys.argv[4])

    with file(sys.argv[1], 'w') as o:
        print >> o, likes

