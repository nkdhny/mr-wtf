#!/usr/bin/env python

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import re
import datetime

_profile_request_re = re.compile(r'/id(\d+)(\?like=1)?$')


def _parse_profile(raw_req):
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

    return _parse_profile(raw_req)


class AppConfig(object):
    appName="NkdhnyErrorsCount"
    batchDuration=15
    appId="nkdhny-spark-streaming-consumer"
    zk = 'hadoop2-10:2181'
    topic = 'bigdatashad-2016'
    partitions = 4


def _setup():
    sc = SparkContext(appName=AppConfig.appName)
    ssc = StreamingContext(sc, AppConfig.batchDuration)
    ssc.checkpoint("nkdhnyCheckpointKafka")

    return KafkaUtils.createStream(
            ssc, AppConfig.zk,
            AppConfig.appId,
            {AppConfig.topic: AppConfig.partitions}
    ), ssc, sc

def main():

    log_dstream, ssc, sc = _setup()

    errors = log_dstream.map(parse_line).cache().filter(lambda rec: rec['code'] != 200)

    errors.window(1).pprint()

    ssc.start()
    ssc.awaitTermination()


if __name__ == '__main__':
    main()

