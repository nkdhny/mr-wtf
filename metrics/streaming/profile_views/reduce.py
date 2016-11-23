#!/usr/bin/env python
import os
import sys
import datetime
import happybase
import random
from collections import defaultdict

def parse_line(line):
    record = line.split()

    return {
        'ip': record[0],
        'epoch': float(record[1])
    }


def compose_table_name(metric):
    return "bigdatashad_{}_{}".format("agolomedov", metric)

def choose_hbase_host():
    HOSTS = ["hadoop2-%02d.yandex.ru" % i for i in xrange(11, 14)]
    return random.choice(HOSTS)

class ProfileViewsReducer(object):

    date_env_key = 'NKDHNY_PROFILEVIEWS_RED_DATE'

    def __init__(self, date=None):
        self._prev_seen_ip = None
        self._prev_seen_profile = None
        self.date = date or datetime.datetime.strptime(os.environ[ProfileViewsReducer.date_env_key], '%Y-%m-%d').date()
        self._views_count = 0

        self._connection = happybase.Connection(choose_hbase_host())
        self._table = self._connection.table(compose_table_name("profileview"))

    def trace_profileview(self):

        def compose_key():
            return b"{}#{}+{}".format(self.date, self._prev_seen_ip, self._prev_seen_profile)

        def compose_value():
            return dict([(b"c:cnt", b'{}'.format(self._views_count))])

        self._table.put(compose_key(), compose_value())

        print self._prev_seen_ip, self._prev_seen_profile, self._views_count

        self._views_count = 0

    def _accum(self):
        self._views_count += 1


    def _should_trace(self, profile, ip):
        return (profile != self._prev_seen_profile and self._prev_seen_profile is not None) or \
               (ip != self._prev_seen_ip and self._prev_seen_ip is not None)

    def __call__(self, line):
        ip, profile_id = [x.strip() for x in line.split()]

        if self._should_trace(profile_id, ip):
            self.trace_profileview()

        self._accum()

        self._prev_seen_profile = profile_id
        self._prev_seen_ip = ip


    def flush(self):
        if self._prev_seen_profile is not None:
            self.trace_profileview()


if __name__ == '__main__':
    reducer = ProfileViewsReducer()
    for line in sys.stdin:
        reducer(line)
    reducer.flush()