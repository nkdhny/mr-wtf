#!/usr/bin/env python

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

class ProfileHitsReducer(object):

    def __init__(self):
        self._prev_seen_profile = None
        self._prev_seen_day = None
        self._prev_seen_hour = None
        self._profile_views = defaultdict(lambda: 0)

        self._connection = happybase.Connection(choose_hbase_host())
        self._table = self._connection.table(compose_table_name("profilehit"))

    def trace_profile(self):

        def compose_key():
            return b"{}#{}".format(self._prev_seen_profile, self._prev_seen_day)

        def compose_value():
            return dict([(b"h:{}".format(k), v) for k, v in self._profile_views.iteritems()])

        self._table.put(compose_key(), compose_value())
        for h, c in self._profile_views.iteritems():
            print "{}\t{}\t{}\t{}".format(self._prev_seen_profile, self._prev_seen_day, h, c)

        self._profile_views = defaultdict(lambda: 0)

    def _should_accum(self, hour):
        return self._prev_seen_profile is not None and hour != self._prev_seen_hour

    def _accum(self, hour):
        self._profile_views[hour] += 1


    def _should_trace(self, profile, day):
        return (profile != self._prev_seen_profile and self._prev_seen_profile is not None) or \
               (profile == self._prev_seen_profile and day != self._prev_seen_day)

    def __call__(self, line):
        profile_id, _, date, hour = [x.strip() for x in line.split()]

        if self._should_trace(profile_id, date, hour):
            self.trace_profile()

        if self._should_accum(hour):
            self._accum(hour)

        self._prev_seen_profile = profile_id
        self._prev_seen_day = date
        self._prev_seen_hour = hour

    def flush(self):
        if self._prev_seen_profile is not None:
            self.trace_profile()


if __name__ == '__main__':
    reducer = ProfileHitsReducer()
    for line in sys.stdin:
        reducer(line)
    reducer.flush()