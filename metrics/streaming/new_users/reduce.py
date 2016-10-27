#!/usr/bin/env python

import sys
import datetime
import os

def parse_line(line):
    record = line.split()

    return {
        'ip': record[0],
        'date': datetime.datetime.strptime(record[1], '%Y-%m-%d').date()
    }


class NewUsersReducer(object):

    date_env_key = 'NKDHNY_NEWUSER_RED_DATE'

    def __init__(self, date=None):
        self._prev_ip = None
        self.date = date or datetime.datetime.strptime(os.environ[NewUsersReducer.date_env_key], '%Y-%m-%d').date()
        self.new_users = 0

    def __call__(self, line):

        record = parse_line(line)

        if self._prev_ip != record['ip']:

            self._prev_ip = record['ip']
            if record['date'] == self.date:
                self.new_users += 1

    def trace_new_users(self):
        print "new_users\t{}".format(self.new_users)

if __name__ == '__main__':
    reducer = NewUsersReducer()
    for line in sys.stdin:
        reducer(line)