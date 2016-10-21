#!/usr/bin/env python

import sys

def parse_line(line):
    record = line.split()

    return {
        'ip': record[0],
        'epoch': record[1]
    }


class SessionsReducer(object):
    session_ttl = 30 * 60 * 60 #seconds

    def __init__(self):
        self._prev_ip = None
        self._prev_visit = 0
        self._session_length = 0

    def _is_same_session(self, epoch):
        return epoch - self._prev_visit <= SessionsReducer.session_ttl

    def flush(self):
        print "{}\t{}".format(self._prev_ip, self._session_length)

    def __call__(self, line):

        record = parse_line(line)

        if self._prev_ip != record['ip']:
            if self._prev_ip is not None:
                self.flush()

            self._prev_ip = record['ip']
            self._session_length = 1

        else:
            if not self._is_same_session(record['epoch']):
                self._session_length += 1

        self._prev_visit = record['epoch']


if __name__ == '__main__':
    reducer = SessionsReducer()
    for line in sys.stdin:
        reducer(line)