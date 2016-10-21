#!/usr/bin/env python

import sys

def parse_line(line):
    record = line.split()

    return {
        'ip': record[0],
        'epoch': record[1]
    }


class SessionsReducer(object):
    def __init__(self):
        self._prev_ip = None
        self._collected_epochs = []

    def flush(self):
        print "{}\t{}".format(self._prev_ip, ';'.join([str(x) for x in self._collected_epochs]))

    def __call__(self, line):
        global _sessions_reduce_prev_ip
        global _sessions_collected_epochs

        record = parse_line(line)

        if self._prev_ip != record['ip']:
            if self._prev_ip is not None:
                self.flush()
            self._prev_ip = record['ip']
            self._collected_epochs = [record['epoch']]
        else:
            self._collected_epochs.append(record['epoch'])


if __name__ == '__main__':
    reducer = SessionsReducer()
    for line in sys.stdin:
        reducer(line)