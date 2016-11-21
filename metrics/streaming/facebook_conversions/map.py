#!/usr/bin/env python

import re
import datetime
import sys

def parse_line(line):
    pat = '([(\d\.)]+) - - \[(.*?)\] "(.*?)" (\d+) (\d+) "(.*?)" "(.*?)"'

    match = re.match(pat, line)

    if match is None:
        return {
            'code': None
        }

    record = match.groups()

    return {
        'code': int(record[3]),
        'ip': record[0],
        'referrer': record[5],
        'req': record[2],
        'file': (record[2]).split(' ')[1],
        #-6 due to dropping tz
        'epoch': (
            datetime.datetime.strptime(
                    record[1][:-6], "%d/%b/%Y:%H:%M:%S") -
            datetime.datetime(year=1970, month=1, day=1)).total_seconds(),
        'date': datetime.datetime.strptime(record[1][:-6], "%d/%b/%Y:%H:%M:%S").date().strftime('%Y-%m-%d')
    }


def mapred_map(line):
    record = parse_line(line)

    def is_from_facebook(referrer):
        return 'facebook.com' in referrer or 'fb.com' in referrer

    def is_signup_action(file):
        return '/signup' == file

    def action(record):
        if is_from_facebook(record['referrer']):
            if is_signup_action(record['file']):
                return "transition+signup"

            return "transition"

        elif is_signup_action(record['file']):
            return "signup"

        else:
            return "other"

    if record['code'] == 200:
        print "{}\t{}\t{}\t{}".format(
                record['ip'],
                record['epoch'],
                record['date'],
                action(record)
        )

if __name__ == '__main__':
    for line in sys.stdin:
        mapred_map(line)