#!/usr/bin/env python

import sys
import datetime
import os

def parse_line(line):
    record = line.split()

    return {
        'ip': record[0],
        'epoch': float(record[1])
    }


class FacebookConversionReducer(object):

    date_beg_env_key = 'NKDHNY_FB_WINDOW_BEG_RED_DATE'
    date_end_env_key = 'NKDHNY_FB_WINDOW_END_RED_DATE'

    _states = ['initial', 'awaiting_signup', 'converted', 'error']
    _actions = [
        'transition_in_window', 'signup_today', 'conversion_today', 'not_in_window', 'other_in_window']
    _transitions = {
        'initial': {
            'not_in_window': 'error',
            'other_in_window': 'error',
            'signup_today': 'error',
            'transition_in_window': 'awaiting_signup',
            'conversion_today': 'converted'
        },
        'awaiting_signup': {
            'not_in_window': 'error',
            'other_in_window': 'awaiting_signup',
            'signup_today': 'converted',
            'transition_in_window': 'awaiting_signup',
            'conversion_today': 'converted'
        },
        'converted': {
            'not_in_window': 'error',
            'other_in_window': 'converted',
            'signup_today': 'converted',
            'transition_in_window': 'converted',
            'conversion_today': 'converted'
        },
        'error': {
            'not_in_window': 'error',
            'other_in_window': 'error',
            'signup_today': 'error',
            'transition_in_window': 'error',
            'conversion_today': 'error'
        }
    }


    def __init__(self, conversion_window_beg=None, conversion_window_end=None):

        self._window_beg = conversion_window_beg or datetime.datetime.strptime(
                os.environ[FacebookConversionReducer.date_beg_env_key], '%Y-%m-%d').date()

        self._window_end = conversion_window_end or datetime.datetime.strptime(
                os.environ[FacebookConversionReducer.date_end_env_key], '%Y-%m-%d').date()

        self._state = 'initial'
        self._prev_ip_seen = None

    def trace_conversion(self):
        print "{}\t{}".format(self._prev_ip_seen, "converted")

    def trace_transfer(self):
        print "{}\t{}".format(self._prev_ip_seen, "transferred")

    def _fsm_action(self, action, date):
        if date < self._window_beg:
            return 'not_in_window'

        if action == 'transition':
            return 'transition_in_window'

        if action == 'transition+signup' and date < self._window_end:
            return 'transition_in_window'

        if action == 'transition+signup' and date == self._window_end:
            return 'conversion_today'

        if action == 'signup' and date == self._window_end:
            return 'signup_today'

        return 'other_in_window'

    def trace(self):
        if self._state in ['awaiting_signup', 'converted']:
            self.trace_transfer()
        if self._state in ['converted']:
            self.trace_conversion()

    def __call__(self, line):
        ip, _, date, action = line.split('\t')
        date = datetime.datetime.strptime(date, '%Y-%m-%d').date()

        if ip != self._prev_ip_seen:
            #self.trace()
            print "{}\t{}".format(self._prev_ip_seen, self._state)
            self._state = 'initial'
            self._prev_ip_seen = ip

        self._state = FacebookConversionReducer._transitions[self._state][self._fsm_action(action, date)]


if __name__ == '__main__':
    reducer = FacebookConversionReducer()
    for line in sys.stdin:
        reducer(line)
    reducer.trace()