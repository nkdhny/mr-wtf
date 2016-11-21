import random

from flask import jsonify
from api import app
from flask import request
from metrics.local import LocalTotalHitsMetric, LocalTotalUsersMetric, LocalUserSessionLength, \
    LocalUsersByCountry, LocalNewUsers, LocalFacebokokConversionRatio
import datetime
import logging
from flask import abort
import happybase

class MetricNotReady(Exception):
    pass

def read_simple_metric(file_obj):
    with file_obj.open() as o:
        line = o.readline()
        try:
            return int(line.split()[1])
        except ValueError:
            return float(line.split()[1])

def read_dict_metric(file_obj):
    res = {}
    with file_obj.open() as o:
        for line in o.readlines():
            parts = line.split('\t')
            k = parts[0]
            v = int(parts[1][:-1])
            res[k] = v

    return res

def metrics_for_day(day):
    total_hits = LocalTotalHitsMetric(date=day)
    total_users = LocalTotalUsersMetric(date=day)
    session_length = LocalUserSessionLength(date=day)
    users_by_country = LocalUsersByCountry(date=day)
    new_users = LocalNewUsers(date=day)
    facebook_conversions = LocalFacebokokConversionRatio(date=day)


    if any([not x.complete() for x in [total_hits, total_users, session_length, users_by_country, new_users, facebook_conversions]]):
        return {}

    return {
        'total_hits': read_simple_metric(total_hits.output()),
        'total_users': read_simple_metric(total_users.output()),
        'average_session_length': read_simple_metric(session_length.output()),
        "new_users": read_simple_metric(new_users.output()),
        "users_by_country": read_dict_metric(users_by_country.output()),
        "facebook_signup_conversion_3": read_simple_metric(facebook_conversions.output())
    }

@app.route('/api/hw1')
def get_metrics():
    start_date = datetime.datetime.strptime(request.args.get("start_date"), "%Y-%m-%d").date()
    end_date = datetime.datetime.strptime(request.args.get("end_date"), "%Y-%m-%d").date()

    logging.debug("Loading metrics during [{}-{}]".format(start_date, end_date))

    metrics = {}

    date = start_date

    try:
        while date <= end_date:
            metrics[str(date)] = metrics_for_day(date)
            date = date + datetime.timedelta(days=1)
    except MetricNotReady:
        abort(500)


    return jsonify(metrics)

#HW-2
def _compose_table_name(metric):
    return "bigdatashad_{}_{}".format("agolomedov", metric)

def _hbase_connection():

    def choose_hbase_host():
        HOSTS = ["hadoop2-%02d.yandex.ru" % i for i in xrange(11, 14)]
        return random.choice(HOSTS)

    return happybase.Connection(choose_hbase_host())


@app.route('/api/hw2/profile_hits')
def get_profile_hits():

    start_date = datetime.datetime.strptime(request.args.get("start_date"), "%Y-%m-%d").date()
    end_date = datetime.datetime.strptime(request.args.get("end_date"), "%Y-%m-%d").date()
    profile_id = int(request.args.get('profile_id')[2:])

    all_hits = _hbase_connection().table(_compose_table_name("profile_hits"))

    def compose_key(profile, day):
        return b"{}#{}".format(profile, day.strptime("%Y-%m-%d"))

    mathched_hits = all_hits.scan(
            row_start=compose_key(profile_id, start_date), row_stop=compose_key(profile_id, end_date))

    def parse_key(key):
        return key.split('#')[1]

    return jsonify(dict([
        (parse_key(k), [d.get(x, 0) for x in range(25)]) for k, d in mathched_hits
    ]))
