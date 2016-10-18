from flask import jsonify
from api import app
from flask import request
from metrics.local import LocalTotalHitsMetric
import datetime
import logging
from flask import abort

class MetricNotReady(Exception):
    pass

def read_total_hits(file_obj):
    with file_obj.open() as o:
        return int(o.readline().split()[1])

def metrics_for_day(day):
    total_hits = LocalTotalHitsMetric(date=day)

    if any([not x.complete() for x in [total_hits]]):
        raise MetricNotReady

    return {
        'total_hits': read_total_hits(total_hits.output())
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
