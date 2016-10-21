#!/usr/bin/env python

import luigi
import luigi.contrib.hadoop
import re
import datetime
import random
from config import AppConfig


def parse_line(line):
    pat = '([(\d\.)]+) - - \[(.*?)\] "(.*?)" (\d+) (\d+) "(.*?)" "(.*?)"'

    record = re.match(pat, line).groups()

    return {
        'code': int(record[3]),
        'ip': record[0],
        'epoch': random.randint(0, 10),
        'file': 'some_file'
    }

class LogFile(luigi.ExternalTask):    
    date = luigi.DateParameter()

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget(self.date.strftime('/user/sandello/logs/access.log.%Y-%m-%d'))

class Metric(luigi.contrib.hadoop.JobTask):
    date = luigi.DateParameter(default=datetime.date.today() - datetime.timedelta(days=1))

    def requires(self):
        return LogFile(self.date)


class ExternalMetric(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today() - datetime.timedelta(days=1))

    def requires(self):
        return LogFile(self.date)



class TotalHitsTask(Metric):
    n_reduce_tasks = 1

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget(
                "/user/agolomedov/total_hits_{}".format(self.date),
                format=luigi.contrib.hdfs.PlainDir
        )

    def mapper(self, line):
        if parse_line(line)['code'] == 200:
            yield "total_hits", 1        

    def reducer(self, key, values):
        yield key, sum(values)

    combiner = reducer


class UniqueUsersTask(Metric):
    n_reduce_tasks = 1

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget(
                "/user/agolomedov/total_users_{}".format(self.date),
                format=luigi.contrib.hdfs.PlainDir
        )

    def mapper(self, line):
        record = parse_line(line)
        if record['code'] == 200:
            yield record['ip'], 1

    def init_reducer(self):
        self.total_users = 0

    def reducer(self, key, values):
        self.total_users += 1
        return []

    def final_reducer(self):
        yield "total_users", self.total_users


class MarkUserSessionTask(ExternalMetric):

    n_reduce_tasks = 3

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget(
                "/user/agolomedov/user_sessions_{}".format(self.date),
                format=luigi.contrib.hdfs.PlainDir
        )

    def run(self):
        from .streaming.sessions import run

        run.run_map_reduce(
                self.input().path, self.output().path, self.task_id,
                self.n_reduce_tasks, AppConfig.streaming_root+'sessions')