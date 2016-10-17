#!/usr/bin/env python

import luigi
import luigi.contrib.hadoop

class LogFile(luigi.ExternalTask):    
    date = luigi.DateParameter()

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget(self.date.strftime('/user/sandello/logs/access.log.%Y-%m-%d'))

class TotalHitsTask(luigi.contrib.hadoop.JobTask):
    date_interval = luigi.DateIntervalParameter()

    n_reduce_tasks = 1

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget("/user/agolomedov/total_hits_{}".format(self.date_interval))

    def requires(self):
        return [LogFile(date) for date in self.date_interval]

    def mapper(self, line):
        yield "total_hits", 1

    def reducer(self, key, values):
        yield key, sum(values)

    combiner = reducer