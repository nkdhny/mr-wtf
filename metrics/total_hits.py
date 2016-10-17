#!/usr/bin/env python

import luigi
from common.luigi_helper import LogFile

class TotalHitsTask(luigi.contrib.hadoop.JobTask):
    date_interval = luigi.DateIntervalParameter()

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget("user/agolomedov/total_hits_{}".format(self.date_interval))

    def requires(self):
        return [LogFile(date) for date in self.date_interval]

    def mapper(self, line):
        "total_hits", 1

    def reducer(self, key, values):
        yield key, sum(values)

    combiner = reducer